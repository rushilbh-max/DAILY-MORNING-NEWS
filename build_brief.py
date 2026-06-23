#!/usr/bin/env python3
"""
GBW Morning Brief — builds a ~30 minute spoken news briefing every day.

Pipeline:
  1. Pull stories from a curated set of RSS feeds (publishers + Google News search feeds).
  2. Clean, de-duplicate and rank them into topic sections.
  3. Assemble a spoken script sized to a target listening time (default 30 min).
  4. (Optional) Polish the script with Claude — OFF by default, set USE_LLM=1.
  5. Speak each section with edge-tts, stitch with ffmpeg, write chapters.
  6. Emit  episodes/<date>.mp3 , episodes/<date>.json , episodes/latest.json , episodes/index.json

Runs free with no API keys. Internet is required (works on GitHub Actions).
"""

import asyncio
import datetime as dt
import html
import json
import os
import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
from bs4 import BeautifulSoup

try:
    from mutagen.mp3 import MP3
except Exception:
    MP3 = None

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
EPISODES_DIR = ROOT / "episodes"

DEFAULTS = {
    "target_minutes": 30,
    "words_per_minute": 150,           # edge-tts neural voice ~ this rate
    "voice": "en-IN-PrabhatNeural",    # Indian-English male; see VOICES note in README
    "rate": "+6%",                     # speak a touch faster to fit more news
    "max_sentences_per_item": 3,       # how much of each story to read
    "max_pool_per_section": 22,        # how many candidate stories to gather
    "feed_window_days": 1,             # Google News "when:" recency window
    "request_timeout": 20,
    "use_llm": False,                  # overridden by env USE_LLM=1
    "llm_model": "claude-haiku-4-5-20251001",
    "title": "Grace Bath World — Morning Brief",
    "tz_offset_minutes": 330,          # IST (UTC+5:30) for the dateline
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Google News RSS search helper — reliable + precisely targetable.
def gnews(query, days):
    q = requests.utils.quote(f"{query} when:{days}d")
    return f"https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en"

def build_feed_plan(days):
    """Sections in reading order. Each: (intro line, [(source_label, url, weight)])."""
    return [
        ("Top headlines", "First, the stories leading the news this morning.", [
            ("Times of India", "https://timesofindia.indiatimes.com/rssfeedstopstories.cms", 3),
            ("BBC", "https://feeds.bbci.co.uk/news/rss.xml", 2),
            ("The Hindu", "https://www.thehindu.com/news/national/feeder/default.rss", 2),
        ]),
        ("Bhopal and Madhya Pradesh", "Now, news closer to home in Bhopal and across Madhya Pradesh.", [
            ("Local", gnews("Bhopal OR \"Madhya Pradesh\"", days), 3),
            ("Times of India", "https://timesofindia.indiatimes.com/rssfeeds/-2128838597.cms", 2),
        ]),
        ("Across India", "Turning to the big national stories.", [
            ("Times of India", "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms", 3),
            ("The Hindu", "https://www.thehindu.com/news/national/feeder/default.rss", 2),
            ("NDTV", "https://feeds.feedburner.com/ndtvnews-india-news", 1),
        ]),
        ("Around the world", "And here is what is happening around the world.", [
            ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml", 3),
            ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml", 2),
            ("The Guardian", "https://www.theguardian.com/world/rss", 2),
        ]),
        ("Business and markets", "Moving to business, the economy and the markets.", [
            ("Economic Times", "https://economictimes.indiatimes.com/rssfeedstopstories.cms", 3),
            ("The Hindu Business", "https://www.thehindu.com/business/feeder/default.rss", 2),
            ("BBC Business", "https://feeds.bbci.co.uk/news/business/rss.xml", 1),
        ]),
        ("Money, tax and personal finance", "Now to your money — taxes, savings and personal finance.", [
            ("Tax & GST", gnews("(GST OR \"income tax\" OR taxation OR \"tax\") India", days), 3),
            ("ET Wealth", "https://economictimes.indiatimes.com/wealth/rssfeeds/837555174.cms", 2),
            ("Livemint Money", gnews("personal finance OR savings OR mutual fund India", days), 1),
        ]),
        ("Science and technology", "Next, science and technology.", [
            ("Science", gnews("science OR research OR space OR ISRO", days), 2),
            ("BBC Science", "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", 2),
            ("The Hindu Sci-Tech", "https://www.thehindu.com/sci-tech/feeder/default.rss", 2),
        ]),
        ("Development and policy", "And finally, development, infrastructure and government policy.", [
            ("Policy", gnews("(infrastructure OR economy OR scheme OR policy OR project) India", days), 3),
            ("The Hindu", "https://www.thehindu.com/news/national/feeder/default.rss", 1),
        ]),
    ]

# --------------------------------------------------------------------------- #
# Text cleaning                                                               #
# --------------------------------------------------------------------------- #

_WS = re.compile(r"\s+")
_URL = re.compile(r"https?://\S+")
_BRACKETS = re.compile(r"\[[^\]]*\]")
_TRAILING_SRC = re.compile(r"\s*[-–—]\s*[A-Z][A-Za-z .&]+$")

def strip_html(text):
    if not text:
        return ""
    try:
        text = BeautifulSoup(text, "lxml").get_text(" ")
    except Exception:
        text = re.sub(r"<[^>]+>", " ", text)
    return text

def speakable(text):
    """Normalise a string so a TTS voice reads it cleanly."""
    if not text:
        return ""
    text = html.unescape(strip_html(text))
    text = unicodedata.normalize("NFKC", text)
    text = _URL.sub("", text)
    text = _BRACKETS.sub("", text)
    text = (text.replace("₹", " rupees ").replace("Rs.", " rupees ").replace("Rs ", " rupees ")
                .replace("&", " and ").replace("%", " percent ")
                .replace("“", '"').replace("”", '"').replace("’", "'").replace("‘", "'"))
    text = re.sub(r"\bRead more\b.*$", "", text, flags=re.I)
    text = _WS.sub(" ", text).strip()
    return text

def first_sentences(text, n):
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", text)
    out = " ".join(parts[:n]).strip()
    return out

def norm_title(t):
    t = unicodedata.normalize("NFKD", t.lower())
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return _WS.sub(" ", t).strip()

def word_count(text):
    return len(text.split())

# --------------------------------------------------------------------------- #
# Fetching                                                                     #
# --------------------------------------------------------------------------- #

def fetch_feed(url, timeout):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
        r.raise_for_status()
        return feedparser.parse(r.content)
    except Exception as e:
        print(f"  ! feed failed {url[:60]} :: {e}", file=sys.stderr)
        return None

def gather_section(name, sources, cfg):
    """Return a ranked, de-duplicated list of story dicts for one section."""
    items = []
    for label, url, weight in sources:
        feed = fetch_feed(url, cfg["request_timeout"])
        if not feed or not getattr(feed, "entries", None):
            continue
        for rank, e in enumerate(feed.entries[: cfg["max_pool_per_section"]]):
            title = speakable(getattr(e, "title", "")).rstrip(".")
            if len(title) < 12:
                continue
            summary = speakable(getattr(e, "summary", "") or getattr(e, "description", ""))
            summary = _TRAILING_SRC.sub("", summary)
            summary = first_sentences(summary, cfg["max_sentences_per_item"])
            # drop summary if it just echoes the title
            if summary and norm_title(summary)[:40] == norm_title(title)[:40]:
                summary = ""
            score = weight * 100 - rank          # earlier in feed + higher weight = better
            items.append({"title": title, "summary": summary, "source": label,
                          "key": norm_title(title), "score": score})
    return items

_STOP = set("a an and the of to in on for at by with from as is are was were be been "
            "it its this that these those over after before new says said will can amid "
            "into out up down off about than then he she his her they them their our we "
            "you your has have had not no more most".split())

def _content_tokens(key):
    return {w for w in key.split() if w not in _STOP and len(w) > 2}

def dedupe(items, seen_keys):
    """Remove cross-section duplicates and near-duplicates (stopword-filtered Jaccard)."""
    out = []
    seen_token_sets = []
    for it in sorted(items, key=lambda x: -x["score"]):
        k = it["key"]
        if not k or k in seen_keys:
            continue
        toks = _content_tokens(k)
        if not toks:
            continue
        dup = False
        for s in seen_token_sets:
            union = toks | s
            if union and len(toks & s) / len(union) >= 0.6:   # Jaccard ≥ 0.6 ⇒ same story
                dup = True
                break
        if dup:
            continue
        seen_keys.add(k)
        seen_token_sets.append(toks)
        out.append(it)
    return out

# --------------------------------------------------------------------------- #
# Script assembly (length-aware)                                              #
# --------------------------------------------------------------------------- #

def item_to_line(it):
    if it["summary"]:
        return f"{it['title']}. {it['summary']}"
    return f"{it['title']}."

def assemble(plan_results, cfg):
    """plan_results: list of (section_name, intro, [items]). Returns sections sized to target."""
    target = cfg["target_minutes"] * cfg["words_per_minute"]
    # start with a base allocation, then top up to hit the target word budget
    base = {"Top headlines": 5, "Bhopal and Madhya Pradesh": 5, "Across India": 6,
            "Around the world": 6, "Business and markets": 5,
            "Money, tax and personal finance": 5, "Science and technology": 4,
            "Development and policy": 4}
    counts = {name: min(base.get(name, 4), len(items)) for name, _, items in plan_results}

    def total_words():
        w = 25  # greeting + outro padding
        for name, intro, items in plan_results:
            if counts[name] == 0:
                continue
            w += word_count(intro)
            for it in items[: counts[name]]:
                w += word_count(item_to_line(it))
        return w

    # top up round-robin until we reach the target (or run out of stories)
    guard = 0
    while total_words() < target and guard < 500:
        progressed = False
        for name, _, items in plan_results:
            if counts[name] < len(items):
                counts[name] += 1
                progressed = True
                if total_words() >= target:
                    break
        guard += 1
        if not progressed:
            break
    # trim if we overshot a lot (drop from lowest-priority sections last->first)
    while total_words() > target * 1.08:
        for name, _, items in reversed(plan_results):
            if counts[name] > base.get(name, 3):
                counts[name] -= 1
                break
        else:
            break

    sections = []
    for name, intro, items in plan_results:
        chosen = items[: counts[name]]
        if not chosen:
            continue
        body = " ".join(item_to_line(it) for it in chosen)
        sections.append({
            "title": name,
            "intro": intro,
            "text": f"{intro} {body}",
            "items": [{"title": it["title"], "source": it["source"]} for it in chosen],
        })
    return sections

# --------------------------------------------------------------------------- #
# Optional Claude polish (off by default)                                     #
# --------------------------------------------------------------------------- #

POLISH_SYSTEM = (
    "You are a radio news editor. Rewrite the supplied headlines and summaries into a smooth, "
    "natural spoken news segment for an audio briefing. Rules: use ONLY the facts given — never "
    "add, infer, or invent any fact, name, number or claim. Neutral newsroom tone. No markdown, "
    "no headings, no bullet points, no emojis. Plain sentences a presenter can read aloud. "
    "Keep roughly the same length."
)

def polish_section(section, cfg):
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return section["text"]
    payload = {
        "model": cfg["llm_model"],
        "max_tokens": 1500,
        "system": POLISH_SYSTEM,
        "messages": [{"role": "user", "content":
                      f"Section: {section['title']}.\n\nRaw items:\n{section['text']}"}],
    }
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
                          headers={"x-api-key": key, "anthropic-version": "2023-06-01",
                                   "content-type": "application/json"},
                          json=payload, timeout=60)
        r.raise_for_status()
        blocks = r.json().get("content", [])
        text = " ".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
        return text or section["text"]
    except Exception as e:
        print(f"  ! polish skipped for {section['title']} :: {e}", file=sys.stderr)
        return section["text"]

# --------------------------------------------------------------------------- #
# Text-to-speech + stitching                                                  #
# --------------------------------------------------------------------------- #

async def _speak(text, voice, rate, out_path):
    import edge_tts
    await edge_tts.Communicate(text, voice, rate=rate).save(str(out_path))

def synth(text, cfg, out_path):
    asyncio.run(_speak(text, cfg["voice"], cfg["rate"], out_path))

def mp3_seconds(path):
    if MP3 is not None:
        try:
            return float(MP3(path).info.length)
        except Exception:
            pass
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=nokey=1:noprint_wrappers=1", str(path)])
        return float(out.strip())
    except Exception:
        return 0.0

def concat_mp3(parts, out_path):
    listing = out_path.parent / "_concat.txt"
    listing.write_text("".join(f"file '{p.name}'\n" for p in parts))
    subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                    "-i", str(listing), "-c", "copy", str(out_path)],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    listing.unlink(missing_ok=True)

# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #

def load_config():
    cfg = dict(DEFAULTS)
    if CONFIG_PATH.exists():
        cfg.update(json.loads(CONFIG_PATH.read_text()))
    if os.environ.get("USE_LLM") == "1":
        cfg["use_llm"] = True
    if os.environ.get("VOICE"):
        cfg["voice"] = os.environ["VOICE"]
    if os.environ.get("TARGET_MINUTES"):
        cfg["target_minutes"] = int(os.environ["TARGET_MINUTES"])
    return cfg

def main():
    no_audio = "--no-audio" in sys.argv
    cfg = load_config()
    now = dt.datetime.utcnow() + dt.timedelta(minutes=cfg["tz_offset_minutes"])
    date_str = now.strftime("%Y-%m-%d")
    dateline = now.strftime("%A, %B ") + str(now.day)   # portable (no %-d / %#d)
    print(f"== Building brief for {date_str} (target {cfg['target_minutes']} min) ==")

    plan = build_feed_plan(cfg["feed_window_days"])
    seen = set()
    plan_results = []
    for name, intro, sources in plan:
        print(f"-> {name}")
        items = dedupe(gather_section(name, sources, cfg), seen)
        plan_results.append((name, intro, items))
        print(f"   {len(items)} unique stories")

    sections = assemble(plan_results, cfg)
    if not sections:
        print("No stories gathered — aborting.", file=sys.stderr)
        sys.exit(1)

    if cfg["use_llm"]:
        print("-> polishing with Claude")
        for s in sections:
            s["text"] = polish_section(s, cfg)

    greeting = (f"Good morning. This is your Grace Bath World briefing for {dateline}. "
                f"Over the next half hour: " +
                ", ".join(s["title"].lower() for s in sections) + ". Let's begin.")
    signoff = "That is your briefing. Stay sharp, and have a strong day."

    EPISODES_DIR.mkdir(exist_ok=True)
    transcript = [{"title": "Opening", "text": greeting}]
    transcript += [{"title": s["title"], "text": s["text"]} for s in sections]
    transcript += [{"title": "Sign-off", "text": signoff}]
    full_words = sum(word_count(t["text"]) for t in transcript)
    est_minutes = round(full_words / cfg["words_per_minute"], 1)
    print(f"   script: {full_words} words  (~{est_minutes} min estimated)")

    chapters, duration = [], 0.0
    mp3_name = f"{date_str}.mp3"
    mp3_path = EPISODES_DIR / mp3_name

    if not no_audio:
        parts = []
        tmp = EPISODES_DIR / "_parts"
        tmp.mkdir(exist_ok=True)
        spoken = [("Opening", greeting)] + [(s["title"], s["text"]) for s in sections] + [("Sign-off", signoff)]
        for i, (title, text) in enumerate(spoken):
            part = tmp / f"{i:02d}.mp3"
            print(f"   speaking: {title}")
            synth(text, cfg, part)
            chapters.append({"title": title, "start": round(duration, 1)})
            duration += mp3_seconds(part)
            parts.append(part)
        concat_mp3(parts, mp3_path)
        duration = mp3_seconds(mp3_path)  # exact, post-stitch
        for p in parts:
            p.unlink(missing_ok=True)
        tmp.rmdir()
        print(f"   audio: {mp3_name}  ({round(duration/60,1)} min)")
    else:
        print("   --no-audio: skipping TTS")

    episode = {
        "date": date_str,
        "dateline": dateline,
        "title": cfg["title"],
        "audio": f"episodes/{mp3_name}",
        "duration_sec": round(duration, 1),
        "est_minutes": est_minutes,
        "word_count": full_words,
        "generated_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "chapters": chapters,
        "sections": [{"title": s["title"], "items": s["items"]} for s in sections],
        "transcript": transcript,
    }
    (EPISODES_DIR / f"{date_str}.json").write_text(json.dumps(episode, ensure_ascii=False, indent=2))
    (EPISODES_DIR / "latest.json").write_text(json.dumps(episode, ensure_ascii=False, indent=2))

    # rolling archive index (newest first, keep 30)
    idx_path = EPISODES_DIR / "index.json"
    index = []
    if idx_path.exists():
        try:
            index = json.loads(idx_path.read_text())
        except Exception:
            index = []
    index = [e for e in index if e.get("date") != date_str]
    index.insert(0, {"date": date_str, "dateline": dateline,
                     "audio": episode["audio"], "json": f"episodes/{date_str}.json",
                     "duration_sec": episode["duration_sec"]})
    index = index[:30]
    idx_path.write_text(json.dumps(index, ensure_ascii=False, indent=2))
    print("Done.")

if __name__ == "__main__":
    main()
