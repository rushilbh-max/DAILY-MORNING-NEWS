"""Offline sanity test — no network. Injects a fake feed and runs the builder."""
import sys, json, feedparser
import build_brief as b

SAMPLE = """<?xml version="1.0"?><rss version="2.0"><channel>
{items}
</channel></rss>"""
ITEM = """<item><title>{t}</title><description>{d}</description></item>"""

import random
SUBJ = ("highway monsoon metro startup tariff wheat vaccine satellite bank court trade "
        "election factory river school airport council rupee export tiger solar hospital "
        "cricket bridge forest pension census drone harvest port refinery campus").split()
VERB = ("clears stalls expands launches reviews approves cuts boosts delays probes "
        "unveils restores audits merges halts revives funds").split()
OBJ = ("plan deal probe reform rollout verdict survey deadline package upgrade ban "
       "subsidy contract alliance fund corridor scheme mission report").split()
_ctr = [0]
def make_feed(seed):
    rnd = random.Random(seed)
    items = ""
    for _ in range(20):
        _ctr[0] += 1
        a, v, o = rnd.choice(SUBJ), rnd.choice(VERB), rnd.choice(OBJ)
        b2, o2 = rnd.choice(SUBJ), rnd.choice(OBJ)
        t = f"{a.title()} {o} {v} as {b2} {o2} draws fresh scrutiny hash{_ctr[0]}"
        d = (f"The {a} {o} moved ahead with ₹{rnd.randint(1,900)} crore committed. "
             f"Sources said the {b2} {o2} could shift by {rnd.randint(2,9)}% this quarter. "
             f"A second panel will report findings before the {o2} deadline. Read more at example.com")
        items += ITEM.format(t=t, d=d)
    return feedparser.parse(SAMPLE.format(items=items))

# different seed per URL so sections differ and dedupe is exercised
def fake_fetch(url, timeout):
    seed = "global"
    for kw in ["Bhopal", "indiatimes", "thehindu", "bbci", "aljazeera", "guardian",
               "economictimes", "wealth", "science", "infrastructure", "GST", "ndtv"]:
        if kw.lower() in url.lower():
            seed = kw
            break
    return make_feed(seed)

b.fetch_feed = fake_fetch
sys.argv = ["build_brief.py", "--no-audio"]
b.main()

print("\n--- latest.json summary ---")
data = json.loads((b.EPISODES_DIR / "latest.json").read_text())
print("date:", data["date"], "| est_minutes:", data["est_minutes"], "| words:", data["word_count"])
print("sections:", [s["title"] + f"({len(s['items'])})" for s in data["sections"]])
print("chapters:", [c["title"] for c in data["chapters"]] or "(none, no-audio)")
print("\nfirst 320 chars of opening:\n", data["transcript"][0]["text"][:320])
print("\nsample business text:\n",
      next(t["text"] for t in data["transcript"] if "usiness" in t["title"])[:300])
