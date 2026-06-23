# GBW Morning Brief

A ~30-minute spoken news briefing, built automatically every morning and played from a phone-friendly web page. Covers Bhopal/MP, national India, world, business, money & tax, science, and development. Free to run — no API keys required.

## How it works
GitHub Actions runs `build_brief.py` daily at **04:45 IST**. It pulls fresh stories from RSS feeds, writes a length-controlled script, speaks it with Microsoft `edge-tts`, stitches the audio with chapters, and commits the result to `episodes/`. GitHub Pages serves `index.html`, which plays the latest brief.

## Setup (one time, ~5 minutes)

1. **Create a repo** (e.g. `GBW-MORNING-BRIEF`) and upload every file here, keeping the folder structure (the `.github/workflows/` folder matters).
2. **Settings → Actions → General → Workflow permissions** → select **Read and write permissions** → Save. (Lets the daily job commit each new episode.)
3. **Settings → Pages → Build and deployment** → Source: **Deploy from a branch** → Branch: **main** / **/(root)** → Save.
4. **Generate the first episode now:** go to the **Actions** tab → **Daily Morning Brief** → **Run workflow**. Wait ~2–3 min.
5. Open `https://<your-username>.github.io/<repo>/` on your phone. In the browser menu choose **Add to Home Screen** so it opens like an app.

That's it. A fresh brief appears every morning. Lock-screen and earbud controls work while you train.

## Optional: smoother narration with Claude
By default the brief reads cleaned RSS text. For a more natural newsroom flow:
- **Settings → Secrets and variables → Actions → Secrets** → add `ANTHROPIC_API_KEY`.
- **→ Variables** → add `USE_LLM` = `1`.

Claude is instructed to use **only** the facts in the feeds (no invented details). Cost is a few cents/day on Haiku.

## Customise
Edit `config.json`:
- `target_minutes` — length of the brief (default 30).
- `voice` — any edge-tts voice. Indian English: `en-IN-PrabhatNeural` (m), `en-IN-NeerjaNeural` (f). Others: `en-US-AndrewNeural`, `en-GB-RyanNeural`, `en-AU-NatashaNeural`.
- `rate` — speaking speed, e.g. `+0%`, `+6%`, `+12%`.
- `max_sentences_per_item` — how much of each story to read.

To change **sources or topics**, edit `build_feed_plan()` in `build_brief.py`. Each section is a list of `(label, feed_url, weight)`; higher weight = higher priority. Use `gnews("your query")` to add any Google-News-targeted topic (e.g. a specific town, sector, or scheme).

## Run locally (optional)
Needs Python 3.10+, ffmpeg, and internet.
```bash
pip install -r requirements.txt
python build_brief.py            # builds today's episode into episodes/
python build_brief.py --no-audio # script + JSON only, skips TTS
```
Then open `index.html` via any static server, e.g. `python -m http.server`.

## Files
- `build_brief.py` — fetch → script → audio → JSON.
- `index.html` — the player (GitHub Pages).
- `config.json` — your settings.
- `.github/workflows/daily-news.yml` — the 04:45 IST schedule.
- `episodes/` — generated audio + transcripts (`latest.json` is what the player loads).
- `test_offline.py` — offline logic check with synthetic feeds.
