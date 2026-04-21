# News scraper client

Python script that talks to the coordinator: claim a batch, download HTML with the same retry behaviour as the scrapers, run `parse_article`, upload JSONL-shaped records.

## Setup

From **repository root** (you need the full clone so `New_Downloader/scrapers/` exists):

```bash
pip install -r client/requirements.txt
```

## Usage

```bash
# One batch then exit
python client/run_client.py --server http://127.0.0.1:8000

# Continuous worker
python client/run_client.py --server http://127.0.0.1:8000 --loop

# Options
python client/run_client.py --server http://HOST:8000 --source tribune --batch-size 50 \
  --concurrency 25 --client-id my-laptop --verbose
```

- **`--source`**: optional; if omitted the server assigns the source with the largest pending queue.
- **`record["source"]`** is set to the server’s canonical source id (e.g. `express`) so it matches the coordinator even when `scraper.SOURCE_NAME` differs (e.g. `express_news`).

## Requirements

- Python 3.10+
- Network access to the coordinator and to the news sites you scrape.
