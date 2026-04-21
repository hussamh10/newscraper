# Pakistani News Scrapers

Async Python scrapers for 17 Pakistani and Pakistani-interest news sources. Each scraper runs independently and writes to its own JSONL file.

## Quick Start

```bash
# Install dependencies
pip install aiohttp aiofiles beautifulsoup4 lxml tqdm brotli

# Test 10 articles from every source (takes ~3–4 minutes)
cd scrapers/
python test_scrapers.py

# Test specific sources
python test_scrapers.py dawn geo brecorder

# Run a full scrape of one source
python dawn.py
python geo.py --concurrency 30
```

## Sources

| Script | Site | Language | Notes |
|--------|------|----------|-------|
| `dawn.py` | Dawn | English | ID-based (1.9M articles) |
| `thenews.py` | The News International | English | Page-scraped discovery |
| `geo.py` | Geo News | English/Urdu | Google News sitemap |
| `jang.py` | Daily Jang (English) | English | Google News sitemap |
| `bbcurdu.py` | BBC Urdu | Urdu | RSS feed |
| `tribune.py` | The Express Tribune | English | Sitemap index (capped at 2) |
| `brecorder.py` | Business Recorder | English | News sitemap |
| `nation.py` | The Nation | English | News sitemap |
| `arynews.py` | ARY News | English | RSS feed |
| `samaa.py` | Samaa TV | English | Article sitemap page 1 |
| `dunyanews.py` | Dunya News | English | Direct sitemap |
| `expressnews.py` | Express News | Urdu | RSS feed |
| `nawaiwaqt.py` | Nawa-i-Waqt | Urdu | Sitemap (3 recent days) |
| `propakistani.py` | ProPakistani | English | ⚠ Blocked (403) |
| `fridaytimes.py` | The Friday Times | English | Sitemap (3 recent days) |
| `profit.py` | Profit (Pakistan Today) | English | RSS feed |
| `dailytimes.py` | Daily Times | English | ⚠ Blocked (403) |

> **Note**: `dailytimes.com.pk` and `propakistani.pk` return 403 Forbidden for all automated requests. These scrapers cannot collect data until those sites lift their restrictions.

## Output Schema

Each scraper writes newline-delimited JSON (`*.jsonl`) to `data/<source>/<source>_articles.jsonl`.

Every line is a JSON object with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Scraper identifier (e.g. `"dawn"`) |
| `url` | string | Canonical article URL |
| `headline` | string \| null | Article title |
| `pub_date` | string \| null | Publication date (ISO 8601 when available) |
| `author` | string \| null | Byline / author name |
| `body` | string \| null | Article body text (paragraphs joined with `\n\n`) |
| `html` | string | Raw HTML of the fetched page |

Example record:

```json
{
  "source": "dawn",
  "url": "https://www.dawn.com/news/1893042/some-article-slug",
  "headline": "Pakistan and India agree on ceasefire",
  "pub_date": "2026-03-14T10:30:00+05:00",
  "author": "Zahid Hussain",
  "body": "ISLAMABAD: Pakistan and India have agreed...\n\nThe agreement was reached...",
  "html": "<!DOCTYPE html>..."
}
```

### Known field limitations

| Source | Missing field | Reason |
|--------|--------------|--------|
| `samaa` | `pub_date` | Date is JavaScript-rendered only |
| `dunyanews` | `author` | Not included in page HTML |

## Running All Scrapers

```bash
# Sequential (default) — runs one at a time, polite to servers
python run_all.py

# Parallel — runs all simultaneously (fast but aggressive)
python run_all.py --parallel

# Selected sources only
python run_all.py --sources dawn geo brecorder

# Custom concurrency per scraper
python run_all.py --concurrency 20
```

## Individual Scraper Options

Every scraper accepts `--output` and `--concurrency`:

```bash
python dawn.py --output /data/dawn.jsonl --concurrency 50
python geo.py  --output geo.jsonl        --concurrency 30
```

`thenews.py` additionally accepts `--start` and `--end` (page range for discovery).

## Resumability

All scrapers are **crash-safe and resumable**:

- On startup, they read the existing JSONL file and load all saved URLs into a dedup set.
- Only unseen URLs are fetched.
- Re-running any scraper picks up exactly where it left off.

## Architecture

```
scrapers/
├── base.py           # Shared async infrastructure (JsonlStore, BaseNewsScraper)
├── dawn.py           # Site-specific scrapers (one per source)
├── geo.py
├── ...
├── test_scrapers.py  # Test harness — downloads 10 articles per source
└── run_all.py        # Batch runner

data/
├── dawn/
│   └── dawn_articles.jsonl
├── geo/
│   └── geo_articles.jsonl
└── ...
```

### `base.py` components

- **`JsonlStore`** — append-only JSONL writer with URL-level deduplication.
- **`BaseNewsScraper`** — abstract base with HTTP retry/backoff, sitemap/RSS parsing, JSON-LD metadata extraction, and the `run()` pipeline.

HTTP behaviour:
- Retries 429 and 5xx responses with exponential backoff (2s, 4s, 8s, …).
- Skips 404s immediately.
- Follows redirects automatically.
- Decodes Urdu/Arabic UTF-8 with `errors="replace"` to prevent crashes.

## Testing

```bash
# All sources
python test_scrapers.py

# Specific sources with lower concurrency
python test_scrapers.py dawn geo --concurrency 3

# Results are saved to:
#   test_report.txt          — summary table
#   data/<name>/test_10.jsonl — 10 records per source with raw HTML
```
