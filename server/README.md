# News scraper coordinator (server)

FastAPI service that:

- Tracks article URLs in SQLite (`state.sqlite`) with statuses: `pending`, `leased`, `done`, `failed`
- On startup, **re-ingests URL lists only when** the corresponding `*_urls.txt` file’s mtime/size changed (see `ingest_meta` table). First run does a full scan.
- Hands out batches (`POST /batch/claim`), accepts results (`POST /batch/{id}/submit`), extends leases (`POST /batch/{id}/heartbeat`)
- Writes successful articles to append-only JSONL: `server/data/<source>/<source>_articles.jsonl`
- Serves a live dashboard at `/` and JSON metrics at `/status`

## Install

From the **repository root** (`news-scraper/`):

```bash
pip install -r server/requirements.txt
```

## Run

```bash
cd /path/to/news-scraper
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

Open `http://<host>:8000/` for the dashboard.

## API (short)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/sources` | Source ids → `parser` string for clients |
| GET | `/status` | Global + per-source counts, chart buckets, open batches |
| POST | `/batch/claim` | Body: `{ "size": 50, "source": null, "client_id": "..." }` |
| POST | `/batch/{id}/heartbeat` | Extend lease by 30 minutes |
| POST | `/batch/{id}/submit` | Body: `{ "results": [ { "url", "ok", "record"?, "error"? } ] }` |
| POST | `/admin/reingest?force=1` | `force=1` re-reads all URL files; omit `force` to only ingest when `*_urls.txt` mtime/size changed |

## Security note

This build has **no authentication**. Anyone who can reach the HTTP port can claim batches and submit data. Put it behind a firewall or reverse-proxy auth if exposed.

## Adding a new source

1. Add `yoursource_urls.txt` under `New_Downloader/`.
2. Add a scraper module under `New_Downloader/scrapers/yoursource.py` with a `BaseNewsScraper` subclass.
3. Register it in [`sources.py`](sources.py): `urls_file` path and `parser` as `module:ClassName` (module name without `.py`).
4. Restart the server (or call `POST /admin/reingest` after a file change).
