# Distributed scraping (server + clients)

This repo can run as a **coordinator + volunteers** setup:

1. **Server** ([`server/README.md`](server/README.md)) — hosts URL queues, lease state, JSONL output, and a web dashboard.
2. **Client** ([`client/README.md`](client/README.md)) — any machine with this repo cloned runs `client/run_client.py` to pull batches of URLs, fetch articles, and upload parsed records.

Article parsers live in [`New_Downloader/scrapers/`](New_Downloader/scrapers/); the client imports them by name using the `parser` field from `GET /sources`.

## Quick start

Terminal A (server machine, from repo root):

```bash
pip install -r server/requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

Terminal B (client, from repo root):

```bash
pip install -r client/requirements.txt
python client/run_client.py --server http://<server-ip>:8000 --loop
```

## Adding URLs

Edit or append to the `*_urls.txt` files under `New_Downloader/`. When the file’s modification time or size changes, the next server startup (or `POST /admin/reingest`) ingests **new** URLs only (`INSERT OR IGNORE`). Use `POST /admin/reingest?force=1` to re-scan every line (slow on huge lists).

## Adding a new source

See **“Adding a new source”** in [`server/README.md`](server/README.md).
