#!/usr/bin/env python3
"""
Distributed scraper client: claim batches from the coordinator, fetch articles
using the same parsers as New_Downloader/scrapers, upload results.

Run from repo root:
  pip install -r client/requirements.txt
  python client/run_client.py --server http://127.0.0.1:8000 --loop
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import random
import sys
import time
from pathlib import Path
from typing import Any, Optional

import aiohttp

log = logging.getLogger(__name__)

CLIENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CLIENT_DIR.parent
SCRAPERS_DIR = REPO_ROOT / "New_Downloader" / "scrapers"


def load_parser_class(parser_spec: str) -> type:
    mod_name, _, cls_name = parser_spec.partition(":")
    if not mod_name or not cls_name:
        raise ValueError(f"bad parser spec: {parser_spec!r}")
    sys.path.insert(0, str(SCRAPERS_DIR))
    mod = importlib.import_module(mod_name)
    return getattr(mod, cls_name)


async def fetch_sources(session: aiohttp.ClientSession, base: str) -> dict[str, Any]:
    url = f"{base.rstrip('/')}/sources"
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()


async def claim_batch(
    session: aiohttp.ClientSession,
    base: str,
    *,
    source: Optional[str],
    size: int,
    client_id: Optional[str],
) -> dict[str, Any]:
    url = f"{base.rstrip('/')}/batch/claim"
    body: dict[str, Any] = {"size": size}
    if source:
        body["source"] = source
    if client_id:
        body["client_id"] = client_id
    async with session.post(url, json=body) as resp:
        if resp.status == 404:
            return {}
        resp.raise_for_status()
        return await resp.json()


async def submit_batch(
    session: aiohttp.ClientSession,
    base: str,
    batch_id: str,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    url = f"{base.rstrip('/')}/batch/{batch_id}/submit"
    async with session.post(url, json={"results": results}) as resp:
        txt = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"submit {resp.status}: {txt}")
        return json.loads(txt)


async def send_heartbeat(session: aiohttp.ClientSession, base: str, batch_id: str) -> None:
    url = f"{base.rstrip('/')}/batch/{batch_id}/heartbeat"
    async with session.post(url) as resp:
        if resp.status >= 400:
            log.warning("heartbeat failed: %s %s", resp.status, await resp.text())
        else:
            data = await resp.json()
            log.debug("heartbeat ok expires_at=%s", data.get("expires_at"))


async def heartbeat_task(session: aiohttp.ClientSession, base: str, batch_id: str) -> None:
    try:
        while True:
            await asyncio.sleep(600)
            await send_heartbeat(session, base, batch_id)
    except asyncio.CancelledError:
        raise


async def scrape_one_url(
    scraper: Any,
    url: str,
    sem: asyncio.Semaphore,
    server_source: str,
) -> dict[str, Any]:
    async with sem:
        try:
            html = await scraper._raw_fetch(url)
            if html is None:
                return {"url": url, "ok": False, "error": "fetch failed or 404"}
            try:
                fields = scraper.parse_article(html, url)
            except Exception as exc:
                return {"url": url, "ok": False, "error": f"parse: {exc}"}
            if not fields:
                return {"url": url, "ok": False, "error": "parse returned empty / skip"}
            # Coordinator source id (e.g. "express") may differ from scraper.SOURCE_NAME ("express_news").
            record = {
                "source": server_source,
                "url": url,
                "html": html,
                "headline": fields.get("headline"),
                "pub_date": fields.get("pub_date"),
                "author": fields.get("author"),
                "body": fields.get("body"),
            }
            return {"url": url, "ok": True, "record": record}
        except Exception as exc:
            return {"url": url, "ok": False, "error": str(exc)}


async def run_one_batch(
    session: aiohttp.ClientSession,
    base: str,
    parser_map: dict[str, str],
    *,
    source: Optional[str],
    size: int,
    concurrency: int,
    client_id: Optional[str],
) -> bool:
    claim = await claim_batch(session, base, source=source, size=size, client_id=client_id)
    if not claim:
        log.warning("no batch available (404)")
        return False

    batch_id = claim["batch_id"]
    batch_source = claim["source"]
    items: list[dict[str, str]] = claim.get("items") or [
        {"url": u, "source": batch_source} for u in claim["urls"]
    ]
    if not items:
        log.warning("empty claim payload")
        return False
    random.shuffle(items)

    parser_instances: dict[str, Any] = {}
    dummy_out = REPO_ROOT / "client" / ".dummy_out.jsonl"
    dummy_out.parent.mkdir(parents=True, exist_ok=True)
    for item in items:
        src = item["source"]
        if src in parser_instances:
            continue
        parser_spec = parser_map.get(src)
        if not parser_spec:
            raise RuntimeError(f"unknown source from server: {src}")
        ParserCls = load_parser_class(parser_spec)
        parser_instances[src] = ParserCls(output_file=str(dummy_out), max_concurrent=concurrency)

    timeout = aiohttp.ClientTimeout(total=35)
    connector = aiohttp.TCPConnector(limit=concurrency + 10, ttl_dns_cache=300)
    headers = {
        "User-Agent": "news-scraper-client/1.0",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7,ur;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    }

    sem = asyncio.Semaphore(concurrency)
    hb = asyncio.create_task(heartbeat_task(session, base, batch_id))

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
        ) as fetch_session:
            for scraper in parser_instances.values():
                scraper._session = fetch_session
            tasks = [
                asyncio.create_task(
                    scrape_one_url(parser_instances[item["source"]], item["url"], sem, item["source"])
                )
                for item in items
            ]
            results = await asyncio.gather(*tasks)
    finally:
        hb.cancel()
        try:
            await hb
        except asyncio.CancelledError:
            pass
        for scraper in parser_instances.values():
            scraper._session = None

    summary = await submit_batch(session, base, batch_id, results)
    log.info(
        "submitted batch %s: ok=%s fail=%s lines=%s",
        batch_id[:8],
        summary.get("processed_ok"),
        summary.get("processed_fail"),
        summary.get("jsonl_lines_written"),
    )
    return True


async def async_main(args: argparse.Namespace) -> None:
    base = args.server.rstrip("/")
    headers = {"User-Agent": "news-scraper-client/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        data = await fetch_sources(session, base)
        parser_map = {k: v["parser"] for k, v in data["sources"].items()}
        log.info("sources: %s", list(parser_map.keys()))

        while True:
            ok = await run_one_batch(
                session,
                base,
                parser_map,
                source=args.source,
                size=args.batch_size,
                concurrency=args.concurrency,
                client_id=args.client_id,
            )
            if not args.loop:
                break
            if not ok:
                log.info("waiting 30s before retry…")
                await asyncio.sleep(30)
            else:
                await asyncio.sleep(0.5)


def main() -> None:
    p = argparse.ArgumentParser(description="News scraper distributed client")
    p.add_argument("--server", required=True, help="Coordinator base URL, e.g. http://127.0.0.1:8000")
    p.add_argument("--loop", action="store_true", help="Keep claiming batches until interrupted")
    p.add_argument("--source", default=None, help="Force a source (express, geo, jang, thenews, tribune)")
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--concurrency", type=int, default=20)
    p.add_argument("--client-id", default=None, help="Shown in server assigned_to field")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not SCRAPERS_DIR.is_dir():
        log.error("Scrapers dir not found: %s (clone full repo)", SCRAPERS_DIR)
        sys.exit(1)

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        log.info("stopped")


if __name__ == "__main__":
    main()
