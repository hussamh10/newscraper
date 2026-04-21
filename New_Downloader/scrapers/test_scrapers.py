"""
test_scrapers.py
================
Fetches and parses 10 articles from every news source.

For sitemap-based scrapers  : discovers URLs, picks most-recent + 9 random.
For ID-based scrapers (Dawn / TheNews): samples IDs near the current top.

Output
------
  data/<name>/test_10.jsonl   — 10 full records (with raw HTML) per source
  test_report.txt             — human-readable summary table

Run
---
    python test_scrapers.py              # all sources
    python test_scrapers.py geo tribune  # specific sources
    python test_scrapers.py --concurrency 3  # gentler on servers
"""

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import Optional

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import configure_logging

# ── lazy-import every scraper so a broken one doesn't block the rest ─────────
def _load(module, cls):
    import importlib
    return getattr(importlib.import_module(module), cls)

SCRAPERS = {
    "dawn":         lambda: _load("dawn",         "DawnScraper"),
    "thenews":      lambda: _load("thenews",      "TheNewsScraper"),
    "geo":          lambda: _load("geo",          "GeoScraper"),
    "jang":         lambda: _load("jang",         "JangScraper"),
    "bbcurdu":      lambda: _load("bbcurdu",      "BbcUrduScraper"),
    "tribune":      lambda: _load("tribune",      "TribuneScraper"),
    "brecorder":    lambda: _load("brecorder",    "BrecorderScraper"),
    "nation":       lambda: _load("nation",       "NationScraper"),
    "dailytimes":   lambda: _load("dailytimes",   "DailyTimesScraper"),
    "arynews":      lambda: _load("arynews",      "AryNewsScraper"),
    "samaa":        lambda: _load("samaa",        "SamaaScraper"),
    "dunyanews":    lambda: _load("dunyanews",    "DunyaNewsScraper"),
    "expressnews":  lambda: _load("expressnews",  "ExpressNewsScraper"),
    "nawaiwaqt":    lambda: _load("nawaiwaqt",    "NawaiWaqtScraper"),
    "propakistani": lambda: _load("propakistani", "ProPakistaniScraper"),
    "fridaytimes":  lambda: _load("fridaytimes",  "FridayTimesScraper"),
    "profit":       lambda: _load("profit",       "ProfitScraper"),
}

N_TEST    = 10    # articles per source
DISC_TMO  = 90    # seconds for URL discovery
FETCH_TMO = 30    # seconds per HTTP request
log       = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pick_sample(all_urls: list[str], n: int) -> list[str]:
    """Most-recent (index 0) + up to n-1 random from the rest."""
    if not all_urls:
        return []
    most_recent = [all_urls[0]]
    pool = all_urls[1:]
    random.shuffle(pool)
    return most_recent + pool[: n - 1]


def _id_sample(base_url: str, lo: int, hi: int, n: int, suffix: str = "") -> list[str]:
    """For integer-ID scrapers: most recent + random IDs."""
    most_recent = [f"{base_url.format(hi)}{suffix}"]
    pool = random.sample(range(lo, hi), min(n - 1, hi - lo))
    return most_recent + [f"{base_url.format(i)}{suffix}" for i in pool]


# ─────────────────────────────────────────────────────────────────────────────
# Per-scraper test
# ─────────────────────────────────────────────────────────────────────────────

async def test_one(name: str, concurrency: int) -> dict:
    t0 = time.monotonic()
    result = {
        "source":      name,
        "discovered":  0,
        "tested":      0,
        "fetched_ok":  0,
        "parsed_ok":   0,
        "fields":      {"headline": 0, "pub_date": 0, "author": 0, "body": 0},
        "errors":      [],
        "records":     [],
        "elapsed_s":   0,
    }

    # ── instantiate ──────────────────────────────────────────────────────────
    try:
        cls     = SCRAPERS[name]()
        scraper = cls(max_concurrent=concurrency)
    except Exception as exc:
        result["errors"].append(f"import/init failed: {exc}")
        return result

    out_dir = Path(scraper.OUTPUT_FILE).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    test_file = out_dir / "test_10.jsonl"
    test_file.unlink(missing_ok=True)   # fresh run each time

    # ── aiohttp session ───────────────────────────────────────────────────────
    timeout   = aiohttp.ClientTimeout(total=FETCH_TMO)
    connector = aiohttp.TCPConnector(limit=concurrency + 5, ssl=False,
                                     ttl_dns_cache=300)
    headers = {
        "User-Agent":      scraper.USER_AGENT,
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7,ur;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    }

    async with aiohttp.ClientSession(connector=connector, timeout=timeout,
                                     headers=headers) as session:
        scraper._session = session

        # ── URL discovery ─────────────────────────────────────────────────────
        try:
            # For ID-based scrapers, cap the generate range so discovery is instant.
            if name == "dawn":
                sample_urls = _id_sample("https://www.dawn.com/news/{}", 1, 1_893_000, N_TEST)
            elif name == "thenews":
                # Scrape /latest page to get real article URLs (slug is required)
                html = await scraper._raw_fetch("https://www.thenews.com.pk/latest")
                import re as _re
                from bs4 import BeautifulSoup as _BS
                soup_tn = _BS(html or "", "lxml")
                tn_urls = list({
                    (a["href"] if a["href"].startswith("http")
                     else "https://www.thenews.com.pk" + a["href"])
                    for a in soup_tn.find_all("a", href=True)
                    if _re.search(r"/latest/\d{7,}-", a.get("href", ""))
                })
                result["discovered"] = len(tn_urls)
                sample_urls = _pick_sample(tn_urls, N_TEST)
            else:
                raw = await asyncio.wait_for(scraper.discover_urls(), timeout=DISC_TMO)
                result["discovered"] = len(raw)
                sample_urls = _pick_sample(raw, N_TEST)
        except asyncio.TimeoutError:
            result["errors"].append("URL discovery timed out")
            result["elapsed_s"] = round(time.monotonic() - t0, 1)
            return result
        except Exception as exc:
            result["errors"].append(f"URL discovery error: {exc}")
            result["elapsed_s"] = round(time.monotonic() - t0, 1)
            return result

        if not sample_urls:
            result["errors"].append("No URLs discovered — sitemap may be blocked or empty")
            result["elapsed_s"] = round(time.monotonic() - t0, 1)
            return result

        result["tested"] = len(sample_urls)
        log.info("[%s] Testing %d URLs", name, len(sample_urls))

        # ── fetch + parse ─────────────────────────────────────────────────────
        for idx, url in enumerate(sample_urls, 1):
            label = "most-recent" if idx == 1 else f"sample-{idx}"
            try:
                html = await scraper._raw_fetch(url)
            except Exception as exc:
                result["errors"].append(f"fetch [{label}] {url}: {exc}")
                continue

            if html is None:
                result["errors"].append(f"fetch returned None [{label}] {url}")
                continue

            result["fetched_ok"] += 1

            try:
                fields = scraper.parse_article(html, url)
            except Exception as exc:
                result["errors"].append(f"parse [{label}] {url}: {exc}")
                continue

            if fields is None:
                result["errors"].append(f"parse returned None [{label}] {url}")
                continue

            result["parsed_ok"] += 1
            for f in ("headline", "pub_date", "author", "body"):
                if fields.get(f):
                    result["fields"][f] += 1

            record = {
                "source":        name,
                "label":         label,
                "url":           url,
                "headline":      fields.get("headline"),
                "pub_date":      fields.get("pub_date"),
                "author":        fields.get("author"),
                "body":          fields.get("body"),
                "html":          html,
            }
            result["records"].append(record)

            # ── live progress line ────────────────────────────────────────────
            h  = (fields.get("headline") or "")[:65]
            d  = (fields.get("pub_date")  or "—")[:20]
            bl = len(fields.get("body") or "")
            status = (
                f"  {'✓' if h else '✗'}H "
                f"{'✓' if d != '—' else '✗'}D "
                f"{'✓' if fields.get('author') else '✗'}A "
                f"{'✓' if bl else '✗'}B({bl:>5})"
            )
            print(f"    [{label:12s}]{status}  {h!r}")

    # ── write test JSONL ──────────────────────────────────────────────────────
    if result["records"]:
        with test_file.open("w", encoding="utf-8") as fh:
            for rec in result["records"]:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        log.info("[%s] Saved %d records → %s", name, len(result["records"]), test_file)

    result["elapsed_s"] = round(time.monotonic() - t0, 1)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

async def run_tests(names: list[str], concurrency: int) -> list[dict]:
    all_results = []
    for name in names:
        sep = "─" * 62
        print(f"\n{sep}")
        print(f"  {name.upper():20s}  (testing {N_TEST} articles)")
        print(sep)
        try:
            r = await test_one(name, concurrency)
        except Exception as exc:
            log.exception("Test for %s crashed", name)
            r = {"source": name, "errors": [str(exc)], "records": [],
                 "discovered": 0, "tested": 0, "fetched_ok": 0,
                 "parsed_ok": 0, "fields": {}, "elapsed_s": 0}

        all_results.append(r)

        errs = r.get("errors", [])
        if errs:
            for e in errs[:4]:
                print(f"    ⚠  {e}")

        print(
            f"  → discovered={r.get('discovered','—'):>6}  "
            f"tested={r.get('tested',0)}/{N_TEST}  "
            f"fetched={r.get('fetched_ok',0)}  "
            f"parsed={r.get('parsed_ok',0)}  "
            f"elapsed={r.get('elapsed_s',0):.1f}s"
        )
        await asyncio.sleep(1)   # polite gap between sites

    return all_results


def print_report(results: list[dict]) -> str:
    header = (
        f"\n{'═'*78}\n"
        f"  {'SOURCE':<18}  {'DISC':>6}  {'FETCH':>5}  {'PARSE':>5}  "
        f"  {'H':>3}  {'D':>3}  {'A':>3}  {'B':>3}  {'TIME':>5}\n"
        f"{'─'*78}"
    )
    lines = [header]
    for r in results:
        f   = r.get("fields", {})
        n   = r.get("tested", 0) or 1
        ok  = "✓" if r.get("parsed_ok", 0) == n else " "
        row = (
            f"  {ok} {r['source']:<17}  "
            f"{r.get('discovered',0):>6}  "
            f"{r.get('fetched_ok',0):>2}/{n:<2}  "
            f"{r.get('parsed_ok',0):>2}/{n:<2}  "
            f"  {f.get('headline',0):>3}  "
            f"{f.get('pub_date',0):>3}  "
            f"{f.get('author',0):>3}  "
            f"{f.get('body',0):>3}  "
            f"{r.get('elapsed_s',0):>4.1f}s"
        )
        lines.append(row)
    lines.append("─" * 78)
    lines.append("  H=headline  D=date  A=author  B=body   ✓=all parsed OK")
    report = "\n".join(lines)
    print(report)

    Path("test_report.txt").write_text(report + "\n", encoding="utf-8")
    print("\n  Full report saved → test_report.txt")
    print("  Per-source JSONL  → data/<name>/test_10.jsonl\n")
    return report


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    configure_logging("test_scrapers.log")

    p = argparse.ArgumentParser(description="Test 10 articles per source")
    p.add_argument(
        "sources", nargs="*",
        metavar="SOURCE",
        help=f"Sources to test (default: all). Choices: {', '.join(SCRAPERS)}",
    )
    p.add_argument(
        "--concurrency", type=int, default=5,
        help="Max concurrent requests per scraper during testing (default: 5)",
    )
    args = p.parse_args()
    # Validate and default manually (avoids argparse nargs="*"+choices bug)
    valid = set(SCRAPERS.keys())
    if args.sources:
        bad = [s for s in args.sources if s not in valid]
        if bad:
            p.error(f"unknown source(s): {bad}. Choose from: {', '.join(sorted(valid))}")
    else:
        args.sources = list(SCRAPERS.keys())

    print(f"\nTesting {len(args.sources)} source(s) × {N_TEST} articles each")
    results = asyncio.run(run_tests(args.sources, args.concurrency))
    print_report(results)


if __name__ == "__main__":
    main()
