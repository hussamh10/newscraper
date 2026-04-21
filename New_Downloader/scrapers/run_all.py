"""
run_all.py — Run any subset of scrapers from one command
=========================================================
Each scraper can also be run individually with `python geo.py`.
This file is a convenience wrapper for batch runs.

Usage:
    # Run all 17 scrapers sequentially (safest, lowest peak load):
    python run_all.py

    # Run specific scrapers:
    python run_all.py --sources dawn geo thenews

    # Run all scrapers concurrently (faster, higher load):
    python run_all.py --parallel

    # Throttle concurrency across all scrapers:
    python run_all.py --sources geo bbc --concurrency 20

Available source names:
    dawn thenews geo jang bbcurdu tribune brecorder nation dailytimes
    arynews samaa dunyanews expressnews nawaiwaqt propakistani fridaytimes profit
"""

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import configure_logging

# Lazy imports — each scraper module is only imported when selected,
# so a broken dependency in one scraper doesn't affect the others.
REGISTRY: dict[str, tuple[str, str]] = {
    # name          → (module,          class_name)
    "dawn":         ("dawn",         "DawnScraper"),
    "thenews":      ("thenews",      "TheNewsScraper"),
    "geo":          ("geo",          "GeoScraper"),
    "jang":         ("jang",         "JangScraper"),
    "bbcurdu":      ("bbcurdu",      "BbcUrduScraper"),
    "tribune":      ("tribune",      "TribuneScraper"),
    "brecorder":    ("brecorder",    "BrecorderScraper"),
    "nation":       ("nation",       "NationScraper"),
    "dailytimes":   ("dailytimes",   "DailyTimesScraper"),
    "arynews":      ("arynews",      "AryNewsScraper"),
    "samaa":        ("samaa",        "SamaaScraper"),
    "dunyanews":    ("dunyanews",    "DunyaNewsScraper"),
    "expressnews":  ("expressnews",  "ExpressNewsScraper"),
    "nawaiwaqt":    ("nawaiwaqt",    "NawaiWaqtScraper"),
    "propakistani": ("propakistani", "ProPakistaniScraper"),
    "fridaytimes":  ("fridaytimes",  "FridayTimesScraper"),
    "profit":       ("profit",       "ProfitScraper"),
}

log = logging.getLogger(__name__)


def _build_scraper(name: str, concurrency: int | None):
    """Dynamically import and instantiate a scraper by registry name."""
    import importlib
    module_name, class_name = REGISTRY[name]
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    return cls(max_concurrent=concurrency)


async def run_sequential(names: list[str], concurrency: int | None) -> None:
    """Run scrapers one at a time — easiest to monitor, lowest peak load."""
    for name in names:
        scraper = _build_scraper(name, concurrency)
        log.info("--- Starting %s ---", name)
        try:
            await scraper.run()
        except Exception as exc:
            log.error("Scraper '%s' failed: %s", name, exc, exc_info=True)


async def run_parallel(names: list[str], concurrency: int | None) -> None:
    """Run all selected scrapers concurrently."""
    scrapers = [_build_scraper(name, concurrency) for name in names]
    results = await asyncio.gather(
        *[s.run() for s in scrapers],
        return_exceptions=True,
    )
    for name, result in zip(names, results):
        if isinstance(result, Exception):
            log.error("Scraper '%s' failed: %s", name, result, exc_info=result)


def main() -> None:
    configure_logging("run_all.log")

    parser = argparse.ArgumentParser(
        description="Run Pakistani news scrapers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list(REGISTRY.keys()),
        default=list(REGISTRY.keys()),
        metavar="SOURCE",
        help="Scrapers to run (default: all).",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        default=False,
        help="Run scrapers concurrently (default: sequential).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        metavar="N",
        help="Override max concurrent requests per scraper.",
    )
    args = parser.parse_args()

    log.info(
        "Starting | sources=%s | parallel=%s | concurrency=%s",
        args.sources, args.parallel, args.concurrency,
    )

    if args.parallel:
        asyncio.run(run_parallel(args.sources, args.concurrency))
    else:
        asyncio.run(run_sequential(args.sources, args.concurrency))

    log.info("All done.")


if __name__ == "__main__":
    main()
