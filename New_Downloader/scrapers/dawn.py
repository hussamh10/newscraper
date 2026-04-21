"""
dawn.py — Dawn.com scraper
==========================
Verified structure (March 2026):
  Sitemap : https://www.dawn.com/feeds/sitemap  (Google News; /sitemap.xml → 404)
  Articles: https://www.dawn.com/news/{ID}/{slug}
  ID range: ~1 – ~1,893,000 (IDs above ~1,895,000 externally redirect)
  Headline: h1  inside  div.story__content
  Body    : div.story__content  > p
  Date    : span.story__time   (or JSON-LD datePublished)
  Author  : a.story__byline__link  /  span.story__byline

Run:
    python dawn.py
    python dawn.py --start 1 --end 1893000 --concurrency 60 --output dawn_articles.jsonl
"""

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, JsonlStore, configure_logging, make_arg_parser
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


class DawnScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/dawn/dawn_articles.jsonl"
    SOURCE_NAME     = "dawn"
    SITEMAP_URLS    = ["https://www.dawn.com/feeds/sitemap"]
    ARTICLE_PATTERN = re.compile(r"dawn\.com/news/\d")
    MAX_CONCURRENT  = 60
    TIMEOUT         = 20
    BASE_URL        = "https://www.dawn.com/news/{}"

    def __init__(
        self,
        start_id: int = 1,
        end_id: int = 1_893_000,
        output_file: str | None = None,
        max_concurrent: int | None = None,
    ) -> None:
        super().__init__(output_file=output_file, max_concurrent=max_concurrent)
        self.start_id = start_id
        self.end_id   = end_id

    # ── URL discovery ────────────────────────────────────────────────────────

    async def discover_urls(self) -> list[str]:
        """
        Primary  : pull valid recent URLs from the Google News sitemap feed.
        Secondary: generate integer-ID URLs for historical coverage.
        The sitemap only holds the last ~1,000 articles; for a full scrape
        use --start/--end to sweep the integer range.
        """
        # Try sitemap first (gives accurate, server-verified URLs)
        sitemap_urls = await self._discover_from_sitemaps()
        if sitemap_urls:
            log.info("[dawn] Using %d sitemap URLs (recent articles)", len(sitemap_urls))
            return sitemap_urls

        # Fall back to integer-ID sweep
        resume_from = self._find_resume_id()
        effective_start = max(self.start_id, resume_from + 1)
        log.info(
            "[dawn] Sitemap empty — generating IDs %d → %d",
            effective_start, self.end_id,
        )
        return [self.BASE_URL.format(i) for i in range(effective_start, self.end_id + 1)]

    def _find_resume_id(self) -> int:
        """Scan JSONL for the highest integer ID already saved."""
        path = Path(self.OUTPUT_FILE)
        if not path.exists() or path.stat().st_size == 0:
            return self.start_id - 1
        max_id = self.start_id - 1
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    m = re.search(r"/news/(\d+)", json.loads(line).get("url", ""))
                    if m:
                        max_id = max(max_id, int(m.group(1)))
                except (json.JSONDecodeError, ValueError):
                    continue
        log.info("[dawn] Resume scan complete. Last saved ID: %d", max_id)
        return max_id

    # ── Parsing ──────────────────────────────────────────────────────────────

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # Headline
        if not fields["headline"]:
            h1 = soup.find("h1")
            fields["headline"] = h1.get_text(strip=True) if h1 else None

        # Date — <span class="story__time"> or <time>
        if not fields["pub_date"]:
            tag = soup.find("span", class_=re.compile(r"story__time", re.I))
            if not tag:
                tag = soup.find("time")
            if tag:
                fields["pub_date"] = tag.get("datetime") or tag.get_text(strip=True)

        # Author — <a class="story__byline__link"> or <span class="story__byline">
        if not fields["author"]:
            tag = (
                soup.find("a",    class_=re.compile(r"story__byline", re.I))
                or soup.find("span", class_=re.compile(r"story__byline", re.I))
            )
            if tag:
                fields["author"] = tag.get_text(strip=True)

        # Body — <div class="story__content">
        fields["body"] = self._extract_body(soup, [r"story__content", r"story-content"])

        return fields


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main(args) -> None:
    configure_logging("dawn_scraper.log")
    scraper = DawnScraper(
        start_id=args.start,
        end_id=args.end,
        output_file=args.output,
        max_concurrent=args.concurrency,
    )
    await scraper.run()


if __name__ == "__main__":
    import argparse
    p = make_arg_parser("Dawn.com scraper", "data/dawn/dawn_articles.jsonl")
    p.add_argument("--start", type=int, default=1,          help="First article ID (default: 1)")
    p.add_argument("--end",   type=int, default=1_893_000,  help="Last article ID (default: 1893000)")
    asyncio.run(_main(p.parse_args()))
