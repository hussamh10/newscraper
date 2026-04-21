"""
thenews.py — The News International scraper
============================================
Verified structure (March 2026):
  Sitemap : NONE for articles (/sitemap.xml only lists section pages)
            → use integer-ID sweep (IDs currently ~1–1,395,595)
  Articles: https://www.thenews.com.pk/latest/{ID}-{slug}
            Server extracts the numeric ID prefix; the slug can be anything.
  Headline: div.detail-heading > h1
  Body    : div.story-detail > p
  Date    : div.category-date
  Author  : div.category-source > a
  JSON-LD : NewsArticle present (datePublished, author)

Run:
    python thenews.py
    python thenews.py --start 1 --end 1395595 --concurrency 50
"""

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


class TheNewsScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/thenews/thenews_articles.jsonl"
    SOURCE_NAME     = "thenews"
    SITEMAP_URLS    = []   # no article sitemap exists — ID-based only
    ARTICLE_PATTERN = re.compile(r"thenews\.com\.pk/latest/\d")
    MAX_CONCURRENT  = 50
    TIMEOUT         = 25
    BASE_URL        = "https://www.thenews.com.pk/latest/{}"

    def __init__(
        self,
        start_id: int = 1,
        end_id: int = 1_395_595,
        output_file: str | None = None,
        max_concurrent: int | None = None,
    ) -> None:
        super().__init__(output_file=output_file, max_concurrent=max_concurrent)
        self.start_id = start_id
        self.end_id   = end_id

    # ── URL discovery ────────────────────────────────────────────────────────

    async def discover_urls(self) -> list[str]:
        """
        TheNews has no article sitemap.  Scrape /latest?page=N to get real
        article URLs that include the slug (slug-less URLs return empty pages).
        Paginates until it reaches already-scraped articles or end of pages.
        """
        resume_from = self._find_resume_id()
        seen: dict[str, None] = {}
        page = 1
        while page <= self.end_id:   # end_id reused as max pages guard
            url = "https://www.thenews.com.pk/latest"
            if page > 1:
                url += f"?page={page}"
            html = await self._raw_fetch(url)
            if not html:
                break
            soup = BeautifulSoup(html, "lxml")
            found_new = False
            for a in soup.find_all("a", href=True):
                href = a["href"]
                m = re.search(r"/latest/(\d+)-", href)
                if m:
                    article_id = int(m.group(1))
                    if article_id <= resume_from:
                        log.info("[thenews] Reached already-scraped ID %d on page %d",
                                 article_id, page)
                        return list(seen)
                    full = href if href.startswith("http") else "https://www.thenews.com.pk" + href
                    if full not in seen:
                        seen[full] = None
                        found_new = True
            log.info("[thenews] Page %d → %d URLs so far", page, len(seen))
            if not found_new:
                break
            page += 1
        return list(seen)

    def _find_resume_id(self) -> int:
        path = Path(self.OUTPUT_FILE)
        if not path.exists() or path.stat().st_size == 0:
            return self.start_id - 1
        max_id = self.start_id - 1
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    m = re.search(r"/latest/(\d+)", json.loads(line).get("url", ""))
                    if m:
                        max_id = max(max_id, int(m.group(1)))
                except (json.JSONDecodeError, ValueError):
                    continue
        log.info("[thenews] Resume scan complete. Last saved ID: %d", max_id)
        return max_id

    # ── Parsing ──────────────────────────────────────────────────────────────

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # Headline — <div class="detail-heading"><h1>
        if not fields["headline"]:
            container = soup.find("div", class_=re.compile(r"detail-heading", re.I))
            h1 = container.find("h1") if container else soup.find("h1")
            fields["headline"] = h1.get_text(strip=True) if h1 else None

        # Date — <div class="category-date">
        if not fields["pub_date"]:
            tag = soup.find(class_=re.compile(r"category-date", re.I))
            if not tag:
                tag = soup.find("time")
            if tag:
                fields["pub_date"] = tag.get("datetime") or tag.get_text(strip=True)

        # Author — <div class="category-source"><a>
        if not fields["author"]:
            container = soup.find(class_=re.compile(r"category-source", re.I))
            if container:
                a = container.find("a")
                fields["author"] = a.get_text(strip=True) if a else container.get_text(strip=True)

        # Body — <div class="story-detail">
        fields["body"] = self._extract_body(
            soup, [r"story-detail", r"story__detail", r"story-content"]
        )

        return fields


async def _main(args) -> None:
    configure_logging("thenews_scraper.log")
    scraper = TheNewsScraper(
        start_id=args.start,
        end_id=args.end,
        output_file=args.output,
        max_concurrent=args.concurrency,
    )
    await scraper.run()


if __name__ == "__main__":
    import argparse
    p = make_arg_parser("The News International scraper", "data/thenews/thenews_articles.jsonl")
    p.add_argument("--start", type=int, default=1,          help="First article ID")
    p.add_argument("--end",   type=int, default=1_395_595,  help="Last article ID")
    asyncio.run(_main(p.parse_args()))
