"""
jang.py — Daily Jang scraper
=============================
https://jang.com.pk  (Urdu — UTF-8)

Urdu note:
  BeautifulSoup get_text() returns Python str (Unicode) directly.
  No manual encode/decode needed — both aiohttp and SQLite/JSONL
  handle UTF-8 transparently.

Run:
    python jang.py
    python jang.py --output jang_articles.jsonl --concurrency 30
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class JangScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/jang/jang_articles.jsonl"
    SOURCE_NAME     = "jang"
    SITEMAP_URLS    = [
        "https://jang.com.pk/en/sitemap/google_eng_latest.xml",
    ]
    # English: /en/<ID>-<slug>-news   Urdu: /news/<ID>
    ARTICLE_PATTERN = re.compile(
        r"jang\.com\.pk/(en/\d|news/\d)"
    )
    MAX_CONCURRENT  = 40   # conservative for smaller site
    TIMEOUT         = 25

    async def discover_urls(self) -> list[str]:
        return await self._discover_from_sitemaps()

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # ---- Headline ----
        if not fields["headline"]:
            for pat in (r"news-title", r"article-title", r"\bheading\b"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    fields["headline"] = tag.get_text(strip=True)
                    break
            if not fields["headline"]:
                h1 = soup.find("h1")
                fields["headline"] = h1.get_text(strip=True) if h1 else None

        # ---- Date ----
        if not fields["pub_date"]:
            t = soup.find("time")
            if t:
                fields["pub_date"] = t.get("datetime") or t.get_text(strip=True)
            else:
                for pat in (r"\bdate\b", r"news-date", r"article-date"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\breporter\b", r"\bauthor\b", r"news-reporter", r"byline"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    fields["author"] = tag.get_text(strip=True)
                    break

        # ---- Body ----
        fields["body"] = self._extract_body(
            soup,
            [r"news-detail", r"news-text", r"story-text",
             r"story-detail", r"article-body", r"article-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("jang_scraper.log")
    scraper = JangScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Daily Jang scraper",
                                      "data/jang/jang_articles.jsonl").parse_args()))
