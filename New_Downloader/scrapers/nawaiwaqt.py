"""
nawaiwaqt.py — Nawa-i-Waqt scraper
=====================================
https://www.nawaiwaqt.com.pk  (Urdu)

One of the oldest Urdu dailies in Pakistan (est. 1940).
Right-leaning / conservative editorial stance — a useful counterpoint
to Dawn and Friday Times for political NLP research.

Article URL format: /news/<numeric-id>/  or  /<category>/<numeric-id>/

Urdu note: content is UTF-8; no special handling needed.

Run:
    python nawaiwaqt.py
    python nawaiwaqt.py --output nawaiwaqt_articles.jsonl --concurrency 40
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class NawaiWaqtScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/nawaiwaqt/nawaiwaqt_articles.jsonl"
    SOURCE_NAME     = "nawaiwaqt"
    SITEMAP_URLS    = [
        "https://www.nawaiwaqt.com.pk/sitemap.xml",
        "https://www.nawaiwaqt.com.pk/feed/",
    ]
    # Numeric-ID segment identifies articles; exclude /tag/ /category/
    # Actual URL format: /DD-Mon-YYYY/<7-digit-ID>  e.g. /13-Mar-2026/1977206
    ARTICLE_PATTERN = re.compile(r"nawaiwaqt\.com\.pk/\d{2}-[A-Z][a-z]+-\d{4}/\d")
    MAX_CONCURRENT  = 40
    TIMEOUT         = 25
    # sitemap.xml has 800+ child sitemaps — cap to 3 most recent daily sitemaps
    MAX_CHILD_SITEMAPS = 3

    async def discover_urls(self) -> list[str]:
        return await self._discover_from_sitemaps()

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # ---- Headline ----
        if not fields["headline"]:
            for pat in (r"news-heading", r"article-heading", r"story-title"):
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
                for pat in (r"\bdate\b", r"news-date", r"publish", r"article-date"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\breporter\b", r"\bauthor\b", r"byline", r"writer"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:
                        fields["author"] = text
                        break

        # ---- Body ----
        fields["body"] = self._extract_body(
            soup,
            [r"news-detail", r"story-detail", r"article-body",
             r"story-content", r"entry-content", r"post-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("nawaiwaqt_scraper.log")
    scraper = NawaiWaqtScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Nawa-i-Waqt scraper",
                                      "data/nawaiwaqt/nawaiwaqt_articles.jsonl").parse_args()))
