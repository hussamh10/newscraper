"""
brecorder.py — Business Recorder scraper
=========================================
https://www.brecorder.com  (English — financial/business)

Pakistan's premier business and financial daily.
Article URL format: /news/<numeric-id>/<slug>

Run:
    python brecorder.py
    python brecorder.py --output brecorder_articles.jsonl --concurrency 40
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class BrecorderScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/brecorder/brecorder_articles.jsonl"
    SOURCE_NAME     = "brecorder"
    SITEMAP_URLS    = [
        "https://www.brecorder.com/feeds/sitemap",
    ]
    # /news/<8-digit-id>/<slug>
    ARTICLE_PATTERN = re.compile(r"brecorder\.com/news/\d")
    MAX_CONCURRENT  = 40
    TIMEOUT         = 25

    async def discover_urls(self) -> list[str]:
        return await self._discover_from_sitemaps()

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # ---- Headline ----
        if not fields["headline"]:
            h1 = soup.find("h1")
            fields["headline"] = h1.get_text(strip=True) if h1 else None

        # ---- Date ----
        if not fields["pub_date"]:
            t = soup.find("time")
            if t:
                fields["pub_date"] = t.get("datetime") or t.get_text(strip=True)
            else:
                for pat in (r"article-date", r"publish-date", r"story-date", r"dateline"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"article-author", r"\bauthor\b", r"byline", r"reporter"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:
                        fields["author"] = text
                        break

        # ---- Body ----
        # Business Recorder uses .article-content or .news-detail
        fields["body"] = self._extract_body(
            soup,
            [r"article-content", r"article-body", r"news-detail",
             r"story-content", r"entry-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("brecorder_scraper.log")
    scraper = BrecorderScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Business Recorder scraper",
                                      "data/brecorder/brecorder_articles.jsonl").parse_args()))
