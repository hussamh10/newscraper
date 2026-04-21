"""
tribune.py — The Express Tribune scraper
=========================================
https://tribune.com.pk  (English)

Partners with the International New York Times.
Article URL format: /story/<numeric-id>/<slug>

Run:
    python tribune.py
    python tribune.py --output tribune_articles.jsonl --concurrency 50
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class TribuneScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/tribune/tribune_articles.jsonl"
    SOURCE_NAME     = "tribune"
    # sitemap.xml is an index with 49 child sitemaps — cap to first 2 to avoid timeout
    SITEMAP_URLS    = [
        "https://tribune.com.pk/sitemap.xml",
    ]
    MAX_CHILD_SITEMAPS = 2
    # /story/<numeric id> distinguishes articles from /tag/ /section/ /author/ pages
    ARTICLE_PATTERN = re.compile(r"tribune\.com\.pk/story/\d")
    MAX_CONCURRENT  = 60
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
                for pat in (r"story-date", r"publish-date", r"article-date"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\bauthor\b", r"byline", r"reporter", r"contributor"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:  # exclude long nav blocks
                        fields["author"] = text
                        break

        # ---- Body ----
        # Tribune wraps article text in .story-text span or .story-body
        fields["body"] = self._extract_body(
            soup,
            [r"story-text", r"story-body", r"full-story", r"story__content",
             r"article-body", r"entry-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("tribune_scraper.log")
    scraper = TribuneScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Express Tribune scraper",
                                      "data/tribune/tribune_articles.jsonl").parse_args()))
