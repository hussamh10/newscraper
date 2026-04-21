"""
profit.py — Profit by Pakistan Today scraper
=============================================
https://profit.pakistantoday.com.pk  (English)

Specialised business and economics publication.
Known for long-form investigative financial journalism.
WordPress-based.

Article URL format: /<YYYY>/<MM>/<DD>/<slug>/

Run:
    python profit.py
    python profit.py --output profit_articles.jsonl --concurrency 40
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class ProfitScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/profit/profit_articles.jsonl"
    SOURCE_NAME     = "profit"
    # sitemap_index.xml has 58 child sitemaps — use RSS feed for fast discovery.
    SITEMAP_URLS    = [
        "https://profit.pakistantoday.com.pk/feed/",
    ]
    # Date-in-path WP articles; exclude /category/ /tag/ /author/ pages
    ARTICLE_PATTERN = re.compile(
        r"profit\.pakistantoday\.com\.pk/\d{4}/\d{2}/\d{2}/"
    )
    MAX_CONCURRENT  = 40
    TIMEOUT         = 25

    async def discover_urls(self) -> list[str]:
        return await self._discover_from_sitemaps()

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # ---- Headline ----
        if not fields["headline"]:
            for cls in (r"entry-title", r"post-title", r"article-title"):
                tag = soup.find("h1", class_=re.compile(cls, re.I))
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
                for pat in (r"entry-date", r"post-date", r"published", r"\bdate\b"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"entry-author", r"\bauthor\b", r"byline", r"post-author"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:
                        fields["author"] = text
                        break

        # ---- Body ----
        fields["body"] = self._extract_body(
            soup,
            [r"entry-content", r"post-content", r"article-content",
             r"story-content", r"article-body"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("profit_scraper.log")
    scraper = ProfitScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Profit (Pakistan Today) scraper",
                                      "data/profit/profit_articles.jsonl").parse_args()))
