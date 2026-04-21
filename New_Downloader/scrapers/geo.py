"""
geo.py — Geo News scraper
=========================
https://www.geo.tv  (English / Urdu mix)

Article URL examples:
  /latest/123456-headline-slug
  /detail/123456-headline-slug

Run:
    python geo.py
    python geo.py --output geo_articles.jsonl --concurrency 40
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class GeoScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/geo/geo_articles.jsonl"
    SOURCE_NAME     = "geo"
    SITEMAP_URLS    = [
        "https://www.geo.tv/assets/uploads/google_news_latest.xml",
    ]
    ARTICLE_PATTERN = re.compile(r"geo\.tv/(latest|detail)/\d")
    MAX_CONCURRENT  = 50
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
            for pat in (r"date-published", r"update-time", r"story-time", r"publish"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    fields["pub_date"] = tag.get_text(strip=True)
                    break
            if not fields["pub_date"]:
                t = soup.find("time")
                if t:
                    fields["pub_date"] = t.get("datetime") or t.get_text(strip=True)

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\breporter\b", r"author-name", r"\bauthor\b", r"byline"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    fields["author"] = tag.get_text(strip=True)
                    break

        # ---- Body ----
        fields["body"] = self._extract_body(
            soup,
            [r"story-area", r"content-area", r"story-detail", r"article__content",
             r"article-content", r"story-content", r"article-body"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("geo_scraper.log")
    scraper = GeoScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Geo News scraper",
                                      "data/geo/geo_articles.jsonl").parse_args()))
