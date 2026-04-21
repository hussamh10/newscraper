"""
dunyanews.py — Dunya News scraper
===================================
https://dunyanews.tv  (English / Urdu)
Also runs the Urdu newspaper Roznama Dunya.

Article URL format: /en/<category>/<numeric-id>-<slug>
                or: /home/detail/<numeric-id>-<slug>

Run:
    python dunyanews.py
    python dunyanews.py --output dunyanews_articles.jsonl --concurrency 50
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class DunyaNewsScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/dunyanews/dunyanews_articles.jsonl"
    SOURCE_NAME     = "dunyanews"
    SITEMAP_URLS    = [
        "https://dunyanews.tv/sitemap.xml",
        "https://dunyanews.tv/index.php/en/rss",
    ]
    # Actual URL format: /en/<Category>/<6-digit-ID>-<slug>
    ARTICLE_PATTERN = re.compile(
        r"dunyanews\.tv/en/[A-Za-z]+/\d"
    )
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
            t = soup.find("time")
            if t:
                fields["pub_date"] = t.get("datetime") or t.get_text(strip=True)
            else:
                for pat in (r"news-date", r"publish-date", r"story-date", r"\bdate\b"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\bauthor\b", r"reporter", r"byline", r"news-reporter"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:
                        fields["author"] = text
                        break

        # ---- Body ----
        fields["body"] = self._extract_body(
            soup,
            [r"news-description", r"news-content", r"article-body",
             r"story-content", r"entry-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("dunyanews_scraper.log")
    scraper = DunyaNewsScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Dunya News scraper",
                                      "data/dunyanews/dunyanews_articles.jsonl").parse_args()))
