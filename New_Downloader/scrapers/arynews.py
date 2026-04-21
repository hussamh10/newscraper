"""
arynews.py — ARY News scraper
==============================
https://arynews.tv  (English / Urdu)

One of Pakistan's largest TV networks with a massive digital presence.
Article URL format: /YYYYMMDD/<slug>/

Run:
    python arynews.py
    python arynews.py --output arynews_articles.jsonl --concurrency 50
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class AryNewsScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/arynews/arynews_articles.jsonl"
    SOURCE_NAME     = "arynews"
    # sitemap.xml returns 404; /sitemap_index.xml also 404. RSS feed works.
    SITEMAP_URLS    = [
        "https://arynews.tv/feed/",
    ]
    # Actual URL format: /slug/ (no date or numeric ID)
    ARTICLE_PATTERN = re.compile(
        r"arynews\.tv/[a-z0-9][a-z0-9-]+"
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
            # ARY uses h1 with class "the-title" on some layouts
            for selector in ({"class": re.compile(r"the-title|entry-title", re.I)}, None):
                if selector:
                    tag = soup.find("h1", selector)
                else:
                    tag = soup.find("h1")
                if tag:
                    fields["headline"] = tag.get_text(strip=True)
                    break

        # ---- Date ----
        if not fields["pub_date"]:
            t = soup.find("time")
            if t:
                fields["pub_date"] = t.get("datetime") or t.get_text(strip=True)
            else:
                for pat in (r"entry-date", r"post-date", r"updated", r"\bdate\b"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\bauthor\b", r"byline", r"entry-author", r"reporter"):
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
             r"news-detail", r"story-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("arynews_scraper.log")
    scraper = AryNewsScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("ARY News scraper",
                                      "data/arynews/arynews_articles.jsonl").parse_args()))
