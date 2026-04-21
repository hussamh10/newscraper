"""
bbcurdu.py — BBC Urdu scraper
==============================
https://www.bbc.com/urdu  (Urdu — UTF-8)

BBC uses CPS (Content Publishing System) markup:
  - Body in <div data-component="text-block"> containers
  - Byline in <div data-component="byline-block">
  - Clean <time datetime="ISO-8601"> elements

Run:
    python bbcurdu.py
    python bbcurdu.py --output bbcurdu_articles.jsonl --concurrency 30
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class BbcUrduScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/bbcurdu/bbcurdu_articles.jsonl"
    SOURCE_NAME     = "bbc_urdu"
    SITEMAP_URLS    = [
        "https://www.bbc.com/urdu/sitemap.xml",
        "https://feeds.bbci.co.uk/urdu/rss.xml",
    ]
    # Match /urdu/<topic>-<id> and /urdu/articles/<id> but NOT /urdu/topics/ etc.
    ARTICLE_PATTERN = re.compile(
        r"bbc\.com/urdu/(?!topics|popular|media|tv-programmes|radio)"
        r"(?:[a-z-]+-\d+|articles/[a-z0-9]+)"
    )
    MAX_CONCURRENT  = 40   # BBC enforces rate limits; stay conservative
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

        # ---- Author ----
        if not fields["author"]:
            byline = soup.find(attrs={"data-component": "byline-block"})
            if byline:
                fields["author"] = byline.get_text(strip=True)
            else:
                for pat in (r"\bbyline\b", r"\bauthor\b", r"contributor"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["author"] = tag.get_text(strip=True)
                        break

        # ---- Body ----
        # BBC CPS wraps each paragraph in data-component="text-block"
        text_blocks = soup.find_all(attrs={"data-component": "text-block"})
        if text_blocks:
            paras = [
                p.get_text(strip=True)
                for block in text_blocks
                for p in block.find_all("p")
                if p.get_text(strip=True)
            ]
            fields["body"] = "\n\n".join(paras) if paras else None
        else:
            fields["body"] = self._extract_body(soup, [r"\barticle\b"])

        return fields


async def _main(args) -> None:
    configure_logging("bbcurdu_scraper.log")
    scraper = BbcUrduScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("BBC Urdu scraper",
                                      "data/bbcurdu/bbcurdu_articles.jsonl").parse_args()))
