"""
expressnews.py — Express News scraper
=======================================
https://www.express.pk  (Urdu)

Urdu counterpart to The Express Tribune.
One of the most visited Urdu news sites in Pakistan.
Article URL format: /story/<numeric-id>/

Urdu note: content is UTF-8; no special handling needed beyond what
the base class already does.

Run:
    python expressnews.py
    python expressnews.py --output expressnews_articles.jsonl --concurrency 50
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class ExpressNewsScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/expressnews/expressnews_articles.jsonl"
    SOURCE_NAME     = "express_news"
    # /sitemap.xml has 139 child sitemaps — use RSS feed directly.
    SITEMAP_URLS    = [
        "https://www.express.pk/feed/",
    ]
    # /story/<numeric-id>/ is the article path; exclude /tag/ /category/
    ARTICLE_PATTERN = re.compile(r"express\.pk/story/\d")
    MAX_CONCURRENT  = 50
    TIMEOUT         = 25

    async def discover_urls(self) -> list[str]:
        return await self._discover_from_sitemaps()

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # ---- Headline ----
        if not fields["headline"]:
            for pat in (r"story-title", r"news-title", r"article-title"):
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
                for pat in (r"story-date", r"news-date", r"\bdate\b", r"publish"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\breporter\b", r"\bauthor\b", r"byline", r"news-reporter"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:
                        fields["author"] = text
                        break

        # ---- Body ----
        # express.pk uses mainstorycontent-parent or storypage-main-section2
        fields["body"] = self._extract_body(
            soup,
            [r"mainstorycontent", r"storypage-main-section2",
             r"story-content", r"story-detail", r"news-content",
             r"article-body", r"entry-content"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("expressnews_scraper.log")
    scraper = ExpressNewsScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Express News (Urdu) scraper",
                                      "data/expressnews/expressnews_articles.jsonl").parse_args()))
