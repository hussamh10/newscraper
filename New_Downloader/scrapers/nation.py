"""
nation.py — The Nation scraper
================================
https://nation.com.pk  (English — Lahore-based)

Article URL format: /YYYY-MM-DD/<slug>/  (date-based slugs)

Run:
    python nation.py
    python nation.py --output nation_articles.jsonl --concurrency 40
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class NationScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/nation/nation_articles.jsonl"
    SOURCE_NAME     = "nation"
    # sitemap_news.xml is a direct urlset of recent articles — avoids the
    # 600-deep index at /sitemap.xml which times out during discovery.
    SITEMAP_URLS    = [
        "https://www.nation.com.pk/sitemap_news.xml",
        "https://nation.com.pk/sitemap_news.xml",
    ]
    # Actual URL format: /DD-Mon-YYYY/<slug>  e.g. /14-Mar-2026/article-slug
    ARTICLE_PATTERN = re.compile(
        r"nation\.com\.pk/\d{2}-[A-Z][a-z]+-\d{4}/"
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
            h1 = soup.find("h1")
            fields["headline"] = h1.get_text(strip=True) if h1 else None

        # ---- Date ----
        if not fields["pub_date"]:
            t = soup.find("time")
            if t:
                fields["pub_date"] = t.get("datetime") or t.get_text(strip=True)
            else:
                for pat in (r"entry-date", r"post-date", r"publish", r"\bdate\b"):
                    tag = soup.find(class_=re.compile(pat, re.I))
                    if tag:
                        fields["pub_date"] = tag.get_text(strip=True)
                        break

        # ---- Author ----
        if not fields["author"]:
            for pat in (r"\bauthor\b", r"byline", r"reporter", r"entry-author"):
                tag = soup.find(class_=re.compile(pat, re.I))
                if tag:
                    text = tag.get_text(strip=True)
                    if text and len(text) < 120:
                        fields["author"] = text
                        break

        # ---- Body ----
        # The Nation runs on WordPress; standard entry-content class
        fields["body"] = self._extract_body(
            soup,
            [r"entry-content", r"post-content", r"article-content",
             r"story-content", r"article-body"],
        )

        return fields


async def _main(args) -> None:
    configure_logging("nation_scraper.log")
    scraper = NationScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("The Nation scraper",
                                      "data/nation/nation_articles.jsonl").parse_args()))
