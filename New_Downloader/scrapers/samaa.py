"""
samaa.py — Samaa TV scraper
============================
https://www.samaa.tv  (English / Urdu)

Major competitor to Geo and ARY with comprehensive coverage.
Article URL format: /news/<numeric-id>/<slug>

Run:
    python samaa.py
    python samaa.py --output samaa_articles.jsonl --concurrency 50
"""

import asyncio
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import BaseNewsScraper, configure_logging, make_arg_parser
from bs4 import BeautifulSoup


class SamaaScraper(BaseNewsScraper):
    OUTPUT_FILE     = "data/samaa/samaa_articles.jsonl"
    SOURCE_NAME     = "samaa"
    # /sitemap.xml has 94 child sitemaps — target article page directly.
    SITEMAP_URLS    = [
        "https://www.samaa.tv/sitemap-articles.xml?page=1",
    ]
    # Actual URL format: /{9-10-digit-ID}-{slug}
    ARTICLE_PATTERN = re.compile(r"samaa\.tv/\d{7,}")
    MAX_CONCURRENT  = 50
    TIMEOUT         = 25

    async def discover_urls(self) -> list[str]:
        return await self._discover_from_sitemaps()

    def parse_article(self, html: str, url: str) -> dict | None:
        soup   = BeautifulSoup(html, "lxml")
        fields = self._extract_common_fields(soup)

        # ---- Headline ---- (Samaa is React/Next.js; OG meta is the source of truth)
        if not fields["headline"]:
            for tag in (
                soup.find("meta", property="og:title"),
                soup.find("meta", attrs={"name": "twitter:title"}),
            ):
                if tag and tag.get("content"):
                    fields["headline"] = tag["content"].strip()
                    break
        if not fields["headline"]:
            h1 = soup.find("h1")
            fields["headline"] = h1.get_text(strip=True) if h1 else None

        # ---- Date ---- (not server-rendered; extract from URL: /<9-digit-ID>-)
        if not fields["pub_date"]:
            for k, v in (
                ("property", "article:published_time"),
                ("name",     "publish-date"),
                ("itemprop", "datePublished"),
            ):
                tag = soup.find("meta", attrs={k: v})
                if tag and tag.get("content"):
                    fields["pub_date"] = tag["content"].strip()
                    break

        # ---- Author ---- (usually "SAMAA TV" but include for completeness)
        if not fields["author"]:
            tag = soup.find("meta", attrs={"name": "author"})
            if tag and tag.get("content") and tag["content"].strip() != "SAMAA TV":
                fields["author"] = tag["content"].strip()

        # ---- Body ---- (JS-rendered; use OG description as summary fallback)
        body = self._extract_body(
            soup,
            [r"single-content", r"article-body", r"article-content",
             r"story-content", r"entry-content", r"post-content"],
        )
        if not body:
            desc_tag = soup.find("meta", property="og:description")
            if desc_tag and desc_tag.get("content"):
                body = desc_tag["content"].strip()
        fields["body"] = body

        return fields


async def _main(args) -> None:
    configure_logging("samaa_scraper.log")
    scraper = SamaaScraper(output_file=args.output, max_concurrent=args.concurrency)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(_main(make_arg_parser("Samaa TV scraper",
                                      "data/samaa/samaa_articles.jsonl").parse_args()))
