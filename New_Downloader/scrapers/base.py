"""
base.py
=======
Shared async infrastructure imported by every standalone scraper.

Provides:
  JsonlStore        — append-only JSONL writer with URL-level resumability
  BaseNewsScraper   — abstract class with HTTP retry, sitemap parsing,
                      JSON-LD extraction, and the main run() pipeline

Each site-specific file imports:
    from base import BaseNewsScraper
and sets class-level config attributes instead of passing constructor args,
so each file is fully self-contained and runnable with `python geo.py`.
"""

import asyncio
import json
import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import aiofiles          # pip install aiofiles
import aiohttp           # pip install aiohttp
from bs4 import BeautifulSoup   # pip install beautifulsoup4 lxml
from tqdm import tqdm           # pip install tqdm

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# JSONL persistence layer
# ─────────────────────────────────────────────────────────────────────────────

class JsonlStore:
    """
    Append-only JSONL file with URL-level deduplication for crash-safe
    resumability.

    Lifecycle:
        async with JsonlStore("geo_articles.jsonl") as store:
            store.is_scraped(url)    → bool
            await store.save(record) → None

    On __aenter__:
        Reads every existing line in the file, extracts the "url" field,
        and loads it into self._seen (a Python set).  This O(n) startup
        scan is what makes resume-without-duplication possible.

    On save():
        Checks self._seen before writing.  An asyncio.Lock serialises
        concurrent coroutine writes so JSON lines are never interleaved.
        ensure_ascii=False preserves Urdu / Arabic Unicode characters.
    """

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self._seen: set[str] = set()
        self._fh = None          # aiofiles file handle, set in initialize()
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        path = Path(self.filepath)
        # Create data/newsname/ directory tree automatically.
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.stat().st_size > 0:
            with path.open("r", encoding="utf-8") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                        if url := obj.get("url"):
                            self._seen.add(url)
                    except json.JSONDecodeError:
                        # Corrupted tail line from a prior crash — skip it.
                        continue
        log.info(
            "JSONL store: %d URLs already in %s", len(self._seen), self.filepath
        )
        self._fh = await aiofiles.open(self.filepath, "a", encoding="utf-8")

    def is_scraped(self, url: str) -> bool:
        return url in self._seen

    @property
    def scraped_count(self) -> int:
        return len(self._seen)

    async def save(self, record: dict) -> None:
        """Persist one record.  Silently ignores duplicate URLs."""
        url = record.get("url", "")
        async with self._lock:
            if url in self._seen:
                return
            await self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            await self._fh.flush()
            self._seen.add(url)

    async def close(self) -> None:
        if self._fh:
            await self._fh.close()
            self._fh = None

    async def __aenter__(self) -> "JsonlStore":
        await self.initialize()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


# ─────────────────────────────────────────────────────────────────────────────
# Abstract base scraper
# ─────────────────────────────────────────────────────────────────────────────

class BaseNewsScraper(ABC):
    """
    Inherit and implement two methods:

        async def discover_urls(self) -> list[str]:
            # return all article URLs — use self._discover_from_sitemaps()
            # or hand-roll for integer-ID sites like Dawn.

        def parse_article(self, html: str, url: str) -> dict | None:
            # return {"headline", "pub_date", "author", "body"}
            # or None to skip the article.

    Set these class attributes in the subclass:

        OUTPUT_FILE     str           path of the output JSONL file
        SOURCE_NAME     str           short id used in logs + records
        SITEMAP_URLS    list[str]     sitemap / RSS entry-point URLs
        ARTICLE_PATTERN re.Pattern    keeps article URLs, drops category/tag pages
        MAX_CONCURRENT  int = 60      semaphore cap
        TIMEOUT         int = 25      seconds per request
        MAX_RETRIES     int = 5       attempts before giving up on a URL
        BASE_BACKOFF    float = 2.0   initial retry wait; doubles each attempt
        EXTRA_HEADERS   dict = {}     any additional HTTP headers

    CLI override (used by each file's if __name__ == "__main__ block):
        python geo.py --output custom.jsonl --concurrency 30
    """

    OUTPUT_FILE:     str        = "output.jsonl"
    SOURCE_NAME:     str        = "unknown"
    SITEMAP_URLS:    list[str]  = []
    ARTICLE_PATTERN: re.Pattern = re.compile(r".")   # match-all default
    MAX_CONCURRENT:  int        = 60
    TIMEOUT:         int        = 25
    MAX_RETRIES:        int        = 5
    BASE_BACKOFF:       float      = 2.0
    MAX_CHILD_SITEMAPS: Optional[int] = None  # cap child-sitemap recursion depth
    USER_AGENT:         str        = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    EXTRA_HEADERS: dict = {}

    def __init__(
        self,
        output_file: str | None = None,
        max_concurrent: int | None = None,
    ) -> None:
        if output_file:
            self.OUTPUT_FILE = output_file
        if max_concurrent:
            self.MAX_CONCURRENT = max_concurrent
        self._sem = asyncio.Semaphore(self.MAX_CONCURRENT)
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Abstract interface ──────────────────────────────────────────────────

    @abstractmethod
    async def discover_urls(self) -> list[str]:
        """Build and return the complete list of article URLs to scrape."""

    @abstractmethod
    def parse_article(self, html: str, url: str) -> dict | None:
        """
        Parse raw HTML and return a dict with keys:
            headline  str | None
            pub_date  str | None   (ISO 8601 preferred)
            author    str | None
            body      str | None   (paragraphs joined with \\n\\n)
        Return None to skip the article entirely.
        """

    # ── Sitemap helpers ─────────────────────────────────────────────────────

    async def _discover_from_sitemaps(self) -> list[str]:
        """Fetch every URL in SITEMAP_URLS, recurse through indexes,
        and return a deduplicated list of article URLs."""
        seen: dict[str, None] = {}
        for entry in self.SITEMAP_URLS:
            for u in await self._fetch_sitemap(entry):
                seen[u] = None
        return list(seen)

    async def _fetch_sitemap(self, url: str, _depth: int = 0) -> list[str]:
        """
        Recursively parse a sitemap, sitemap-index, or RSS feed and return
        all URLs that match ARTICLE_PATTERN.

        Auto-detects:
          <sitemapindex>  — recurse into child sitemaps
          <urlset>        — regular sitemap with <url><loc> entries
          RSS <channel>   — <item><link> entries
        """
        if _depth > 4:
            log.warning("Sitemap recursion depth exceeded at %s", url)
            return []

        log.info("[%s] Fetching sitemap: %s", self.SOURCE_NAME, url)
        xml = await self._raw_fetch(url, is_xml=True)
        if not xml:
            return []

        soup = BeautifulSoup(xml, "lxml-xml")

        # ---- Sitemap index ----
        if soup.find_all("sitemap"):
            results: list[str] = []
            child_tags = soup.find_all("sitemap")
            if self.MAX_CHILD_SITEMAPS is not None:
                child_tags = child_tags[:self.MAX_CHILD_SITEMAPS]
            for tag in child_tags:
                loc = tag.find("loc")
                if loc and loc.text:
                    results.extend(
                        await self._fetch_sitemap(loc.text.strip(), _depth + 1)
                    )
            log.info("[%s] Sitemap index → %d article URLs", self.SOURCE_NAME, len(results))
            return results

        # ---- RSS feed ----
        if soup.find("channel"):
            results = []
            for item in soup.find_all("item"):
                href = self._rss_item_link(item)
                if href and self.ARTICLE_PATTERN.search(href):
                    results.append(href)
            return results

        # ---- Regular sitemap urlset ----
        results = []
        for tag in soup.find_all("url"):
            loc = tag.find("loc")
            if loc and loc.text:
                href = loc.text.strip()
                if self.ARTICLE_PATTERN.search(href):
                    results.append(href)
        log.info("[%s] Sitemap %s → %d article URLs", self.SOURCE_NAME, url, len(results))
        return results

    @staticmethod
    def _rss_item_link(item) -> Optional[str]:
        """Extract the canonical link from an RSS <item>."""
        link = item.find("link")
        if link:
            text = (link.string or "").strip()
            if text:
                return text
            href = link.get("href")
            if href:
                return href.strip()
        return None

    # ── Common metadata extraction ──────────────────────────────────────────

    def _extract_common_fields(self, soup: BeautifulSoup) -> dict:
        """
        Extract headline, pub_date, author from structured metadata.

        Priority chain:
          1. JSON-LD application/ld+json (most authoritative — all major
             Pakistani news sites embed this)
          2. OpenGraph <meta property="og:title">
          3. Standard <meta name="author"> / article:published_time

        Returns a dict with keys headline / pub_date / author, any may be None.
        Subclasses call this first, then patch gaps with CSS selectors.
        """
        headline = pub_date = author = None

        # 1. JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                # Flatten @graph arrays
                if isinstance(data, dict) and "@graph" in data:
                    data = next(
                        (x for x in data["@graph"]
                         if x.get("@type") in ("NewsArticle", "Article")),
                        data["@graph"][0] if data["@graph"] else {},
                    )
                if isinstance(data, list):
                    data = next(
                        (x for x in data
                         if x.get("@type") in ("NewsArticle", "Article")),
                        data[0] if data else {},
                    )
                if isinstance(data, dict):
                    # Only extract from article-type nodes, not Organisation/Website nodes
                    dtype = str(data.get("@type", ""))
                    is_article = any(t in dtype for t in
                                     ("Article", "NewsItem", "ReportageNewsArticle"))
                    if is_article:
                        headline = headline or data.get("headline") or data.get("name")
                        pub_date = pub_date or data.get("datePublished") or data.get("dateCreated")
                    auth = data.get("author") if is_article else None
                    if auth and not author:
                        if isinstance(auth, dict):
                            author = auth.get("name")
                        elif isinstance(auth, list) and auth:
                            first = auth[0]
                            author = (
                                first.get("name") if isinstance(first, dict) else str(first)
                            )
                        elif isinstance(auth, str):
                            author = auth
                if headline and pub_date:
                    break
            except (json.JSONDecodeError, AttributeError, TypeError, KeyError):
                continue

        # 2. OpenGraph / meta fallbacks
        if not headline:
            tag = soup.find("meta", property="og:title")
            headline = tag.get("content", "").strip() or None if tag else None

        if not pub_date:
            for k, v in (
                ("property", "article:published_time"),
                ("name",     "publish-date"),
                ("name",     "date"),
                ("itemprop", "datePublished"),
            ):
                tag = soup.find("meta", attrs={k: v})
                if tag and tag.get("content"):
                    pub_date = tag["content"].strip()
                    break

        if not author:
            for k, v in (
                ("name",     "author"),
                ("property", "article:author"),
            ):
                tag = soup.find("meta", attrs={k: v})
                if tag and tag.get("content"):
                    author = tag["content"].strip()
                    break

        return {
            "headline": headline or None,
            "pub_date": pub_date or None,
            "author":   author or None,
        }

    def _extract_body(
        self,
        soup: BeautifulSoup,
        container_patterns: list[str],
    ) -> Optional[str]:
        """
        Generic body-text extractor.

        Tries each CSS class pattern in `container_patterns` to find the
        article body div, then concatenates its <p> tags.  Falls back to
        <article> and then to the page's <main> element.

        Args:
            container_patterns: list of regex strings tried in order, e.g.
                ["story-detail", r"article[_-]body", "entry-content"]
        """
        container = None
        for pat in container_patterns:
            container = soup.find(class_=re.compile(pat, re.I))
            if container:
                break

        if not container:
            container = soup.find("article")
        if not container:
            container = soup.find("main")

        if not container:
            return None

        paras = [
            p.get_text(strip=True)
            for p in container.find_all("p")
            if p.get_text(strip=True)
        ]
        if paras:
            return "\n\n".join(paras)

        # Some sites use <div> children instead of <p>; fall back to bulk text.
        text = container.get_text(separator="\n", strip=True)
        return text if text else None

    # ── HTTP fetch with retry / backoff ─────────────────────────────────────

    async def _raw_fetch(self, url: str, *, is_xml: bool = False) -> Optional[str]:
        """
        Perform one HTTP GET behind the shared semaphore.

        Behaviour by status:
          404          → return None immediately (no retry)
          429 / 5xx    → exponential backoff, up to MAX_RETRIES
          other non-200→ return None (permanent skip)
          200          → decode bytes

        Encoding:
          XML sitemaps → always UTF-8 (XML spec default).
          HTML         → charset from HTTP Content-Type header, fallback UTF-8.
                         errors="replace" so Urdu / Arabic never raises.
        """
        async with self._sem:
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    async with self._session.get(url, allow_redirects=True) as resp:
                        if resp.status == 404:
                            log.debug("404  %s — skipped", url)
                            return None

                        if resp.status not in (200, 429) and resp.status < 500:
                            log.debug("HTTP %d  %s — skipped", resp.status, url)
                            return None

                        if resp.status == 429 or resp.status >= 500:
                            wait = self.BASE_BACKOFF * (2 ** (attempt - 1))
                            log.warning(
                                "HTTP %d  %s  (attempt %d/%d) — retry in %.1fs",
                                resp.status, url, attempt, self.MAX_RETRIES, wait,
                            )
                            await asyncio.sleep(wait)
                            continue

                        raw = await resp.read()

                        if is_xml:
                            return raw.decode("utf-8", errors="replace")

                        charset = resp.charset or "utf-8"
                        try:
                            return raw.decode(charset, errors="replace")
                        except (LookupError, UnicodeDecodeError):
                            return raw.decode("utf-8", errors="replace")

                except asyncio.TimeoutError:
                    wait = self.BASE_BACKOFF * (2 ** (attempt - 1))
                    log.warning(
                        "Timeout  %s  (attempt %d/%d) — retry in %.1fs",
                        url, attempt, self.MAX_RETRIES, wait,
                    )
                    await asyncio.sleep(wait)

                except aiohttp.ClientConnectionError as exc:
                    wait = self.BASE_BACKOFF * (2 ** (attempt - 1))
                    log.warning(
                        "ConnectionError  %s: %s  (attempt %d/%d) — retry in %.1fs",
                        url, exc, attempt, self.MAX_RETRIES, wait,
                    )
                    await asyncio.sleep(wait)

                except aiohttp.ClientError as exc:
                    log.error("Unrecoverable ClientError  %s: %s — skipped", url, exc)
                    return None

            log.error("Gave up  %s  after %d attempts", url, self.MAX_RETRIES)
            return None

    # ── Per-URL processing ──────────────────────────────────────────────────

    async def _process_url(
        self,
        url: str,
        store: JsonlStore,
        pbar: tqdm,
    ) -> None:
        """Fetch one URL, parse it, save it, tick the progress bar."""
        html = await self._raw_fetch(url)
        pbar.update(1)
        if html is None:
            return

        try:
            fields = self.parse_article(html, url)
        except Exception as exc:
            log.error("Parse error  %s: %s", url, exc, exc_info=True)
            return

        if fields:
            await store.save({
                "source":   self.SOURCE_NAME,
                "url":      url,
                "html":     html,
                "headline": fields.get("headline"),
                "pub_date": fields.get("pub_date"),
                "author":   fields.get("author"),
                "body":     fields.get("body"),
            })

    # ── Main pipeline ───────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Full pipeline:
          1. Open JSONL store (loads already-scraped URLs for resumability).
          2. Create shared aiohttp session.
          3. Call discover_urls() — uses the session for sitemap fetches.
          4. Filter to unseen URLs only.
          5. Fetch + parse + save concurrently, capped by semaphore.
        """
        timeout   = aiohttp.ClientTimeout(total=self.TIMEOUT)
        connector = aiohttp.TCPConnector(
            limit=self.MAX_CONCURRENT + 20,
            ssl=False,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        headers = {
            "User-Agent":      self.USER_AGENT,
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.7,ur;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            **self.EXTRA_HEADERS,
        }

        async with JsonlStore(self.OUTPUT_FILE) as store:
            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout, headers=headers,
            ) as session:
                self._session = session

                log.info("=== [%s] Discovering URLs ===", self.SOURCE_NAME)
                all_urls = await self.discover_urls()
                log.info("[%s] Discovered %d URLs", self.SOURCE_NAME, len(all_urls))

                to_fetch = [u for u in all_urls if not store.is_scraped(u)]
                log.info(
                    "[%s] %d total | %d already scraped | %d to fetch",
                    self.SOURCE_NAME, len(all_urls), store.scraped_count, len(to_fetch),
                )

                if not to_fetch:
                    log.info("[%s] Nothing new to fetch.", self.SOURCE_NAME)
                    return

                # Process in chunks of 500 to bound peak coroutine-object count.
                # The semaphore limits actual I/O concurrency independently.
                CHUNK = 500
                with tqdm(
                    total=len(to_fetch),
                    desc=self.SOURCE_NAME,
                    unit="art",
                    dynamic_ncols=True,
                ) as pbar:
                    for i in range(0, len(to_fetch), CHUNK):
                        batch = to_fetch[i : i + CHUNK]
                        await asyncio.gather(
                            *[self._process_url(u, store, pbar) for u in batch],
                            return_exceptions=True,
                        )

        self._session = None
        log.info("=== [%s] Done ===", self.SOURCE_NAME)


# ─────────────────────────────────────────────────────────────────────────────
# Shared CLI helper
# ─────────────────────────────────────────────────────────────────────────────

def configure_logging(log_file: str) -> None:
    """Call this from each scraper's __main__ block."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8", mode="a"),
        ],
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def make_arg_parser(description: str, default_output: str):
    """Return a pre-built ArgumentParser for standalone scraper scripts."""
    import argparse
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "--output", default=default_output,
        help=f"JSONL output file (default: {default_output})",
    )
    p.add_argument(
        "--concurrency", type=int, default=None,
        help="Override max concurrent requests",
    )
    return p
