"""Registry: source id -> URL list file + scraper class (for /sources API)."""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRAPERS = _REPO_ROOT / "New_Downloader" / "scrapers"

# Parser: module name under scrapers/ (no package) + class name
SOURCES: dict[str, dict[str, str]] = {
    "express": {
        "urls_file": str(_REPO_ROOT / "New_Downloader" / "express_urls.txt"),
        "parser": "expressnews:ExpressNewsScraper",
    },
    "geo": {
        "urls_file": str(_REPO_ROOT / "New_Downloader" / "geo_urls.txt"),
        "parser": "geo:GeoScraper",
    },
    "jang": {
        "urls_file": str(_REPO_ROOT / "New_Downloader" / "jang_urls.txt"),
        "parser": "jang:JangScraper",
    },
    "thenews": {
        "urls_file": str(_REPO_ROOT / "New_Downloader" / "thenews_urls.txt"),
        "parser": "thenews:TheNewsScraper",
    },
    "tribune": {
        "urls_file": str(_REPO_ROOT / "New_Downloader" / "tribune_urls.txt"),
        "parser": "tribune:TribuneScraper",
    },
}


def list_sources() -> list[str]:
    return list(SOURCES.keys())


def scrapers_dir() -> Path:
    return _SCRAPERS
