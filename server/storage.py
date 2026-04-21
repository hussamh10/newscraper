"""Append-only per-source JSONL writer with URL deduplication."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import aiofiles


class JsonlArticleStore:
    """
    Same semantics as New_Downloader/scrapers/base.JsonlStore:
    load existing URLs on init, skip duplicates on append.
    """

    def __init__(self, filepath: Path) -> None:
        self.filepath = filepath
        self._seen: set[str] = set()
        self._fh: Optional[object] = None
        self._lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        if self.filepath.exists() and self.filepath.stat().st_size > 0:
            with self.filepath.open("r", encoding="utf-8") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                        if url := obj.get("url"):
                            self._seen.add(url)
                    except json.JSONDecodeError:
                        continue
        self._fh = await aiofiles.open(self.filepath, "a", encoding="utf-8")
        self._initialized = True

    def is_saved(self, url: str) -> bool:
        return url in self._seen

    async def append(self, record: dict) -> bool:
        """Return True if written, False if duplicate URL skipped."""
        url = record.get("url", "")
        if not url:
            return False
        await self.initialize()
        assert self._fh is not None
        async with self._lock:
            if url in self._seen:
                return False
            await self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            await self._fh.flush()
            self._seen.add(url)
            return True

    async def close(self) -> None:
        if self._fh is not None:
            await self._fh.close()
            self._fh = None


class StorageRegistry:
    """One JsonlArticleStore per source id."""

    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self._stores: dict[str, JsonlArticleStore] = {}

    def path_for_source(self, source: str) -> Path:
        return self.base_dir / source / f"{source}_articles.jsonl"

    def get_store(self, source: str) -> JsonlArticleStore:
        if source not in self._stores:
            self._stores[source] = JsonlArticleStore(self.path_for_source(source))
        return self._stores[source]

    async def warm_all(self, sources: list[str]) -> None:
        for s in sources:
            st = self.get_store(s)
            await st.initialize()

    async def close_all(self) -> None:
        for st in self._stores.values():
            await st.close()
