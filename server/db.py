"""SQLite connection, schema, and helpers."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Optional

import aiosqlite

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS urls (
  id            INTEGER PRIMARY KEY,
  source        TEXT NOT NULL,
  url           TEXT NOT NULL UNIQUE,
  status        TEXT NOT NULL CHECK(status IN ('pending','leased','done','failed')),
  lease_token   TEXT,
  lease_expires INTEGER,
  attempts      INTEGER NOT NULL DEFAULT 0,
  assigned_to   TEXT,
  done_at       INTEGER,
  error         TEXT
);

CREATE INDEX IF NOT EXISTS idx_urls_claim
  ON urls(source, status, lease_expires);

CREATE INDEX IF NOT EXISTS idx_urls_done_at
  ON urls(done_at);

CREATE TABLE IF NOT EXISTS batches (
  batch_id      TEXT PRIMARY KEY,
  source        TEXT NOT NULL,
  size          INTEGER NOT NULL,
  created_at    INTEGER NOT NULL,
  expires_at    INTEGER NOT NULL,
  client_ip     TEXT,
  state         TEXT NOT NULL CHECK(state IN ('open','submitted','expired'))
);

CREATE TABLE IF NOT EXISTS batch_items (
  batch_id TEXT NOT NULL,
  url_id   INTEGER NOT NULL,
  url      TEXT NOT NULL,
  PRIMARY KEY (batch_id, url_id),
  FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_batch_items_batch ON batch_items(batch_id);

CREATE TABLE IF NOT EXISTS ingest_meta (
  source      TEXT PRIMARY KEY,
  mtime_ns    INTEGER NOT NULL,
  file_size   INTEGER NOT NULL,
  last_ingest INTEGER NOT NULL
);
"""


def _split_schema_statements(sql: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        buf.append(line)
        if s.endswith(";"):
            parts.append("\n".join(buf))
            buf = []
    if buf:
        parts.append("\n".join(buf))
    return parts


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys=ON")
        for stmt in _split_schema_statements(SCHEMA):
            await self._conn.execute(stmt)
        await self._conn.commit()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """Single-writer IMMEDIATE transaction (claim / submit / expire)."""
        async with self._lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> aiosqlite.Cursor:
        async with self._lock:
            cur = await self.conn.execute(sql, params)
            await self.conn.commit()
            return cur

    async def executemany(self, sql: str, seq: list[tuple[Any, ...]]) -> None:
        async with self._lock:
            await self.conn.executemany(sql, seq)
            await self.conn.commit()

    async def fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
        async with self._lock:
            cur = await self.conn.execute(sql, params)
            rows = await cur.fetchall()
            return list(rows)

    async def fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
        async with self._lock:
            cur = await self.conn.execute(sql, params)
            return await cur.fetchone()
