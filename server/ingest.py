"""Stream URL .txt files into SQLite (INSERT OR IGNORE)."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from server.db import Database
from server.sources import SOURCES

log = logging.getLogger(__name__)

CHUNK = 10_000


async def ingest_source(db: Database, source: str, urls_path: Path) -> int:
    """Return number of rows attempted (including duplicates ignored by DB)."""
    if not urls_path.is_file():
        log.warning("Missing URL file for %s: %s", source, urls_path)
        return 0

    inserted = 0
    batch: list[tuple[str, str, str]] = []

    with urls_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            batch.append((source, url, "pending"))
            if len(batch) >= CHUNK:
                await db.executemany(
                    "INSERT OR IGNORE INTO urls(source, url, status) VALUES (?,?,?)",
                    batch,
                )
                inserted += len(batch)
                batch.clear()

    if batch:
        await db.executemany(
            "INSERT OR IGNORE INTO urls(source, url, status) VALUES (?,?,?)",
            batch,
        )
        inserted += len(batch)

    log.info("Ingest %s: flushed %d URL rows from %s", source, inserted, urls_path)
    return inserted


async def ingest_all(db: Database) -> dict[str, int]:
    counts: dict[str, int] = {}
    for source, meta in SOURCES.items():
        p = Path(meta["urls_file"])
        counts[source] = await ingest_source(db, source, p)
        if p.is_file():
            st = p.stat()
            ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
            now = int(time.time())
            await db.execute(
                "INSERT INTO ingest_meta(source, mtime_ns, file_size, last_ingest) "
                "VALUES (?,?,?,?) ON CONFLICT(source) DO UPDATE SET "
                "mtime_ns=excluded.mtime_ns, file_size=excluded.file_size, "
                "last_ingest=excluded.last_ingest",
                (source, ns, int(st.st_size), now),
            )
    return counts


def _file_fingerprint(path: Path) -> tuple[int, int]:
    st = path.stat()
    ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
    return ns, int(st.st_size)


async def ingest_if_changed(db: Database) -> dict[str, str]:
    """
    Re-read a source's URL file only when its mtime/size changed vs ingest_meta.
    Avoids re-scanning multi-million-line .txt files on every server restart.
    """
    summary: dict[str, str] = {}
    for source, meta in SOURCES.items():
        p = Path(meta["urls_file"])
        if not p.is_file():
            summary[source] = "missing_file"
            continue
        ns, sz = _file_fingerprint(p)
        row = await db.fetchone(
            "SELECT mtime_ns, file_size FROM ingest_meta WHERE source=?",
            (source,),
        )
        if row and int(row["mtime_ns"]) == ns and int(row["file_size"]) == sz:
            summary[source] = "unchanged"
            continue
        n = await ingest_source(db, source, p)
        now = int(time.time())
        await db.execute(
            "INSERT INTO ingest_meta(source, mtime_ns, file_size, last_ingest) "
            "VALUES (?,?,?,?) ON CONFLICT(source) DO UPDATE SET "
            "mtime_ns=excluded.mtime_ns, file_size=excluded.file_size, "
            "last_ingest=excluded.last_ingest",
            (source, ns, sz, now),
        )
        summary[source] = f"ingested_lines={n}"
    return summary


async def _main() -> None:
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).resolve().parent
    db_path = root / "state.sqlite"
    db = Database(db_path)
    await db.connect()
    try:
        out = await ingest_all(db)
        print(out)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_main())
