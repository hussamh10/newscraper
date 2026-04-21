"""FastAPI coordinator: claim batches, accept uploads, status dashboard."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from server.db import Database
from server.ingest import ingest_all, ingest_if_changed
from server.sources import SOURCES, list_sources
from server.storage import StorageRegistry

log = logging.getLogger(__name__)

SERVER_DIR = Path(__file__).resolve().parent
DB_PATH = SERVER_DIR / "state.sqlite"
DATA_DIR = SERVER_DIR / "data"
LEASE_SECONDS = 30 * 60
HEARTBEAT_EXTEND = LEASE_SECONDS

templates = Jinja2Templates(directory=str(SERVER_DIR / "templates"))

db: Database
storage: StorageRegistry
_bg_task: Optional[asyncio.Task] = None


class ClaimBody(BaseModel):
    source: Optional[str] = None
    size: int = Field(default=50, ge=1, le=200)
    client_id: Optional[str] = None


class ArticleResult(BaseModel):
    url: str
    ok: bool
    record: Optional[dict[str, Any]] = None
    error: Optional[str] = None


class SubmitBody(BaseModel):
    results: list[ArticleResult]


async def _expire_job_loop() -> None:
    while True:
        try:
            await asyncio.sleep(60)
            now = int(time.time())
            async with db.transaction() as conn:
                await conn.execute(
                    "UPDATE urls SET status='pending', lease_token=NULL, lease_expires=NULL "
                    "WHERE status='leased' AND lease_expires IS NOT NULL AND lease_expires < ?",
                    (now,),
                )
                await conn.execute(
                    "UPDATE batches SET state='expired' WHERE state='open' AND expires_at < ?",
                    (now,),
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("expire job failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, storage, _bg_task
    logging.basicConfig(level=logging.INFO)
    db = Database(DB_PATH)
    await db.connect()
    storage = StorageRegistry(DATA_DIR)
    ingest_summary = await ingest_if_changed(db)
    log.info("Ingest: %s", ingest_summary)
    await storage.warm_all(list_sources())
    _bg_task = asyncio.create_task(_expire_job_loop())
    log.info("Server ready; DB=%s", DB_PATH)
    yield
    if _bg_task:
        _bg_task.cancel()
        try:
            await _bg_task
        except asyncio.CancelledError:
            pass
    await storage.close_all()
    await db.close()


app = FastAPI(title="News scraper coordinator", lifespan=lifespan)


@app.get("/sources")
async def get_sources() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, meta in SOURCES.items():
        out[name] = {"parser": meta["parser"]}
    return {"sources": out}


@app.post("/admin/reingest")
async def admin_reingest(force: bool = Query(False, description="If true, re-read all URL files fully")) -> dict[str, Any]:
    if force:
        counts = await ingest_all(db)
        return {"mode": "force", "ingested_rows": counts}
    summary = await ingest_if_changed(db)
    return {"mode": "if_changed", "summary": summary}


@app.get("/status")
async def get_status() -> dict[str, Any]:
    now = int(time.time())
    hour_ago = now - 3600

    rows = await db.fetchall(
        "SELECT source, status, COUNT(*) AS c FROM urls GROUP BY source, status",
        (),
    )
    per: dict[str, dict[str, int]] = {}
    for r in rows:
        src = r["source"]
        st = r["status"]
        per.setdefault(src, {})[st] = r["c"]

    global_counts = {"pending": 0, "leased": 0, "done": 0, "failed": 0}
    sources_out: dict[str, Any] = {}
    for src in list_sources():
        counts = per.get(src, {})
        pending = counts.get("pending", 0)
        leased = counts.get("leased", 0)
        done = counts.get("done", 0)
        failed = counts.get("failed", 0)
        total = pending + leased + done + failed
        global_counts["pending"] += pending
        global_counts["leased"] += leased
        global_counts["done"] += done
        global_counts["failed"] += failed
        pct = round(100.0 * done / total, 2) if total else 0.0
        sources_out[src] = {
            "total": total,
            "pending": pending,
            "leased": leased,
            "done": done,
            "failed": failed,
            "pct_done": pct,
        }

    done_last_hour_row = await db.fetchone(
        "SELECT COUNT(*) AS c FROM urls WHERE done_at IS NOT NULL AND done_at >= ?",
        (hour_ago,),
    )
    d1h = int(done_last_hour_row["c"]) if done_last_hour_row else 0
    global_counts["rate_per_min_last_hour"] = round(d1h / 60.0, 2)

    for src in list_sources():
        r = await db.fetchone(
            "SELECT COUNT(*) AS c FROM urls WHERE source=? AND done_at IS NOT NULL AND done_at >= ?",
            (src, hour_ago),
        )
        c = int(r["c"]) if r else 0
        sources_out[src]["rate_per_min_last_hour"] = round(c / 60.0, 2)

    chart: list[int] = []
    for i in range(12):
        t0 = hour_ago + i * 300
        t1 = t0 + 300
        rr = await db.fetchone(
            "SELECT COUNT(*) AS c FROM urls WHERE done_at IS NOT NULL AND done_at >= ? AND done_at < ?",
            (t0, t1),
        )
        chart.append(int(rr["c"]) if rr else 0)

    open_batches = await db.fetchall(
        "SELECT batch_id, source, expires_at, size FROM batches WHERE state='open' ORDER BY expires_at ASC",
        (),
    )
    expiring = []
    for b in open_batches:
        rem = int(b["expires_at"]) - now
        if rem <= 600:
            expiring.append(
                {
                    "batch_id": b["batch_id"],
                    "source": b["source"],
                    "expires_at": int(b["expires_at"]),
                    "remaining_sec": rem,
                    "size": int(b["size"]),
                }
            )

    g_total = sum(global_counts[k] for k in ("pending", "leased", "done", "failed"))
    g_pct = round(100.0 * global_counts["done"] / g_total, 2) if g_total else 0.0

    return {
        "server_time": now,
        "global": {
            **global_counts,
            "total": g_total,
            "pct_done": g_pct,
        },
        "sources": sources_out,
        "chart_done_per_5min_last_hour": chart,
        "batches": {
            "open_count": len(open_batches),
            "expiring_soon": expiring,
        },
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> Any:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/batch/claim")
async def batch_claim(body: ClaimBody, request: Request) -> dict[str, Any]:
    source = body.source
    size = body.size
    now = int(time.time())
    expires_at = now + LEASE_SECONDS
    client_ip = request.client.host if request.client else ""
    assigned = body.client_id or client_ip or "unknown"

    async with db.transaction() as conn:
        if source is not None and source not in SOURCES:
            raise HTTPException(status_code=400, detail=f"unknown source: {source}")

        picked_rows: list[dict[str, Any]] = []
        batch_id = str(uuid.uuid4())
        if source is None:
            # Default mode: spread claims across multiple sources to reduce
            # per-domain request bursts and lower ban risk.
            cur = await conn.execute(
                "SELECT source, COUNT(*) AS c FROM urls WHERE status='pending' "
                "GROUP BY source ORDER BY c DESC",
            )
            active_sources = [r["source"] for r in await cur.fetchall()]
            if not active_sources:
                raise HTTPException(status_code=404, detail="no pending URLs in any source")

            remaining = size
            while remaining > 0 and active_sources:
                next_sources: list[str] = []
                for src in active_sources:
                    if remaining == 0:
                        break
                    # Lease immediately while selecting to avoid picking the same row
                    # multiple times before the final UPDATE.
                    one = await conn.execute(
                        "UPDATE urls SET status='leased', lease_token=?, lease_expires=?, assigned_to=? "
                        "WHERE id = ("
                        "  SELECT id FROM urls WHERE source=? AND status='pending' ORDER BY id LIMIT 1"
                        ") AND status='pending' "
                        "RETURNING id, url, source",
                        (batch_id, expires_at, assigned, src),
                    )
                    row = await one.fetchone()
                    if row:
                        picked_rows.append(
                            {"id": int(row["id"]), "url": row["url"], "source": row["source"]}
                        )
                        remaining -= 1
                        next_sources.append(src)
                active_sources = next_sources
        else:
            cur = await conn.execute(
                "SELECT id, url, source FROM urls WHERE source=? AND status='pending' ORDER BY id LIMIT ?",
                (source, size),
            )
            for r in await cur.fetchall():
                picked_rows.append({"id": int(r["id"]), "url": r["url"], "source": r["source"]})

        if not picked_rows:
            if source is None:
                raise HTTPException(status_code=404, detail="no pending URLs available")
            raise HTTPException(status_code=404, detail=f"no pending URLs for source {source}")

        ids = [r["id"] for r in picked_rows]
        urls = [r["url"] for r in picked_rows]
        if source is not None:
            qmarks = ",".join("?" * len(ids))
            await conn.execute(
                f"UPDATE urls SET status='leased', lease_token=?, lease_expires=?, assigned_to=? "
                f"WHERE id IN ({qmarks}) AND status='pending'",
                (batch_id, expires_at, assigned, *ids),
            )

        batch_source = source if source is not None else "mixed"
        await conn.execute(
            "INSERT INTO batches(batch_id, source, size, created_at, expires_at, client_ip, state) "
            "VALUES (?,?,?,?,?,?, 'open')",
            (batch_id, batch_source, len(ids), now, expires_at, client_ip),
        )
        await conn.executemany(
            "INSERT INTO batch_items(batch_id, url_id, url) VALUES (?,?,?)",
            [(batch_id, i, u) for i, u in zip(ids, urls)],
        )

    return {
        "batch_id": batch_id,
        "source": batch_source,
        "urls": urls,
        "items": [{"url": r["url"], "source": r["source"]} for r in picked_rows],
        "expires_at": expires_at,
        "size": len(urls),
    }


@app.post("/batch/{batch_id}/heartbeat")
async def batch_heartbeat(batch_id: str) -> dict[str, Any]:
    now = int(time.time())
    async with db.transaction() as conn:
        cur = await conn.execute(
            "SELECT batch_id, state, expires_at FROM batches WHERE batch_id=?",
            (batch_id,),
        )
        b = await cur.fetchone()
        if not b:
            raise HTTPException(status_code=404, detail="batch not found")
        if b["state"] != "open":
            raise HTTPException(status_code=400, detail="batch is not open")
        new_exp = int(b["expires_at"]) + HEARTBEAT_EXTEND
        await conn.execute(
            "UPDATE batches SET expires_at=? WHERE batch_id=? AND state='open'",
            (new_exp, batch_id),
        )
        await conn.execute(
            "UPDATE urls SET lease_expires=lease_expires+? WHERE lease_token=? AND status='leased'",
            (HEARTBEAT_EXTEND, batch_id),
        )
    return {"batch_id": batch_id, "expires_at": new_exp, "server_time": now}


@app.post("/batch/{batch_id}/submit")
async def batch_submit(batch_id: str, body: SubmitBody) -> dict[str, Any]:
    now = int(time.time())
    if not body.results:
        raise HTTPException(status_code=400, detail="empty results")

    async with db.transaction() as conn:
        cur = await conn.execute(
            "SELECT batch_id, source, state, expires_at FROM batches WHERE batch_id=?",
            (batch_id,),
        )
        b = await cur.fetchone()
        if not b:
            raise HTTPException(status_code=404, detail="batch not found")
        if b["state"] != "open":
            raise HTTPException(status_code=400, detail="batch already closed")
        if int(b["expires_at"]) < now:
            raise HTTPException(status_code=400, detail="batch lease expired")

        batch_source = b["source"]
        cur = await conn.execute(
            "SELECT bi.url_id, bi.url, u.source FROM batch_items bi "
            "JOIN urls u ON u.id = bi.url_id WHERE bi.batch_id=?",
            (batch_id,),
        )
        items = await cur.fetchall()
        allowed = {r["url"]: (int(r["url_id"]), r["source"]) for r in items}
        seen_urls: set[str] = set()

        done_n = 0
        failed_n = 0
        ok_records: list[tuple[str, dict[str, Any]]] = []

        for res in body.results:
            if res.url in seen_urls:
                raise HTTPException(status_code=400, detail=f"duplicate url in results: {res.url}")
            seen_urls.add(res.url)
            if res.url not in allowed:
                raise HTTPException(status_code=400, detail=f"url not in batch: {res.url}")

            url_id, expected_source = allowed[res.url]
            if res.ok:
                if not res.record:
                    raise HTTPException(status_code=400, detail=f"missing record for ok url {res.url}")
                rec = res.record
                if rec.get("url") != res.url:
                    raise HTTPException(status_code=400, detail="record.url mismatch")
                if rec.get("source") != expected_source:
                    raise HTTPException(
                        status_code=400,
                        detail=f"record.source mismatch for url {res.url}",
                    )

                ok_records.append((expected_source, rec))
                await conn.execute(
                    "UPDATE urls SET status='done', done_at=?, error=NULL, lease_token=NULL, "
                    "lease_expires=NULL WHERE id=?",
                    (now, url_id),
                )
                done_n += 1
            else:
                err = (res.error or "unknown")[:2000]
                await conn.execute(
                    "UPDATE urls SET status='failed', error=?, done_at=?, lease_token=NULL, "
                    "lease_expires=NULL WHERE id=?",
                    (err, now, url_id),
                )
                failed_n += 1

        await conn.execute(
            "UPDATE batches SET state='submitted' WHERE batch_id=?",
            (batch_id,),
        )

    written = 0
    for expected_source, rec in ok_records:
        try:
            store = storage.get_store(expected_source)
            if await store.append(rec):
                written += 1
        except Exception:
            log.exception("JSONL append failed after DB commit (url=%s)", rec.get("url"))

    return {
        "batch_id": batch_id,
        "processed_ok": done_n,
        "processed_fail": failed_n,
        "jsonl_lines_written": written,
    }
