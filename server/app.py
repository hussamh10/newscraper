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
        if source is None:
            cur = await conn.execute(
                "SELECT source FROM urls WHERE status='pending' "
                "GROUP BY source ORDER BY COUNT(*) DESC LIMIT 1",
            )
            pick = await cur.fetchone()
            if not pick:
                raise HTTPException(status_code=404, detail="no pending URLs in any source")
            source = pick["source"]
        elif source not in SOURCES:
            raise HTTPException(status_code=400, detail=f"unknown source: {source}")

        cur = await conn.execute(
            "SELECT id, url FROM urls WHERE source=? AND status='pending' ORDER BY id LIMIT ?",
            (source, size),
        )
        picked = await cur.fetchall()
        if not picked:
            raise HTTPException(status_code=404, detail=f"no pending URLs for source {source}")

        ids = [int(r["id"]) for r in picked]
        urls = [r["url"] for r in picked]
        batch_id = str(uuid.uuid4())
        qmarks = ",".join("?" * len(ids))
        await conn.execute(
            f"UPDATE urls SET status='leased', lease_token=?, lease_expires=?, assigned_to=? "
            f"WHERE id IN ({qmarks}) AND status='pending'",
            (batch_id, expires_at, assigned, *ids),
        )

        await conn.execute(
            "INSERT INTO batches(batch_id, source, size, created_at, expires_at, client_ip, state) "
            "VALUES (?,?,?,?,?,?, 'open')",
            (batch_id, source, len(ids), now, expires_at, client_ip),
        )
        await conn.executemany(
            "INSERT INTO batch_items(batch_id, url_id, url) VALUES (?,?,?)",
            [(batch_id, i, u) for i, u in zip(ids, urls)],
        )

    return {
        "batch_id": batch_id,
        "source": source,
        "urls": urls,
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

        source = b["source"]
        cur = await conn.execute(
            "SELECT url_id, url FROM batch_items WHERE batch_id=?",
            (batch_id,),
        )
        items = await cur.fetchall()
        allowed = {r["url"]: int(r["url_id"]) for r in items}
        seen_urls: set[str] = set()

        done_n = 0
        failed_n = 0
        ok_records: list[dict[str, Any]] = []

        for res in body.results:
            if res.url in seen_urls:
                raise HTTPException(status_code=400, detail=f"duplicate url in results: {res.url}")
            seen_urls.add(res.url)
            if res.url not in allowed:
                raise HTTPException(status_code=400, detail=f"url not in batch: {res.url}")

            url_id = allowed[res.url]
            if res.ok:
                if not res.record:
                    raise HTTPException(status_code=400, detail=f"missing record for ok url {res.url}")
                rec = res.record
                if rec.get("url") != res.url:
                    raise HTTPException(status_code=400, detail="record.url mismatch")
                if rec.get("source") != source:
                    raise HTTPException(status_code=400, detail="record.source mismatch")

                ok_records.append(rec)
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
    store = storage.get_store(source)
    for rec in ok_records:
        try:
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
