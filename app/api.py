"""HTTP surface: Superset webhook, direct upload, job/batch lookup. Heavy work runs in a thread
pool here; for high volume, prefer POST /jobs/enqueue which just publishes to Kafka."""
from __future__ import annotations
import json
import os
import shutil
import tempfile
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Header
from fastapi.concurrency import run_in_threadpool
from confluent_kafka import Producer

from . import db
from .config import get_settings
from .pipeline import process

app = FastAPI(title="DocParser", version="1.0.0")
_producer: Producer | None = None


DEV_MODE = os.getenv("DP_DEV_ENDPOINTS", "false").lower() in ("1", "true", "yes")


@app.on_event("startup")
def _startup():
    if DEV_MODE and os.getenv("DP_SKIP_DB", "true").lower() in ("1", "true", "yes"):
        return                      # dev: fixture endpoints work without Postgres
    db.init_schema()


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/webhooks/superset")
async def superset_webhook(body: dict[str, Any], x_webhook_token: str | None = Header(default=None)):
    """body: {"chart_id": 42, "query_context": {...}?, "extra_filters": [...]?, "fileSeqId"?, "reconId"?,
              "columnDetails"?: [...], "batchSize"?: 500}"""
    expected = os.getenv("WEBHOOK_TOKEN")
    if expected and x_webhook_token != expected:
        raise HTTPException(401, "bad webhook token")
    sup = {k: body.pop(k) for k in ("chart_id", "query_context", "extra_filters", "row_limit") if k in body}
    body["superset"] = sup
    body.setdefault("fileSeqId", f"SUPERSET-{sup.get('chart_id')}-{uuid.uuid4().hex[:8]}")
    return await run_in_threadpool(process, body)


@app.post("/parse/upload")
async def upload(file: UploadFile = File(...), request: str = Form("{}")):
    """Multipart upload + optional JSON request (same shape as the Kafka message)."""
    req = json.loads(request)
    tmpdir = tempfile.mkdtemp(prefix="dp_", dir=get_settings().storage.allowed_roots[0]
                              if os.path.isdir(get_settings().storage.allowed_roots[0]) else None)
    path = os.path.join(tmpdir, os.path.basename(file.filename))
    with open(path, "wb") as fh:
        shutil.copyfileobj(file.file, fh)
    req["path"] = path
    try:
        return await run_in_threadpool(process, req)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@app.post("/jobs/enqueue")
def enqueue(body: dict[str, Any]):
    global _producer
    s = get_settings().kafka
    _producer = _producer or Producer({"bootstrap.servers": s.bootstrap_servers})
    _producer.produce(s.topic, json.dumps(body).encode(), key=(body.get("fileSeqId") or "").encode())
    _producer.flush(5)
    return {"queued": True, "fileSeqId": body.get("fileSeqId")}


@app.get("/jobs/{file_seq_id}")
def job(file_seq_id: str):
    j = db.get_job(file_seq_id)
    if not j:
        raise HTTPException(404)
    return j


@app.get("/jobs/{file_seq_id}/batches/{batch_no}")
def batch(file_seq_id: str, batch_no: int):
    b = db.get_batch(file_seq_id, batch_no)
    if b is None:
        raise HTTPException(404)
    return b


# ---------------------------------------------------------------- dev / testing
# Enabled only when DP_DEV_ENDPOINTS=true. No Postgres or Kafka needed.
from .devtools import run_fixture, list_fixture_files, FixtureError


def _dev_guard():
    if not DEV_MODE:
        raise HTTPException(404)


@app.get("/dev/fixtures")
def list_fixtures():
    _dev_guard()
    return list_fixture_files()


@app.post("/dev/fixtures/{filename}/parse")
async def parse_fixture(filename: str, overrides: dict[str, Any] | None = None):
    """Parses tests/fixtures/<filename>, writes tests/fixtures/outputs/<stem>/batch_NNNN.json + _job.json.
    Optional body = request overrides, e.g. {"batchSize": 10} or
    {"parserOptions": {"pdf": {"engine": "textract"}}}."""
    _dev_guard()
    try:
        return await run_in_threadpool(run_fixture, filename, overrides)
    except FixtureError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(422, repr(e))
