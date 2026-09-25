from __future__ import annotations
import json
from contextlib import contextmanager
from pathlib import Path
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from .config import get_settings

_pool: ConnectionPool | None = None


def pool() -> ConnectionPool:
    global _pool

    if _pool is None:
        c = get_settings().postgres
        schema = c.schema_

        def configure(conn):
            conn.execute(f'SET search_path TO "{schema}", public')
            conn.commit()

        _pool = ConnectionPool(
            c.conninfo(),
            min_size=c.pool_min,
            max_size=c.pool_max,
            configure=configure,
            check=ConnectionPool.check_connection,
            open=True,
            timeout=c.connect_timeout + 5,
            max_idle=300,
            max_lifetime=1200,
            reconnect_timeout=30
        )

    return _pool
# def pool() -> ConnectionPool:
#     global _pool
#     if _pool is None:
#         c = get_settings().postgres
#         schema = c.schema_
#
#         def configure(conn):                       # every pooled connection uses our schema
#             conn.execute(f'SET search_path TO "{schema}", public')
#             conn.commit()
#
#         _pool = ConnectionPool(c.conninfo(), min_size=c.pool_min, max_size=c.pool_max,
#                                configure=configure, open=True, timeout=c.connect_timeout + 5)
#     return _pool


def ping() -> str:
    with pool().connection() as conn:
        return conn.execute("SELECT version()").fetchone()[0]


def init_schema():
    sql = (Path(__file__).parent.parent / "sql" / "init.sql").read_text()
    schema = get_settings().postgres.schema_
    with pool().connection() as conn:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.execute(f'SET search_path TO "{schema}", public')
        conn.execute(sql)


@contextmanager
def tx():
    with pool().connection() as conn:
        with conn.transaction():
            yield conn


def claim_job(file_seq_id, req: dict, source_type, path, engine, batch_size) -> tuple[int, bool]:
    """Returns (job_id, should_process). A COMPLETED job is skipped (idempotent re-delivery);
    a FAILED/stuck one is reset and reprocessed from scratch."""
    with tx() as c:
        row = c.execute("""
            INSERT INTO parse_jobs(file_seq_id, event_id, recon_id, rule_id, source_name, source_type,
                                   file_path, engine, batch_size, request, status, started_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PROCESSING',now())
            ON CONFLICT (file_seq_id) DO UPDATE SET status = CASE WHEN parse_jobs.status='COMPLETED'
                     THEN 'COMPLETED' ELSE 'PROCESSING' END,
                 started_at = now(), error = NULL, engine = EXCLUDED.engine, batch_size = EXCLUDED.batch_size
            RETURNING id, (xmax = 0) AS inserted, status""",
            (file_seq_id, req.get("eventId"), req.get("reconId"), req.get("ruleId"),
             req.get("sourceName") or req.get("source"), source_type, path, engine,
             batch_size, Jsonb(req))).fetchone()
        job_id, _, status = row
        if status == "COMPLETED" and not req.get("isSourceReprocess"):
            return job_id, False
        c.execute("DELETE FROM parsed_batches WHERE job_id=%s", (job_id,))
        return job_id, True


def insert_batch(job_id, file_seq_id, batch_no, first_row, records, meta):
    with tx() as c:
        c.execute("""INSERT INTO parsed_batches(job_id, file_seq_id, batch_no, record_count, first_row, payload)
                     VALUES (%s,%s,%s,%s,%s,%s)
                     ON CONFLICT (file_seq_id, batch_no) DO UPDATE
                     SET payload=EXCLUDED.payload, record_count=EXCLUDED.record_count""",
                  (job_id, file_seq_id, batch_no, len(records), first_row,
                   Jsonb({"records": records, "meta": meta})))


def finish_job(job_id, status, total_records=0, total_batches=0, warnings=None, error=None):
    with tx() as c:
        c.execute("""UPDATE parse_jobs SET status=%s, total_records=%s, total_batches=%s,
                     warnings=%s, error=%s, finished_at=now() WHERE id=%s""",
                  (status, total_records, total_batches, Jsonb(warnings or []), error, job_id))


def get_job(file_seq_id):
    with pool().connection() as c:
        r = c.execute("""SELECT id,file_seq_id,status,source_type,engine,total_records,total_batches,
                         warnings,error,created_at,finished_at FROM parse_jobs WHERE file_seq_id=%s""",
                      (file_seq_id,)).fetchone()
        if not r:
            return None
        keys = ["id", "file_seq_id", "status", "source_type", "engine", "total_records", "total_batches",
                "warnings", "error", "created_at", "finished_at"]
        return dict(zip(keys, r))


def get_batch(file_seq_id, batch_no):
    with pool().connection() as c:
        r = c.execute("SELECT payload FROM parsed_batches WHERE file_seq_id=%s AND batch_no=%s",
                      (file_seq_id, batch_no)).fetchone()
        return r[0] if r else None
