"""Where parsed batches go. DbSink = production (Postgres). FileSink = local testing (JSON files)."""
from __future__ import annotations
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import db


class DbSink:
    def claim(self, file_seq_id, req, fmt, path, engine, batch_size):
        return db.claim_job(file_seq_id, req, fmt, path, engine, batch_size)

    def batch(self, job_id, file_seq_id, batch_no, first_row, records, meta):
        db.insert_batch(job_id, file_seq_id, batch_no, first_row, records, meta)

    def finish(self, job_id, status, total=0, batches=0, warnings=None, error=None):
        db.finish_job(job_id, status, total, batches, warnings, error)


class FileSink:
    """Writes <out_dir>/batch_0001.json ... plus _job.json (status, counts, warnings).
    Each batch file has the exact shape stored in parsed_batches.payload."""

    def __init__(self, out_dir: Path, clean: bool = True):
        self.out = Path(out_dir)
        if clean and self.out.exists():
            shutil.rmtree(self.out)
        self.out.mkdir(parents=True, exist_ok=True)
        self.job: dict[str, Any] = {}
        self.files: list[str] = []

    @staticmethod
    def _dump(path: Path, obj):
        path.write_text(json.dumps(obj, indent=2, default=str, ensure_ascii=False), encoding="utf-8")

    def claim(self, file_seq_id, req, fmt, path, engine, batch_size):
        self.job = {"file_seq_id": file_seq_id, "source_type": fmt, "file_path": path, "engine": engine,
                    "batch_size": batch_size, "status": "PROCESSING",
                    "started_at": datetime.now(timezone.utc).isoformat(), "request": req}
        return 0, True

    def batch(self, job_id, file_seq_id, batch_no, first_row, records, meta):
        name = f"batch_{batch_no:04d}.json"
        self._dump(self.out / name, {"file_seq_id": file_seq_id, "batch_no": batch_no,
                                     "record_count": len(records), "first_row": first_row,
                                     "payload": {"records": records, "meta": meta}})
        self.files.append(name)

    def finish(self, job_id, status, total=0, batches=0, warnings=None, error=None):
        self.job.update(status=status, total_records=total, total_batches=batches, warnings=warnings or [],
                        error=error, finished_at=datetime.now(timezone.utc).isoformat(), files=self.files)
        self._dump(self.out / "_job.json", self.job)
