"""Core: resolve file -> parser -> map/coerce -> fixed-size batches -> parsed_batches rows."""
from __future__ import annotations
import glob
import os
import re
import uuid
from datetime import date
from typing import Any

import structlog

from . import db
from .config import get_settings, merged
from .models import ParseRequest
from .schema import ColumnMapper, Coercer
from .parsers.csv_parser import CsvParser
from .parsers.excel_parser import ExcelParser
from .parsers.pdf_native import PdfNativeParser
from .parsers.pdf_textract import PdfTextractParser
from .parsers.superset import SupersetParser
from .parsers.pdf_statement import PdfStatementParser, load_profiles, detect_profile

log = structlog.get_logger()


class FileResolutionError(Exception):
    pass


def resolve_paths(req: ParseRequest, pick: str, allowed_roots: list[str]) -> list[str]:
    """Supports an explicit path, or a wildcard fileName under fileImportDetails.subPath."""
    cands: list[str] = []
    if req.path and not any(ch in os.path.basename(req.path) for ch in "*?["):
        cands = [req.path]
    else:
        base = (req.fileImportDetails.subPath if req.fileImportDetails else None) \
               or (os.path.dirname(req.path) if req.path else None)
        pattern = os.path.basename(req.path) if req.path and "*" in req.path else (req.fileName or "*")
        if not base:
            raise FileResolutionError("no path / subPath provided")
        cands = sorted(glob.glob(os.path.join(base, pattern)), key=os.path.getmtime)
        if not cands:
            raise FileResolutionError(f"no file matches {os.path.join(base, pattern)}")
        if pick == "latest":
            cands = cands[-1:]
    out = []
    for p in cands:
        real = os.path.realpath(p)
        if allowed_roots and not any(real.startswith(os.path.realpath(r) + os.sep) for r in allowed_roots):
            raise FileResolutionError(f"path {p} outside allowed roots {allowed_roots}")
        if not os.path.isfile(real):
            raise FileResolutionError(f"file not found: {p}")
        out.append(real)
    return out


def build_parser(fmt: str, req: ParseRequest, s, path: str | None = None):
    if fmt == "csv":
        return CsvParser(s, req)
    if fmt == "excel":
        return ExcelParser(s, req)
    if fmt == "superset":
        return SupersetParser(s, req)
    if fmt == "pdf":
        pdf_cfg = merged(s.pdf, req.parserOptions.get("pdf"))      # per-message override
        if pdf_cfg.layout in ("auto", "statement"):
            profiles = load_profiles(pdf_cfg.profiles_dir)
            if pdf_cfg.profile:
                if pdf_cfg.profile not in profiles:
                    raise ValueError(f"unknown pdf profile {pdf_cfg.profile!r}; have {sorted(profiles)}")
                prof = profiles[pdf_cfg.profile]
            else:
                prof = detect_profile(path, profiles, pdf_cfg.engine) if path else None
            if prof:
                return PdfStatementParser(s, req, pdf_cfg, prof)
            if pdf_cfg.layout == "statement":
                raise ValueError("layout=statement but no profile matched this PDF")
        cls = PdfTextractParser if pdf_cfg.engine == "textract" else PdfNativeParser
        return cls(s, req, pdf_cfg)
    raise ValueError(f"unsupported format {fmt}")


def _enrich(rec: dict, req: ParseRequest, file_path: str | None, batch_id: str, doc: dict, types: dict):
    """Fill system columns only when they exist in columnDetails and are empty in the source."""
    fill = {
        "file_seq_id": req.fileSeqId,
        "bloc_recon_file_name": os.path.basename(file_path) if file_path else None,
        "sourcetype": req.sourceName or req.source,
        "batch_id": batch_id,
        "process_date": date.today().isoformat(),
    }
    for k, v in fill.items():
        if k in types and not rec.get(k) and v:
            rec[k] = v
    for k, v in doc.items():                           # ocr_keywords custom1 -> custom_field_1
        m = re.fullmatch(r"custom(\d+)", k)
        if m and f"custom_field_{m.group(1)}" in types and not rec.get(f"custom_field_{m.group(1)}"):
            rec[f"custom_field_{m.group(1)}"] = v


def process(req_dict: dict[str, Any]) -> dict[str, Any]:
    s = get_settings()
    req = ParseRequest.model_validate(req_dict)
    file_seq_id = req.fileSeqId or f"AUTO-{uuid.uuid4().hex[:16]}"
    req.fileSeqId = file_seq_id
    batch_size = req.batchSize or s.batch.size

    paths: list[str | None] = [None]
    fmt = "superset" if req.superset else None
    if fmt is None:
        paths = resolve_paths(req, s.storage.wildcard_pick, s.storage.allowed_roots)
        fmt = req.resolved_format(paths[0])

    parser = build_parser(fmt, req, s, paths[0])
    job_id, go = db.claim_job(file_seq_id, req_dict, fmt, ",".join(p or "" for p in paths),
                              parser.engine, batch_size)
    if not go:
        log.info("job.skip_completed", file_seq_id=file_seq_id)
        return {"fileSeqId": file_seq_id, "status": "COMPLETED", "skipped": True}

    mapper = ColumnMapper(req)
    coercer = Coercer(s.typing, s.pdf.numeric, req.customDateFormat)
    run_batch_id = uuid.uuid4().hex[:12]
    total, batch_no, buf, buf_first = 0, 0, [], None

    def flush(meta):
        nonlocal batch_no, buf, buf_first
        if not buf:
            return
        batch_no += 1
        db.insert_batch(job_id, file_seq_id, batch_no, buf_first, buf,
                        {**meta, "batch_no": batch_no, "batch_size": batch_size, "format": fmt,
                         "engine": parser.engine, "reconId": req.reconId, "eventId": req.eventId})
        buf, buf_first = [], None

    try:
        meta: dict = {}
        for path in paths:
            for table in parser.tables(path):
                cols = list(table.headers) if table.meta.get("premapped") else mapper.map_headers(table.headers)
                doc = table.meta.get("doc", {})
                meta = {k: v for k, v in table.meta.items() if k not in ("doc", "premapped")} | {"file": path, "doc": doc}
                for i, row in enumerate(table.rows):
                    rec = {c: coercer.coerce(row[j] if j < len(row) else None, mapper.types.get(c))
                           for j, c in enumerate(cols) if mapper.keep(c)}
                    if req.enrichFlow:
                        _enrich(rec, req, path, run_batch_id, doc, mapper.types)
                    rec["_row"] = table.first_row_no + i
                    if buf_first is None:
                        buf_first = rec["_row"]
                    buf.append(rec)
                    total += 1
                    if len(buf) >= batch_size:
                        flush(meta)
        flush(meta)
        warnings = mapper.warnings + parser.warnings
        db.finish_job(job_id, "COMPLETED", total, batch_no, warnings)
        log.info("job.completed", file_seq_id=file_seq_id, records=total, batches=batch_no)
        return {"fileSeqId": file_seq_id, "status": "COMPLETED", "records": total,
                "batches": batch_no, "warnings": warnings, "reconId": req.reconId, "eventId": req.eventId}
    except Exception as e:
        db.finish_job(job_id, "FAILED", total, batch_no, mapper.warnings + parser.warnings, repr(e))
        raise
