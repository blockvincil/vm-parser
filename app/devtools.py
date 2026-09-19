"""Local testing helpers: parse tests/fixtures/<file> into tests/fixtures/outputs/<stem>/*.json.

CLI:   python -m app.devtools                      # list fixtures
       python -m app.devtools northern_trust_sample.pdf
       python -m app.devtools Book12.xlsx --batch-size 10
       python -m app.devtools scan.pdf --set parserOptions.pdf.engine=textract
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Any

from .pipeline import process
from .sinks import FileSink

import os
from .config import load_dotenv

load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Override with DP_FIXTURES_DIR (absolute, or relative to the project root) if your files live elsewhere
FIXTURES = (PROJECT_ROOT / os.getenv("DP_FIXTURES_DIR", "tests/fixtures")).resolve()
OUTPUTS = (PROJECT_ROOT / os.getenv("DP_FIXTURES_OUTPUT_DIR", str(FIXTURES / "outputs"))).resolve()
SAMPLE_REQUEST = PROJECT_ROOT / "tests" / "sample_request.json"
PARSEABLE = {".pdf", ".csv", ".txt", ".tsv", ".xlsx", ".xlsm", ".xls"}


class FixtureError(Exception):
    pass


def list_fixture_files() -> list[str]:
    if not FIXTURES.is_dir():
        return []
    return sorted(p.name for p in FIXTURES.iterdir() if p.is_file() and p.suffix.lower() in PARSEABLE)


def fixtures_info() -> dict[str, Any]:
    """Where we look, so an empty list is never a mystery."""
    return {"fixtures_dir": str(FIXTURES), "exists": FIXTURES.is_dir(),
            "outputs_dir": str(OUTPUTS), "files": list_fixture_files(),
            "hint": None if list_fixture_files() else
                    f"copy files into {FIXTURES} or set DP_FIXTURES_DIR in .env"}


def run_fixture(filename: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    src = (FIXTURES / filename).resolve()
    if src.parent != FIXTURES.resolve() or not src.is_file():     # blocks ../ escapes
        raise FixtureError(f"fixture not found: {filename} in {FIXTURES} "
                           f"(folder exists: {FIXTURES.is_dir()}, available: {list_fixture_files()})")

    # start from the real Kafka message so column mapping/typing is exercised too
    req: dict[str, Any] = json.loads(SAMPLE_REQUEST.read_text()) if SAMPLE_REQUEST.exists() else {}
    req.update({"path": str(src), "fileName": src.name, "format": None,
                "fileSeqId": f"DEV-{src.stem}", "sheetIdentifiers": []})
    for k, v in (overrides or {}).items():
        if isinstance(v, dict) and isinstance(req.get(k), dict):
            req[k] = {**req[k], **v}
        else:
            req[k] = v

    out_dir = OUTPUTS / src.name.replace(".", "_")     # Book12.xlsx -> Book12_xlsx
    sink = FileSink(out_dir)
    try:
        result = process(req, sink=sink, allowed_roots=[str(FIXTURES)])
    except Exception as e:
        sink.finish(0, "FAILED", error=repr(e))
        raise
    return {**result, "output_dir": str(out_dir), "files": sink.files + ["_job.json"]}


def _set(d: dict, dotted: str, value: str):
    keys = dotted.split(".")
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    try:
        d[keys[-1]] = json.loads(value)
    except json.JSONDecodeError:
        d[keys[-1]] = value


def main():
    ap = argparse.ArgumentParser(description="Parse a file from tests/fixtures into JSON outputs")
    ap.add_argument("filename", nargs="?")
    ap.add_argument("--batch-size", type=int)
    ap.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                    help="request override, dotted keys allowed")
    a = ap.parse_args()
    if not a.filename:
        info = fixtures_info()
        print(f"fixtures dir: {info['fixtures_dir']}  (exists: {info['exists']})")
        print("\n".join(info["files"]) or f"  no parseable files - {info['hint']}")
        return
    ov: dict[str, Any] = {}
    if a.batch_size:
        ov["batchSize"] = a.batch_size
    for kv in a.set:
        k, _, v = kv.partition("=")
        _set(ov, k, v)
    res = run_fixture(a.filename, ov)
    print(json.dumps({k: v for k, v in res.items() if k != "files"}, indent=2, default=str))
    print(f"{len(res['files'])} files written to {res['output_dir']}")


if __name__ == "__main__":
    main()
