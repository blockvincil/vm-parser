"""Run inside the container:  docker compose run --rm api pytest -q
DB is faked so tests need no Postgres/Kafka."""
import csv
import json
from pathlib import Path
import pytest
from app import pipeline

SAMPLE = json.loads((Path(__file__).parent / "sample_request.json").read_text())


@pytest.fixture
def fake_db(monkeypatch):
    store = {"batches": {}, "job": None}
    monkeypatch.setattr(pipeline.db, "claim_job", lambda *a, **k: (1, True))
    monkeypatch.setattr(pipeline.db, "insert_batch",
                        lambda job, fs, no, first, recs, meta: store["batches"].__setitem__(no, recs))
    monkeypatch.setattr(pipeline.db, "finish_job", lambda *a, **k: store.__setitem__("job", a))
    return store


def _req(tmp_path, path, fmt):
    r = dict(SAMPLE, path=str(path), format=fmt, fileSeqId="T1")
    r["fileImportDetails"] = dict(r["fileImportDetails"], subPath=str(tmp_path))
    return r


def test_csv_10000_rows_batch_1000(tmp_path, fake_db, monkeypatch):
    monkeypatch.setenv("DP__STORAGE__ALLOWED_ROOTS", f"['{tmp_path}']")
    pipeline.get_settings.cache_clear()
    f = tmp_path / "cash.csv"
    with f.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["Sub Account", "Currency", "Amount", "Item Date", "DB/CR"])
        for i in range(10_000):
            w.writerow([f"SA{i}", "USD", f"{i},000.25" if i % 2 else f"({i})", "23/01/2026", "CR"])
    out = pipeline.process(_req(tmp_path, f, "CSV"))
    assert out["records"] == 10_000 and out["batches"] == 10
    rec = fake_db["batches"][1][1]
    assert rec["subaccount"] == "SA1" and rec["amount"] == "1000.25" and rec["itemdate"] == "2026-01-23"
    assert fake_db["batches"][1][2]["amount"] == "-2"
    assert rec["file_seq_id"] == "T1"                          # enrichFlow


def test_excel(tmp_path, fake_db, monkeypatch):
    from openpyxl import Workbook
    monkeypatch.setenv("DP__STORAGE__ALLOWED_ROOTS", f"['{tmp_path}']")
    pipeline.get_settings.cache_clear()
    wb = Workbook(); ws = wb.active
    ws.append(["ISIN", "Amount", "Closing Balance Date"])
    for i in range(2500):
        ws.append([f"US{i:010d}", 12.5 * i, "2026-01-31"])
    f = tmp_path / "Book12.xlsx"; wb.save(f)
    out = pipeline.process(_req(tmp_path, f, "Excel"))
    assert out["batches"] == 3 and len(fake_db["batches"][3]) == 500
    assert fake_db["batches"][1][0]["closingbalancedate"] == "2026-01-31"
