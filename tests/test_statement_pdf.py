from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
from app.parsers import pdf_statement as ps

PDF = Path(__file__).parent / "fixtures" / "northern_trust_sample.pdf"
PROFILE = ps.load_profiles("config/pdf_profiles")["northern_trust_daily"]


def _parse(pages=None):
    p = ps.PdfStatementParser(None, None, NS(engine="native"), PROFILE)
    return list(p.records(str(PDF))), p.warnings


def test_profile_detected():
    assert ps.detect_profile(str(PDF), {"x": PROFILE}, "native")["name"] == "northern_trust_daily"


def test_all_transactions_and_checks_pass():
    recs, warns = _parse()
    tx = [r for r in recs if r["record_type"] == "transaction"]
    bal = [r for r in recs if r["record_type"] == "balance"]
    active = {(r["posting_date"], r["account_number"]) for r in tx}
    assert len(tx) == 34
    assert len(bal) == 69 - len(active)                 # 23 days x 3 accounts, minus days with activity
    assert not active & {(r["posting_date"], r["account_number"]) for r in bal}   # no account-day twice
    assert not [w for w in warns if w.startswith(("VALIDATION", "unparsed"))], warns


def test_page_split_and_right_column_amounts():
    tx = {r["reference"]: r for r in _parse()[0] if r["record_type"] == "transaction"}
    # starts on page 7, description continues after the repeated page-8 header
    t = tx["2026071401879778"]
    assert t["amount"] == "35000.00" and "SHOLDCO FINANCING LIMITED" in t["description"]
    assert tx["2026072002118810"]["amount"] == "9473606.37" and tx["2026072002118810"]["dbcr"] == "C"
    assert tx["20260730R5000026"]["value_date"] == "2026-07-31"      # value date != posting date


def test_negative_balance():
    bal = [r for r in _parse()[0]
           if r["account_number"] == "23567891234" and r["posting_date"] == "2026-07-13"][0]
    assert bal["closing_ledger"] == "-85.00" and bal["closing_ledger_dbcr"] == "DR"


def test_tampered_amount_is_caught(monkeypatch):
    orig = ps.native_pages
    def bad(path):
        for ws in orig(path):
            for w in ws:
                if w.text == "67,047.86":
                    w.text = "67,047.68"
            yield ws
    monkeypatch.setattr(ps, "native_pages", bad)
    _, warns = _parse()
    assert any("2026-07-08" in w and "VALIDATION" in w for w in warns)
