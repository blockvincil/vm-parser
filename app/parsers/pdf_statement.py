"""
Profile-driven bank statement parser (layout = 'statement').

Statements like the Northern Trust daily detail are not tables: they are a sequence of
  date -> account header -> balance lines -> summary totals -> section (DEBITS/CREDITS)
  -> transactions (code | multi-line description | right-aligned amount) -> control totals
with the flow freely crossing page breaks (headers are repeated, descriptions continue).

A state machine over position-grouped rows handles that, and every account-day is
cross-checked against the statement's own control totals and balance roll-forward.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

import yaml

from .base import BaseParser, RawTable
from .pdf_words import Row, native_pages, textract_pages, to_rows

_AMT = re.compile(r"\(?-?[\d,]+\.\d{2}\)?-?")


def to_dec(s: str | None) -> Decimal | None:
    if not s:
        return None
    s = s.strip()
    neg = (s.startswith("(") and s.endswith(")")) or s.startswith("-") or s.endswith("-")
    try:
        d = Decimal(re.sub(r"[^\d.]", "", s))
    except InvalidOperation:
        return None
    return -d if neg else d


def load_profiles(directory: str) -> dict[str, dict]:
    out = {}
    p = Path(directory)
    if not p.is_absolute():
        p = Path(__file__).resolve().parents[2] / directory
    for f in sorted(p.glob("*.y*ml")):
        prof = yaml.safe_load(f.read_text())
        out[prof["name"]] = prof
    return out


@dataclass
class Txn:
    code: str
    lines: list[str]
    amount: Decimal | None
    page: int
    section: str


@dataclass
class AcctDay:
    date: str
    bank: str
    account: str
    currency: str | None
    name: str
    balances: dict[str, Decimal | None] = field(default_factory=dict)
    summary: dict[str, Decimal] = field(default_factory=dict)       # CREDITS/DEBITS from 100/400 lines
    control: dict[str, dict] = field(default_factory=dict)          # from TOTALS FOR ACCOUNT
    txns: dict[str, list[Decimal]] = field(default_factory=lambda: {"CREDITS": [], "DEBITS": []})
    seq: int = 0


class StatementMachine:
    def __init__(self, profile: dict, warn):
        self.p = profile
        self.warn = warn
        r = profile["rules"]
        self.rx = {k: re.compile(v) for k, v in r.items()
                   if isinstance(v, str) and not k.endswith("_format")}
        self.skip = [re.compile(s) for s in profile.get("skip_rows", [])]
        self.docrx = {k: re.compile(v) for k, v in profile.get("doc_fields", {}).items()}
        g = profile["geometry"]
        self.code_max, self.amt_min = g["code_max_x0"], g["amount_min_x0"]
        self.tag_rx = re.compile(r"\b(" + "|".join(map(re.escape, r["tags"])) + r")=")
        self.out = profile["output"]
        self.val = profile.get("validation", {})
        self.doc: dict[str, str] = {}
        self.date: str | None = None
        self.ctx: AcctDay | None = None
        self.section: str | None = None
        self.txn: Txn | None = None
        self.state = "idle"            # idle | txn | header | totals
        self.totals_buf: list[str] = []
        self.accounts: dict[str, tuple[str | None, str]] = {}   # account -> (currency, name)
        self.errors = 0
        self.checks = 0
        self.unparsed = 0

    # ------------------------------------------------------------------ helpers
    def _dbcr(self, section):
        return self.out["debit_value"] if section == "DEBITS" else self.out["credit_value"]

    @staticmethod
    def _sign(v: Decimal | None):
        return None if v is None else ("DR" if v < 0 else "CR")

    def _fail(self, msg):
        self.errors += 1
        self.warn(f"VALIDATION: {msg}")
        if self.val.get("fail_on_error"):
            raise ValueError(msg)

    def _tags(self, text: str) -> dict[str, Any]:
        tags: dict[str, Any] = {}
        parts = self.tag_rx.split(text)
        for k, v in zip(parts[1::2], parts[2::2]):
            v = v.strip()
            tags[k] = v if k not in tags else (tags[k] if isinstance(tags[k], list) else [tags[k]]) + [v]
        return tags

    def _base(self) -> dict[str, Any]:
        c = self.ctx
        b = {k: (str(v) if v is not None else None) for k, v in c.balances.items()}
        for k in ("opening_ledger", "closing_ledger", "opening_available", "closing_available"):
            b.setdefault(k, None)
            b[f"{k}_dbcr"] = self._sign(c.balances.get(k))
        return {"posting_date": c.date, "bank": c.bank, "account_number": c.account,
                "currency": c.currency, "account_name": c.name, **b, **self.doc}

    # ------------------------------------------------------------------ record emission
    def _close_txn(self) -> list[dict]:
        t, self.txn = self.txn, None
        if t is None or self.ctx is None:
            return []
        desc = " ".join(t.lines).strip()
        self.ctx.seq += 1
        if t.amount is None:
            self._fail(f"{self.ctx.date} acct {self.ctx.account}: transaction '{desc[:60]}' has no amount")
        else:
            self.ctx.txns[t.section].append(t.amount)
        if self.val.get("bai_type_direction") and t.code.isdigit():
            expect = "CREDITS" if 100 <= int(t.code) < 400 else "DEBITS" if 400 <= int(t.code) < 700 else None
            self.checks += 1
            if expect and expect != t.section:
                self._fail(f"{self.ctx.date} acct {self.ctx.account}: type {t.code} in {t.section} section")
        if "transaction" not in self.out["emit"]:
            return []
        vd = self.rx["value_date"].search(desc)
        ref = self.rx["reference"].search(desc)
        rec = self._base() | {
            "record_type": "transaction", "seq": self.ctx.seq, "type_code": t.code,
            "section": t.section, "dbcr": self._dbcr(t.section),
            "amount": str(t.amount) if t.amount is not None else None,
            "reference": ref.group("ref") if ref else None,
            "value_date": datetime.strptime(vd.group("d"), self.p["rules"]["value_date_format"]).date().isoformat()
                          if vd else self.ctx.date,
            "description": desc, "tags": self._tags(desc), "page": t.page,
        }
        return [rec]

    def _close_ctx(self) -> list[dict]:
        out = self._close_txn()
        self._flush_totals()
        c, self.ctx = self.ctx, None
        self.section, self.state = None, "idle"
        if c is None:
            return out
        cr, dr = sum(c.txns["CREDITS"], Decimal(0)), sum(c.txns["DEBITS"], Decimal(0))
        tag = f"{c.date} acct {c.account} ({c.currency})"
        if self.val.get("control_totals"):
            for kind in ("CREDITS", "DEBITS"):
                got, n = (cr if kind == "CREDITS" else dr), len(c.txns[kind])
                if kind in c.summary:
                    self.checks += 1
                    if c.summary[kind] != got:
                        self._fail(f"{tag}: TOTAL {kind} {c.summary[kind]} != sum of parsed txns {got}")
                ctl = c.control.get(kind)
                if ctl:
                    self.checks += 1
                    if ctl.get("count") is not None and ctl["count"] != n:
                        self._fail(f"{tag}: {kind} COUNT {ctl['count']} != parsed {n}")
                    if ctl.get("amount") is not None and ctl["amount"] != got:
                        self._fail(f"{tag}: control {kind} {ctl['amount']} != parsed {got}")
                elif n:
                    self._fail(f"{tag}: {n} {kind} parsed but no TOTALS FOR ACCOUNT line found")
        ol, cl = c.balances.get("opening_ledger"), c.balances.get("closing_ledger")
        if self.val.get("balance_roll") and ol is not None and cl is not None:
            self.checks += 1
            if ol + cr - dr != cl:
                self._fail(f"{tag}: opening {ol} + credits {cr} - debits {dr} = {ol + cr - dr} != closing {cl}")
        if "balance" in self.out["emit"]:
            self.ctx = c
            rec = self._base() | {"record_type": "balance", "total_credits": str(cr), "total_debits": str(dr),
                                  "credit_count": len(c.txns["CREDITS"]), "debit_count": len(c.txns["DEBITS"])}
            self.ctx = None
            out.append(rec)
        return out

    def _flush_totals(self):
        if not self.totals_buf or self.ctx is None:
            self.totals_buf = []
            return
        text = " ".join(self.totals_buf)
        m = self.rx["totals_count"].search(text)
        amts = _AMT.findall(text.split("COUNT:")[-1])
        kind = (m.group("kind") + "S") if m else self.section
        if kind:
            self.ctx.control[kind] = {"count": int(m.group("count")) if m else None,
                                      "amount": to_dec(amts[-1]) if amts else None}
        self.totals_buf = []

    # ------------------------------------------------------------------ main step
    def feed(self, row: Row) -> list[dict]:
        left = row.text(0, self.amt_min)
        right = row.text(self.amt_min)
        full = f"{left} {right}".strip()
        if any(s.search(full) for s in self.skip):
            for k, rx in self.docrx.items():
                if k not in self.doc and (m := rx.search(full)):
                    self.doc[k] = m.group("v").strip()
            return []
        out: list[dict] = []

        if m := self.rx["date"].match(full):
            d = datetime.strptime(m.group("date"), self.p["rules"]["date_format"]).date().isoformat()
            if d != self.date:                       # same date = repeated page header
                out += self._close_ctx()
                self.date = d
            return out

        if m := self.rx["account"].match(full):
            acct = m.group("account")
            name = (m.group("name1") or m.group("name2") or "").strip()
            ccy = m.group("currency")
            prev = self.accounts.get(acct, (None, ""))
            ccy, name = ccy or prev[0], name or prev[1]
            self.accounts[acct] = (ccy, name)
            if self.ctx and self.ctx.account == acct and self.ctx.date == self.date:
                self.state = "header"                # page-break continuation: keep txn open
                return out
            out += self._close_ctx()
            self.ctx = AcctDay(self.date, m.group("bank"), acct, ccy, name)
            return out

        if self.ctx is None:
            self._unparsed(row, full)
            return out

        if m := self.rx["balance"].match(left):
            field_ = self.p["balance_fields"].get(m.group("code"))
            amt = m.group("amount") or (right if _AMT.fullmatch(right or "-") else None)
            if field_:
                self.ctx.balances[field_] = to_dec(amt)
                if amt is None:
                    self._fail(f"{self.ctx.date} acct {self.ctx.account}: balance {m.group('label')} has no value")
            return out

        if m := self.rx["summary"].match(left):
            self.ctx.summary[m.group("kind")] = to_dec(m.group("amount"))
            return out

        if m := self.rx["section"].match(left):
            kind = m.group("kind")
            if self.state == "header" and self.txn and self.txn.section == kind:
                self.state = "txn"                   # resume transaction split across pages
            else:
                out += self._close_txn()
                self._flush_totals()
                self.state = "idle"
            self.section = kind
            return out

        if self.rx["totals_start"].match(full):
            out += self._close_txn()
            self._flush_totals()
            self.totals_buf, self.state = [full], "totals"
            return out

        if self.state == "totals":
            if self.rx["totals_count"].search(full) or re.fullmatch(r"[\d\s£$€¥.,()-]+", full):
                self.totals_buf.append(full)
                return out
            self._flush_totals()
            self.state = "idle"

        first = row.words[0]
        if self.section and first.x0 < self.code_max and self.rx["txn_code"].match(first.text):
            out += self._close_txn()
            desc = row.text(self.code_max, self.amt_min)
            self.txn = Txn(first.text, [desc] if desc else [], to_dec(right) if _AMT.search(right) else None,
                           row.page, self.section)
            self.state = "txn"
            return out

        if self.state == "txn" and self.txn:
            if left:
                self.txn.lines.append(left)
            if right and self.txn.amount is None and _AMT.search(right):
                self.txn.amount = to_dec(right)
            return out

        if self.state != "header":
            self._unparsed(row, full)
        return out

    def _unparsed(self, row: Row, text: str):
        if not text:
            return
        self.unparsed += 1
        if self.unparsed <= 50:
            self.warn(f"unparsed row p{row.page}: {text[:100]}")

    def finish(self) -> list[dict]:
        out = self._close_ctx()
        self.warn(f"SUMMARY: {self.checks} validation checks, {self.errors} failed, "
                  f"{self.unparsed} unparsed rows")
        return out


class PdfStatementParser(BaseParser):
    def __init__(self, settings, request, pdf_cfg, profile: dict):
        super().__init__(settings, request)
        self.cfg, self.profile = pdf_cfg, profile
        self.engine = f"statement:{profile['name']}:{pdf_cfg.engine}"

    def _pages(self, path):
        if self.cfg.engine == "textract":
            from .pdf_textract import PdfTextractParser
            return textract_pages(PdfTextractParser(self.s, self.req, self.cfg)._blocks(path))
        return native_pages(path)

    def records(self, path) -> Iterator[dict]:
        m = StatementMachine(self.profile, self.warnings.append)
        tol = self.profile["geometry"]["row_tolerance"]
        for pno, words in enumerate(self._pages(path), start=1):
            for row in to_rows(words, pno, tol):
                yield from m.feed(row)
        yield from m.finish()

    def tables(self, path):
        fmap: dict[str, str] = self.profile["output"]["field_map"]
        headers = list(fmap) + ["record_type"] + (["_raw"] if self.profile["output"].get("include_raw") else [])

        def rows():
            for rec in self.records(path):
                row = [rec.get(src) for src in fmap.values()] + [rec["record_type"]]
                if "_raw" in headers:
                    row.append(rec)
                yield row
        yield RawTable(headers, rows(), 1, {"premapped": True, "profile": self.profile["name"]})


def detect_profile(path: str, profiles: dict[str, dict], engine: str) -> dict | None:
    """Match profile.detect against page-1 text (text layer; scanned docs need an explicit profile)."""
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            text = pdf.pages[0].extract_text() or "" if pdf.pages else ""
    except Exception:
        return None
    for prof in profiles.values():
        if prof.get("detect") and re.search(prof["detect"], text):
            return prof
    return None
