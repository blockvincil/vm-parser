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
        mode = self.out.get("balance_records", "no_activity")      # all | no_activity | none
        has_txns = bool(c.txns["CREDITS"] or c.txns["DEBITS"])
        if mode == "all" or (mode == "no_activity" and not has_txns):
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

class TransactionTableMachine:
    """
    Generic transaction-table statement parser.

    Intended for statements such as Charles Schwab where:

      - account / statement period are document-level fields
      - transactions appear inside a Transaction Details section
      - the date may appear only on the first transaction of a date
      - following transactions inherit the previous date
      - descriptions can continue across multiple visual rows
      - categories can wrap across rows, e.g. "Other" + "Activity"
    """

    def __init__(self, profile: dict, warn):
        self.p = profile
        self.warn = warn

        self.out = profile["output"]
        self.geometry = profile["geometry"]

        # Document-level fields from YAML.
        self.docrx = {
            k: re.compile(v, re.I)
            for k, v in profile.get("doc_fields", {}).items()
        }

        section = profile["transaction_section"]

        self.start_rx = re.compile(
            section["start"],
            re.I
        )

        self.continue_rx = (
            re.compile(section["continue"], re.I)
            if section.get("continue")
            else None
        )

        self.end_rx = [
            re.compile(v, re.I)
            for v in section.get("end", [])
        ]

        self.skip = [
            re.compile(v, re.I)
            for v in profile.get("skip_rows", [])
        ]

        # Document-level values:
        #
        # account_number
        # statement_period
        # period_start_date
        # period_end_date
        # opening_balance
        # closing_balance
        self.doc: dict[str, Any] = {}
        self.doc.update(
            profile.get("static_fields", {})
        )

        self.in_transactions = False

        # Track page changes while a transaction table is active.
        # Profiles with a continuation heading (for example Schwab)
        # should ignore repeated page headers until that heading appears.
        self.last_page: int | None = None
        self.awaiting_continuation = False

        # Schwab prints the date only on the first transaction
        # belonging to that date.
        self.current_date: str | None = None

        self.current_txn: dict[str, Any] | None = None

        # Used for rows such as:
        #
        # Other
        # Activity   Redemption ...
        self.pending_category: str | None = None

        self.seq = 0
        self.unparsed = 0

        # Set after we encounter Transactions - Summary.
        # The next value row contains Beginning Cash / Ending Cash.
        self.summary_pending = False

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""

        return re.sub(
            r"\s+",
            " ",
            value
        ).strip()

    def _column(
        self,
        row: Row,
        name: str
    ) -> str:
        """
        Read one logical column using x-coordinate boundaries
        configured in the YAML.

        Example:

            geometry:
              columns:
                date: [0.00, 0.055]
                category: [0.055, 0.125]
                action: [0.125, 0.24]
        """

        cols = self.geometry.get(
            "columns",
            {}
        )

        bounds = cols.get(name)

        if not bounds:
            return ""

        start, end = bounds

        return self._clean(
            row.text(start, end)
        )

    def _extract_doc_fields(
        self,
        full: str
    ):
        """
        Extract values configured under doc_fields.

        Each regex must expose the value using:

            (?P<v>...)
        """

        for key, rx in self.docrx.items():

            if key in self.doc:
                continue

            m = rx.search(full)

            if not m:
                continue

            try:
                value = m.group("v")
            except (IndexError, KeyError):
                continue

            if value:
                self.doc[key] = value.strip()

    def _parse_period(self):
        """
        Convert:

            July 1-31, 2026

        into:

            period_start_date = 2026-07-01
            period_end_date   = 2026-07-31
        """

        # Already parsed.
        if (
            self.doc.get("period_start_date")
            and self.doc.get("period_end_date")
        ):
            return

        value = self.doc.get(
            "statement_period"
        )

        if not value:
            return

        rx = re.compile(
            r"(?P<month>[A-Za-z]+)\s*"
            r"(?P<start>\d{1,2})\s*-\s*"
            r"(?P<end>\d{1,2}),\s*"
            r"(?P<year>\d{4})"
        )

        m = rx.search(value)

        if not m:
            return

        try:
            month = m.group("month")
            year = m.group("year")

            start = datetime.strptime(
                f"{month} "
                f"{m.group('start')}, "
                f"{year}",
                "%B %d, %Y"
            ).date()

            end = datetime.strptime(
                f"{month} "
                f"{m.group('end')}, "
                f"{year}",
                "%B %d, %Y"
            ).date()

            self.doc[
                "period_start_date"
            ] = start.isoformat()

            self.doc[
                "period_end_date"
            ] = end.isoformat()

        except ValueError:
            self.warn(
                f"unable to parse statement period: {value}"
            )

    @staticmethod
    def _parse_item_date(
        value: str | None,
        year: int | None
    ) -> str | None:
        """
        Convert:

            07/06

        into:

            2026-07-06

        using the statement-period year.
        """

        if not value:
            return None

        value = value.strip()

        if not re.fullmatch(
            r"\d{1,2}/\d{1,2}",
            value
        ):
            return None

        if year is None:
            return value

        try:
            dt = datetime.strptime(
                f"{value}/{year}",
                "%m/%d/%Y"
            )

            return dt.date().isoformat()

        except ValueError:
            return None

    def _statement_year(
        self
    ) -> int | None:

        end = self.doc.get(
            "period_end_date"
        )

        if not end:
            self._parse_period()

            end = self.doc.get(
                "period_end_date"
            )

        if not end:
            return None

        try:
            return datetime.strptime(
                end,
                "%Y-%m-%d"
            ).year

        except ValueError:
            return None

    def _dbcr(
        self,
        amount: Decimal | None
    ) -> str | None:

        if amount is None:
            return None

        if amount < 0:
            return self.out.get(
                "debit_value",
                "DB"
            )

        return self.out.get(
            "credit_value",
            "CR"
        )

    # ------------------------------------------------------------------
    # transaction handling
    # ------------------------------------------------------------------
    def _close_txn(
            self
    ) -> list[dict]:

        txn = self.current_txn
        self.current_txn = None

        if not txn:
            return []

        amount = txn.get("amount")

        # Do not emit transactions without an amount.
        if amount is None:
            return []

        # Ignore completely empty rows.
        if not any([
            txn.get("category"),
            txn.get("action"),
            txn.get("symbol"),
            txn.get("description_lines")
        ]):
            return []

        self.seq += 1

        description = self._clean(
            " ".join(
                txn.get(
                    "description_lines",
                    []
                )
            )
        )

        rec = {
            **self.doc,

            "record_type": "transaction",
            "seq": self.seq,

            "posting_date": txn.get("date"),

            "category": txn.get("category"),

            "action": txn.get("action"),

            "symbol": txn.get("symbol"),

            "description": description,

            "amount": str(amount),

            "dbcr": self._dbcr(amount),

            "page": txn.get("page")
        }

        return [rec]
    # def _close_txn(
    #     self
    # ) -> list[dict]:
    #
    #     txn = self.current_txn
    #     self.current_txn = None
    #
    #     if not txn:
    #         return []
    #
    #     amount = txn.get("amount")
    #
    #     # Ignore accidental / empty rows.
    #     #
    #     # FIX:
    #     # The transaction stores "description_lines",
    #     # not "description".
    #     if not any([
    #         txn.get("category"),
    #         txn.get("action"),
    #         txn.get("symbol"),
    #         txn.get("description_lines"),
    #         amount is not None
    #     ]):
    #         return []
    #
    #     self.seq += 1
    #
    #     description = self._clean(
    #         " ".join(
    #             txn.get(
    #                 "description_lines",
    #                 []
    #             )
    #         )
    #     )
    #
    #     rec = {
    #         **self.doc,
    #
    #         "record_type": "transaction",
    #         "seq": self.seq,
    #
    #         "posting_date": txn.get(
    #             "date"
    #         ),
    #
    #         "category": txn.get(
    #             "category"
    #         ),
    #
    #         "action": txn.get(
    #             "action"
    #         ),
    #
    #         "symbol": txn.get(
    #             "symbol"
    #         ),
    #
    #         "description": description,
    #
    #         "amount": (
    #             str(amount)
    #             if amount is not None
    #             else None
    #         ),
    #
    #         "dbcr": self._dbcr(
    #             amount
    #         ),
    #
    #         "page": txn.get(
    #             "page"
    #         )
    #     }
    #
    #     return [rec]

    def _start_txn(
        self,
        row: Row,
        date_text: str,
        category: str,
        action: str,
        symbol: str,
        description: str,
        amount_text: str
    ) -> list[dict]:

        # Finish the previous transaction first.
        out = self._close_txn()

        # If this transaction has a date,
        # update the carried-forward date.
        if date_text:

            parsed_date = self._parse_item_date(
                date_text,
                self._statement_year()
            )

            if parsed_date:
                self.current_date = parsed_date

        amount = None

        if amount_text:

            amount_match = _AMT.search(
                amount_text
            )

            if amount_match:
                amount = to_dec(
                    amount_match.group(0)
                )

        self.current_txn = {
            "date": self.current_date,

            "category": (
                category or None
            ),

            "action": (
                action or None
            ),

            "symbol": (
                symbol or None
            ),

            "description_lines": (
                [description]
                if description
                else []
            ),

            "amount": amount,

            "page": row.page
        }

        return out

    # ------------------------------------------------------------------
    # cash summary
    # ------------------------------------------------------------------

    def _process_cash_summary(
        self,
        row: Row
    ) -> bool:
        """
        Extract Beginning Cash / Ending Cash.

        Important:
        Only parse real money values matching _AMT.

        This prevents dates such as 07/01 from being
        interpreted by to_dec() as 701.
        """

        opening = self._column(
            row,
            "summary_opening"
        )

        closing = self._column(
            row,
            "summary_closing"
        )

        opening_match = (
            _AMT.search(opening)
            if opening
            else None
        )

        closing_match = (
            _AMT.search(closing)
            if closing
            else None
        )

        opening_amt = (
            to_dec(
                opening_match.group(0)
            )
            if opening_match
            else None
        )

        closing_amt = (
            to_dec(
                closing_match.group(0)
            )
            if closing_match
            else None
        )

        if opening_amt is not None:
            self.doc[
                "opening_balance"
            ] = str(opening_amt)

        if closing_amt is not None:
            self.doc[
                "closing_balance"
            ] = str(closing_amt)

        found = (
            opening_amt is not None
            or closing_amt is not None
        )

        if found:
            self.summary_pending = False

        return found

    # ------------------------------------------------------------------
    # main row processing
    # ------------------------------------------------------------------

    def feed(
        self,
        row: Row
    ) -> list[dict]:

        full = self._clean(
            row.text(0, 1)
        )

        # --------------------------------------------------------------
        # page-break handling
        # --------------------------------------------------------------
        #
        # Schwab repeats account/statement headers at the top of page 5
        # before "Transaction Details (continued)".  Without this guard,
        # those page-header rows can be appended to the previous transaction
        # or even emitted as fake transactions.

        if self.last_page is None:
            self.last_page = row.page

        elif row.page != self.last_page:
            self.last_page = row.page

            if self.in_transactions and self.continue_rx:
                self.in_transactions = False
                self.awaiting_continuation = True

        if not full:
            return []

        # --------------------------------------------------------------
        # document-level fields
        # --------------------------------------------------------------

        self._extract_doc_fields(
            full
        )

        self._parse_period()

        # --------------------------------------------------------------
        # page continuation gate
        # --------------------------------------------------------------
        #
        # While waiting for the repeated transaction heading on the new
        # page, ignore page number/account/statement-period header rows.

        if self.awaiting_continuation:

            # Some PDFs split "Transaction Details (continued)" into
            # two visual rows: "Transaction Details" and "(continued)".
            # Treat either the normal section heading or the explicit
            # continuation heading as the resume point.
            if (
                self.start_rx.search(full)
                or (
                    self.continue_rx
                    and self.continue_rx.search(full)
                )
            ):
                self.awaiting_continuation = False
                self.in_transactions = True

            return []

        # --------------------------------------------------------------
        # ignored rows
        # --------------------------------------------------------------

        if any(
            rx.search(full)
            for rx in self.skip
        ):
            return []

        # --------------------------------------------------------------
        # cash summary
        # --------------------------------------------------------------

        summary = self.p.get(
            "cash_summary",
            {}
        )

        if summary:

            start_rx = summary.get(
                "start"
            )

            if (
                start_rx
                and re.search(
                    start_rx,
                    full,
                    re.I
                )
            ):
                self.summary_pending = True

                # Do not try to parse the heading itself.
                return []

            if self.summary_pending:

                if self._process_cash_summary(
                    row
                ):
                    return []

        # --------------------------------------------------------------
        # transaction section start / continuation
        # --------------------------------------------------------------

        if self.start_rx.search(full):

            self.in_transactions = True
            return []

        if (
            self.continue_rx
            and self.continue_rx.search(full)
        ):

            self.in_transactions = True
            return []

        # --------------------------------------------------------------
        # transaction section end
        # --------------------------------------------------------------

        if (
            self.in_transactions
            and any(
                rx.search(full)
                for rx in self.end_rx
            )
        ):
            self.in_transactions = False

            return self._close_txn()

        if not self.in_transactions:
            return []

        # --------------------------------------------------------------
        # repeated table header
        # --------------------------------------------------------------

        if re.search(
            r"\bDate\b.*\bCategory\b.*\bAction\b",
            full,
            re.I
        ):
            return []

        # --------------------------------------------------------------
        # extract configured columns
        # --------------------------------------------------------------

        date_text = self._column(
            row,
            "date"
        )

        category = self._column(
            row,
            "category"
        )

        action = self._column(
            row,
            "action"
        )

        symbol = self._column(
            row,
            "symbol"
        )

        description = self._column(
            row,
            "description"
        )

        amount_text = self._column(
            row,
            "amount"
        )

        date_is_valid = bool(
            re.fullmatch(
                r"\d{1,2}/\d{1,2}",
                date_text or ""
            )
        )

        # --------------------------------------------------------------
        # wrapped category
        # --------------------------------------------------------------
        #
        # Schwab can have:
        #
        #     Other
        #     Activity   Redemption ...
        #
        # "Other" belongs to the NEXT transaction.
        # --------------------------------------------------------------

        category_continuations = {
            str(v).strip().lower()
            for v in self.p.get(
                "transaction_section", {}
            ).get(
                "category_continuations", []
            )
        }

        # Profile-defined category continuation.
        #
        # Schwab renders the single logical category "Other Activity" as:
        #
        #     Other      Redemption ...
        #     Activity   **MATURED**
        #
        # The second visual row can also contain description text, so this
        # check intentionally allows description while requiring no new
        # date/action/symbol/amount.
        is_category_continuation = bool(
            self.current_txn
            and category
            and category.strip().lower() in category_continuations
            and not date_is_valid
            and not action
            and not symbol
            and not amount_text
        )

        if is_category_continuation:

            self.current_txn["category"] = self._clean(
                f"{self.current_txn.get('category') or ''} "
                f"{category}"
            )

            if description:
                self.current_txn[
                    "description_lines"
                ].append(description)

            return []

        only_category = bool(
            category
            and not date_is_valid
            and not action
            and not symbol
            and not description
            and not amount_text
        )

        if only_category:

            # Generic fallback for layouts where a category appears alone
            # before the next transaction row.
            out = self._close_txn()

            self.pending_category = (
                f"{self.pending_category or ''} "
                f"{category}"
            ).strip()

            return out

        # If we previously collected:
        #
        # Other
        #
        # and this row contains:
        #
        # Activity  Redemption ...
        #
        # combine them.
        if self.pending_category:

            category = (
                f"{self.pending_category} "
                f"{category or ''}"
            ).strip()

            self.pending_category = None

        # --------------------------------------------------------------
        # identify new transaction
        # --------------------------------------------------------------

        # A transaction may start without a date.
        #
        # Example:
        #
        # 07/06 Sale ...
        #       Withdrawal Funds Paid ...
        #
        # Withdrawal inherits 07/06.
        new_txn = bool(
            date_is_valid
            or category
            or action
        )

        if new_txn:

            return self._start_txn(
                row=row,

                date_text=(
                    date_text
                    if date_is_valid
                    else ""
                ),

                category=category,
                action=action,
                symbol=symbol,
                description=description,
                amount_text=amount_text
            )

        # --------------------------------------------------------------
        # continuation of previous transaction
        # --------------------------------------------------------------

        if self.current_txn:

            continuation = " ".join(
                value
                for value in [
                    symbol,
                    description
                ]
                if value
            ).strip()

            if continuation:

                self.current_txn[
                    "description_lines"
                ].append(
                    continuation
                )

            # Sometimes the amount is printed
            # on a following visual row.
            if (
                self.current_txn.get(
                    "amount"
                ) is None
                and amount_text
            ):

                amount_match = _AMT.search(
                    amount_text
                )

                if amount_match:

                    amount = to_dec(
                        amount_match.group(0)
                    )

                    if amount is not None:

                        self.current_txn[
                            "amount"
                        ] = amount

            return []

        # --------------------------------------------------------------
        # unmatched transaction row
        # --------------------------------------------------------------

        self.unparsed += 1

        if self.unparsed <= 50:

            self.warn(
                f"unparsed transaction row "
                f"p{row.page}: "
                f"{full[:120]}"
            )

        return []

    # ------------------------------------------------------------------
    # finish
    # ------------------------------------------------------------------

    def finish(
        self
    ) -> list[dict]:

        out = self._close_txn()

        self.warn(
            f"SUMMARY: transaction-table parser, "
            f"{self.seq} transactions, "
            f"{self.unparsed} unparsed rows"
        )

        return out

class PdfStatementParser(BaseParser):

    def __init__(
        self,
        settings,
        request,
        pdf_cfg,
        profile: dict
    ):
        super().__init__(
            settings,
            request
        )

        self.cfg = pdf_cfg
        self.profile = profile

        self.engine = (
            f"statement:"
            f"{profile['name']}:"
            f"{pdf_cfg.engine}"
        )

    def _pages(self, path):

        if self.cfg.engine == "textract":

            from .pdf_textract import PdfTextractParser

            return textract_pages(
                PdfTextractParser(
                    self.s,
                    self.req,
                    self.cfg
                )._blocks(path)
            )

        return native_pages(path)

    def records(
        self,
        path
    ) -> Iterator[dict]:

        mode = self.profile.get(
            "mode",
            "bai_statement"
        )

        if mode == "transaction_table":

            m = TransactionTableMachine(
                self.profile,
                self.warnings.append
            )

        else:

            m = StatementMachine(
                self.profile,
                self.warnings.append
            )

        tol = self.profile[
            "geometry"
        ]["row_tolerance"]

        for pno, words in enumerate(
            self._pages(path),
            start=1
        ):

            for row in to_rows(
                words,
                pno,
                tol
            ):

                yield from m.feed(
                    row
                )

        yield from m.finish()

    def tables(self, path):

        fmap: dict[str, str] = (
            self.profile[
                "output"
            ]["field_map"]
        )

        headers = (
            list(fmap)
            + ["record_type"]
            + (
                ["_raw"]
                if self.profile[
                    "output"
                ].get("include_raw")
                else []
            )
        )

        def rows():

            for rec in self.records(path):

                row = [
                    rec.get(src)
                    for src
                    in fmap.values()
                ] + [
                    rec["record_type"]
                ]

                if "_raw" in headers:
                    row.append(rec)

                yield row

        yield RawTable(
            headers,
            rows(),
            1,
            {
                "premapped": True,
                "profile": self.profile[
                    "name"
                ]
            }
        )

def detect_profile(
    path: str,
    profiles: dict[str, dict],
    engine: str
) -> dict | None:
    """
    Match profile.detect against the first page text.

    Example:

        northern_trust_daily.yaml
            detect: "Northern Trust Treasury Passport"

        charles_schwab.yaml
            detect: "Charles Schwab"

    PDFs with a text layer can be detected automatically.
    Scanned PDFs should normally specify the profile explicitly.
    """

    try:
        import pdfplumber

        with pdfplumber.open(path) as pdf:

            if not pdf.pages:
                return None

            text = (
                pdf.pages[0].extract_text()
                or ""
            )

    except Exception:
        return None

    for profile in profiles.values():

        detect = profile.get(
            "detect"
        )

        if not detect:
            continue

        if re.search(
            detect,
            text,
            re.I
        ):
            return profile

    return None
