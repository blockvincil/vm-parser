"""
Native PDF table extraction (pdfplumber) with the fixes that matter for financial statements:
  * strategy 'auto'  : ruled tables via 'lines', falls back to whitespace 'text' alignment
  * header detection : first row whose cells are mostly non-numeric
  * stitching        : a table continuing onto the next page (same column count, header
                       repeated or absent) becomes ONE logical table
  * wrapped rows     : a description spilling to a 2nd line (empty key column) is merged up
  * noise rows       : page footers / 'Total' / 'Continued' filtered by regex
  * key-values       : document-level fields (statement date, balances) + ocr_keywords
"""
from __future__ import annotations
import re
from typing import Any, Iterator
from .base import BaseParser, RawTable

_NUMERICISH = re.compile(r"^[\s\-+(]*[\d,.\s]+\)?\s*(CR|DR)?-?$", re.I)


def _clean(c: Any) -> str:
    return re.sub(r"\s+", " ", str(c)).strip() if c is not None else ""


def parse_pages(spec: str, n: int) -> list[int]:
    if not spec or spec == "all":
        return list(range(n))
    out = []
    for part in spec.split(","):
        a, _, b = part.partition("-")
        out.extend(range(int(a) - 1, int(b or a)))
    return [p for p in out if 0 <= p < n]


class PdfNativeParser(BaseParser):
    engine = "pdfplumber"

    def __init__(self, settings, request, pdf_cfg):
        super().__init__(settings, request)
        self.cfg = pdf_cfg
        self.skip = [re.compile(p, re.I) for p in pdf_cfg.native.skip_row_patterns]

    # ---------- table extraction ----------
    def _settings(self, strategy: str) -> dict:
        n = self.cfg.native
        base = {"snap_tolerance": n.snap_tolerance, "join_tolerance": n.join_tolerance,
                "intersection_tolerance": n.intersection_tolerance, "text_x_tolerance": n.text_x_tolerance}
        if strategy == "lines":
            base.update(vertical_strategy="lines", horizontal_strategy="lines")
        else:
            base.update(vertical_strategy="text", horizontal_strategy="text",
                        min_words_vertical=2, min_words_horizontal=1)
        return base

    def _page_tables(self, page) -> list[list[list[str]]]:
        n = self.cfg.native
        strategies = ["lines", "text"] if n.table_strategy == "auto" else [n.table_strategy]
        for strat in strategies:
            tables = []
            for t in page.extract_tables(self._settings(strat)):
                rows = [[_clean(c) for c in r] for r in t if r and any(_clean(c) for c in r)]
                if len(rows) >= n.min_table_rows and max(len(r) for r in rows) >= n.min_table_cols:
                    tables.append(rows)
            if tables:
                return tables
        return []

    @staticmethod
    def _is_header(row: list[str]) -> bool:
        filled = [c for c in row if c]
        if len(filled) < max(2, len(row) // 2):
            return False
        return sum(bool(_NUMERICISH.match(c)) for c in filled) / len(filled) < 0.2

    def _split_header(self, rows):
        for i, r in enumerate(rows[:5]):              # header is within first 5 rows (title rows above)
            if self._is_header(r):
                return r, rows[i + 1:]
        return [f"col_{i+1}" for i in range(len(rows[0]))], rows

    def _logical_tables(self, pdf, pages) -> Iterator[tuple[list[str], list[list[str]], int]]:
        n = self.cfg.native
        cur_h, cur_rows, cur_page = None, [], None
        for pno in pages:
            for t in self._page_tables(pdf.pages[pno]):
                width = max(len(r) for r in t)
                t = [r + [""] * (width - len(r)) for r in t]
                h, body = self._split_header(t)
                continues = (n.stitch_across_pages and cur_h is not None and len(cur_h) == width
                             and (h == cur_h or h[0].startswith("col_")))
                if continues:                            # repeated header (if any) already split off
                    cur_rows.extend(body)
                else:
                    if cur_h is not None:
                        yield cur_h, cur_rows, cur_page
                    cur_h, cur_rows, cur_page = h, body, pno + 1
        if cur_h is not None:
            yield cur_h, cur_rows, cur_page

    def _post(self, header, rows):
        n = self.cfg.native
        k = min(n.key_column_index, len(header) - 1)
        out: list[list[str]] = []
        for r in rows:
            line = " ".join(r)
            if any(p.search(line) for p in self.skip):
                continue
            if n.drop_repeated_headers and r == header:
                continue
            # wrapped line: key col empty and at most the text columns filled -> append to previous row
            if n.merge_wrapped_rows and out and not r[k] and \
                    not any(_NUMERICISH.match(c) for c in r if c):
                prev = out[-1]
                for i, c in enumerate(r):
                    if c:
                        prev[i] = f"{prev[i]} {c}".strip()
                continue
            out.append(list(r))
        return out

    # ---------- document-level values ----------
    def key_values(self, text: str) -> dict[str, str]:
        kv = {}
        if self.cfg.key_values.enabled:
            for name, pat in self.cfg.key_values.patterns.items():
                m = re.search(pat, text, re.I)
                if m:
                    kv[name] = _clean(m.group(1))
        for field, kw in (self.req.ocr_keywords or {}).items():
            kw = kw.strip().strip("'\"")
            m = re.search(rf"{re.escape(kw)}\s*[:\-]?\s*(.+)", text, re.I)
            if m:
                kv[field] = _clean(m.group(1).splitlines()[0])
            else:
                self.warnings.append(f"ocr keyword '{kw}' ({field}) not found in PDF text")
        return kv

    def tables(self, path):
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            pages = parse_pages(self.cfg.native.pages, len(pdf.pages))
            text = "\n".join((pdf.pages[p].extract_text() or "") for p in pages)
            if not text.strip():
                self.warnings.append("PDF has no text layer (scanned?) - use pdf.engine=textract")
            kv = self.key_values(text)
            found = False
            for header, rows, page in self._logical_tables(pdf, pages):
                found = True
                rows = self._post(header, rows)
                yield RawTable(header, iter(rows), 1, {"page": page, "doc": kv})
            if not found:
                self.warnings.append("no tables detected; try table_strategy=text or textract")
