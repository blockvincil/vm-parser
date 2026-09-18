"""Column mapping (display header -> target column) and type coercion driven by columnDetails."""
from __future__ import annotations
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from dateutil import parser as dparser

from .config import TypingCfg, NumericCfg
from .models import ParseRequest

_NUM_CLEAN = re.compile(r"[,\s$€£¥]")
_CRDR = re.compile(r"\s*(CR|DR|C|D)\s*$", re.I)


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


class ColumnMapper:
    """
    Resolves each source header to a target column:
      1. columnPositionDetails source_display_column / source_column (case/space/punct-insensitive)
      2. columnDetails columnName
    Unmapped headers are kept under their normalized name unless include/exclude says otherwise.
    """
    def __init__(self, req: ParseRequest):
        self.req = req
        self.types = {c.columnName: c.columnType.lower() for c in req.columnDetails}
        self.alias: dict[str, str] = {}
        fid = req.fileImportDetails
        for c in req.columnDetails:
            self.alias[_norm(c.columnName)] = c.columnName
        self.positions = []
        if fid:
            for p in fid.columnPositionDetails:
                self.alias[_norm(p.source_column)] = p.source_column
                if p.source_display_column:
                    self.alias[_norm(p.source_display_column)] = p.source_column
                if p.field_type and p.source_column not in self.types:
                    self.types[p.source_column] = p.field_type.lower()
            self.positions = [p for p in fid.columnPositionDetails if p.start_position is not None]
        self.warnings: list[str] = []
        self._validate_contract()

    def _validate_contract(self):
        """Flag positionDetails columns that don't exist in columnDetails (common config drift)."""
        known = {c.columnName for c in self.req.columnDetails}
        if not known or not self.req.fileImportDetails:
            return
        for p in self.req.fileImportDetails.columnPositionDetails:
            if p.source_column not in known:
                self.warnings.append(
                    f"columnPositionDetails.source_column '{p.source_column}' not present in columnDetails")

    def map_headers(self, headers: Iterable[Any]) -> list[str]:
        out, seen = [], {}
        for i, h in enumerate(headers):
            if self.req.fetchByName is False or (self.req.fileImportDetails and
                                                self.req.fileImportDetails.isReadByColumnNumber):
                name = self.req.columnDetails[i].columnName if i < len(self.req.columnDetails) else f"col_{i+1}"
            else:
                key = _norm(h)
                name = self.alias.get(key) or (key if key else f"col_{i+1}")
            if name in seen:                       # duplicate header -> suffix
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 0
            out.append(name)
        unmapped = [h for h, n in zip(headers, out) if _norm(h) not in self.alias]
        if unmapped and self.alias:
            self.warnings.append(f"unmapped source headers kept as-is: {unmapped[:20]}")
        return out

    def keep(self, col: str) -> bool:
        fid = self.req.fileImportDetails
        if not fid or not fid.includeExcludeCols:
            return True
        listed = col in fid.includeExcludeCols
        return not listed if fid.isExcludeColumns else listed


class Coercer:
    def __init__(self, typing_cfg: TypingCfg, numeric: NumericCfg, custom_date_fmt: str | None):
        self.t = typing_cfg
        self.n = numeric
        self.fmts = ([custom_date_fmt] if custom_date_fmt else []) + typing_cfg.date_formats

    def numeric(self, v: Any) -> Decimal | None:
        if v is None:
            return None
        if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool):
            return Decimal(str(v))
        s = str(v).strip()
        if not s or s in ("-", "--"):
            return None
        neg = False
        if self.n.crdr_suffix:
            m = _CRDR.search(s)
            if m:                                     # sign unchanged; DB/CR indicator lives in its own column
                s = s[:m.start()]
        if self.n.parentheses_negative and s.startswith("(") and s.endswith(")"):
            s, neg = s[1:-1], True
        if self.n.trailing_minus and s.endswith("-"):
            s, neg = s[:-1], True
        s = _NUM_CLEAN.sub("", s)
        d = Decimal(s)                                # raises InvalidOperation
        return -d if neg else d

    def date(self, v: Any, with_time: bool) -> str | None:
        if v is None or str(v).strip() == "":
            return None
        if isinstance(v, datetime):
            return v.isoformat() if with_time else v.date().isoformat()
        if isinstance(v, date):
            return v.isoformat()
        s = str(v).strip()
        for f in self.fmts:
            try:
                dt = datetime.strptime(s, f)
                return dt.isoformat() if with_time else dt.date().isoformat()
            except ValueError:
                pass
        dt = dparser.parse(s, dayfirst=self.t.dayfirst)   # raises ValueError
        return dt.isoformat() if with_time else dt.date().isoformat()

    def coerce(self, value: Any, col_type: str | None) -> Any:
        if value is None or (isinstance(value, float) and value != value):   # NaN
            return None
        if isinstance(value, (dict, list)):          # nested payloads (_raw, tags) pass through
            return value
        t = (col_type or "varchar").lower()
        try:
            if t in ("numeric", "decimal", "number", "float", "double", "int", "integer", "bigint"):
                d = self.numeric(value)
                if d is None:
                    return None
                return int(d) if t in ("int", "integer", "bigint") else str(d)   # str keeps precision in JSON
            if t == "date":
                return self.date(value, with_time=False)
            if t in ("timestamp", "datetime", "timestamptz"):
                return self.date(value, with_time=True)
            if t in ("bool", "boolean"):
                return str(value).strip().lower() in ("1", "true", "y", "yes", "t")
            if isinstance(value, float) and value.is_integer():
                return str(int(value))                # 12345.0 from Excel -> "12345" (ids!)
            if isinstance(value, (datetime, date)):
                return value.isoformat()
            return str(value).strip()
        except (InvalidOperation, ValueError, OverflowError) as e:
            if self.t.on_coerce_error == "fail":
                raise ValueError(f"cannot coerce {value!r} to {t}: {e}") from e
            return None if self.t.on_coerce_error == "null" else str(value)
