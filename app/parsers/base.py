from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Iterator


@dataclass
class RawTable:
    """Header + row iterator produced by any parser, before mapping/coercion."""
    headers: list[Any]
    rows: Iterator[list[Any]]          # streamed: never materialize the whole file
    first_row_no: int = 1              # 1-based source row of first data row
    meta: dict[str, Any] = field(default_factory=dict)   # sheet name, page, doc-level key-values


class BaseParser:
    engine = "base"
    def __init__(self, settings, request):
        self.s = settings
        self.req = request
        self.warnings: list[str] = []
    def tables(self, path: str) -> Iterator[RawTable]:
        raise NotImplementedError
