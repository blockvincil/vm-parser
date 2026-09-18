"""Engine-independent word source: yields pages of words with coordinates as page FRACTIONS.
native  -> pdfplumber character geometry
textract-> Textract WORD blocks (BoundingBox is already normalised)"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterator


@dataclass
class Word:
    text: str
    x0: float
    x1: float
    top: float


@dataclass
class Row:
    words: list[Word]
    top: float
    page: int

    def text(self, x_min=0.0, x_max=9.0) -> str:
        return " ".join(w.text for w in self.words if x_min <= w.x0 < x_max).strip()


def native_pages(path: str) -> Iterator[list[Word]]:
    import pdfplumber
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:              # one page in memory at a time
            W, H = float(page.width), float(page.height)
            yield [Word(w["text"], w["x0"] / W, w["x1"] / W, w["top"] / H)
                   for w in page.extract_words(keep_blank_chars=False, use_text_flow=False)]
            page.flush_cache()


def textract_pages(blocks: list[dict]) -> Iterator[list[Word]]:
    pages: dict[int, list[Word]] = defaultdict(list)
    for b in blocks:
        if b["BlockType"] == "WORD":
            bb = b["Geometry"]["BoundingBox"]
            pages[b.get("Page", 1)].append(Word(b["Text"], bb["Left"], bb["Left"] + bb["Width"], bb["Top"]))
    for p in sorted(pages):
        yield pages[p]


def to_rows(words: list[Word], page_no: int, tol: float) -> list[Row]:
    """Group words into visual rows by vertical position, then order left-to-right.
    This is what fixes the reading-order problems of plain text extraction
    (amounts printed in the right column, codes printed in the left column)."""
    rows: list[Row] = []
    for w in sorted(words, key=lambda w: (w.top, w.x0)):
        if rows and abs(w.top - rows[-1].top) <= tol:
            rows[-1].words.append(w)
        else:
            rows.append(Row([w], w.top, page_no))
    for r in rows:
        r.words.sort(key=lambda w: w.x0)
    return rows
