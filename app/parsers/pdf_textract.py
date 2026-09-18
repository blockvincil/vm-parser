"""
Amazon Textract engine. Sync AnalyzeDocument handles single-page docs from bytes;
multi-page PDFs need async StartDocumentAnalysis via S3 (Textract limitation).
Output is normalised to the same RawTable shape as the native engine and then goes
through the SAME post-processing (stitching, wrapped rows, noise filtering).
"""
from __future__ import annotations
import time
import uuid
from collections import defaultdict
from .pdf_native import PdfNativeParser, _clean
from .base import RawTable


class PdfTextractParser(PdfNativeParser):
    engine = "textract"

    def _client(self, name):
        import boto3
        return boto3.client(name, region_name=self.cfg.textract.region)

    def _blocks(self, path) -> list[dict]:
        t = self.cfg.textract
        tx = self._client("textract")
        pages = _page_count(path)
        if t.mode == "sync" or (t.mode == "auto" and pages == 1):
            with open(path, "rb") as fh:
                return tx.analyze_document(Document={"Bytes": fh.read()},
                                           FeatureTypes=t.features)["Blocks"]
        if not t.s3_bucket:
            raise ValueError("Textract async mode (multi-page PDF) requires pdf.textract.s3_bucket")
        key = f"{t.s3_prefix}{uuid.uuid4()}.pdf"
        s3 = self._client("s3")
        s3.upload_file(path, t.s3_bucket, key)
        try:
            job = tx.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": t.s3_bucket, "Name": key}},
                FeatureTypes=t.features)["JobId"]
            deadline = time.time() + t.timeout_seconds
            while True:
                r = tx.get_document_analysis(JobId=job)
                if r["JobStatus"] == "SUCCEEDED":
                    break
                if r["JobStatus"] == "FAILED" or time.time() > deadline:
                    raise RuntimeError(f"Textract job {job}: {r.get('StatusMessage', 'timeout')}")
                time.sleep(t.poll_seconds)
            blocks, token = [], None
            while True:
                kw = {"JobId": job, **({"NextToken": token} if token else {})}
                r = tx.get_document_analysis(**kw)
                blocks += r["Blocks"]
                token = r.get("NextToken")
                if not token:
                    return blocks
        finally:
            s3.delete_object(Bucket=t.s3_bucket, Key=key)

    def tables(self, path):
        blocks = self._blocks(path)
        by_id = {b["Id"]: b for b in blocks}

        def text_of(b):
            words = []
            for rel in b.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for cid in rel["Ids"]:
                        c = by_id[cid]
                        if c["BlockType"] == "WORD":
                            words.append(c["Text"])
                        elif c["BlockType"] == "SELECTION_ELEMENT" and c["SelectionStatus"] == "SELECTED":
                            words.append("X")
            return _clean(" ".join(words))

        # raw tables per page -> reuse native stitching logic
        raw_by_page: dict[int, list[list[list[str]]]] = defaultdict(list)
        for b in blocks:
            if b["BlockType"] != "TABLE":
                continue
            grid: dict[tuple[int, int], str] = {}
            for rel in b.get("Relationships", []):
                if rel["Type"] != "CHILD":
                    continue
                for cid in rel["Ids"]:
                    cell = by_id[cid]
                    if cell["BlockType"] != "CELL":
                        continue
                    txt = text_of(cell)
                    # merged cells: replicate value across span so columns stay aligned
                    for dr in range(cell.get("RowSpan", 1)):
                        for dc in range(cell.get("ColumnSpan", 1)):
                            grid[(cell["RowIndex"] + dr, cell["ColumnIndex"] + dc)] = txt
            if grid:
                nr = max(r for r, _ in grid); nc = max(c for _, c in grid)
                raw_by_page[b.get("Page", 1)].append(
                    [[grid.get((r, c), "") for c in range(1, nc + 1)] for r in range(1, nr + 1)])

        lines = "\n".join(b["Text"] for b in blocks if b["BlockType"] == "LINE")
        kv = self.key_values(lines)
        kv.update(self._forms(blocks, by_id, text_of))

        fake_pages = sorted(raw_by_page)
        self._page_tables = lambda pno: raw_by_page[pno]          # plug into native stitcher
        found = False
        for header, rows, page in self._logical_tables(_Pages(fake_pages), range(len(fake_pages))):
            found = True
            yield RawTable(header, iter(self._post(header, rows)), 1, {"page": page, "doc": kv})
        if not found:
            self.warnings.append("Textract found no tables in document")

    @staticmethod
    def _forms(blocks, by_id, text_of) -> dict[str, str]:
        out = {}
        for b in blocks:
            if b["BlockType"] == "KEY_VALUE_SET" and "KEY" in b.get("EntityTypes", []):
                key = text_of(b)
                for rel in b.get("Relationships", []):
                    if rel["Type"] == "VALUE":
                        val = " ".join(text_of(by_id[v]) for v in rel["Ids"])
                        if key:
                            out[f"form:{key.rstrip(':')}"] = val
        return out


class _Pages:
    """Adapter so the native stitcher can index 'pages' that are Textract page numbers."""
    def __init__(self, nums): self.pages = nums


def _page_count(path) -> int:
    try:
        import pdfplumber
        with pdfplumber.open(path) as p:
            return len(p.pages)
    except Exception:
        return 2   # unknown -> force async path
