import csv
from itertools import islice
from .base import BaseParser, RawTable


class CsvParser(BaseParser):
    engine = "csv-stream"

    def tables(self, path):
        delim = (self.req.fileImportDetails.delimiter if self.req.fileImportDetails else None) \
                or self.req.delimiter or ","
        delim = "\t" if delim in ("\\t", "tab") else delim
        header_row = max(int(self.req.headersRow or 1), 1)
        f = open(path, newline="", encoding=_sniff_encoding(path))
        reader = csv.reader(f, delimiter=delim)
        for _ in range(header_row - 1):
            next(reader, None)
        headers = next(reader, None)
        if headers is None:
            f.close()
            return
        if headers and headers[0].startswith("\ufeff"):
            headers[0] = headers[0][1:]

        def rows():
            try:
                for r in reader:
                    if any(c.strip() for c in r):
                        yield r
            finally:
                f.close()
        yield RawTable(headers=headers, rows=rows(), first_row_no=header_row + 1)


def _sniff_encoding(path: str) -> str:
    with open(path, "rb") as fh:
        head = fh.read(65536)
    for enc in ("utf-8-sig", "utf-8"):
        try:
            head.decode(enc)
            return enc
        except UnicodeDecodeError:
            pass
    return "latin-1"
