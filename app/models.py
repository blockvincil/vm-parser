"""Kafka request contract for topic bd-ocr-flow. Unknown fields are kept (extra='allow')."""
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field


class ColumnDetail(BaseModel):
    columnName: str
    columnType: str = "varchar"

class ColumnPosition(BaseModel):
    model_config = ConfigDict(extra="allow")
    source_column: str
    source_display_column: Optional[str] = None
    field_type: Optional[str] = None
    start_position: Optional[int] = None
    end_position: Optional[int] = None

class FileImportDetails(BaseModel):
    model_config = ConfigDict(extra="allow")
    fileName: Optional[str] = None
    format: Optional[str] = None
    subPath: Optional[str] = None
    path: Optional[str] = None
    delimiter: Optional[str] = ","
    headersRow: Optional[int] = 1
    isReadByColumnNumber: bool = False
    isReadByPosition: bool = False
    columnPositionDetails: list[ColumnPosition] = []
    isExcludeColumns: bool = False
    includeExcludeCols: list[str] = []

class ParseRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    path: Optional[str] = None
    sourceName: Optional[str] = None
    fileName: Optional[str] = None
    format: Optional[str] = None                 # Excel | CSV | PDF | Superset
    delimiter: Optional[str] = ","
    headersRow: Optional[int] = 1
    fetchByName: bool = True
    sheetIdentifiers: list[str] = []
    columnDetails: list[ColumnDetail] = []
    fileImportDetails: Optional[FileImportDetails] = None
    customDateFormat: Optional[str] = ""
    fileSeqId: Optional[str] = None
    eventId: Optional[str] = None
    eventSeqId: Optional[int] = None
    reconId: Optional[str] = None
    reconType: Optional[str] = None
    ruleId: Optional[str] = None
    source: Optional[str] = None
    ocr_keywords: dict[str, str] = {}
    enrichFlow: bool = False
    # extensions (optional, not in the upstream contract)
    parserOptions: dict[str, Any] = Field(default_factory=dict)   # e.g. {"pdf": {"engine": "textract"}}
    batchSize: Optional[int] = None
    superset: Optional[dict[str, Any]] = None                     # {"chart_id": 12, "form_data": {...}}

    def resolved_format(self, file_path: str | None) -> str:
        f = (self.format or (self.fileImportDetails.format if self.fileImportDetails else "") or "").lower()
        if self.superset:
            return "superset"
        ext = (file_path or self.fileName or "").lower().rsplit(".", 1)[-1]
        if ext in ("xlsx", "xlsm", "xls"): return "excel"
        if ext in ("csv", "txt", "tsv"):   return "csv"
        if ext == "pdf":                   return "pdf"
        if f in ("excel", "xlsx", "xls"):  return "excel"
        if f in ("csv", "delimited"):      return "csv"
        if f == "pdf":                     return "pdf"
        raise ValueError(f"Cannot determine file format (format={self.format!r}, file={file_path!r})")
