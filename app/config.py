"""Layered config: config.yaml  <-  env vars DP__SECTION__KEY."""
from __future__ import annotations
import os
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml
from pydantic import BaseModel, Field


class BatchCfg(BaseModel):
    size: int = Field(1000, gt=0)

class PgCfg(BaseModel):
    dsn: str
    pool_min: int = 2
    pool_max: int = 10

class KafkaCfg(BaseModel):
    bootstrap_servers: str
    topic: str = "bd-ocr-flow"
    group_id: str = "docparser-workers"
    result_topic: str = ""
    dlq_topic: str = ""
    max_poll_interval_ms: int = 1_800_000

class StorageCfg(BaseModel):
    allowed_roots: list[str] = ["/blocdata"]
    wildcard_pick: str = "latest"

class TypingCfg(BaseModel):
    date_formats: list[str] = ["%Y-%m-%d"]
    dayfirst: bool = True
    on_coerce_error: str = "keep_raw"

class PdfNativeCfg(BaseModel):
    table_strategy: str = "auto"
    snap_tolerance: float = 3
    join_tolerance: float = 3
    intersection_tolerance: float = 3
    text_x_tolerance: float = 2
    min_table_rows: int = 2
    min_table_cols: int = 2
    stitch_across_pages: bool = True
    drop_repeated_headers: bool = True
    merge_wrapped_rows: bool = True
    key_column_index: int = 0
    skip_row_patterns: list[str] = []
    pages: str = "all"

class TextractCfg(BaseModel):
    region: str = "us-east-1"
    mode: str = "auto"
    s3_bucket: str = ""
    s3_prefix: str = "docparser/"
    features: list[str] = ["TABLES", "FORMS"]
    poll_seconds: float = 3
    timeout_seconds: int = 900

class NumericCfg(BaseModel):
    parentheses_negative: bool = True
    trailing_minus: bool = True
    crdr_suffix: bool = True

class KeyValueCfg(BaseModel):
    enabled: bool = True
    patterns: dict[str, str] = {}

class PdfCfg(BaseModel):
    engine: str = "native"          # word/text source: native | textract
    layout: str = "auto"            # auto | statement | table
    profile: str = ""               # force a statement profile by name (else auto-detect)
    profiles_dir: str = "config/pdf_profiles"
    native: PdfNativeCfg = PdfNativeCfg()
    textract: TextractCfg = TextractCfg()
    numeric: NumericCfg = NumericCfg()
    key_values: KeyValueCfg = KeyValueCfg()

class SupersetCfg(BaseModel):
    base_url: str = "http://superset:8088"
    username: str = "admin"
    password: str = "admin"
    provider: str = "db"
    verify_tls: bool = True
    timeout_seconds: int = 120
    row_limit: int = 1_000_000

class Settings(BaseModel):
    batch: BatchCfg
    postgres: PgCfg
    kafka: KafkaCfg
    storage: StorageCfg = StorageCfg()
    typing: TypingCfg = TypingCfg()
    pdf: PdfCfg = PdfCfg()
    superset: SupersetCfg = SupersetCfg()


def _apply_env(d: dict[str, Any], prefix: str = "DP__") -> dict[str, Any]:
    for key, val in os.environ.items():
        if not key.startswith(prefix):
            continue
        parts = key[len(prefix):].lower().split("__")
        node = d
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        try:
            node[parts[-1]] = yaml.safe_load(val)   # "500" -> 500, "true" -> True, "[a]" -> list
        except yaml.YAMLError:
            node[parts[-1]] = val
    return d


@lru_cache
def get_settings() -> Settings:
    path = Path(os.getenv("DP_CONFIG", Path(__file__).parent.parent / "config" / "config.yaml"))
    raw = yaml.safe_load(path.read_text()) or {}
    return Settings.model_validate(_apply_env(raw))


def merged(base: BaseModel, overrides: dict | None) -> Any:
    """Per-request overrides, e.g. {"engine": "textract"} from the Kafka message."""
    if not overrides:
        return base
    data = base.model_dump()
    def deep(a, b):
        for k, v in b.items():
            a[k] = deep(a.get(k, {}), v) if isinstance(v, dict) and isinstance(a.get(k), dict) else v
        return a
    return type(base).model_validate(deep(data, overrides))
