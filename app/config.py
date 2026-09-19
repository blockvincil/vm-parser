"""Layered config: config.yaml  <-  env vars DP__SECTION__KEY."""
from __future__ import annotations
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any
import yaml
from pydantic import BaseModel, Field


class BatchCfg(BaseModel):
    size: int = Field(1000, gt=0)

class PgCfg(BaseModel):
    """Either a full dsn, or the individual fields (used when dsn is empty)."""
    dsn: str = ""
    host: str = "localhost"
    port: int = 5432
    database: str = "docparser"
    user: str = "docparser"
    password: str = ""
    sslmode: str = "prefer"            # disable | prefer | require | verify-ca | verify-full
    sslrootcert: str = ""
    schema_: str = Field("public", alias="schema")   # tables are created here
    application_name: str = "docparser"
    connect_timeout: int = 10
    pool_min: int = 2
    pool_max: int = 10
    model_config = {"populate_by_name": True}

    def conninfo(self) -> str:
        if self.dsn:
            return self.dsn
        from psycopg.conninfo import make_conninfo
        extra = {"sslrootcert": self.sslrootcert} if self.sslrootcert else {}
        return make_conninfo(host=self.host, port=self.port, dbname=self.database, user=self.user,
                             password=self.password, sslmode=self.sslmode,
                             application_name=self.application_name,
                             connect_timeout=self.connect_timeout, **extra)

    def safe(self) -> str:
        """For logs: never print the password (keyword or URL form)."""
        info = re.sub(r"(password=)(\S+)", r"\1***", self.conninfo())
        return re.sub(r"(://[^:/@\s]+:)([^@\s]+)(@)", r"\1***\3", info)

class KafkaCfg(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"   # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
    sasl_mechanism: str = ""               # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER
    sasl_username: str = ""
    sasl_password: str = ""
    ssl_ca_location: str = ""              # CA bundle (PEM) for SSL/SASL_SSL
    ssl_certificate_location: str = ""     # mTLS client cert
    ssl_key_location: str = ""
    ssl_key_password: str = ""
    client_id: str = "docparser"
    extra: dict[str, Any] = {}             # any other librdkafka property, passed through as-is
    topic: str = "bd-ocr-flow"
    group_id: str = "docparser-workers"
    result_topic: str = ""
    dlq_topic: str = ""
    max_poll_interval_ms: int = 1_800_000
    auto_offset_reset: str = "earliest"

    def client_conf(self) -> dict[str, Any]:
        """Common librdkafka config for consumers and producers."""
        c: dict[str, Any] = {"bootstrap.servers": self.bootstrap_servers,
                             "security.protocol": self.security_protocol,
                             "client.id": self.client_id}
        opt = {"sasl.mechanism": self.sasl_mechanism, "sasl.username": self.sasl_username,
               "sasl.password": self.sasl_password, "ssl.ca.location": self.ssl_ca_location,
               "ssl.certificate.location": self.ssl_certificate_location,
               "ssl.key.location": self.ssl_key_location, "ssl.key.password": self.ssl_key_password}
        c.update({k: v for k, v in opt.items() if v})
        c.update(self.extra)
        return c

    def consumer_conf(self) -> dict[str, Any]:
        return self.client_conf() | {"group.id": self.group_id, "enable.auto.commit": False,
                                     "auto.offset.reset": self.auto_offset_reset,
                                     "max.poll.interval.ms": self.max_poll_interval_ms}

    def producer_conf(self) -> dict[str, Any]:
        return self.client_conf() | {"enable.idempotence": True, "acks": "all"}

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


_STRING_KEYS = {"password", "sasl_password", "ssl_key_password", "dsn", "user", "sasl_username", "database"}


def _apply_env(d: dict[str, Any], prefix: str = "DP__") -> dict[str, Any]:
    for key, val in os.environ.items():
        if not key.startswith(prefix):
            continue
        parts = key[len(prefix):].lower().split("__")
        node = d
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        if parts[-1] in _STRING_KEYS:              # never let YAML turn a password into int/bool
            node[parts[-1]] = val
            continue
        try:
            node[parts[-1]] = yaml.safe_load(val)   # "500" -> 500, "true" -> True, "[a]" -> list
        except yaml.YAMLError:
            node[parts[-1]] = val
    return d


ROOT = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path | None = None) -> None:
    """Minimal .env loader (KEY=VALUE, # comments, optional quotes). Real env vars win."""
    path = path or Path(os.getenv("DP_ENV_FILE", ROOT / ".env"))
    if not Path(path).is_file():
        return
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip().removeprefix("export ").strip(), v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
            v = v[1:-1]
        os.environ.setdefault(k, v)


@lru_cache
def get_settings() -> Settings:
    load_dotenv()
    path = Path(os.getenv("DP_CONFIG", ROOT / "config" / "config.yaml"))
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
