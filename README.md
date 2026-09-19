# DocParser

Parses **CSV / Excel / PDF / Superset chart data** into JSON and stores it in Postgres in fixed-size
batches (10,000 rows @ batch size 1,000 → 10 rows in `parsed_batches`). Jobs arrive on Kafka topic
`bd-ocr-flow` (or HTTP), and scale horizontally by adding worker replicas.

```
Kafka bd-ocr-flow ─► worker(s) ─┐                       ┌─► parse_jobs      (1 row / file, status, warnings)
HTTP upload / webhook ─► api ───┼─► resolve ─► parser ─►│
                                │   (wildcard,  csv│xlsx│pdf(native|textract)│superset
                                │    root guard)  ─► map headers ─► coerce types ─► batch
                                                        └─► parsed_batches  (1 row / batch, JSONB)
Results ─► bd-ocr-flow-result      Failures ─► bd-ocr-flow-dlq
```

## Run against your own Postgres and Kafka
```powershell
copy .env.example .env          # fill in host, user, password, brokers (and SASL/SSL if used)
python -m app.check --init-db   # verifies both connections, creates tables in your schema, lists topics
python -m app.worker            # consume bd-ocr-flow
uvicorn app.api:app --port 8000 # HTTP API
```
Docker: `docker compose up -d --build` reads the same `.env`. Postgres/Kafka running on your own machine →
use `host.docker.internal` as the host in `.env`. Bundled throwaway infra is still available with
`docker compose --profile local up -d`.

Settings resolve in this order (later wins): `config/config.yaml` → `.env` → real environment variables.
Any key can be set as `DP__<SECTION>__<KEY>`, e.g. `DP__POSTGRES__SCHEMA=recon`.

| | Supported |
|---|---|
| Postgres | full `dsn` **or** host/port/database/user/password; `sslmode` + `sslrootcert`; own `schema` (created if missing, tables live there); pool size |
| Kafka | multiple brokers; `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`; PLAIN / SCRAM-SHA-256/512; CA + mTLS client certs; any other librdkafka property via `kafka.extra` |

The DB user needs `CREATE` on the schema for the first `--init-db`, or have a DBA run `sql/init.sql` once.
Topics must exist unless your cluster auto-creates them; `app.check` warns if they don't.
Passwords are never logged.

## Local testing with fixtures (no Postgres / Kafka)
Put any file in `tests/fixtures/`; output lands in `tests/fixtures/outputs/<name_ext>/` (e.g. `Book12_xlsx/`):
`batch_0001.json`, `batch_0002.json`, ... (same shape as a `parsed_batches` row) plus `_job.json`
(status, counts, warnings, validation summary). The folder is wiped on each run.

CLI (quickest):
```powershell
python -m app.devtools                                   # list fixtures
python -m app.devtools northern_trust_sample.pdf
python -m app.devtools sample_cash.xlsx --batch-size 10
python -m app.devtools scan.pdf --set parserOptions.pdf.engine=textract
```
HTTP:
```powershell
$env:DP_DEV_ENDPOINTS="true"; uvicorn app.api:app --reload
curl http://localhost:8000/dev/fixtures
curl -X POST http://localhost:8000/dev/fixtures/northern_trust_sample.pdf/parse `
     -H "Content-Type: application/json" -d '{"batchSize": 25}'
```
Or open http://localhost:8000/docs and use the Swagger UI.

Requests start from `tests/sample_request.json` (the real Kafka message), so column mapping, typing and
enrichment are exercised exactly as in production. The body / `--set` overrides any field.
Dev endpoints return 404 unless `DP_DEV_ENDPOINTS=true`, and the API skips Postgres init in dev mode
(`DP_SKIP_DB=false` to keep it).

## Key behaviours
| Concern | How |
|---|---|
| Memory | CSV via `csv.reader`, Excel via openpyxl `read_only` — rows are streamed, never loaded whole |
| Batching | `batch.size` in config, overridable per message with `batchSize` |
| Header mapping | `source_display_column` ("Closing Balance") → `source_column` (`closingbalance`), case/space/punct-insensitive; falls back to `columnDetails` |
| Types | from `columnDetails.columnType` (numeric/date/timestamp/varchar). Numerics are emitted as strings to keep decimal precision. `(1,234.00)` → `-1234.00` |
| Excel ids | `12345.0` in a varchar column becomes `"12345"` |
| Wildcards | `fileName: "*.xlsx"` resolved under `fileImportDetails.subPath`; `storage.wildcard_pick: latest|all` |
| Security | paths must resolve under `storage.allowed_roots` |
| Idempotency | `fileSeqId` is the key; completed jobs are skipped on redelivery unless `isSourceReprocess`; `UNIQUE(file_seq_id,batch_no)` |
| Enrich (`enrichFlow: true`) | fills `file_seq_id`, `bloc_recon_file_name`, `sourcetype`, `batch_id`, `process_date` when present in `columnDetails` and empty; `ocr_keywords.customN` → `custom_field_N` |
| Warnings | stored on the job: unmapped headers, config drift, missing OCR keywords, no text layer |

## PDF

Two independent choices:
- `pdf.engine: native | textract`: where the words come from (text layer vs AWS OCR)
- `pdf.layout: auto | statement | table`: how they are interpreted

### Statement layout (bank statements, e.g. Northern Trust)
Most bank statement PDFs contain **no tables**; the sample Northern Trust file has zero detectable
tables. They are position-based reports, so `statement` layout:
1. groups words into visual rows by y-position (fixes amounts that plain text extraction prints
   lines away from their transaction), and splits each row into code / text / amount columns by x-position;
2. runs a state machine: date → account → balances → totals → DEBITS/CREDITS → transactions → control totals,
   carrying open transactions across page breaks and ignoring repeated page headers;
3. **self-validates every account-day** against the statement's own numbers:
   `TOTAL DEBITS/CREDITS`, `TOTALS FOR ACCOUNT` count and amount, and opening + credits − debits = closing.
   Failures are job warnings (or abort with `validation.fail_on_error: true`).

Sample result: 34 transactions and 69 daily balance records, 145 checks, 0 failures.

A layout is one YAML in `config/pdf_profiles/` (regexes, column positions as page fractions, BAI code map,
`field_map` to recon columns, `emit: [transaction, balance]`). Add a new bank by adding a profile. It is
auto-selected when its `detect` regex matches page 1, or forced with `pdf.profile` / `parserOptions.pdf.profile`.
Positions are page fractions, so the same profile works with `engine: textract` for scanned copies.

Each record also carries `_raw`: posting and value dates, reference, BAI type code, account name, all four
balances, and the SWIFT-style tags (`ORG`, `OBI`, `BNF`, `IBK`, ...) split out of the description.

### Table layout (generic grids)
`pdf.engine: native | textract` globally, or per message: `"parserOptions": {"pdf": {"engine": "textract"}}`
(any `pdf.*` key can be overridden that way, e.g. `{"pdf": {"native": {"table_strategy": "text"}}}`).

Both engines share the same post-processing, which is where most PDF table bugs live:
- **auto strategy** — ruled grid first, whitespace alignment fallback for borderless statements
- **cross-page stitching** — a table continuing on the next page with the same column count is one table; repeated headers dropped
- **wrapped rows** — a row with an empty key column and no numbers is merged into the previous row's cells
- **noise filter** — `skip_row_patterns` removes page footers, "Total", "Continued"
- **document fields** — `key_values.patterns` regexes + `ocr_keywords` land in each batch's `meta.doc`

Textract: single-page → sync `AnalyzeDocument`; multi-page → async via `pdf.textract.s3_bucket`
(Textract requirement). Merged cells are replicated across their span so columns stay aligned; FORMS key/values
are captured as `form:<key>`.

## Superset
`POST /webhooks/superset` (header `X-Webhook-Token`) with
`{"chart_id": 42, "extra_filters": [{"col":"currency","op":"==","val":"USD"}], "columnDetails": [...]}`
or a full custom `query_context`. Uses the saved chart's query context by default.

## Scaling
- Workers: one per partition max (`KAFKA_CFG_NUM_PARTITIONS`), `deploy.replicas` in compose / HPA in k8s.
- Large PDFs/Textract jobs: `max_poll_interval_ms` is 30 min; raise if needed.
- High-volume HTTP: use `POST /jobs/enqueue` instead of synchronous `/parse/upload`.
