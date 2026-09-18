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

## Run
```bash
docker compose up -d --build                 # postgres, kafka, 3 workers, api
docker compose run --rm api pytest -q        # tests (DB faked)
# send the sample message
docker compose exec kafka kafka-console-producer.sh --bootstrap-server kafka:9092 --topic bd-ocr-flow \
  < <(jq -c . tests/sample_request.json)
curl localhost:8000/jobs/FILE20260918000513954_0
curl localhost:8000/jobs/FILE20260918000513954_0/batches/1
```

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
