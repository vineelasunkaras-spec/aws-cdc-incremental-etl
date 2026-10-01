# AWS CDC & Incremental ETL Pipeline (PySpark + Delta Lake)

A reference implementation of a **Change Data Capture (CDC)** pipeline that loads incremental and historical data from relational sources (PostgreSQL / MSSQL) into an S3-based Delta Lake and Redshift/Athena-ready warehouse tables.

## Highlights
- **Incremental loads** driven by a high-watermark column (`updated_at`) persisted per table
- **CDC upserts** with Delta Lake `MERGE` (insert / update / soft-delete via `op` flag)
- **Deduplication** of late or repeated change events using window functions
- **PII masking** (SHA-256 hashing + partial masking) before data lands in the curated zone
- Runs locally with PySpark or as an **AWS Glue** job (`src/glue_job.py`)
- Config-driven: add a table by editing `config/pipeline.yaml`

## Architecture
```
PostgreSQL / MSSQL ──JDBC──► Raw (S3, Parquet) ──► CDC MERGE ──► Curated (Delta) ──► Redshift / Athena
                                   │                    ▲
                                   └── watermark store ─┘
```

## Project layout
| Path | Purpose |
|------|---------|
| `src/cdc_merge.py` | Dedup + Delta MERGE logic |
| `src/watermark.py` | Read/advance high-watermarks |
| `src/masking.py` | PII hashing / masking helpers |
| `src/glue_job.py` | AWS Glue entry point |
| `config/pipeline.yaml` | Table definitions |
| `tests/` | Pytest unit tests |

## Run locally
```bash
pip install -r requirements.txt
pytest -q
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 src/cdc_merge.py --config config/pipeline.yaml --table customers
```

## Tech
Python · PySpark · Delta Lake · AWS Glue · S3 · Redshift · Athena · AWS KMS
