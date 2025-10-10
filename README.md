# QUAP Validator

**Template-driven, memory-safe validator for CSV / GeoParquet / GeoPackage / Shapefile.**

QUAP Validator validates large geospatial and tabular datasets using **JSON templates** that define expected structure, data types, and value constraints — without ever loading entire files into memory.

---

## Features

- **Template-first:** rules live in JSON templates (no hard-coding)
- **Out-of-core:** powered by DuckDB, pyarrow, and pyogrio
- **Uniform reporting:** JSON reports, summaries, and exit codes
- **Extensible:** adapters for CSV, GeoParquet, GeoPackage, and Shapefile
- **Django-ready:** clean core library, usable via CLI or Celery

---

## Quick start

```bash
pip install quap-validator

quap-validate \
  --input /path/to/parcels.parquet \
  --template-id parcels
