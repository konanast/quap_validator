# QUAP Validator

A **template-driven, memory-safe validator** for large geospatial and tabular datasets.

Works uniformly across **CSV**, **GeoParquet**, **GeoPackage**, and **Shapefile** — without loading entire files into RAM.

---

## Overview

QUAP Validator uses **JSON templates** to describe expected dataset structure and constraints.  
It then validates data efficiently using DuckDB and pyarrow, producing JSON reports and clean CLI summaries.

### Key capabilities
- Validate structure, types, nulls, enums, ranges, duplicates, and corruption
- Process large files out-of-core
- Uniform command-line interface
- Easy backend integration (Celery, Django)

---

## Quick example

```bash
quap-validate \
  --input /data/parcels.parquet \
  --template-id parcels
````

Output:

```
[OK] template=parcels:1.2.0 rows=12834902 errors=0 warnings=1 duration=7.83s
```

---

## Next steps

* [Installation](install.md)
* [Usage & examples](usage.md)
* [GitHub repository](https://github.com/konanast/quap_validator)