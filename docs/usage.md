# Usage

## Basic command

```bash
quap-validate \
  --input /path/to/parcels.parquet \
  --template-id parcels \
  --templates-dir ./templates
````

---

## Supported formats

| Format     | Engine                                        | Notes                           |
| ---------- | --------------------------------------------- | ------------------------------- |
| CSV        | DuckDB `read_csv_auto`                        | Out-of-core parsing             |
| GeoParquet | DuckDB `read_parquet`                         | Fast metadata probe             |
| GeoPackage | DuckDB `sqlite_scanner` (fallback to sqlite3) | No GDAL dependency              |
| Shapefile  | pyogrio `read_arrow`                          | Requires `.shp`, `.dbf`, `.shx` |

---

## Template example

Example (`templates/parcels.json`):

```json
{
  "template_id": "parcels",
  "version": "1.2.0",
  "columns": [
    {"name": "parcel_id", "dtype": "int64", "required": true, "unique": true},
    {"name": "owner", "dtype": "string", "required": true},
    {"name": "area_ha", "dtype": "float64", "range": {"min": 0}}
  ]
}
```

Validate the template against the schema:

```bash
python -m jsonschema validate \
  --instance templates/parcels.json \
  --schema templates/template.schema.json
```

---

## Report format and exit codes

| Code | Meaning                          |
| ---- | -------------------------------- |
| 0    | OK                               |
| 2    | Corrupted file or failed to open |
| 3    | Schema error (missing columns)   |
| 4    | Type/enum/range/null violations  |
| 5    | Duplicates found                 |
| 6    | Other errors                     |

---

## Development workflow

```bash
pip install -e .[dev]
ruff check .
black .
mypy .
pytest -q
```

---

## Integration notes

You can import and call the validator from within Django or Celery:

* Store validation templates in a database
* Run checks asynchronously
* Store `report_json` results in a `JSONField`
