# Installation

## Requirements

- **Python:** 3.10 or later  
- **OS:** Linux (tested)  
- **Dependencies:** DuckDB, pyarrow, pandas, pyogrio, jsonschema, click

---

## Install from PyPI

```bash
pip install quap-validator
````

After installation, the CLI command `quap-validate` becomes available.

---

## Install for development

```bash
git clone https://github.com/konanast/quap_validator.git
cd quap_validator
pip install -e .[dev]
```

This installs additional tools for linting, type checking, and tests:

* **ruff** – linting
* **black** – formatting
* **mypy** – static type checking
* **pytest** – unit testing

---

## Verify installation

```bash
quap-validate --help
```

Expected output:

```
Usage: quap-validate [OPTIONS]
  Template-driven validator for CSV / GeoParquet / GeoPackage / Shapefile.
```
