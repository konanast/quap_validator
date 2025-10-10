from __future__ import annotations
import os
import json
import click
import duckdb

from core.templates import TemplateRegistry
from core.engine import validate_with_duckdb
from core.reporting import build_report, render_text

from adapters.csv_adapter import load_csv_view
from adapters.geoparquet_adapter import load_parquet_view
from adapters.gpkg_adapter import load_gpkg_view, gpkg_integrity_errors, gpkg_list_layers, gpkg_table_columns
from adapters.shapefile_adapter import load_shapefile_into_duck, shapefile_integrity_errors

def detect_format(path: str) -> str:
    p = path.lower()
    if p.endswith(".parquet"): return "GEOPARQUET"
    if p.endswith(".gpkg"): return "GEOPACKAGE"
    if p.endswith(".shp"): return "SHAPEFILE"
    if p.endswith(".csv") or p.endswith(".csv.gz") or p.endswith(".csv.bz2"): return "CSV"
    raise click.UsageError("Cannot detect format from extension; pass --format")

@click.command()
@click.option("--input", "input_path", required=True, help="Path/URI to input file")
@click.option("--format", "fmt", type=click.Choice(["CSV","GEOPARQUET","GEOPACKAGE","SHAPEFILE"]), required=False)
@click.option("--layer", default=None, help="Layer (table) for GeoPackage")
@click.option("--template-id", required=True)
@click.option("--template-version", default=None)
@click.option("--templates-dir", multiple=True, default=["./templates"])
@click.option("--template-schema", default="templates/template.schema.json", show_default=True)
@click.option("--report", default=None, help="Write JSON report to this path")
@click.option("--print-json", is_flag=True, help="Print JSON to stdout (in addition to text summary)")
def main(input_path, fmt, layer, template_id, template_version, templates_dir, template_schema, report, print_json):
    # Load template
    reg = TemplateRegistry(list(templates_dir), schema_path=template_schema if template_schema and os.path.exists(template_schema) else None)
    tpl = reg.load(template_id, version=template_version)

    # Determine format if not given
    fmt = fmt or detect_format(input_path)

    # Prepare DuckDB connection
    con = duckdb.connect()

    # Columns we actually need to validate
    needed_cols = [c["name"] for c in tpl.get("columns", [])]

    # Pre-checks and view creation
    engine_result = {"ok": True, "errors": [], "warnings": [], "metrics": {}}
    diagnostics = {}

    try:
        if fmt == "CSV":
            load_csv_view(con, input_path, "v")
        elif fmt == "GEOPARQUET":
            # Quick corruption probe via pyarrow (optional)
            try:
                import pyarrow.parquet as pq
                _ = pq.ParquetFile(input_path).metadata  # raises if corrupt
            except Exception as e:
                engine_result["errors"].append({"code":"CORRUPTED_FILE", "detail": f"parquet_metadata: {e}"})
                engine_result["ok"] = False
            load_parquet_view(con, input_path, "v")
        elif fmt == "GEOPACKAGE":
            # Integrity check
            errs = gpkg_integrity_errors(input_path)
            if errs:
                engine_result["errors"].append({"code":"CORRUPTED_FILE", "detail": "; ".join(errs)})
                engine_result["ok"] = False
            # Resolve layer if not provided
            if not layer:
                layers = gpkg_list_layers(input_path)
                if not layers:
                    raise click.UsageError("No feature layers found in GeoPackage; use --layer")
                layer = layers[0]
            _, diagnostics = load_gpkg_view(con, input_path, layer, needed_cols, view_name="v")
        elif fmt == "SHAPEFILE":
            errs = shapefile_integrity_errors(input_path)
            if errs:
                engine_result["errors"].append({"code":"CORRUPTED_FILE", "detail": "; ".join(errs)})
                engine_result["ok"] = False
            _, diagnostics = load_shapefile_into_duck(con, input_path, needed_cols, table_name="_shape_tmp", view_name="v")
        else:
            raise click.UsageError(f"Unsupported format: {fmt}")
    except Exception as e:
        engine_result["errors"].append({"code":"CORRUPTED_FILE", "detail": f"open_failed: {e}"})
        engine_result["ok"] = False

    # Run validation on view 'v' if we were able to create it
    try:
        # row count (may still work even if corruption flagged)
        try:
            row_count = con.execute("SELECT COUNT(*) FROM v").fetchone()[0]
            engine_result["row_count"] = row_count
        except Exception:
            engine_result["row_count"] = None

        # Only run detailed checks if we have at least a view
        if engine_result["ok"]:
            vr = validate_with_duckdb("v", tpl, con)
            # merge results
            for k in ("errors", "warnings"):
                engine_result[k].extend(vr.get(k, []))
            engine_result["metrics"] = {**engine_result.get("metrics", {}), **vr.get("metrics", {})}
            engine_result["timing_sec"] = vr.get("timing_sec")
            engine_result["ok"] = vr.get("ok", False) and engine_result["ok"]
    except Exception as e:
        engine_result["errors"].append({"code":"CORRUPTED_FILE", "detail": f"validation_failed: {e}"})
        engine_result["ok"] = False

    # Build final report
    template_meta = {"template_id": tpl["template_id"], "version": tpl["version"]}
    input_meta = {"path": input_path, "format": fmt, "layer": layer, "size_bytes": os.path.getsize(input_path) if os.path.exists(input_path) else None}
    provenance = {"tool_version": "0.2.0", "git_rev": os.environ.get("GIT_REV"), "run_id": os.environ.get("RUN_ID")}
    rep = build_report(engine_result, template_meta, input_meta, provenance=provenance)

    # Output
    print(render_text(rep))
    if print_json:
        print(rep.to_json())
    if report:
        rep.write_json(report)

    raise SystemExit(rep.exit_code())

if __name__ == "__main__":
    main()
