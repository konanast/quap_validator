from __future__ import annotations
import os
import json
import click
import duckdb

from validator.core.templates import TemplateRegistry
from validator.core.engine import validate_with_duckdb
from validator.core.reporting import build_report, render_text
from validator.core.compression import maybe_decompress, UnpackError

from validator.adapters.csv_adapter import load_csv_view
from validator.adapters.geoparquet_adapter import load_parquet_view
from validator.adapters.gpkg_adapter import (
    load_gpkg_view,
    gpkg_integrity_errors,
    gpkg_list_layers,
)
from validator.adapters.shapefile_adapter import (
    load_shapefile_into_duck,
    shapefile_integrity_errors,
)


def detect_format(path: str) -> str:
    p = path.lower()
    if p.endswith(".parquet"):
        return "GEOPARQUET"
    if p.endswith(".gpkg"):
        return "GEOPACKAGE"
    if p.endswith(".shp"):
        return "SHAPEFILE"
    if p.endswith(".csv") or p.endswith(".csv.gz") or p.endswith(".csv.bz2"):
        return "CSV"
    raise click.UsageError("Cannot detect format from extension; pass --format")


def _list_templates(search_dirs: list[str]) -> int:
    """
    List available templates from (in order):
    - any CLI-provided directories,
    - DS_TEMPLATES_DIR (':' separated),
    - packaged defaults under validator/templates/.
    """
    from importlib.resources import files as pkg_files

    env_dirs = []
    env = os.getenv("DS_TEMPLATES_DIR")
    if env:
        env_dirs = [p for p in env.split(":") if p.strip()]

    try:
        pkg_dir = str(pkg_files("validator").joinpath("templates"))
        packaged = [pkg_dir] if os.path.isdir(pkg_dir) else []
    except Exception:
        packaged = []

    dirs = [*search_dirs, *env_dirs, *packaged]

    seen = set()
    listed = []

    for d in [p for p in dirs if p and os.path.isdir(p)]:
        idx = os.path.join(d, "index.json")
        if not os.path.exists(idx):
            continue
        try:
            data = json.load(open(idx, "r", encoding="utf-8"))
        except Exception:
            continue
        for t in data.get("templates", []):
            tid = t.get("template_id")
            ver = t.get("version", "")
            label = t.get("label", "")
            key = (tid, ver)
            if key in seen:
                continue
            seen.add(key)
            listed.append((tid, ver, label, d))

    if not listed:
        click.echo("No templates found.")
        return 0

    click.echo("Available templates (first match wins):")
    for tid, ver, label, d in listed:
        click.echo(f"  {tid:25} {ver:10}  {label}   [from: {d}]")
    return 0


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--input",
    "input_path",
    required=False,
    help="Path/URI to input file",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["CSV", "GEOPARQUET", "GEOPACKAGE", "SHAPEFILE"]),
    required=False,
    help="Input format (auto-detected from extension if omitted)",
)
@click.option("--layer", default=None, help="Layer (table) for GeoPackage")
@click.option("--template-id", required=False, help="Template id to use")
@click.option("--template-version", default=None, help="Specific template version")
@click.option(
    "--templates-dir",
    multiple=True,
    default=[],
    help="Directory with templates (can be used multiple times). "
    "Overrides DS_TEMPLATES_DIR and packaged defaults.",
)
@click.option(
    "--template-schema",
    default=None,
    help="Path to template JSON-Schema (optional)",
)
@click.option("--report", default=None, help="Write JSON report to this path")
@click.option(
    "--print-json",
    is_flag=True,
    help="Print JSON report to stdout (in addition to text summary)",
)
@click.option(
    "--list-templates",
    is_flag=True,
    help="List available templates and exit (no validation performed).",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Print extra diagnostics (adapters).",
)
def main(
    input_path: str | None,
    fmt: str | None,
    layer: str | None,
    template_id: str | None,
    template_version: str | None,
    templates_dir: tuple[str, ...],
    template_schema: str | None,
    report: str | None,
    print_json: bool,
    list_templates: bool,
    debug: bool,
):
    """
    Template-driven, memory-safe validator for CSV/GeoParquet/GeoPackage/Shapefile.

    Examples:
      quap-validate --list-templates
      quap-validate --input data.parquet --template-id lpis_population --print-json
    """
    if list_templates:
        search_dirs = list(templates_dir or [])
        raise SystemExit(_list_templates(search_dirs))

    if not input_path:
        raise click.UsageError("--input is required (unless you pass --list-templates)")
    if not template_id:
        raise click.UsageError(
            "--template-id is required (unless you pass --list-templates)"
        )

    reg = TemplateRegistry(
        list(templates_dir or []),
        schema_path=(
            template_schema
            if template_schema and os.path.exists(template_schema)
            else None
        ),
    )
    tpl = reg.load(template_id, version=template_version)

    cleanup = lambda: None
    try:
        dataset_path, cleanup = maybe_decompress(input_path)
    except UnpackError as e:
        con = duckdb.connect()
        engine_result = {
            "ok": False,
            "errors": [{"code": "UNPACK_ERROR", "detail": str(e)}],
            "warnings": [],
            "metrics": {},
        }
        template_meta = {"template_id": tpl["template_id"], "version": tpl["version"]}
        input_meta = {
            "path": input_path,
            "format": None,
            "layer": None,
            "size_bytes": (
                os.path.getsize(input_path) if os.path.exists(input_path) else None
            ),
        }
        provenance = {
            "tool_version": "0.2.0",
            "git_rev": os.environ.get("GIT_REV"),
            "run_id": os.environ.get("RUN_ID"),
        }
        rep = build_report(
            engine_result, template_meta, input_meta, provenance=provenance
        )
        click.echo(render_text(rep))
        if print_json:
            click.echo(rep.to_json())
        try:
            con.close()
        except Exception:
            pass
        raise SystemExit(rep.exit_code())

    fmt = fmt or detect_format(str(dataset_path))

    con = duckdb.connect()
    diagnostics: dict = {}

    engine_result = {"ok": True, "errors": [], "warnings": [], "metrics": {}}
    needed_cols = [c["name"] for c in tpl.get("columns", [])] or None

    try:
        if fmt == "CSV":
            load_csv_view(con, str(dataset_path), "v")
        elif fmt == "GEOPARQUET":
            try:
                import pyarrow.parquet as pq

                _ = pq.ParquetFile(str(dataset_path)).metadata
            except Exception as e:
                engine_result["errors"].append(
                    {"code": "CORRUPTED_FILE", "detail": f"parquet_metadata: {e}"}
                )
                engine_result["ok"] = False
            load_parquet_view(con, str(dataset_path), "v")
        elif fmt == "GEOPACKAGE":
            errs = gpkg_integrity_errors(str(dataset_path))
            if errs:
                engine_result["errors"].append(
                    {"code": "CORRUPTED_FILE", "detail": "; ".join(errs)}
                )
                engine_result["ok"] = False
            if not layer:
                layers = gpkg_list_layers(str(dataset_path))
                if not layers:
                    raise click.UsageError(
                        "No feature layers found in GeoPackage; use --layer"
                    )
                layer = layers[0]
            _, diagnostics = load_gpkg_view(
                con, str(dataset_path), layer, needed_cols, view_name="v"
            )
        elif fmt == "SHAPEFILE":
            errs = shapefile_integrity_errors(str(dataset_path))
            if errs:
                engine_result["errors"].append(
                    {"code": "CORRUPTED_FILE", "detail": "; ".join(errs)}
                )
                engine_result["ok"] = False
            _, diagnostics = load_shapefile_into_duck(
                con,
                str(dataset_path),
                needed_cols or [],
                table_name="_shape_tmp",
                view_name="v",
            )
        else:
            raise click.UsageError(f"Unsupported format: {fmt}")
    except Exception as e:
        engine_result["errors"].append(
            {"code": "CORRUPTED_FILE", "detail": f"open_failed: {e}"}
        )
        engine_result["ok"] = False

    try:
        row_count = con.execute("SELECT COUNT(*) FROM v").fetchone()[0]
        engine_result["row_count"] = row_count
    except Exception:
        engine_result["row_count"] = None

    if engine_result["ok"]:
        try:
            vr = validate_with_duckdb("v", tpl, con)
            for k in ("errors", "warnings"):
                engine_result[k].extend(vr.get(k, []))
            engine_result["metrics"] = {
                **engine_result.get("metrics", {}),
                **vr.get("metrics", {}),
            }
            engine_result["timing_sec"] = vr.get("timing_sec")
            engine_result["ok"] = vr.get("ok", False) and engine_result["ok"]
        except Exception as e:
            engine_result["errors"].append(
                {"code": "CORRUPTED_FILE", "detail": f"validation_failed: {e}"}
            )
            engine_result["ok"] = False

    unpack_diag = {}
    if str(dataset_path) != str(input_path):
        unpack_diag = {
            "unpacked": True,
            "from": input_path,
            "to": str(dataset_path),
        }
    if unpack_diag or diagnostics:
        engine_result["metrics"] = {
            **engine_result.get("metrics", {}),
            "adapter_diagnostics": {
                **diagnostics,
                **({"unpack": unpack_diag} if unpack_diag else {}),
            },
        }

    template_meta = {"template_id": tpl["template_id"], "version": tpl["version"]}
    input_meta = {
        "path": input_path,
        "format": fmt,
        "layer": layer,
        "size_bytes": (
            os.path.getsize(input_path) if os.path.exists(input_path) else None
        ),
    }
    provenance = {
        "tool_version": "0.2.0",
        "git_rev": os.environ.get("GIT_REV"),
        "run_id": os.environ.get("RUN_ID"),
    }
    rep = build_report(engine_result, template_meta, input_meta, provenance=provenance)

    click.echo(render_text(rep))
    if print_json:
        click.echo(rep.to_json())
    if report:
        rep.write_json(report)

    if debug and diagnostics:
        click.echo(
            f"\n[debug] adapter diagnostics: {json.dumps(engine_result.get('metrics', {}).get('adapter_diagnostics', {}), indent=2)}"
        )

    try:
        con.close()
    finally:
        try:
            cleanup()
        except Exception:
            pass

    raise SystemExit(rep.exit_code())
