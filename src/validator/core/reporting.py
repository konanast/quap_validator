from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Literal
import json
import datetime as _dt
import os

Severity = Literal["error", "warning", "info"]

# Normalized issue codes (keep these stable as your public API)
ISSUE = {
    "CORRUPTED_FILE": "CORRUPTED_FILE",
    "MISSING_COLUMNS": "MISSING_COLUMNS",
    "EXTRA_COLUMNS": "EXTRA_COLUMNS",
    "DTYPE_MISMATCH": "DTYPE_MISMATCH",
    "ENUM_VIOLATION": "ENUM_VIOLATION",
    "RANGE_VIOLATION": "RANGE_VIOLATION",
    "NULL_REQUIRED": "NULL_REQUIRED",
    "DUPLICATES": "DUPLICATES",
    # geo-related (even if you add them later)
    "GEOMETRY_CRS_MISMATCH": "GEOMETRY_CRS_MISMATCH",
    "GEOMETRY_TYPE_MISMATCH": "GEOMETRY_TYPE_MISMATCH",
}

# Exit codes (keep deterministic; 0 means success)
EXIT_OK = 0
EXIT_CORRUPTED = 2
EXIT_SCHEMA = 3
EXIT_TYPES_OR_VALUES = 4
EXIT_DUPLICATES = 5
EXIT_OTHER = 6

@dataclass
class InputMeta:
    path: str
    format: str
    layer: Optional[str] = None
    size_bytes: Optional[int] = None

@dataclass
class TemplateMeta:
    template_id: str
    version: str

@dataclass
class Provenance:
    tool_version: str = "0.1.0"
    git_rev: Optional[str] = None
    run_id: Optional[str] = None

@dataclass
class Issue:
    code: str
    severity: Severity = "error"
    detail: Optional[str] = None
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    keys: Optional[List[str]] = None
    invalid_rows: Optional[int] = None
    examples: Optional[List[Dict[str, Any]]] = None
    extra: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Report:
    ok: bool
    started_at: str
    finished_at: str
    duration_sec: float

    template: TemplateMeta
    input: InputMeta
    provenance: Provenance

    row_count: Optional[int] = None
    summary: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[Issue] = field(default_factory=list)
    warnings: List[Issue] = field(default_factory=list)
    infos: List[Issue] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        # dataclasses → dict, converting nested dataclasses too
        def _conv(obj):
            if hasattr(obj, "__dataclass_fields__"):
                return {k: _conv(v) for k, v in asdict(obj).items()}
            if isinstance(obj, list):
                return [_conv(x) for x in obj]
            if isinstance(obj, dict):
                return {k: _conv(v) for k, v in obj.items()}
            return obj
        return _conv(self)

    def to_json(self, indent: Optional[int] = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    def write_json(self, path: str, indent: Optional[int] = 2) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json(indent=indent))

    def write_ndjson(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(self.to_dict(), ensure_ascii=False) + "\n")

    def severity_counts(self) -> Dict[str, int]:
        return {
            "errors": len(self.errors),
            "warnings": len(self.warnings),
            "infos": len(self.infos),
        }

    def exit_code(self) -> int:
        # Prioritize the most severe/meaningful exit code based on issue codes
        codes = [i.code for i in self.errors]
        if ISSUE["CORRUPTED_FILE"] in codes:
            return EXIT_CORRUPTED
        if ISSUE["MISSING_COLUMNS"] in codes:
            return EXIT_SCHEMA
        if ISSUE["DTYPE_MISMATCH"] in codes or ISSUE["ENUM_VIOLATION"] in codes \
           or ISSUE["RANGE_VIOLATION"] in codes or ISSUE["NULL_REQUIRED"] in codes:
            return EXIT_TYPES_OR_VALUES
        if ISSUE["DUPLICATES"] in codes:
            return EXIT_DUPLICATES
        return EXIT_OK if self.ok else EXIT_OTHER

def _now_iso() -> str:
    return _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def redact_path(p: str) -> str:
    # Optional: redact credentials in URIs (s3://key:secret@bucket/path → s3://***@bucket/path)
    if "@" in p and "://" in p:
        scheme, rest = p.split("://", 1)
        if "@" in rest and ":" in rest.split("@")[0]:
            creds, tail = rest.split("@", 1)
            return f"{scheme}://***@{tail}"
    return p

def build_report(
    engine_result: Dict[str, Any],
    template_meta: Dict[str, Any],
    input_meta: Dict[str, Any],
    started_at: Optional[str] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Report:
    """
    Normalize the engine output (whatever adapter produced) into a stable Report.
    Expected engine_result keys (as per your engine):
      - ok: bool
      - errors: List[Dict]
      - warnings: List[Dict]
      - metrics: Dict
      - timing_sec: float
      - row_count: Optional[int] (if provided by engine; else None)
      - summary: Optional[Dict]
    """
    started = started_at or _now_iso()
    finished = _now_iso()
    duration = float(engine_result.get("timing_sec") or 0.0)

    # Coerce issues
    def _coerce_issues(items: List[Dict[str, Any]] | None, sev: Severity) -> List[Issue]:
        res = []
        for it in items or []:
            res.append(Issue(
                code=str(it.get("code")),
                severity=sev,
                detail=it.get("detail"),
                column=it.get("column"),
                columns=it.get("columns"),
                keys=it.get("keys"),
                invalid_rows=it.get("invalid_rows"),
                examples=it.get("examples"),
                extra={k:v for k,v in it.items() if k not in {"code","detail","column","columns","keys","invalid_rows","examples"}}
            ))
        return res

    report = Report(
        ok=bool(engine_result.get("ok", False)),
        started_at=started,
        finished_at=finished,
        duration_sec=duration,
        template=TemplateMeta(**template_meta),
        input=InputMeta(**{**input_meta, "path": redact_path(input_meta["path"])}),
        provenance=Provenance(**(provenance or {})),
        row_count=engine_result.get("row_count"),
        summary=engine_result.get("summary") or {},
        metrics=engine_result.get("metrics") or {},
        errors=_coerce_issues(engine_result.get("errors"), "error"),
        warnings=_coerce_issues(engine_result.get("warnings"), "warning"),
        infos=_coerce_issues(engine_result.get("infos"), "info"),
    )
    return report

def render_text(report: Report) -> str:
    """Human-readable single-paragraph summary for CLI/stdout."""
    sev = report.severity_counts()
    tpl = f"{report.template.template_id}:{report.template.version}"
    src = report.input.path
    layer = f" layer={report.input.layer}" if report.input.layer else ""
    rc = f" rows={report.row_count}" if report.row_count is not None else ""
    status = "OK" if report.ok else "FAILED"
    line1 = f"[{status}] template={tpl} input={src}{layer}{rc} " \
            f"errors={sev['errors']} warnings={sev['warnings']} duration={report.duration_sec:.2f}s"

    def _sample_issues(items: List[Issue], n=3) -> List[str]:
        out = []
        for i in items[:n]:
            if i.columns:
                out.append(f"{i.code}({','.join(i.columns)})")
            elif i.column:
                out.append(f"{i.code}({i.column})")
            else:
                out.append(i.code)
        if len(items) > n:
            out.append(f"... +{len(items)-n} more")
        return out

    details = []
    if report.errors:
        details.append("errors: " + "; ".join(_sample_issues(report.errors)))
    if report.warnings:
        details.append("warnings: " + "; ".join(_sample_issues(report.warnings)))
    return line1 + ("" if not details else "\n  " + "\n  ".join(details))
