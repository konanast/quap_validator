from __future__ import annotations
import json
import os
import glob
from typing import List, Dict, Optional, Tuple
from jsonschema import Draft202012Validator
from importlib.resources import files as pkg_files


class TemplateRegistry:
    """
    Finds and loads JSON templates by id/version with the following precedence:
      1) CLI-provided directories (search_dirs)
      2) DS_TEMPLATES_DIR (':'-separated)
      3) Packaged defaults under validator/templates/

    Also supports:
      - index.json (optional) with aliases and file 'path'
      - direct filename match: <template_id>.json
      - naive semver-ish 'max version' selection when version not provided
    """

    def __init__(
        self, search_dirs: List[str] | None = None, schema_path: str | None = None
    ):
        cli_dirs = list(search_dirs or [])

        env_dirs: List[str] = []
        env = os.getenv("DS_TEMPLATES_DIR")
        if env:
            env_dirs = [p for p in env.split(":") if p.strip()]

        try:
            pkg_dir = str(pkg_files("validator").joinpath("templates"))
            packaged = [pkg_dir] if os.path.isdir(pkg_dir) else []
        except Exception:
            packaged = []

        self.search_dirs: List[str] = [*cli_dirs, *env_dirs, *packaged]

        self.schema = None
        self.validator = None
        if schema_path and os.path.exists(schema_path):
            with open(schema_path, "r", encoding="utf-8") as f:
                self.schema = json.load(f)
            self.validator = Draft202012Validator(self.schema)

        self._indices: Dict[str, dict] = {}
        for d in self.search_dirs:
            idx = os.path.join(d, "index.json")
            if os.path.isdir(d) and os.path.exists(idx):
                try:
                    with open(idx, "r", encoding="utf-8") as f:
                        self._indices[d] = json.load(f)
                except Exception:
                    pass

    # ---------- internal helpers ----------

    def _iter_template_files(self) -> List[str]:
        """Return all candidate template json files across search dirs (excluding index.json)."""
        out: List[str] = []
        seen: set[str] = set()
        for d in self.search_dirs:
            if not d or not os.path.isdir(d):
                continue
            for p in glob.glob(os.path.join(d, "*.json")):
                name = os.path.basename(p).lower()
                if name == "index.json":
                    continue
                if p not in seen:
                    seen.add(p)
                    out.append(p)
        return out

    def _resolve_via_index(self, template_id: str) -> Optional[Tuple[str, dict]]:
        """
        Use index.json to resolve template_id or an alias to a concrete template file path.
        Returns (abs_path, template_entry) or None.
        """
        for d, idx in self._indices.items():
            templates = idx.get("templates", [])
            for t in templates:
                if t.get("template_id") == template_id:
                    rel = t.get("path")
                    if rel:
                        path = os.path.join(d, rel)
                        if os.path.exists(path):
                            return path, t
            for t in templates:
                aliases = t.get("aliases", []) or []
                if template_id in aliases:
                    rel = t.get("path")
                    if rel:
                        path = os.path.join(d, rel)
                        if os.path.exists(path):
                            return path, t
        return None

    def _resolve_direct_filename(self, template_id: str) -> Optional[str]:
        """Look for a direct file named <template_id>.json under any search dir."""
        fname = f"{template_id}.json"
        for d in self.search_dirs:
            if not d or not os.path.isdir(d):
                continue
            p = os.path.join(d, fname)
            if os.path.exists(p):
                return p
        return None

    @staticmethod
    def _parse_json(path: str) -> Optional[dict]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    @staticmethod
    def _ver_tuple(s: str) -> tuple:
        parts = (s or "0.0.0").split(".")
        out = []
        for x in parts:
            try:
                out.append(int(x))
            except Exception:
                out.append(0)
        while len(out) < 3:
            out.append(0)
        return tuple(out[:3])

    # ---------- public API ----------

    def load(self, template_id: str, version: str | None = None) -> dict:
        candidates: List[dict] = []

        # 1) Try to resolve via index (supports aliases)
        idx_res = self._resolve_via_index(template_id)
        if idx_res:
            p, t_entry = idx_res
            obj = self._parse_json(p)
            if obj:
                obj["_source_path"] = p
                candidates.append(obj)

        # 2) Try direct filename match <template_id>.json
        direct = self._resolve_direct_filename(template_id)
        if direct:
            obj = self._parse_json(direct)
            if obj and obj.get("template_id") == template_id:
                obj["_source_path"] = direct
                candidates.append(obj)

        # 3) Fallback: scan all JSONs and pick those with matching template_id
        if not candidates:
            for p in self._iter_template_files():
                obj = self._parse_json(p)
                if not obj:
                    continue
                if "template_id" not in obj:
                    continue
                if obj.get("template_id") == template_id:
                    obj["_source_path"] = p
                    candidates.append(obj)

        if not candidates:
            raise FileNotFoundError(
                f"Template '{template_id}' not found in: {self.search_dirs}"
            )

        if version:
            matches = [t for t in candidates if t.get("version") == version]
            if not matches:
                raise FileNotFoundError(
                    f"Template '{template_id}' version '{version}' not found"
                )
            tpl = matches[0]
        else:
            tpl = sorted(
                candidates, key=lambda t: self._ver_tuple(t.get("version", "0.0.0"))
            )[-1]

        if self.validator:
            self.validator.validate(tpl)

        return tpl
