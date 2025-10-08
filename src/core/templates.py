from __future__ import annotations
import json, os, glob
from jsonschema import validate as js_validate, Draft202012Validator

class TemplateRegistry:
    def __init__(self, search_dirs: list[str], schema_path: str | None = None):
        self.search_dirs = search_dirs
        self.schema = json.load(open(schema_path)) if schema_path else None

    def _iter_template_files(self):
        for d in self.search_dirs:
            for p in glob.glob(os.path.join(d, "*.json")):
                yield p

    def load(self, template_id: str, version: str | None = None) -> dict:
        candidates = []
        for p in self._iter_template_files():
            obj = json.load(open(p))
            if obj.get("template_id") == template_id:
                candidates.append(obj)
        if not candidates:
            raise FileNotFoundError(f"Template '{template_id}' not found")
        if version:
            matches = [t for t in candidates if t.get("version") == version]
            if not matches:
                raise FileNotFoundError(f"Template '{template_id}' version '{version}' not found")
            tpl = matches[0]
        else:
            # pick highest semver-ish version
            tpl = sorted(candidates, key=lambda t: t.get("version","0.0.0"))[-1]
        if self.schema:
            Draft202012Validator(self.schema).validate(tpl)
        return tpl

