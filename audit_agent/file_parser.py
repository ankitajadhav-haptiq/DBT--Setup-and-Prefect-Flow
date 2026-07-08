"""
Repository Parser
Walks the repo and builds a JSON manifest that all scanners consume.
No scanner needs direct filesystem access — they read from this manifest.
"""
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional
import yaml

SKIP_DIRS = {
    ".git", ".venv", "venv", "env", "__pycache__", ".DS_Store",
    "target", "dbt_packages", "node_modules", "logs",
    "site-packages", "dist-packages",
}
PRIVATE_KEY_EXT    = {".p8", ".pem", ".key", ".pfx"}
SQL_EXT            = {".sql"}
PYTHON_EXT         = {".py"}
YAML_EXT           = {".yml", ".yaml"}

RE_UUID            = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE
)
RE_JINJA_VAR       = re.compile(r"\{\{\s*(\w+)\s*\}\}")
RE_SELECT_STAR     = re.compile(r"\bselect\s+\*", re.IGNORECASE)
RE_ORDER_BY        = re.compile(r"\border\s+by\b", re.IGNORECASE)
RE_UPPER_TRIM      = re.compile(r"UPPER\s*\(\s*TRIM\s*\(", re.IGNORECASE)
RE_CREDENTIAL      = re.compile(
    r"(?i)(password|secret|private_key|api_key|token)\s*[=:]\s*['\"]?[^\s'\",]+",
)


class RepositoryParser:
    def __init__(self, root_path: Path):
        self.root = root_path.resolve()

    def build_manifest(self) -> Dict[str, Any]:
        manifest: Dict[str, Any] = {
            "root": str(self.root),
            "summary": {
                "total_sql_files":    0,
                "total_python_files": 0,
                "total_yaml_files":   0,
                "private_key_files":  [],
                "env_files":          [],
            },
            "dbt_project":    {},
            "dbt_domains":    {},
            "sql_files":      [],
            "python_files":   [],
            "yaml_configs":   [],
            "credential_risks": [],
        }

        for root_dir, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            root_p   = Path(root_dir)

            for fname in files:
                fpath = root_p / fname
                rel   = str(fpath.relative_to(self.root))

                # Private key files
                if fpath.suffix in PRIVATE_KEY_EXT:
                    manifest["summary"]["private_key_files"].append(rel)
                    manifest["credential_risks"].append({
                        "file":     rel,
                        "risk":     "private_key_in_repo",
                        "severity": "CRITICAL",
                        "cwe":      "CWE-312",
                    })

                # .env files
                if fname == ".env":
                    manifest["summary"]["env_files"].append(rel)
                    self._scan_env(fpath, rel, manifest)

                # profiles.yml
                if fname == "profiles.yml":
                    manifest["credential_risks"].append({
                        "file":     rel,
                        "risk":     "profiles_yml_in_repo",
                        "severity": "HIGH",
                        "cwe":      "CWE-538",
                        "note":     "profiles.yml should live in ~/.dbt/, not in VCS",
                    })

                # Route by extension
                if fpath.suffix in SQL_EXT:
                    manifest["summary"]["total_sql_files"] += 1
                    self._parse_sql(fpath, rel, manifest)
                elif fpath.suffix in PYTHON_EXT:
                    manifest["summary"]["total_python_files"] += 1
                    self._parse_python(fpath, rel, manifest)
                elif fpath.suffix in YAML_EXT:
                    manifest["summary"]["total_yaml_files"] += 1
                    self._parse_yaml(fpath, rel, fname, manifest)

        return manifest

    # ── SQL ────────────────────────────────────────────────────────────────

    def _parse_sql(self, fpath: Path, rel: str, manifest: dict):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return

        is_macro  = "/macros/" in rel
        domain    = self._extract_domain(rel)
        cte_count = content.lower().count(" as (")

        entry = {
            "path":              rel,
            "domain":            domain,
            "is_macro":          is_macro,
            "cte_count":         cte_count,
            "has_select_star":   bool(RE_SELECT_STAR.search(content)),
            "has_order_by":      bool(RE_ORDER_BY.search(content)),
            "hardcoded_uuids":   RE_UUID.findall(content),
            "jinja_variables":   RE_JINJA_VAR.findall(content),
            "upper_trim_count":  len(RE_UPPER_TRIM.findall(content)),
            "line_count":        content.count("\n"),
            "full_content":      content,
        }

        if is_macro and entry["jinja_variables"]:
            entry["jinja_injection_risk"] = True

        manifest["sql_files"].append(entry)

        if domain:
            if domain not in manifest["dbt_domains"]:
                manifest["dbt_domains"][domain] = {"models": [], "macros": []}
            key = "macros" if is_macro else "models"
            manifest["dbt_domains"][domain][key].append(rel)

    # ── Python ─────────────────────────────────────────────────────────────

    def _parse_python(self, fpath: Path, rel: str, manifest: dict):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return
        manifest["python_files"].append({
            "path":               rel,
            "line_count":         content.count("\n"),
            "sensitive_patterns": RE_CREDENTIAL.findall(content),
            "full_content":       content,
        })

    # ── YAML ───────────────────────────────────────────────────────────────

    def _parse_yaml(self, fpath: Path, rel: str, fname: str, manifest: dict):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")
            data    = yaml.safe_load(content) or {}
        except Exception:
            data = {}

        if fname == "dbt_project.yml":
            manifest["dbt_project"] = {
                "path":   rel,
                "name":   data.get("name"),
                "models": data.get("models", {}),
                "vars":   data.get("vars", {}),
                "seeds":  data.get("seeds", {}),
            }

        manifest["yaml_configs"].append({"path": rel, "type": fname})

    # ── Helpers ────────────────────────────────────────────────────────────

    def _scan_env(self, fpath: Path, rel: str, manifest: dict):
        # Only flag .env files that contain actual credential keys —
        # not just path/config variables like DBT_PROJECT_DIR.
        CREDENTIAL_KEYS = re.compile(
            r"(?i)^(password|passwd|secret|token|api_key|private_key|"
            r"account|snowflake_account|sf_account|auth|key_path|"
            r"aws_secret|azure_key|client_secret)\s*=",
        )
        try:
            lines = fpath.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            return
        active = [
            l for l in lines
            if "=" in l
            and not l.strip().startswith("#")
            and CREDENTIAL_KEYS.match(l.strip())
        ]
        if active:
            manifest["credential_risks"].append({
                "file":                 rel,
                "risk":                 "active_credentials_in_env",
                "severity":             "CRITICAL",
                "cwe":                  "CWE-798",
                "active_variable_count": len(active),
            })

    def _extract_domain(self, rel: str) -> Optional[str]:
        for part in Path(rel).parts:
            if part in {"GOPOD", "PHOTO", "UNIFI", "macros"}:
                return part
        return None
