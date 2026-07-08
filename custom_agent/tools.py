"""
Tools the custom agent can call during its ReAct loop.

Each tool is a simple Python function wrapped in a Tool object.
The agent sees the tool's name and description and decides when to use it.
The agent passes a string argument; the tool parses it and returns a string.

Git tools:   list_staged_files, git_file_diff, read_staged_file, git_log
Scan tools:  scan_sql_file, scan_python_file, check_secrets
Util tools:  read_file, search_codebase
"""
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, List, Optional

# Allow importing from audit_agent (which lives next to custom_agent)
_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))

try:
    from audit_agent.scanners.sql_scanner    import SQLScanner
    from audit_agent.scanners.python_scanner import PythonScanner
    from audit_agent.scanners.secrets_scanner import SecretsScanner
    _SCANNERS_OK = True
except ImportError:
    _SCANNERS_OK = False


@dataclass
class Tool:
    name:        str
    description: str          # Shown to the LLM so it knows when to use the tool
    func:        Callable      # The actual function
    auto_run:    bool = False  # True → runs automatically in static/fallback mode

    def run(self, arg: str) -> str:
        try:
            return str(self.func(arg.strip()))[:6000]   # Cap at 6KB per tool call
        except Exception as e:
            return f"Tool error ({self.name}): {e}"


# ── Git tools ──────────────────────────────────────────────────────────────

def _git(args: List[str], cwd: Optional[str] = None) -> str:
    result = subprocess.run(
        ["git"] + args,
        capture_output = True,
        text           = True,
        cwd            = cwd or str(_REPO_ROOT),
    )
    return (result.stdout + result.stderr).strip()


def _list_staged_files(_: str) -> str:
    out = _git(["diff", "--cached", "--name-only", "--diff-filter=ACM"])
    if not out:
        return "No staged files."
    return out


def _git_file_diff(path: str) -> str:
    """Returns the staged diff for one file (what's changing)."""
    if not path:
        return _git(["diff", "--cached", "--stat"])
    return _git(["diff", "--cached", "--", path])


def _read_staged_file(path: str) -> str:
    """Returns the staged (index) version of a file — what will be committed."""
    if not path:
        return "Provide a file path."
    out = _git(["show", f":{path}"])
    return out[:5000] if out else f"File not staged or not found: {path}"


def _git_log(n: str) -> str:
    """Returns recent commit history. Pass a number (default 5)."""
    count = n.strip() if n.strip().isdigit() else "5"
    return _git(["log", f"-{count}", "--oneline", "--no-decorate",
                 "--pretty=format:%h %s (%an, %ar)"])


def _git_diff_branch(base: str) -> str:
    """Returns diff summary between current branch and base (e.g. 'main')."""
    base = base.strip() or "main"
    return _git(["diff", f"{base}...HEAD", "--stat"])


# ── File / search tools ────────────────────────────────────────────────────

def _read_file(path: str) -> str:
    """Reads a file from the working tree. Returns up to 5 KB."""
    if not path:
        return "Provide a file path."
    fp = _REPO_ROOT / path
    if not fp.exists():
        return f"File not found: {path}"
    try:
        return fp.read_text(errors="ignore")[:5000]
    except OSError as e:
        return f"Error reading {path}: {e}"


def _search_codebase(query: str) -> str:
    """
    Search the repo for a pattern.
    Pass a plain string or JSON: {"pattern": "...", "path": "...", "ext": ".sql"}
    """
    pattern = query
    search_path = str(_REPO_ROOT)
    ext_filter: Optional[str] = None

    # Try parsing as JSON for structured input
    try:
        parsed = json.loads(query)
        pattern     = parsed.get("pattern", query)
        search_path = str(_REPO_ROOT / parsed.get("path", "."))
        ext_filter  = parsed.get("ext")
    except (json.JSONDecodeError, TypeError):
        pass

    args = ["grep", "-r", "--include=*" + (ext_filter or ""), "-l", "-n",
            "--max-count=3", pattern, search_path]
    if not ext_filter:
        args = ["grep", "-r", "-l", "-n", "--max-count=3", pattern, search_path]

    result = subprocess.run(args, capture_output=True, text=True)
    output = (result.stdout + result.stderr).strip()
    return output[:3000] if output else f"No matches for: {pattern}"


# ── Scan tools ─────────────────────────────────────────────────────────────

def _scan_sql_file(path: str) -> str:
    """Analyzes a SQL file for time/space complexity and security issues."""
    if not _SCANNERS_OK:
        return "audit_agent scanners not available. Run: pip install -r audit_agent/requirements.txt"

    fp = _REPO_ROOT / path.strip()
    if not fp.exists():
        # Try reading staged version
        content = _git(["show", f":{path.strip()}"])
    else:
        content = fp.read_text(errors="ignore")

    if not content:
        return f"Could not read: {path}"

    manifest = {
        "sql_files": [{
            "path":            path.strip(),
            "domain":          None,
            "is_macro":        "/macros/" in path,
            "cte_count":       content.lower().count(" as ("),
            "has_select_star": bool(re.search(r"\bselect\s+\*", content, re.IGNORECASE)),
            "has_order_by":    bool(re.search(r"\border\s+by\b", content, re.IGNORECASE)),
            "hardcoded_uuids": re.findall(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                content, re.IGNORECASE),
            "jinja_variables": re.findall(r"\{\{\s*(\w+)\s*\}\}", content),
            "upper_trim_count": len(re.findall(r"UPPER\s*\(\s*TRIM\s*\(", content, re.IGNORECASE)),
            "line_count":      content.count("\n"),
            "full_content":    content,
        }],
        "credential_risks": [],
        "python_files":     [],
        "summary":          {"private_key_files": [], "env_files": []},
    }

    findings = SQLScanner().scan(manifest)
    if not findings:
        return f"✅ No issues found in {path}"

    lines = [f"Found {len(findings)} issue(s) in `{path}`:\n"]
    for f in findings:
        lines.append(
            f"- [{f.severity.value}] [{f.check_id}] {f.title}\n"
            f"  {f.description[:200]}\n"
            f"  Fix: {f.suggestion[:150]}"
        )
        if f.time_complexity:
            lines.append(f"  Time:  {f.time_complexity}")
        if f.space_complexity:
            lines.append(f"  Space: {f.space_complexity}")
    return "\n".join(lines)


def _scan_python_file(path: str) -> str:
    """Analyzes a Python file for complexity, security, and quality issues."""
    if not _SCANNERS_OK:
        return "audit_agent scanners not available."

    fp = _REPO_ROOT / path.strip()
    if not fp.exists():
        content = _git(["show", f":{path.strip()}"])
    else:
        content = fp.read_text(errors="ignore")

    if not content:
        return f"Could not read: {path}"

    manifest = {
        "python_files": [{"path": path.strip(), "full_content": content}],
        "credential_risks": [],
        "sql_files": [],
        "summary": {"private_key_files": [], "env_files": []},
    }

    findings = PythonScanner().scan(manifest)
    if not findings:
        return f"✅ No issues found in {path}"

    lines = [f"Found {len(findings)} issue(s) in `{path}`:\n"]
    for f in findings:
        lines.append(
            f"- [{f.severity.value}] [{f.check_id}] {f.title} (line {f.line})\n"
            f"  {f.description[:200]}\n"
            f"  Fix: {f.suggestion[:150]}"
        )
        if f.time_complexity:
            lines.append(f"  Time:  {f.time_complexity}")
        if f.space_complexity:
            lines.append(f"  Space: {f.space_complexity}")
    return "\n".join(lines)


def _check_secrets(path: str) -> str:
    """Checks a file or the repo root for exposed secrets and credentials."""
    target_path = path.strip() or "."
    manifest: dict[str, Any] = {
        "root": str(_REPO_ROOT),
        "summary": {"private_key_files": [], "env_files": []},
        "credential_risks": [],
        "python_files": [],
        "sql_files": [],
    }

    if _SCANNERS_OK:
        from audit_agent.file_parser import RepositoryParser
        scan_root = _REPO_ROOT / target_path if target_path != "." else _REPO_ROOT
        if scan_root.is_file():
            if scan_root.suffix == ".py":
                manifest["python_files"] = [{
                    "path": target_path,
                    "full_content": scan_root.read_text(errors="ignore"),
                }]
        else:
            # Quick scan just for secrets
            for fp in scan_root.rglob("*"):
                if fp.suffix in {".p8", ".pem", ".key"}:
                    manifest["summary"]["private_key_files"].append(str(fp.relative_to(_REPO_ROOT)))
                    manifest["credential_risks"].append({
                        "file": str(fp.relative_to(_REPO_ROOT)),
                        "risk": "private_key_in_repo",
                        "severity": "CRITICAL",
                        "cwe": "CWE-312",
                    })
                if fp.name == ".env":
                    lines = fp.read_text(errors="ignore").splitlines()
                    active = [l for l in lines if "=" in l and not l.strip().startswith("#")]
                    if active:
                        manifest["credential_risks"].append({
                            "file": str(fp.relative_to(_REPO_ROOT)),
                            "risk": "active_credentials_in_env",
                            "severity": "CRITICAL",
                            "cwe": "CWE-798",
                            "active_variable_count": len(active),
                        })
        findings = SecretsScanner().scan(manifest)
        if not findings:
            return f"✅ No secrets found in '{target_path}'"
        return "\n".join(
            f"- [{f.severity.value}] [{f.check_id}] {f.title}: {f.description[:200]}"
            for f in findings
        )

    return "audit_agent scanners not available."


# ── Full-repo listing ──────────────────────────────────────────────────────

_SKIP_DIRS = {
    ".git", ".venv", "__pycache__", "target", "dbt_packages",
    "node_modules", "audit_workspace", ".mypy_cache",
}


def _list_all_repo_files(ext_filter: str) -> str:
    """
    List every SQL and Python file in the repo (not just staged ones).
    Input: comma-separated extensions to include, e.g. '.sql,.py' (default: both).
    """
    exts_raw = ext_filter.strip()
    if exts_raw:
        exts = {e.strip() if e.strip().startswith(".") else f".{e.strip()}"
                for e in exts_raw.split(",")}
    else:
        exts = {".sql", ".py"}

    files = []
    for fp in _REPO_ROOT.rglob("*"):
        if any(part in _SKIP_DIRS for part in fp.parts):
            continue
        if fp.suffix in exts and fp.is_file():
            files.append(str(fp.relative_to(_REPO_ROOT)))

    if not files:
        return f"No {exts} files found in repo."
    return "\n".join(sorted(files))


# ── Tool registry ──────────────────────────────────────────────────────────

ALL_TOOLS: List[Tool] = [
    Tool(
        name        = "list_staged_files",
        description = "List all files staged for commit (about to be committed). Returns file paths, one per line.",
        func        = _list_staged_files,
        auto_run    = True,
    ),
    Tool(
        name        = "git_file_diff",
        description = "Show the staged diff for a specific file. Input: file path (e.g. vision_dbt/macros/foo.sql).",
        func        = _git_file_diff,
        auto_run    = False,
    ),
    Tool(
        name        = "read_staged_file",
        description = "Read the full staged (index) content of a file — exactly what will be committed.",
        func        = _read_staged_file,
        auto_run    = False,
    ),
    Tool(
        name        = "read_file",
        description = "Read any file from the working tree. Input: relative file path from repo root.",
        func        = _read_file,
        auto_run    = False,
    ),
    Tool(
        name        = "git_log",
        description = "Show recent git commits. Input: number of commits to show (default 5).",
        func        = _git_log,
        auto_run    = False,
    ),
    Tool(
        name        = "git_diff_branch",
        description = "Show a summary of all changes vs a base branch. Input: base branch name (e.g. 'main').",
        func        = _git_diff_branch,
        auto_run    = False,
    ),
    Tool(
        name        = "scan_sql_file",
        description = (
            "Analyze a SQL or dbt model file for: time complexity (O-notation), "
            "space complexity, Jinja injection risks, SELECT * abuse, ORDER BY waste, "
            "deep CTEs. Input: file path."
        ),
        func        = _scan_sql_file,
        auto_run    = False,
    ),
    Tool(
        name        = "scan_python_file",
        description = (
            "Analyze a Python file for: cyclomatic complexity, nested loops (O-notation), "
            "eval/exec usage, SQL string concatenation, subprocess shell=True. Input: file path."
        ),
        func        = _scan_python_file,
        auto_run    = False,
    ),
    Tool(
        name        = "check_secrets",
        description = (
            "Scan for exposed credentials: private key files (.p8/.pem), .env with active "
            "variables, hardcoded passwords in Python. Input: file path or '.' for full scan."
        ),
        func        = _check_secrets,
        auto_run    = True,
    ),
    Tool(
        name        = "search_codebase",
        description = (
            "Search for a pattern in the codebase using grep. "
            "Input: plain string, or JSON: {\"pattern\": \"ORDER BY\", \"path\": \"vision_dbt\", \"ext\": \".sql\"}"
        ),
        func        = _search_codebase,
        auto_run    = False,
    ),
    Tool(
        name        = "list_all_repo_files",
        description = (
            "List ALL SQL and Python files in the entire repository (not just staged). "
            "Input: comma-separated extensions to filter, e.g. '.sql,.py', or empty for both."
        ),
        func        = _list_all_repo_files,
        auto_run    = False,
    ),
]
