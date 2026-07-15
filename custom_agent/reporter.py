"""
Pretty terminal output and clean Markdown reports for the custom agent.

Terminal output uses ANSI colors + box-drawing characters.
Colors are suppressed automatically when stdout is not a TTY (CI, pipes).
"""
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from audit_agent.scanners.base import Finding, Severity, Category


# ── ANSI helpers ──────────────────────────────────────────────────────────────

_USE_COLOR = sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"{code}{text}\033[0m" if _USE_COLOR else text

RED    = '\033[91m'
ORANGE = '\033[33m'
YELLOW = '\033[93m'
BLUE   = '\033[94m'
CYAN   = '\033[96m'
GREY   = '\033[90m'
GREEN  = '\033[92m'
BOLD   = '\033[1m'
DIM    = '\033[2m'

_SEV_COLOR = {
    Severity.CRITICAL: RED,
    Severity.HIGH:     ORANGE,
    Severity.MEDIUM:   YELLOW,
    Severity.LOW:      BLUE,
    Severity.INFO:     GREY,
}

_SEV_ICON = {
    Severity.CRITICAL: "🔴",
    Severity.HIGH:     "🟠",
    Severity.MEDIUM:   "🟡",
    Severity.LOW:      "🔵",
    Severity.INFO:     "⚪",
}

_VERDICT_COLOR = {
    "BLOCK": RED,
    "WARN":  ORANGE,
    "PASS":  GREEN,
}

_VERDICT_ICON = {
    "BLOCK": "🚫",
    "WARN":  "⚠️ ",
    "PASS":  "✅",
}


# ── Scanner helpers ───────────────────────────────────────────────────────────

_BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".zip", ".tar", ".gz",
    ".whl", ".pyc", ".so", ".dll", ".exe", ".parquet", ".woff", ".woff2",
    ".ttf", ".eot", ".db", ".sqlite",
}
_GENERIC_SCAN_MAX_BYTES = 200_000


def scan_path(path: str) -> List[Finding]:
    """Run the right scanner for a file path and return Finding objects."""
    try:
        from audit_agent.scanners.sql_scanner    import SQLScanner
        from audit_agent.scanners.python_scanner import PythonScanner
    except ImportError:
        return []

    fp = _REPO_ROOT / path

    if fp.suffix.lower() in _BINARY_EXTENSIONS:
        return [Finding(
            check_id    = "GEN-000",
            title       = "Binary file — skipped",
            severity    = Severity.INFO,
            category    = Category.GENERIC,
            file        = path,
            description = "Binary file type — no text-based security/quality scan applies.",
        )]

    content = fp.read_text(errors="ignore") if fp.exists() else _git_show(path)
    if not content:
        return []

    if path.endswith(".sql"):
        manifest = _sql_manifest(path, content)
        return SQLScanner().scan(manifest)
    elif path.endswith(".py"):
        manifest = _py_manifest(path, content)
        return PythonScanner().scan(manifest)
    return _scan_generic(path, content[:_GENERIC_SCAN_MAX_BYTES])


def _scan_generic(path: str, content: str) -> List[Finding]:
    """
    Fallback scan for file types with no dedicated scanner (.csv, .yml, .md, .json, ...).
    Checks for exposed secrets only — time/space complexity analysis is not
    meaningful for non-code files, so it's explicitly called out as N/A.
    """
    from audit_agent.scanners.secrets_scanner import _RE_GENERIC_SECRET, _RE_PRIVATE_KEY_BLOCK

    findings: List[Finding] = []

    if _RE_PRIVATE_KEY_BLOCK.search(content):
        findings.append(Finding(
            check_id    = "SEC-006",
            title       = "Private key block embedded in file",
            severity    = Severity.CRITICAL,
            category    = Category.GENERIC,
            file        = path,
            description = "A PEM-format private key block was found in this file.",
            suggestion  = "Remove the key from version control and rotate it immediately.",
            cwe         = "CWE-312",
        ))

    for match in _RE_GENERIC_SECRET.finditer(content):
        line_no = content[: match.start()].count("\n") + 1
        findings.append(Finding(
            check_id     = "SEC-007",
            title        = "Potential hardcoded credential",
            severity     = Severity.HIGH,
            category     = Category.GENERIC,
            file         = path,
            line         = line_no,
            code_snippet = match.group(0)[:80],
            description  = f"Found what appears to be a hardcoded credential at line {line_no}.",
            suggestion   = "Move this value to an environment variable or secret manager.",
            cwe          = "CWE-798",
        ))

    if not findings:
        findings.append(Finding(
            check_id    = "GEN-001",
            title       = "No dedicated scanner for this file type",
            severity    = Severity.INFO,
            category    = Category.GENERIC,
            file        = path,
            description = (
                "This file was checked for exposed secrets/credentials only. "
                "Time/space complexity and code-quality checks only run for .sql and .py files."
            ),
        ))

    return findings


def scan_secrets(root: str = ".") -> List[Finding]:
    """Run the secrets scanner over a directory."""
    try:
        from audit_agent.scanners.secrets_scanner import SecretsScanner
    except ImportError:
        return []

    scan_root = _REPO_ROOT / root if root != "." else _REPO_ROOT
    manifest = _secrets_manifest(scan_root)
    return SecretsScanner().scan(manifest)


# ── Terminal printer ──────────────────────────────────────────────────────────

def print_report(
    file_findings: List[Tuple[str, List[Finding]]],
    secret_findings: List[Finding],
    trigger: str     = "manual",
    mode: str        = "static",
    total_scanned: int = 0,
) -> str:
    """
    Print a structured, colour-coded report to stdout.
    Returns the verdict string: BLOCK / WARN / PASS.
    """
    W = 68   # box width

    # ── Header ────────────────────────────────────────────────────────────────
    now = datetime.now().strftime("%Y-%m-%d  %H:%M")
    trigger_label = trigger.upper().replace("-", " ")
    _box_top(W)
    _box_row(f"  CODE REVIEW — {trigger_label}  ·  {now}", W)
    _box_row(f"  Mode: {mode}  ·  Files scanned: {total_scanned}", W)
    _box_bot(W)
    print()

    # ── Flatten all findings ──────────────────────────────────────────────────
    all_findings: List[Finding] = [f for _, fs in file_findings for f in fs] + secret_findings
    counts: Dict[str, int]      = {s.value: 0 for s in Severity}
    for f in all_findings:
        counts[f.severity.value] += 1

    # ── Summary table ─────────────────────────────────────────────────────────
    _section("SUMMARY", W)
    _summary_row(Severity.CRITICAL, counts["CRITICAL"])
    _summary_row(Severity.HIGH,     counts["HIGH"])
    _summary_row(Severity.MEDIUM,   counts["MEDIUM"])
    _summary_row(Severity.LOW,      counts["LOW"])
    if counts["INFO"]:
        _summary_row(Severity.INFO, counts["INFO"])
    print()

    # ── Secrets block (always first) ──────────────────────────────────────────
    if secret_findings:
        _section("SECRETS SCAN", W)
        for f in sorted(secret_findings, key=lambda x: x.severity.value):
            _print_finding(f, "  ")
        print()

    # ── Per-file findings ─────────────────────────────────────────────────────
    if file_findings:
        _section("FINDINGS BY FILE", W)
        for filepath, findings in file_findings:
            if not findings:
                _clean_file(filepath)
                continue
            _file_header(filepath, findings, W)
            for f in sorted(findings, key=lambda x: x.severity.value):
                _print_finding(f, "    ")
            print()

    # ── Verdict ───────────────────────────────────────────────────────────────
    verdict = _derive_verdict(counts)
    _print_verdict(verdict, counts, W)

    return verdict


def print_progress(filepath: str, idx: int, total: int):
    """Print a one-line progress update (overwritten each call)."""
    if not _USE_COLOR:
        return
    pct   = int((idx / total) * 100)
    bar   = "█" * (pct // 5) + "░" * (20 - pct // 5)
    label = filepath[-50:] if len(filepath) > 50 else filepath
    print(f"\r  [{bar}] {pct:3d}%  {label:<52}", end="", flush=True)
    if idx == total:
        print()   # newline on completion


# ── Markdown report ───────────────────────────────────────────────────────────

def build_markdown(
    file_findings: List[Tuple[str, List[Finding]]],
    secret_findings: List[Finding],
    trigger: str    = "manual",
    mode: str       = "static",
    total_scanned: int = 0,
    verdict: str    = "PASS",
) -> str:
    """Generate a clean Markdown report suitable for saving or posting to GitHub."""
    now    = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines  = [
        f"# Code Review Report",
        f"",
        f"| | |",
        f"|---|---|",
        f"| **Date** | {now} |",
        f"| **Trigger** | {trigger} |",
        f"| **Mode** | {mode} |",
        f"| **Files scanned** | {total_scanned} |",
        f"| **Verdict** | **{verdict}** |",
        f"",
    ]

    # Summary table
    all_findings = [f for _, fs in file_findings for f in fs] + secret_findings
    counts: Dict[str, int] = {s.value: 0 for s in Severity}
    for f in all_findings:
        counts[f.severity.value] += 1

    lines += [
        "## Summary",
        "",
        "| Severity | Count |",
        "|----------|------:|",
        f"| 🔴 CRITICAL | {counts['CRITICAL']} |",
        f"| 🟠 HIGH     | {counts['HIGH']} |",
        f"| 🟡 MEDIUM   | {counts['MEDIUM']} |",
        f"| 🔵 LOW      | {counts['LOW']} |",
        "",
    ]

    # Secrets
    if secret_findings:
        lines += ["## Secrets Scan", ""]
        for f in sorted(secret_findings, key=lambda x: x.severity.value):
            lines.append(_md_finding(f))
        lines.append("")

    # Per-file findings
    if file_findings:
        lines += ["## Findings by File", ""]
        for filepath, findings in file_findings:
            if not findings:
                lines.append(f"### ✅ `{filepath}`\nNo issues found.\n")
                continue
            sev_counts = _count_by_sev(findings)
            badge = "  ".join(
                f"{_SEV_ICON[s]} {n} {s.value}"
                for s, n in sev_counts.items() if n
            )
            lines.append(f"### `{filepath}`")
            lines.append(f"{badge}\n")
            for f in sorted(findings, key=lambda x: x.severity.value):
                lines.append(_md_finding(f))
            lines.append("")

    # Verdict
    icon = _VERDICT_ICON.get(verdict, "")
    lines += [
        "---",
        f"## {icon} Verdict: {verdict}",
        "",
        _verdict_rationale(verdict, counts),
        "",
        "_Generated by [custom_agent](custom_agent/) — ReAct + audit_agent scanners_",
    ]

    return "\n".join(lines)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _print_finding(f: Finding, indent: str):
    color  = _SEV_COLOR[f.severity]
    icon   = _SEV_ICON[f.severity]
    sev    = f.severity.value
    line   = f"  line {f.line}" if f.line else ""
    check  = f.check_id or ""

    print(f"{indent}{_c(color+BOLD, f'{icon} {sev:<8}')}  {_c(BOLD, f.title)}")
    print(f"{indent}          {_c(GREY, f'[{check}]{line}')}")
    print(f"{indent}          {f.description[:100]}")
    if f.suggestion:
        print(f"{indent}          {_c(CYAN, 'Fix:')} {f.suggestion[:100]}")
    if f.time_complexity:
        space = f.space_complexity or "-"
        print(f"{indent}          {_c(DIM, 'Time: ' + f.time_complexity + '  Space: ' + space)}")
    print()


def _file_header(filepath: str, findings: List[Finding], W: int):
    sev_counts = _count_by_sev(findings)
    badges = "  ".join(
        f"{_SEV_ICON[s]}{n}"
        for s, n in sev_counts.items() if n
    )
    short = filepath[-60:] if len(filepath) > 60 else filepath
    print(f"  {_c(BOLD, '📄 ' + short)}  {badges}")
    print(f"  {'─' * (W - 2)}")


def _clean_file(filepath: str):
    short = filepath[-60:] if len(filepath) > 60 else filepath
    print(f"  {_c(GREEN, '✅')} {_c(GREY, short)}")


def _summary_row(sev: Severity, count: int):
    color = _SEV_COLOR[sev]
    icon  = _SEV_ICON[sev]
    label = f"{icon}  {sev.value:<8}"
    bar   = _c(color, "▓" * min(count, 40)) if count else _c(GREY, "░")
    num   = _c(BOLD + color, str(count)) if count else _c(GREY, "0")
    print(f"  {_c(color, label)}  {bar}  {num}")


def _section(title: str, W: int):
    print(f"  {_c(BOLD, title)}")
    print(f"  {'═' * (W - 2)}")


def _box_top(W: int):
    print(_c(BOLD, f"  ╔{'═' * (W - 4)}╗"))

def _box_row(text: str, W: int):
    pad = W - 4 - len(text)
    print(_c(BOLD, f"  ║{text}{' ' * max(pad, 0)}║"))

def _box_bot(W: int):
    print(_c(BOLD, f"  ╚{'═' * (W - 4)}╝"))


def _print_verdict(verdict: str, counts: Dict[str, int], W: int):
    color  = _VERDICT_COLOR.get(verdict, GREEN)
    icon   = _VERDICT_ICON.get(verdict, "")
    rationale = _verdict_rationale(verdict, counts)
    print()
    _box_top(W)
    _box_row(_c(color + BOLD, f"  {icon}  VERDICT: {verdict}"), W)
    _box_row(f"  {rationale}", W)
    _box_bot(W)
    print()


def _derive_verdict(counts: Dict[str, int]) -> str:
    if counts["CRITICAL"] > 0:
        return "BLOCK"
    if counts["HIGH"] > 0:
        return "WARN"
    return "PASS"


def _verdict_rationale(verdict: str, counts: Dict[str, int]) -> str:
    if verdict == "BLOCK":
        return f"{counts['CRITICAL']} CRITICAL finding(s) must be fixed before committing."
    if verdict == "WARN":
        return f"{counts['HIGH']} HIGH finding(s) found — review before merging."
    return "No critical or high severity issues found."


def _count_by_sev(findings: List[Finding]) -> Dict[Severity, int]:
    result: Dict[Severity, int] = {s: 0 for s in Severity}
    for f in findings:
        result[f.severity] += 1
    return result


def _md_finding(f: Finding) -> str:
    icon  = _SEV_ICON[f.severity]
    line  = f" · line {f.line}" if f.line else ""
    check = f" `[{f.check_id}]`" if f.check_id else ""
    parts = [
        f"**{icon} {f.severity.value}**{check}{line} — **{f.title}**  ",
        f"{f.description[:200]}  ",
    ]
    if f.suggestion:
        parts.append(f"> 💡 **Fix:** {f.suggestion[:200]}  ")
    if f.time_complexity:
        parts.append(f"> 📊 Time: `{f.time_complexity}`  Space: `{f.space_complexity or '-'}`  ")
    return "\n".join(parts) + "\n"


def _git_show(path: str) -> str:
    import subprocess
    r = subprocess.run(
        ["git", "show", f":{path}"],
        capture_output=True, text=True,
        cwd=str(_REPO_ROOT),
    )
    return r.stdout


def _github_repo_slug() -> str:
    """Return 'owner/repo' parsed from the git remote origin URL, or '' if unavailable."""
    import subprocess
    r = subprocess.run(
        ["git", "config", "--get", "remote.origin.url"],
        capture_output=True, text=True, cwd=str(_REPO_ROOT),
    )
    url = r.stdout.strip()
    m = re.search(r"github\.com[:/](.+?)(?:\.git)?$", url)
    return m.group(1) if m else ""


def _github_ref() -> str:
    """Return the current commit SHA, so links point at exactly what was scanned."""
    import subprocess
    r = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, cwd=str(_REPO_ROOT),
    )
    return r.stdout.strip() or "main"


def _sql_manifest(path: str, content: str) -> dict:
    return {
        "sql_files": [{
            "path": path, "domain": None,
            "is_macro": "/macros/" in path,
            "cte_count": content.lower().count(" as ("),
            "has_select_star": bool(re.search(r"\bselect\s+\*", content, re.I)),
            "has_order_by":    bool(re.search(r"\border\s+by\b", content, re.I)),
            "hardcoded_uuids": re.findall(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                content, re.I),
            "jinja_variables": re.findall(r"\{\{\s*(\w+)\s*\}\}", content),
            "upper_trim_count": len(re.findall(r"UPPER\s*\(\s*TRIM\s*\(", content, re.I)),
            "line_count": content.count("\n"),
            "full_content": content,
        }],
        "credential_risks": [], "python_files": [],
        "summary": {"private_key_files": [], "env_files": []},
    }


def _py_manifest(path: str, content: str) -> dict:
    return {
        "python_files": [{"path": path, "full_content": content}],
        "credential_risks": [], "sql_files": [],
        "summary": {"private_key_files": [], "env_files": []},
    }


# ── Rich finding templates ───────────────────────────────────────────────────
# Maps check_id → mathematical proof + multiple fix options with before/after diffs
_RICH: dict = {
    "SQL-C001": {
        "title": "CROSS JOIN + WHERE — Snowflake builds the full Cartesian product before filtering",
        "proof": [
            "SQL Standard ISO/IEC 9075, Section 7.7 defines:",
            "<code>A CROSS JOIN B WHERE A.x = B.x</code>  ≡  <code>A INNER JOIN B ON A.x = B.x</code>",
            "Both expressions produce the set of all rows from A × B where the predicate holds. "
            "They are semantically identical — every RDBMS including Snowflake guarantees this.",
        ],
        "verdict": "✓ Options A, B, C all return exactly the same rows and columns.",
        "options": [
            {
                "label": "A", "title": "Replace CROSS JOIN with INNER JOIN ON (recommended)",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ eliminates Cartesian product",
                "desc": "Snowflake can now plan a hash join — reads only matching key pairs instead of building a full Cartesian product first.",
                "before_lbl": "✕ before — CROSS JOIN + WHERE",
                "before": "from hours_by_site ah\ncross join sizes_by_site ls\nwhere ah.site_id   = ls.site_id\n  and ah.usage_date = ls.usage_date",
                "after_lbl": "✓ after — Option A",
                "after": "from hours_by_site ah\ninner join sizes_by_site ls\n    on ah.site_id   = ls.site_id\n   and ah.usage_date = ls.usage_date",
            },
            {
                "label": "B", "title": "Keep CROSS JOIN, add QUALIFY row_number() to filter instead",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ same plan improvement via QUALIFY pushdown",
                "desc": "QUALIFY lets Snowflake apply the filter as a window-function predicate and push it down into the scan. Least-change option if you prefer to leave CROSS JOIN in place.",
                "before_lbl": "✕ before",
                "before": "cross join sizes_by_site ls\nwhere ah.site_id   = ls.site_id\n  and ah.usage_date = ls.usage_date",
                "after_lbl": "✓ after — Option B",
                "after": "cross join sizes_by_site ls\nqualify\n    row_number() over (\n        partition by ah.site_id, ah.usage_date,\n                     ls.locker_size, ah.usage_hour\n        order by 1\n    ) = 1",
                "note": None,
            },
            {
                "label": "C", "title": "No change — document the CROSS JOIN as intentional (safe, no perf gain)",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "— no perf change",
                "desc": "If the Cartesian product is small in practice the cost may not matter. Add a comment so future readers understand the intent.",
                "before_lbl": None, "before": None,
                "after_lbl": "✓ after — Option C (comment only)",
                "after": "-- cross join intentional: pairs all hours × all sizes\n-- for the same site+date. row count = hours × sizes.\ncross join sizes_by_site ls\nwhere ah.site_id = ls.site_id\n  and ah.usage_date = ls.usage_date",
                "note": "Then suppress the warning: python custom_agent/main.py --dismiss SQL-C001",
            },
        ],
    },
    "SQL-Q002": {
        "title": "ORDER BY at end of a TABLE materialisation — Snowflake runs the sort then discards it every run",
        "proof": [
            "Snowflake documentation (CREATE TABLE AS SELECT):",
            "<em>\"The order of rows in the result set of a SELECT is not guaranteed unless ORDER BY is "
            "specified in the outermost query that retrieves FROM the table.\"</em>",
            "A TABLE stores rows in micro-partitions (columnar storage). After CREATE TABLE AS SELECT … "
            "ORDER BY, Snowflake writes micro-partitions in its own internal order — the sort order you "
            "specified is <strong>never preserved</strong>.",
            "Result: the sort runs (consuming credits at O(n log n)), then the result is discarded.",
        ],
        "verdict": "✓ Removing ORDER BY from a TABLE materialisation produces byte-identical stored data.",
        "options": [
            {
                "label": "A", "title": "Remove ORDER BY from the final SELECT (recommended)",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ O(n log n) sort cost removed from every scheduled run",
                "desc": "Delete the ORDER BY line. Downstream models read micro-partition order regardless — removing ORDER BY changes nothing they see.",
                "before_lbl": "✕ before",
                "before": "select ...\nfrom final_data\norder by revenue_date desc,\n         x3_customer_name,\n         site_name, product_type",
                "after_lbl": "✓ after — Option A",
                "after": "select ...\nfrom final_data\n-- ORDER BY removed: table mat ignores sort",
                "note": None,
            },
            {
                "label": "B", "title": "Change materialisation to VIEW for sort-sensitive consumers",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ ORDER BY evaluated at query time where it actually works",
                "desc": "A VIEW applies ORDER BY at read time — Snowflake honours it for the consumer. No wasted sort at write time. Trade-off: re-runs the full query on every read.",
                "before_lbl": "✕ before (dbt config)",
                "before": "{{ config(materialized='table') }}\n\nselect ...\norder by revenue_date desc",
                "after_lbl": "✓ after — Option B",
                "after": "{{ config(materialized='view') }}\n\n-- ORDER BY now honoured at each read\nselect ...\norder by revenue_date desc",
                "note": None,
            },
            {
                "label": "C", "title": "Use CLUSTER BY for physical locality instead of ORDER BY",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ improves scan pruning on common filter columns",
                "desc": "CLUSTER BY rearranges micro-partitions on a background maintenance schedule — downstream date-range scans become faster. Requires Snowflake Automatic Clustering.",
                "before_lbl": "✕ before",
                "before": "{{ config(materialized='table') }}\nselect ...\norder by revenue_date",
                "after_lbl": "✓ after — Option C",
                "after": "{{ config(\n    materialized='table',\n    cluster_by=['revenue_date']\n) }}\nselect ...\n-- ORDER BY removed from final SELECT",
                "note": None,
            },
        ],
    },
    "SQL-C003": {
        "title": "Window function without a ROWS frame — Snowflake defaults to the slower RANGE scan mode",
        "proof": [
            "<strong>RANGE mode</strong> (default when ORDER BY present, no frame clause): groups rows "
            "with equal ORDER BY values into the same frame boundary — extra tie-checking work per row.",
            "<strong>ROWS mode</strong>: processes exactly one row at a time in ORDER — no tie-checking. "
            "Snowflake uses a simple running-aggregate cursor instead of the RANGE algorithm.",
            "For <code>row_number()</code> where the ORDER BY columns are unique per partition: ROWS and "
            "RANGE produce <strong>byte-identical values</strong>. The frame clause is a planner hint only.",
        ],
        "verdict": "✓ Safe to add ROWS frame when ORDER BY keys are unique per partition.",
        "options": [
            {
                "label": "A", "title": "Add ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (recommended)",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ removes tie-checking overhead from every window evaluation",
                "desc": "Add the ROWS frame clause explicitly. Output row_number() values are identical — only the planner hint changes.",
                "before_lbl": "✕ before — no frame clause (defaults to RANGE)",
                "before": "row_number() over (\n    partition by accounting_date_formatted\n    order by x3_customer_id asc, gl desc\n    -- RANGE UNBOUNDED default (implicit)\n) as \"LINE NUMBER\"",
                "after_lbl": "✓ after — Option A",
                "after": "row_number() over (\n    partition by accounting_date_formatted\n    order by x3_customer_id asc, gl desc\n    rows between unbounded preceding\n         and current row\n) as \"LINE NUMBER\"",
                "note": None,
            },
            {
                "label": "B", "title": "Restructure dedup using QUALIFY instead of a wrapping subquery",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ same plan improvement, cleaner SQL",
                "desc": "Where row_number() is used as a dedup filter (keep rn = 1), QUALIFY removes the outer subquery. The window function runs once instead of twice.",
                "before_lbl": "✕ before — wrapping subquery",
                "before": "select * from (\n    select ...,\n        row_number() over (\n            partition by ...\n            order by ...\n        ) as rn\n    from base\n) where rn = 1",
                "after_lbl": "✓ after — Option B",
                "after": "select ...,\n    row_number() over (\n        partition by ...\n        order by ...\n        rows between unbounded preceding\n             and current row\n    ) as rn\nfrom base\nqualify rn = 1",
                "note": None,
            },
            {
                "label": "C", "title": "Verify uniqueness first, then apply Option A",
                "safety": "⚠ verify before applying", "safety_class": "caution",
                "perf": "— same result as A once verified",
                "desc": "Run this check in Snowflake. If it returns 0 rows, ORDER BY keys are unique per partition and Option A is 100% safe.",
                "before_lbl": None, "before": None,
                "after_lbl": "Verification query — run this first",
                "after": "-- verify no ties in partition ORDER BY keys\nselect\n    accounting_date_formatted,\n    x3_customer_id, gl,\n    count(*) as cnt\nfrom your_source_table\ngroup by 1, 2, 3\nhaving cnt > 1;\n-- 0 rows → no ties → Option A is 100% safe",
                "note": None,
            },
        ],
    },
    "SQL-C004": {
        "title": "UPPER(TRIM()) on join keys at query time — forces full table scan, CPU cost on every row",
        "proof": [
            "<code>upper(trim(x))</code> is a <strong>pure deterministic function</strong> — same input "
            "always produces the same output, no side effects, no randomness.",
            "Pre-computing it in a staging model and storing the result produces the same value as "
            "computing it at join time.",
            "The join condition matches exactly the same row pairs before and after the change.",
        ],
        "verdict": "✓ All three options return exactly the same joined rows. No row is gained or lost.",
        "options": [
            {
                "label": "A", "title": "Normalise in a staging model — pre-compute UPPER(TRIM()) once (recommended)",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡⚡ enables micro-partition pruning + eliminates per-row CPU",
                "desc": "Create or update a staging model that stores the normalised key. All downstream joins use the pre-normalised column — no function at query time on the lookup side.",
                "before_lbl": "✕ before — computed at every join",
                "before": "on upper(trim(acr.Business_Partner))\n = upper(trim(c.x3_customer))\n-- repeated 8× in flash_net_revenue alone",
                "after_lbl": "✓ after — Option A",
                "after": "-- stg_active_customers.sql (new/updated):\nselect\n    upper(trim(Business_Partner))\n        as business_partner_key,\n    ...\nfrom {{ source('gopod', 'active_customers') }}\n\n-- in flash_net_revenue:\non acr.business_partner_key\n = upper(trim(c.x3_customer))",
                "note": None,
            },
            {
                "label": "B", "title": "Keep UPPER(TRIM()) — add a dbt test to enforce clean source data",
                "safety": "✓ 100% safe, no SQL changes", "safety_class": "safe",
                "perf": "— no perf gain now, prevents the need long-term",
                "desc": "Add a dbt generic test to the source. Once it passes consistently, remove UPPER(TRIM()) from joins in a later PR — the source is provably clean.",
                "before_lbl": None, "before": None,
                "after_lbl": "✓ after — Option B (schema.yml test)",
                "after": "sources:\n  - name: gopod\n    tables:\n      - name: active_customers_report\n        columns:\n          - name: Business_Partner\n            tests:\n              - dbt_utils.expression_is_true:\n                  expression: >\n                    Business_Partner =\n                    upper(trim(Business_Partner))",
                "note": None,
            },
            {
                "label": "C", "title": "Partial fix — normalise only the smaller lookup side via an inline CTE",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ halves UPPER(TRIM()) calls, enables partial pruning",
                "desc": "Without a staging model, normalise the smaller table inline via a CTE. One side benefits from pruning without creating a new file.",
                "before_lbl": "✕ before",
                "before": "left join active_customers acr\n  on upper(trim(acr.Business_Partner))\n   = upper(trim(c.x3_customer))",
                "after_lbl": "✓ after — Option C (inline CTE)",
                "after": "clean_customers as (\n    select\n        upper(trim(Business_Partner))\n            as business_partner_key,\n        customer_name, region, ...\n    from {{ ref('active_customers_report') }}\n),\n-- in the join:\nleft join clean_customers acr\n  on acr.business_partner_key\n   = upper(trim(c.x3_customer))",
                "note": None,
            },
        ],
    },
    "SQL-Q001": {
        "title": "SELECT * in final output — prevents Snowflake column pruning, schema drift is invisible",
        "proof": [
            "Expanding <code>SELECT *</code> to an explicit column list produces identical output "
            "<strong>if and only if</strong> the list contains exactly the same columns in the same "
            "order as <code>*</code> resolves to.",
            "The CTEs being selected from are defined in the same model file — their column list is "
            "fixed and readable right above the final SELECT.",
            "<strong>Risk to avoid for UNION ALL:</strong> both branches must list the same column names "
            "in the same order — verify CTE column lists match before expanding.",
        ],
        "verdict": "✓ Listing the same columns that SELECT * returns produces byte-identical output.",
        "options": [
            {
                "label": "A", "title": "Expand SELECT * to an explicit column list (recommended)",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ enables Snowflake column pruning for downstream models",
                "desc": "Run dbt compile and inspect the compiled SQL to get the exact column list — avoids transcription errors.",
                "before_lbl": "✕ before",
                "before": "select * from direct_sales_data\nunion all\nselect * from revenue_non_shareable_data",
                "after_lbl": "✓ after — Option A",
                "after": "select\n    transaction_date, reporting_month,\n    country, state, industry,\n    customer, customer_name,\n    product_dim, category,\n    is_diamond_cust, cust_group,\n    region, company_name,\n    total_rentals, daily_gross_revenue,\n    monthly_gross_revenue, monthly_net_revenue,\n    daily_weight, daily_allocated_net_revenue\nfrom direct_sales_data\nunion all\nselect  -- same column list in same order\n    ...\nfrom revenue_non_shareable_data",
                "note": None,
            },
            {
                "label": "B", "title": "Keep SELECT * — add dbt schema.yml column definitions to lock the contract",
                "safety": "✓ 100% safe, no SQL changes", "safety_class": "safe",
                "perf": "— no pruning gain, but schema drift causes CI failure",
                "desc": "Codify the expected schema in schema.yml. Any future upstream change that adds or renames a column surfaces as a dbt test failure instead of silently propagating.",
                "before_lbl": None, "before": None,
                "after_lbl": "✓ after — Option B (schema.yml)",
                "after": "models:\n  - name: locker_direct_and_fixed_dbt\n    columns:\n      - name: transaction_date\n        tests: [not_null]\n      - name: monthly_net_revenue\n        tests: [not_null]\n      # ... full list from dbt compile output",
                "note": None,
            },
            {
                "label": "C", "title": "Use dbt_utils.star() macro to auto-expand SELECT *",
                "safety": "✓ 100% safe", "safety_class": "safe",
                "perf": "⚡ auto-generates and keeps column list in sync at compile time",
                "desc": "The dbt_utils.star() macro generates an explicit column list from a relation at compile time. Requires dbt-utils >= 0.9.0 in packages.yml.",
                "before_lbl": "✕ before",
                "before": "select * from direct_sales_data\nunion all\nselect * from revenue_non_shareable_data",
                "after_lbl": "✓ after — Option C",
                "after": "-- requires dbt-utils in packages.yml\n{{ dbt_utils.star(ref('direct_sales_data')) }}\nfrom direct_sales_data\nunion all\n{{ dbt_utils.star(ref('revenue_non_shareable_data')) }}\nfrom revenue_non_shareable_data",
                "note": None,
            },
        ],
    },
}


# ── HTML report (artifact-style) ─────────────────────────────────────────────

def build_html(
    file_findings: List[Tuple[str, List[Finding]]],
    secret_findings: List[Finding],
    trigger: str    = "manual",
    mode: str       = "static",
    total_scanned: int = 0,
    verdict: str    = "PASS",
) -> str:
    """
    Generate a self-contained dark-theme HTML report.
    For known check_ids (SQL-C001/C003/C004, SQL-Q001/Q002) the card shows:
      - Real code extracted from the scanned file as the "before"
      - Multiple fix options (A/B/C) with transformed "after" code
      - Mathematical equivalence proof
      - Safety rating per option
    For other findings: basic card with description + suggestion.
    """
    import html as _html
    from custom_agent.transformer import get_options as _get_options

    now = datetime.now().strftime("%Y-%m-%d  %H:%M")

    # Deep-link each finding's filepath to the exact scanned commit on GitHub
    # (with a #L{line} anchor) so clicking it opens the real file, highlighted
    # at the flagged line — falls back to plain text if there's no remote.
    _repo_slug = _github_repo_slug()
    _ref       = _github_ref()

    def _source_link(filepath: str, line: int = 0) -> str:
        if not _repo_slug:
            return ""
        anchor = f"#L{line}" if line else ""
        return f"https://github.com/{_repo_slug}/blob/{_ref}/{filepath}{anchor}"

    def _filepath_html(filepath: str, line: int = 0) -> str:
        link = _source_link(filepath, line)
        if link:
            return (f'<a class="filepath" href="{_esc(link)}" '
                     f'target="_blank" rel="noopener">{_esc(filepath)}</a>')
        return f'<span class="filepath">{_esc(filepath)}</span>'

    # ── Count by severity ──────────────────────────────────────────────────────
    all_f: List[Finding] = [f for _, fs in file_findings for f in fs] + secret_findings
    counts: Dict[str, int] = {s.value: 0 for s in Severity}
    for f in all_f:
        counts[f.severity.value] += 1

    clean_files = [fp for fp, fs in file_findings if not fs]

    # ── CSS ───────────────────────────────────────────────────────────────────
    css = """
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg:#0E1117; --bg2:#161B22; --bg3:#1C2330; --bg4:#10161F;
      --border:#21262D; --text:#CDD5E0; --muted:#6E7B8C; --heading:#E6EDF3;
      --red:#E05252; --amber:#F0C060; --blue:#4A8FCC; --cyan:#56CCF2;
      --green:#3FB950; --green-dim:#1A3A22; --indigo:#818CF8; --orange:#E07830;
      --font: ui-monospace,'Cascadia Code','Fira Code','Menlo',monospace;
    }
    html { background:var(--bg); color:var(--text); font-family:var(--font);
           font-size:13px; line-height:1.6; }

    /* TOP BAR */
    .top-bar {
      position:sticky; top:0; z-index:100;
      background:var(--bg2); border-bottom:1px solid var(--border);
      padding:11px 24px; display:flex; align-items:center; gap:16px; flex-wrap:wrap;
    }
    .top-bar h1 { font-size:13px; font-weight:600; color:var(--heading); }
    .repo-tag { color:var(--muted); font-weight:400; }
    .stats { display:flex; gap:18px; margin-left:auto; flex-wrap:wrap; }
    .stat { display:flex; align-items:center; gap:6px; font-size:12px; }
    .dot { width:8px; height:8px; border-radius:2px; flex-shrink:0; }
    .dot.critical { background:var(--red); }
    .dot.high     { background:var(--orange); }
    .dot.medium   { background:var(--amber); }
    .dot.low      { background:var(--blue); }
    .dot.pass     { background:var(--green); }
    .stat-n { font-weight:600; color:var(--heading); font-variant-numeric:tabular-nums; }

    /* VERDICT BAR */
    .verdict-bar {
      padding:9px 24px; font-size:12px; font-weight:600;
      border-bottom:1px solid var(--border);
    }
    .verdict-bar.block { background:rgba(224,82,82,.12); color:var(--red); }
    .verdict-bar.warn  { background:rgba(240,192,96,.10); color:var(--amber); }
    .verdict-bar.pass  { background:rgba(63,185,80,.08);  color:var(--green); }

    /* FILTER TABS */
    .filter-bar {
      padding:10px 24px; display:flex; gap:4px; flex-wrap:wrap;
      border-bottom:1px solid var(--border); background:var(--bg);
      position:sticky; top:43px; z-index:99;
    }
    .tab {
      padding:4px 12px; border-radius:4px; border:1px solid transparent;
      cursor:pointer; font-family:var(--font); font-size:12px; color:var(--muted);
      background:none; transition:color .1s,background .1s,border-color .1s;
    }
    .tab:hover  { color:var(--heading); border-color:var(--border); }
    .tab.active { background:var(--bg3); color:var(--heading); border-color:var(--border); }

    /* DOMAIN PILLS */
    .domain-bar {
      padding:8px 24px; display:flex; gap:8px; flex-wrap:wrap;
      border-bottom:1px solid var(--border); background:var(--bg); align-items:center;
    }
    .domain-label { font-size:11px; color:var(--muted); margin-right:4px; }
    .dpill {
      font-size:11px; padding:2px 9px; border-radius:20px;
      border:1px solid var(--border); color:var(--muted); cursor:pointer; transition:all .1s;
    }
    .dpill:hover, .dpill.active { border-color:var(--blue); color:var(--blue); }
    .dpill.active { background:rgba(74,143,204,.08); }

    /* SECTION LABEL */
    .section-label {
      padding:20px 24px 8px;
      font-size:11px; letter-spacing:.08em; text-transform:uppercase; color:var(--muted);
    }

    /* CARDS */
    .cards { padding:0 24px 40px; display:flex; flex-direction:column; gap:12px; }
    .card {
      border:1px solid var(--border); border-radius:6px;
      background:var(--bg2); overflow:hidden;
      display:grid; grid-template-columns:4px 1fr;
    }
    .card[data-sev="CRITICAL"] .stripe { background:var(--red); }
    .card[data-sev="HIGH"]     .stripe { background:var(--orange); }
    .card[data-sev="MEDIUM"]   .stripe { background:var(--amber); }
    .card[data-sev="LOW"]      .stripe { background:var(--blue); }
    .card[data-sev="INFO"]     .stripe { background:var(--muted); }
    .card-body { padding:16px 20px 14px; }

    /* HEAD */
    .card-head { display:flex; align-items:flex-start; gap:9px; flex-wrap:wrap; margin-bottom:10px; }
    .badge {
      font-size:11px; font-weight:600; padding:2px 8px; border-radius:3px;
      letter-spacing:.04em; white-space:nowrap; flex-shrink:0;
    }
    .badge.CRITICAL { background:rgba(224,82,82,.15);  color:var(--red); }
    .badge.HIGH     { background:rgba(224,120,48,.15); color:var(--orange); }
    .badge.MEDIUM   { background:rgba(240,192,96,.13); color:var(--amber); }
    .badge.LOW      { background:rgba(74,143,204,.13); color:var(--blue); }
    .badge.INFO     { background:rgba(110,123,140,.15);color:var(--muted); }
    .filepath { font-size:12px; color:var(--blue); flex:1; word-break:break-all; line-height:1.5;
                text-decoration:none; }
    a.filepath:hover { text-decoration:underline; color:var(--cyan); }
    .line-tag { font-size:11px; color:var(--muted); white-space:nowrap; }

    /* DOMAIN CHIPS */
    .dchip {
      display:inline-block; font-size:10px; padding:1px 7px; border-radius:20px;
      border:1px solid var(--border); color:var(--muted); margin-right:4px; margin-bottom:8px;
    }
    .dchip.gopod  { border-color:rgba(74,143,204,.4);  color:#6AAEDD; }
    .dchip.photo  { border-color:rgba(240,192,96,.4);  color:#D4AA60; }
    .dchip.unifi  { border-color:rgba(129,140,248,.4); color:var(--indigo); }
    .dchip.macro  { border-color:rgba(63,185,80,.3);   color:#5DBB75; }
    .dchip.python { border-color:rgba(86,204,242,.3);  color:var(--cyan); }
    .dchip.secret { border-color:rgba(224,82,82,.4);   color:var(--red); }

    /* CARD TITLE */
    .card-title { font-size:13px; font-weight:700; color:var(--heading); margin-bottom:12px; line-height:1.4; }

    /* EXPLAIN PANELS */
    .explain-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:12px; }
    @media(max-width:680px) { .explain-grid { grid-template-columns:1fr; } }
    .explain-pane { border-radius:5px; overflow:hidden; border:1px solid var(--border); }
    .explain-header {
      padding:6px 12px; font-size:11px; font-weight:600; letter-spacing:.05em;
      display:flex; align-items:center; gap:6px; text-transform:uppercase;
    }
    .explain-header.what  { background:rgba(110,123,140,.08); color:var(--muted); }
    .explain-header.fix   { background:rgba(63,185,80,.07);   color:#5A9A6A; }
    .explain-body {
      padding:12px 14px; font-size:12px; line-height:1.8;
      color:var(--text); background:var(--bg3); white-space:pre-wrap; word-break:break-word;
    }

    /* COMPLEXITY */
    .complexity {
      font-size:11px; color:var(--muted); margin-top:8px;
      display:flex; gap:16px; flex-wrap:wrap;
    }
    .complexity span { display:flex; align-items:center; gap:5px; }
    .complexity code { color:var(--indigo); font-size:11px; }

    /* CLEAN FILE */
    .clean-section { padding:8px 24px 16px; }
    .clean-file {
      display:flex; align-items:center; gap:8px;
      font-size:12px; color:var(--muted); padding:4px 0;
    }
    .clean-file .tick { color:var(--green); }

    /* SUMMARY TABLE */
    .summary-table { width:100%; border-collapse:collapse; font-size:12px; }
    .summary-table td { padding:6px 12px; border:1px solid var(--border); }
    .summary-table td:last-child { text-align:right; font-variant-numeric:tabular-nums; font-weight:600; }

    /* HIDDEN */
    .card.hidden, .section-label.hidden, .clean-section.hidden { display:none; }

    /* ── PROOF BOX ── */
    .proof {
      background:#0D1F17; border:1px solid rgba(63,185,80,.22);
      border-radius:5px; padding:13px 16px; margin-bottom:16px; font-size:12px; line-height:1.85;
    }
    .proof-header {
      font-size:11px; font-weight:700; letter-spacing:.07em; text-transform:uppercase;
      color:#56D364; margin-bottom:9px;
    }
    .proof-body p { color:var(--text); margin-bottom:4px; }
    .proof-body code { color:var(--indigo); font-size:11.5px; }
    .proof-body strong { color:var(--heading); }
    .proof-body em { color:var(--text); font-style:italic; }
    .proof-verdict { color:#56D364; font-weight:600; margin-top:10px; font-size:12px; }

    /* ── OPTIONS ── */
    .options-label {
      font-size:11px; font-weight:700; letter-spacing:.07em; text-transform:uppercase;
      color:var(--muted); margin-bottom:10px;
    }
    .option { border:1px solid var(--border); border-radius:5px; overflow:hidden; margin-bottom:8px; }
    .option-head {
      display:flex; align-items:center; gap:10px; flex-wrap:wrap;
      padding:9px 14px; background:var(--bg3); cursor:pointer; user-select:none;
    }
    .option-head:hover { background:rgba(255,255,255,.025); }
    .opt-num {
      font-size:12px; font-weight:700; color:var(--heading);
      background:var(--bg4); border:1px solid var(--border);
      border-radius:3px; padding:2px 8px; flex-shrink:0;
    }
    .opt-title { font-size:12.5px; font-weight:600; color:var(--heading); flex:1; min-width:160px; }
    .risk { font-size:11px; padding:2px 9px; border-radius:20px; flex-shrink:0; font-weight:600; }
    .risk.safe    { background:rgba(63,185,80,.12);  color:#56D364; border:1px solid rgba(63,185,80,.25); }
    .risk.caution { background:rgba(240,192,96,.12); color:var(--amber); border:1px solid rgba(240,192,96,.30); }
    .opt-perf { font-size:11px; color:var(--muted); border-left:1px solid var(--border); padding-left:10px; }
    .expand-hint { font-size:11px; color:var(--muted); margin-left:auto; flex-shrink:0; }
    .option-body { padding:14px 16px; background:var(--bg2); display:none; }
    .option-body.open { display:block; }
    .opt-desc { font-size:12px; color:var(--text); line-height:1.75; margin-bottom:12px; }
    .opt-note { font-size:11.5px; color:var(--muted); margin-top:10px; line-height:1.6; }

    /* ── DIFF GRID ── */
    .diff-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:8px; }
    .diff-grid.single { grid-template-columns:1fr; }
    @media(max-width:700px){ .diff-grid { grid-template-columns:1fr; } }
    .diff-pane { border-radius:4px; overflow:hidden; border:1px solid var(--border); }
    .diff-header { padding:6px 12px; font-size:11px; font-weight:600; }
    .diff-header.before { background:rgba(224,82,82,.08); color:#A07070; }
    .diff-header.after  { background:rgba(63,185,80,.08); color:#5A9A6A; }
    .diff-code { padding:10px 0; background:var(--bg4); overflow-x:auto;
                 font-size:11.5px; line-height:1.75; white-space:pre; color:var(--text); }
    .line-ctx { display:block; padding:0 14px; color:var(--text); }
    .line-ctx::before { content:'  '; white-space:pre; }
    .line-del { display:block; padding:0 14px;
                background:rgba(224,82,82,.13); color:#E08080;
                border-left:2px solid var(--red); }
    .line-del::before { content:'- '; color:var(--red); font-weight:700; }
    .line-add { display:block; padding:0 14px;
                background:rgba(63,185,80,.10); color:#80D880;
                border-left:2px solid var(--green); }
    .line-add::before { content:'+ '; color:var(--green); font-weight:700; }

    /* ── WHY / MECHANISM sections ── */
    .why-box {
      background:rgba(129,140,248,.07); border:1px solid rgba(129,140,248,.2);
      border-radius:5px; padding:11px 14px; margin-bottom:12px; font-size:12px; line-height:1.8;
    }
    .why-label {
      font-size:10.5px; font-weight:700; letter-spacing:.08em; text-transform:uppercase;
      color:var(--indigo); margin-bottom:6px;
    }
    .why-text { color:var(--text); }
    .mech-row { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:10px; }
    @media(max-width:680px){ .mech-row { grid-template-columns:1fr; } }
    .mech-pane { border-radius:5px; overflow:hidden; border:1px solid var(--border); }
    .mech-header { padding:7px 12px; font-size:10.5px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; }
    .mech-header.before { background:rgba(224,82,82,.08); color:#B07070; border-bottom:1px solid rgba(224,82,82,.15); }
    .mech-header.after  { background:rgba(63,185,80,.08);  color:#60A870; border-bottom:1px solid rgba(63,185,80,.15); }
    .mech-text { padding:10px 12px; font-size:11.5px; line-height:1.8; color:var(--text); background:var(--bg3); }
    """

    # ── Severity section config ────────────────────────────────────────────────
    _SEV_LABEL = {
        "CRITICAL": "🔴 CRITICAL",
        "HIGH":     "🟠 HIGH",
        "MEDIUM":   "🟡 MEDIUM",
        "LOW":      "🔵 LOW",
        "INFO":     "⚪ INFO",
    }
    _SEV_GROUP = {
        "CRITICAL": "critical",
        "HIGH":     "high",
        "MEDIUM":   "medium",
        "LOW":      "low",
        "INFO":     "info",
    }

    def _esc(s: str) -> str:
        return _html.escape(str(s))

    def _domain_from_path(fp: str) -> str:
        up = fp.upper()
        if "/MACROS/" in up or "MACRO" in up: return "macro"
        if "GOPOD" in up:  return "gopod"
        if "PHOTO" in up:  return "photo"
        if "UNIFI" in up:  return "unifi"
        if fp.endswith(".py"): return "python"
        return "other"

    def _domain_chip(domain: str) -> str:
        label = {"gopod":"GOPOD","photo":"PHOTO","unifi":"UNIFI",
                 "macro":"macro","python":"Python","other":"other"}.get(domain, domain)
        return f'<span class="dchip {domain}">{label}</span>'

    # ── Proof text per check_id (what the code does + math equivalence) ────────
    _PROOF = {
        "SQL-C001": {
            "lines": [
                "SQL Standard ISO/IEC 9075, Section 7.7 defines:",
                "<code>A CROSS JOIN B WHERE A.x = B.x</code>  ≡  <code>A INNER JOIN B ON A.x = B.x</code>",
                "Both produce the same set of rows. Snowflake guarantees this — "
                "any optimizer-level rewrite preserves the output.",
            ],
            "verdict": "✓ Options A, B, C all return exactly the same rows and columns.",
            "before_label": "What your code does now",
            "after_label": "What changes after optimization",
        },
        "SQL-Q002": {
            "lines": [
                "Snowflake documentation (CREATE TABLE AS SELECT):",
                "<em>\"The order of rows stored in a TABLE is not guaranteed by ORDER BY.\"</em>",
                "TABLE materialisation writes rows in Snowflake's internal micro-partition order. "
                "The sort runs (consuming credits) then the result is <strong>immediately discarded</strong>.",
            ],
            "verdict": "✓ Removing ORDER BY from a TABLE produces byte-identical stored data.",
            "before_label": "What your code does now (wasted sort)",
            "after_label": "What changes after optimization",
        },
        "SQL-C003": {
            "lines": [
                "<strong>RANGE mode</strong> (default, no frame clause): groups rows with equal ORDER BY values "
                "into the same frame boundary — extra tie-checking per row.",
                "<strong>ROWS mode</strong>: processes exactly one row at a time — no tie-checking. "
                "Snowflake uses a simple running-aggregate cursor instead.",
                "For <code>row_number()</code> with unique ORDER BY keys per partition: "
                "ROWS and RANGE produce <strong>byte-identical values</strong>.",
            ],
            "verdict": "✓ Adding the ROWS frame is safe when ORDER BY keys are unique per partition.",
            "before_label": "What your code does now (RANGE default)",
            "after_label": "What changes after optimization",
        },
        "SQL-C004": {
            "lines": [
                "<code>upper(trim(x))</code> is a <strong>pure deterministic function</strong>: "
                "same input → same output, no side effects.",
                "Pre-computing it in a staging model and storing the result produces the "
                "identical value as computing it at join time.",
                "The join matches the same row pairs before and after the change.",
            ],
            "verdict": "✓ All three options return exactly the same joined rows — no row gained or lost.",
            "before_label": "What your code does now (per-row CPU)",
            "after_label": "What changes after optimization",
        },
        "SQL-Q001": {
            "lines": [
                "Expanding <code>SELECT *</code> to an explicit column list produces identical output "
                "<strong>if and only if</strong> the list contains the same columns in the same order.",
                "The CTEs are defined in the same file — their column list is readable and fixed.",
                "<strong>Risk for UNION ALL:</strong> both branches must list columns in the same order.",
            ],
            "verdict": "✓ Listing the same columns SELECT * returns produces byte-identical output.",
            "before_label": "What your code does now (no column pruning)",
            "after_label": "What changes after optimization",
        },
        # ── Python security ─────────────────────────────────────────────────
        "PY-S001": {
            "lines": [
                "<code>subprocess.run(cmd, shell=True)</code> passes <code>cmd</code> to "
                "<code>/bin/sh -c</code>.",
                "If <code>cmd</code> contains any user-controlled string, shell metacharacters "
                "(<code>;</code>, <code>&amp;&amp;</code>, <code>$(…)</code>) are interpreted — "
                "arbitrary OS command execution (CWE-78).",
                "Passing a <strong>list</strong> of arguments calls <code>execvp()</code> directly — "
                "the shell is never invoked.",
            ],
            "verdict": "✓ All three options produce the same subprocess result with no injection surface.",
            "before_label": "How it runs now (shell=True)",
            "after_label": "How it runs after fix (no shell)",
        },
        "PY-S002": {
            "lines": [
                "<code>eval(expr)</code> compiles and executes any Python expression, "
                "including <code>__import__('os').system('rm -rf /')</code>.",
                "<code>ast.literal_eval(expr)</code> parses only Python literal structures "
                "(strings, numbers, lists, dicts, tuples, booleans, None) — "
                "any other node raises <code>ValueError</code> before execution (CWE-78).",
                "<code>json.loads(s)</code> enforces strict RFC 8259 JSON — "
                "Python expressions are not valid JSON and will never execute.",
            ],
            "verdict": "✓ All three options parse the same data values with no code-execution surface.",
            "before_label": "How eval() works now (arbitrary execution)",
            "after_label": "How it works after fix (safe parse only)",
        },
        "PY-S003": {
            "lines": [
                "Building SQL with <code>+</code> or f-strings embeds user data into the query text. "
                "The database cannot distinguish your SQL structure from injected data (CWE-89, OWASP A03).",
                "Parameterised queries send the query template and values <strong>separately</strong>. "
                "The database driver escapes and quotes each value — user data is always a "
                "<em>value</em>, never SQL syntax.",
                "Snowflake connector, SQLAlchemy <code>text()</code>, and raw <code>cursor.execute(query, params)</code> "
                "all enforce this separation automatically.",
            ],
            "verdict": "✓ All three options return the same query results with SQL injection eliminated.",
            "before_label": "How SQL is built now (string concat)",
            "after_label": "How it works after fix (parameterised)",
        },
        # ── Python complexity ───────────────────────────────────────────────
        "PY-C001": {
            "lines": [
                "<strong>Cyclomatic complexity (CC)</strong> = number of independent execution paths. "
                "Every <code>if</code>, <code>elif</code>, <code>for</code>, <code>while</code>, "
                "<code>except</code>, and comprehension adds 1.",
                "Studies show CC &gt; 10 correlates with 3× more defects than CC ≤ 5. "
                "CC &gt; 15 requires 15+ test cases for full branch coverage.",
                "Extracting helper functions, using dispatch dicts, or converting to a class "
                "reduces each unit's CC without changing the overall behaviour.",
            ],
            "verdict": "✓ All three options preserve existing behaviour — only structure changes.",
            "before_label": "Current structure (high CC)",
            "after_label": "Proposed structure (lower CC per unit)",
        },
        "PY-C002": {
            "lines": [
                "Nested loops produce <strong>O(n × m) iterations</strong>. "
                "At n=10,000 and m=5,000: 50,000,000 Python iterations.",
                "A <strong>dict/set pre-built from the inner list</strong> turns the inner loop "
                "into an O(1) hash lookup — total O(n + m) instead of O(n × m).",
                "For data already in Snowflake, pushing the join to SQL eliminates Python "
                "iteration entirely — the warehouse uses distributed parallel hash joins.",
            ],
            "verdict": "✓ All three options return the same matched rows — only the algorithm changes.",
            "before_label": "Current approach (O(n × m) loop)",
            "after_label": "Optimised approach (O(n) or O(1) per item)",
        },
        "PY-C003": {
            "lines": [
                "Python strings are <strong>immutable</strong>. Every <code>s += fragment</code> "
                "allocates a new string of length len(s)+len(fragment) and copies both.",
                "After n iterations: total bytes copied = 1+2+…+n = <strong>O(n²)</strong>. "
                "For 10,000 iterations of 10-char fragments: ~500MB of intermediate allocations.",
                "<code>''.join(parts)</code> computes total length once, allocates one buffer, "
                "copies each part exactly once — <strong>O(n) time and memory</strong>.",
            ],
            "verdict": "✓ All three options produce the identical final string — only the build method changes.",
            "before_label": "Current build method (O(n²) += loop)",
            "after_label": "Optimised build method (O(n) join)",
        },
        "PY-C004": {
            "lines": [
                "Python's default recursion limit is <strong>1,000 frames</strong>. "
                "Exceeding it raises <code>RecursionError</code> — the program crashes.",
                "Each stack frame consumes ~1–2 KB of C stack space. "
                "At depth 1,000: ~2 MB stack. Most OS defaults are 8 MB.",
                "An iterative version using a Python <code>list</code> as a stack uses "
                "<strong>heap memory</strong> — no depth limit, no frame overhead.",
            ],
            "verdict": "✓ All three options compute the same result — only the call mechanism changes.",
            "before_label": "Current approach (recursive call frames)",
            "after_label": "Optimised approach (iterative or memoised)",
        },
        "PY-C005": {
            "lines": [
                "Building a list inside nested loops calls <code>list.append()</code> O(n×m) times. "
                "Python's list doubles its backing array periodically — each doubling copies all "
                "existing elements.",
                "For n×m = 1,000,000 items: ~20 doublings, each copying progressively more items. "
                "Peak memory = final list size; GC pressure grows throughout.",
                "Pre-allocating with <code>numpy.empty(n*m)</code> eliminates all intermediate "
                "copies — one allocation, O(1) element writes.",
            ],
            "verdict": "✓ All three options accumulate the same data — only the allocation strategy changes.",
            "before_label": "Current accumulation (incremental appends)",
            "after_label": "Optimised accumulation (pre-allocated or streamed)",
        },
        # ── Python quality ──────────────────────────────────────────────────
        "PY-Q002": {
            "lines": [
                "Functions longer than ~50 lines are statistically harder to review correctly — "
                "reviewers cannot hold full context in working memory.",
                "A long function typically handles multiple concerns in sequence (validate → load → "
                "transform → output). Each concern can become an independent, testable helper.",
                "Extracting helpers does not change the function's behaviour — only its internal "
                "structure and testability.",
            ],
            "verdict": "✓ All three options preserve existing behaviour — only structure and readability change.",
            "before_label": "Current function (long, multi-concern)",
            "after_label": "Proposed structure (short helpers, single concern each)",
        },
    }

    def _diff_html(before: str, after: str):
        """
        Compute a line-level diff and return (before_html, after_html).
        Removed lines get .line-del (red), added lines get .line-add (green),
        unchanged lines get .line-ctx (neutral).
        """
        import difflib
        b_lines = (before or "").splitlines()
        a_lines = (after  or "").splitlines()
        b_out, a_out = [], []
        for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(
            None, b_lines, a_lines, autojunk=False
        ).get_opcodes():
            if tag == "equal":
                for ln in b_lines[i1:i2]:
                    b_out.append(f'<span class="line-ctx">{_esc(ln) or " "}</span>')
                for ln in a_lines[j1:j2]:
                    a_out.append(f'<span class="line-ctx">{_esc(ln) or " "}</span>')
            elif tag in ("delete", "replace"):
                for ln in b_lines[i1:i2]:
                    b_out.append(f'<span class="line-del">{_esc(ln) or " "}</span>')
                if tag == "replace":
                    for ln in a_lines[j1:j2]:
                        a_out.append(f'<span class="line-add">{_esc(ln) or " "}</span>')
            elif tag == "insert":
                for ln in a_lines[j1:j2]:
                    a_out.append(f'<span class="line-add">{_esc(ln) or " "}</span>')
        return "\n".join(b_out), "\n".join(a_out)

    def _rich_card(filepath: str, f: Finding) -> str:
        """Render a multi-option card with diff highlighting and detailed explanations."""
        domain   = _domain_from_path(filepath)
        sev      = f.severity.value
        group    = _SEV_GROUP.get(sev, "info")
        check    = _esc(f.check_id or sev)
        line_tag = f'<span class="line-tag">line {f.line}</span>' if f.line else ""
        proof    = _PROOF.get(f.check_id, {})
        options  = _get_options(f.check_id, f.code_snippet or "")

        # Proof box
        proof_lines_html = "".join(f"<p>{p}</p>" for p in proof.get("lines", []))
        proof_html = (
            f'<div class="proof">'
            f'<div class="proof-header">📐 MATHEMATICAL EQUIVALENCE PROOF</div>'
            f'<div class="proof-body">{proof_lines_html}</div>'
            f'<div class="proof-verdict">{proof.get("verdict","")}</div>'
            f'</div>'
        ) if proof else ""

        # Option cards
        opts_html = ""
        for opt in options:
            sc = opt.get("safety_class", "safe")

            # WHY section
            why_html = ""
            if opt.get("why"):
                why_html = (
                    f'<div class="why-box">'
                    f'<div class="why-label">💡 Why we change this</div>'
                    f'<div class="why-text">{_esc(opt["why"])}</div>'
                    f'</div>'
                )

            # HOW BEFORE / HOW AFTER mechanism explanation
            mech_html = ""
            if opt.get("how_before") or opt.get("how_after"):
                before_mech = (
                    f'<div class="mech-pane">'
                    f'<div class="mech-header before">⚠ How it worked before</div>'
                    f'<div class="mech-text">{_esc(opt.get("how_before",""))}</div>'
                    f'</div>'
                ) if opt.get("how_before") else ""
                after_mech = (
                    f'<div class="mech-pane">'
                    f'<div class="mech-header after">✓ How it works after</div>'
                    f'<div class="mech-text">{_esc(opt.get("how_after",""))}</div>'
                    f'</div>'
                ) if opt.get("how_after") else ""
                mech_html = f'<div class="mech-row">{before_mech}{after_mech}</div>'

            # Diff-highlighted code panels
            has_before = bool(opt.get("before"))
            grid_cls   = "" if has_before else " single"
            if has_before and opt.get("after"):
                b_html, a_html = _diff_html(opt["before"], opt.get("after", ""))
            else:
                b_html = _esc(opt.get("before", ""))
                a_html = _esc(opt.get("after",  ""))

            before_pane = ""
            if has_before:
                before_pane = (
                    f'<div class="diff-pane">'
                    f'<div class="diff-header before">'
                    f'✕ {_esc(proof.get("before_label","Your code — before"))}'
                    f'</div>'
                    f'<pre class="diff-code">{b_html}</pre>'
                    f'</div>'
                )
            after_lbl = f'✓ After — Option {opt["label"]}'
            after_pane = (
                f'<div class="diff-pane">'
                f'<div class="diff-header after">{_esc(after_lbl)}</div>'
                f'<pre class="diff-code">{a_html}</pre>'
                f'</div>'
            )

            note_html = (
                f'<p class="opt-note">📝 {_esc(opt["note"])}</p>'
                if opt.get("note") else ""
            )

            opts_html += (
                f'<div class="option">'
                f'<div class="option-head" onclick="toggleOpt(this)">'
                f'<span class="opt-num">{opt["label"]}</span>'
                f'<span class="opt-title">{_esc(opt["title"])}</span>'
                f'<span class="risk {sc}">{_esc(opt["safety"])}</span>'
                f'<span class="opt-perf">{_esc(opt["perf"])}</span>'
                f'<span class="expand-hint">▸ expand</span>'
                f'</div>'
                f'<div class="option-body">'
                f'{why_html}'
                f'{mech_html}'
                f'<div class="diff-grid{grid_cls}">{before_pane}{after_pane}</div>'
                f'{note_html}'
                f'</div>'
                f'</div>\n'
            )

        # Complexity chips
        complexity = ""
        if f.time_complexity:
            space = _esc(f.space_complexity or "—")
            complexity = (
                f'<div class="complexity">'
                f'<span>⏱ Time <code>{_esc(f.time_complexity)}</code></span>'
                f'<span>💾 Space <code>{space}</code></span>'
                f'</div>'
            )

        opts_section = (
            f'<div class="options-label">CHOOSE AN OPTION — CLICK TO EXPAND</div>'
            f'{opts_html}'
        ) if opts_html else ""

        return (
            f'<div class="card" data-sev="{sev}" data-group="{group}" data-domains="{domain}">'
            f'<div class="stripe"></div>'
            f'<div class="card-body">'
            f'<div class="card-head">'
            f'<span class="badge {sev}">{check}</span>'
            f'{_filepath_html(filepath, f.line)}'
            f'{line_tag}'
            f'</div>'
            f'{_domain_chip(domain)}'
            f'<div class="card-title">{_esc(f.title)}</div>'
            f'{proof_html}'
            f'{opts_section}'
            f'{complexity}'
            f'</div></div>\n'
        )

    def _finding_card(filepath: str, f: Finding) -> str:
        if f.check_id and f.check_id in _PROOF:
            return _rich_card(filepath, f)
        domain   = _domain_from_path(filepath)
        sev      = f.severity.value
        group    = _SEV_GROUP.get(sev, "info")
        check    = _esc(f.check_id or sev)
        line_tag = f'<span class="line-tag">line {f.line}</span>' if f.line else ""
        desc_panel = (
            f'<div class="explain-pane">'
            f'<div class="explain-header what">⚠ What it does</div>'
            f'<div class="explain-body">{_esc(f.description)}</div>'
            f'</div>'
        ) if f.description else ""
        fix_panel = (
            f'<div class="explain-pane">'
            f'<div class="explain-header fix">✓ How to fix</div>'
            f'<div class="explain-body">{_esc(f.suggestion)}</div>'
            f'</div>'
        ) if f.suggestion else ""
        grid_open  = '<div class="explain-grid">' if (desc_panel or fix_panel) else ""
        grid_close = '</div>' if (desc_panel or fix_panel) else ""
        complexity = ""
        if f.time_complexity:
            space = _esc(f.space_complexity or "—")
            complexity = (
                f'<div class="complexity">'
                f'<span>⏱ Time <code>{_esc(f.time_complexity)}</code></span>'
                f'<span>💾 Space <code>{space}</code></span>'
                f'</div>'
            )
        return (
            f'<div class="card" data-sev="{sev}" data-group="{group}" data-domains="{domain}">'
            f'<div class="stripe"></div>'
            f'<div class="card-body">'
            f'<div class="card-head">'
            f'<span class="badge {sev}">{check}</span>'
            f'{_filepath_html(filepath, f.line)}'
            f'{line_tag}'
            f'</div>'
            f'{_domain_chip(domain)}'
            f'<div class="card-title">{_esc(f.title)}</div>'
            f'{grid_open}{desc_panel}{fix_panel}{grid_close}'
            f'{complexity}'
            f'</div></div>\n'
        )

    # ── Build sections ─────────────────────────────────────────────────────────
    sections_html = ""
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
        if counts[sev] == 0:
            continue
        label = _SEV_LABEL[sev]
        group = _SEV_GROUP[sev]
        cards_for_sev = ""
        for filepath, findings in file_findings:
            for f in findings:
                if f.severity.value == sev:
                    cards_for_sev += _finding_card(filepath, f)
        for f in secret_findings:
            if f.severity.value == sev:
                cards_for_sev += _finding_card(f.file or "secrets-scan", f)
        if cards_for_sev:
            sections_html += (
                f'<div class="section-label" id="lbl-{group}">{label}</div>\n'
                f'<div class="cards" id="sec-{group}">\n{cards_for_sev}</div>\n'
            )

    # ── Clean files section ────────────────────────────────────────────────────
    clean_html = ""
    if clean_files:
        rows = "".join(
            f'<div class="clean-file"><span class="tick">✅</span>{_esc(fp)}</div>'
            for fp in clean_files
        )
        clean_html = (
            f'<div class="section-label" id="lbl-clean">✅ CLEAN FILES — No Issues Found</div>\n'
            f'<div class="clean-section" id="sec-clean">{rows}</div>\n'
        )

    # ── Collect all unique domains ─────────────────────────────────────────────
    domains_seen: List[str] = []
    for fp, fs in file_findings:
        d = _domain_from_path(fp)
        if d not in domains_seen:
            domains_seen.append(d)
    for f in secret_findings:
        if "secret" not in domains_seen:
            domains_seen.append("secret")

    _DLABEL = {
        "gopod": "GOPOD", "photo": "PHOTO", "unifi": "UNIFI",
        "macro": "macros", "python": "Python", "secret": "Secrets", "other": "Other",
    }
    domain_pills = "".join(
        f'<button class="dpill" onclick="filterDomain(this,\'{d}\')">{_DLABEL.get(d, d)}</button>'
        for d in domains_seen
    )

    # ── Stats bar ──────────────────────────────────────────────────────────────
    stat_items = ""
    _DOT = {"CRITICAL":"critical","HIGH":"high","MEDIUM":"medium","LOW":"low","INFO":"pass"}
    for sev in ["CRITICAL","HIGH","MEDIUM","LOW"]:
        n = counts[sev]
        if n:
            stat_items += (
                f'<div class="stat">'
                f'<div class="dot {_DOT[sev]}"></div>'
                f'<span class="stat-n">{n}</span>&nbsp;{sev}'
                f'</div>'
            )
    if not stat_items:
        stat_items = '<div class="stat"><div class="dot pass"></div><span class="stat-n">0</span>&nbsp;issues</div>'

    # ── Verdict bar ────────────────────────────────────────────────────────────
    verdict_icon  = {"BLOCK":"🚫","WARN":"⚠️","PASS":"✅"}.get(verdict, "✅")
    verdict_class = verdict.lower()
    verdict_msg   = {
        "BLOCK": f'{counts["CRITICAL"]} CRITICAL issue(s) must be fixed before committing.',
        "WARN":  f'{counts["HIGH"]} HIGH issue(s) found — review before merging.',
        "PASS":  "No critical or high severity issues found.",
    }.get(verdict, "")

    # ── Filter tab buttons ─────────────────────────────────────────────────────
    tab_buttons = '<button class="tab active" onclick="filter(this,\'all\')">ALL</button>'
    for sev, group in _SEV_GROUP.items():
        if counts[sev]:
            icon = {"CRITICAL":"🔴","HIGH":"🟠","MEDIUM":"🟡","LOW":"🔵","INFO":"⚪"}[sev]
            tab_buttons += (
                f'<button class="tab" onclick="filter(this,\'{group}\')">'
                f'{icon} {sev} ({counts[sev]})'
                f'</button>'
            )
    if clean_files:
        tab_buttons += (
            f'<button class="tab" onclick="filter(this,\'clean\')">✅ CLEAN ({len(clean_files)})</button>'
        )

    # ── JavaScript ────────────────────────────────────────────────────────────
    js = """
    function toggleOpt(head) {
        const body = head.nextElementSibling;
        const hint = head.querySelector('.expand-hint');
        const open = body.classList.toggle('open');
        hint.textContent = open ? '▾ collapse' : '▸ expand';
    }
    let activeSev = 'all', activeDomain = 'all';
    function filter(btn, group) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        btn.classList.add('active');
        activeSev = group;
        applyFilters();
    }
    function filterDomain(btn, domain) {
        document.querySelectorAll('.dpill').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        activeDomain = domain;
        applyFilters();
    }
    function applyFilters() {
        document.querySelectorAll('.card').forEach(c => {
            const sevOk = activeSev === 'all' || c.dataset.group === activeSev;
            const domOk = activeDomain === 'all' || (c.dataset.domains || '').includes(activeDomain);
            c.classList.toggle('hidden', !(sevOk && domOk));
        });
        ['critical','high','medium','low','info'].forEach(g => {
            const lbl = document.getElementById('lbl-' + g);
            const sec = document.getElementById('sec-' + g);
            if (!lbl) return;
            const any = [...document.querySelectorAll(`.card[data-group="${g}"]`)]
                          .some(c => !c.classList.contains('hidden'));
            lbl.classList.toggle('hidden', !any);
            if (sec) sec.classList.toggle('hidden', !any);
        });
        const cleanLbl = document.getElementById('lbl-clean');
        const cleanSec = document.getElementById('sec-clean');
        if (cleanLbl) {
            const show = activeSev === 'all' || activeSev === 'clean';
            cleanLbl.classList.toggle('hidden', !show);
            if (cleanSec) cleanSec.classList.toggle('hidden', !show);
        }
    }
    """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Code Review — {_esc(trigger.upper())} · {_esc(now)}</title>
<style>{css}</style>
</head>
<body>

<div class="top-bar">
  <h1>Code Review Report <span class="repo-tag">· {_esc(trigger.upper())} · {_esc(mode)} · {total_scanned} files · {_esc(now)}</span></h1>
  <div class="stats">{stat_items}</div>
</div>

<div class="verdict-bar {verdict_class}">
  {verdict_icon}&nbsp;&nbsp;VERDICT: {verdict} — {_esc(verdict_msg)}
</div>

<div class="filter-bar">{tab_buttons}</div>

<div class="domain-bar">
  <span class="domain-label">Domain:</span>
  <button class="dpill active" onclick="filterDomain(this,'all')">All</button>
  {domain_pills}
</div>

{sections_html}
{clean_html}

<script>{js}</script>
</body>
</html>"""


def _secrets_manifest(scan_root: Path) -> dict:
    from audit_agent.scanners.secrets_scanner import _RE_PRIVATE_KEY_BLOCK

    manifest: dict = {
        "root": str(scan_root),
        "summary": {"private_key_files": [], "env_files": []},
        "credential_risks": [], "python_files": [], "sql_files": [],
    }
    _SKIP = {".git", ".venv", "__pycache__", "target", "dbt_packages"}
    for fp in scan_root.rglob("*"):
        if any(p in _SKIP for p in fp.parts):
            continue
        if fp.suffix in {".p8", ".pem", ".key"} and fp.is_file():
            # Match by content, not just extension — a vendored CA bundle
            # (e.g. certifi's cacert.pem) has the same extension as a real
            # private key but contains CERTIFICATE blocks, not PRIVATE KEY ones.
            if _RE_PRIVATE_KEY_BLOCK.search(fp.read_text(errors="ignore")):
                rel = str(fp.relative_to(scan_root))
                manifest["summary"]["private_key_files"].append(rel)
                manifest["credential_risks"].append({
                    "file": rel, "risk": "private_key_in_repo",
                    "severity": "CRITICAL", "cwe": "CWE-312",
                })
        if fp.name == ".env" and fp.is_file():
            active = [l for l in fp.read_text(errors="ignore").splitlines()
                      if "=" in l and not l.strip().startswith("#")]
            if active:
                rel = str(fp.relative_to(scan_root))
                manifest["credential_risks"].append({
                    "file": rel, "risk": "active_credentials_in_env",
                    "severity": "CRITICAL", "cwe": "CWE-798",
                    "active_variable_count": len(active),
                })
    return manifest
