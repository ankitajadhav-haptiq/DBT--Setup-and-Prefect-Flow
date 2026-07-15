"""
SQL Scanner — Time Complexity, Space Complexity, Security, and Quality checks.

Time complexity is estimated from structural signals:
  - JOIN count and type     → O(n log n) to O(n²)
  - Subquery nesting        → multiplies complexity
  - Window functions        → O(n log n) sort cost
  - CROSS JOIN              → O(n²) always

Space complexity is estimated from:
  - SELECT *                → O(n × all_columns)
  - CTE materialization     → O(n) per CTE
  - DISTINCT / UNION        → O(n) dedup buffer
  - ORDER BY on large sets  → O(n) sort buffer
"""
import re
from typing import List, Tuple

from .base import BaseScanner, Finding, Severity, Category

# ── Regex patterns ──────────────────────────────────────────────────────────
RE_JOIN         = re.compile(r"\bJOIN\b",             re.IGNORECASE)
RE_CROSS_JOIN   = re.compile(r"\bCROSS\s+JOIN\b",    re.IGNORECASE)
RE_LEFT_JOIN    = re.compile(r"\bLEFT\s+JOIN\b",     re.IGNORECASE)
RE_SUBQUERY     = re.compile(r"\(\s*SELECT\b",        re.IGNORECASE)
RE_WINDOW_FN    = re.compile(r"\bOVER\s*\(",          re.IGNORECASE)
RE_SELECT_STAR  = re.compile(r"\bSELECT\s+\*",       re.IGNORECASE)
RE_ORDER_BY     = re.compile(r"\bORDER\s+BY\b",       re.IGNORECASE)
RE_DISTINCT     = re.compile(r"\bDISTINCT\b",         re.IGNORECASE)
RE_GROUP_BY     = re.compile(r"\bGROUP\s+BY\b",       re.IGNORECASE)
RE_UNION        = re.compile(r"\bUNION(?!\s+ALL)\b",  re.IGNORECASE)
RE_CTE          = re.compile(r"^\s*\w+\s+AS\s*\(",    re.IGNORECASE | re.MULTILINE)
RE_WINDOW_NOFRAME = re.compile(
    r"\bOVER\s*\(\s*(?:PARTITION\s+BY\s+\S+\s+)?ORDER\s+BY\s+\S+\s*\)",
    re.IGNORECASE,
)
RE_JINJA_VAR_IN_WHERE = re.compile(
    r"WHERE.*\{\{\s*(\w+)\s*\}\}",
    re.IGNORECASE | re.DOTALL,
)
RE_UPPER_TRIM   = re.compile(r"UPPER\s*\(\s*TRIM\s*\(",  re.IGNORECASE)
RE_TRY_CAST_LOOP = re.compile(r"TRY_CAST",              re.IGNORECASE)

# Jinja macro argument directly in WHERE/HAVING
RE_JINJA_MACRO_ARG = re.compile(
    r"['\"]?\s*\{\{\s*(\w+)\s*\}\}\s*['\"]?",
    re.IGNORECASE,
)


def _extract_lines(sql: str, pattern: re.Pattern, context: int = 6,
                   last: bool = False) -> str:
    """Extract lines around a regex match (first or last) returning real file context."""
    matches = list(pattern.finditer(sql))
    if not matches:
        return ""
    m = matches[-1] if last else matches[0]
    lines = sql.splitlines()
    char_pos = 0
    match_line = 0
    for i, line in enumerate(lines):
        if char_pos <= m.start() < char_pos + len(line) + 1:
            match_line = i
            break
        char_pos += len(line) + 1
    start = max(0, match_line - context)
    end   = min(len(lines), match_line + context + 1)
    return "\n".join(lines[start:end]).strip()


def _count_cte_depth(sql: str) -> int:
    """Estimate CTE count from 'WITH ... AS (' patterns."""
    withs = re.findall(r"\bWITH\b", sql, re.IGNORECASE)
    ctes  = re.findall(r"\)\s*,\s*\n?\s*\w+\s+AS\s*\(", sql, re.IGNORECASE)
    return max(1, len(ctes) + len(withs))


def _estimate_time_complexity(
    join_count: int,
    cross_join_count: int,
    subquery_count: int,
    window_count: int,
    cte_depth: int,
) -> Tuple[str, str]:
    """
    Returns (complexity_string, rationale).
    Cross join is always O(n²).
    Deep CTEs (>6) with many joins can approach O(n³).
    """
    if cross_join_count > 0:
        return (
            "O(n²) — CROSS JOIN",
            f"CROSS JOIN detected: every row in left table joined to every row in right ({cross_join_count} cross joins). "
            "Result set grows as n × m.",
        )
    if subquery_count > 2:
        return (
            f"O(n²) — {subquery_count} nested subqueries",
            f"{subquery_count} correlated subqueries found. Each may execute once per outer row.",
        )
    if join_count > 4 and cte_depth > 5:
        return (
            f"O(n log n) – O(n²) — {join_count} JOINs + {cte_depth} CTEs",
            f"{join_count} JOIN operations across {cte_depth} CTE layers. "
            "Risk of full micro-partition scans on un-clustered tables.",
        )
    if join_count > 0 and window_count > 0:
        return (
            "O(n log n) — JOINs + window functions",
            f"{join_count} JOIN(s) with {window_count} window function(s). "
            "Both require sort operations. Ensure clustering keys match sort columns.",
        )
    if window_count > 0:
        return (
            "O(n log n) — window functions",
            f"{window_count} window function(s) require an implicit sort on the ORDER BY column(s).",
        )
    if join_count > 2:
        return (
            "O(n log n) — multi-table joins",
            f"{join_count} JOIN operations. Performance depends on whether joined columns "
            "are clustering keys or have Snowflake search optimization enabled.",
        )
    if join_count == 1:
        return (
            "O(n log n) — single join",
            "One JOIN; efficient if both sides are clustered or small.",
        )
    return (
        "O(n) — sequential scan",
        "No joins or window functions. Linear scan of the source table(s).",
    )


def _estimate_space_complexity(
    has_select_star: bool,
    distinct_count: int,
    cte_depth: int,
    union_count: int,
    order_by_in_table: bool,
) -> Tuple[str, str]:
    if has_select_star and cte_depth > 4:
        return (
            f"O(n × columns × {cte_depth} CTEs)",
            "SELECT * passes ALL columns through each CTE layer. "
            "Column pruning is disabled; full row width is materialized at each step.",
        )
    if has_select_star:
        return (
            "O(n × all_columns)",
            "SELECT * prevents Snowflake's column pruning. "
            "All columns are scanned from storage even if only 3 are needed downstream.",
        )
    if union_count > 0:
        return (
            "O(n) — UNION dedup buffer",
            f"{union_count} UNION (not UNION ALL) operations require a full dedup pass. "
            "Use UNION ALL if duplicates are not expected.",
        )
    if distinct_count > 0:
        return (
            "O(n) — DISTINCT buffer",
            f"{distinct_count} DISTINCT operation(s) materialize a dedup set in memory/temp storage.",
        )
    if cte_depth > 6:
        return (
            f"O(n × {cte_depth}) — deep CTE chain",
            f"{cte_depth} CTEs. Snowflake may materialize intermediate results. "
            "Consider breaking into 2–3 intermediate dbt models.",
        )
    return (
        "O(n) — standard",
        "No SELECT *, DISTINCT, or UNION detected. Standard linear space.",
    )


class SQLScanner(BaseScanner):
    name = "sql"

    def scan(self, manifest: dict) -> List[Finding]:
        findings: List[Finding] = []
        for sql_file in manifest.get("sql_files", []):
            content  = sql_file.get("full_content", "")
            path     = sql_file["path"]
            is_macro = sql_file.get("is_macro", False)

            findings.extend(self._analyze_complexity(content, path, sql_file))
            findings.extend(self._analyze_quality(content, path, sql_file))
            if is_macro:
                findings.extend(self._analyze_macro_injection(content, path))
        return findings

    # ── Complexity ─────────────────────────────────────────────────────────

    def _analyze_complexity(self, sql: str, path: str, meta: dict) -> List[Finding]:
        out: List[Finding] = []

        join_count       = len(RE_JOIN.findall(sql))
        cross_join_count = len(RE_CROSS_JOIN.findall(sql))
        subquery_count   = len(RE_SUBQUERY.findall(sql))
        window_count     = len(RE_WINDOW_FN.findall(sql))
        cte_depth        = _count_cte_depth(sql)
        has_select_star  = bool(RE_SELECT_STAR.search(sql))
        distinct_count   = len(RE_DISTINCT.findall(sql))
        union_count      = len(RE_UNION.findall(sql))
        has_order_by     = bool(RE_ORDER_BY.search(sql))

        time_complexity, time_rationale   = _estimate_time_complexity(
            join_count, cross_join_count, subquery_count, window_count, cte_depth
        )
        space_complexity, space_rationale = _estimate_space_complexity(
            has_select_star, distinct_count, cte_depth, union_count,
            has_order_by and not is_incremental(sql),
        )

        # Cross join — always flag
        if cross_join_count > 0:
            out.append(Finding(
                check_id        = "SQL-C001",
                title           = "CROSS JOIN detected — potential Cartesian product",
                severity        = Severity.HIGH,
                category        = Category.SQL_COMPLEXITY,
                file            = path,
                code_snippet    = _extract_lines(sql, RE_CROSS_JOIN, context=6),
                description     = f"{cross_join_count} CROSS JOIN(s) found. {time_rationale}",
                suggestion      = (
                    "Replace CROSS JOIN with an explicit JOIN ... ON condition. "
                    "If a cross join is intentional (e.g., dim_date), add a comment explaining why."
                ),
                time_complexity  = time_complexity,
                space_complexity = space_complexity,
            ))

        # Deep CTE chains
        if cte_depth > 6:
            severity = Severity.MEDIUM if cte_depth <= 9 else Severity.HIGH
            out.append(Finding(
                check_id        = "SQL-C002",
                title           = f"Deep CTE chain — {cte_depth} CTEs",
                severity        = severity,
                category        = Category.SQL_COMPLEXITY,
                file            = path,
                description     = (
                    f"{cte_depth} CTEs detected. {space_rationale} "
                    "Snowflake may re-materialize intermediate results, increasing scan cost."
                ),
                suggestion      = (
                    "Split into 2–3 intermediate dbt models materialized as 'table'. "
                    "This gives the query optimizer fresh statistics at each boundary."
                ),
                time_complexity  = time_complexity,
                space_complexity = space_complexity,
            ))

        # Window functions without explicit ROWS frame
        noframe_matches = RE_WINDOW_NOFRAME.findall(sql)
        if noframe_matches:
            out.append(Finding(
                check_id        = "SQL-C003",
                title           = "Window function without ROWS frame specification",
                severity        = Severity.MEDIUM,
                category        = Category.SQL_COMPLEXITY,
                file            = path,
                code_snippet    = _extract_lines(sql, RE_WINDOW_NOFRAME, context=5),
                description     = (
                    f"{len(noframe_matches)} window function(s) have ORDER BY but no ROWS/RANGE frame. "
                    "Snowflake defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, "
                    "which can be unexpectedly slow on large partitions."
                ),
                suggestion      = (
                    "Add explicit frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                    "Example:\n"
                    "  sum(gross_revenue) OVER (\n"
                    "    PARTITION BY x3_cust_number, contract_year_bucket\n"
                    "    ORDER BY order_date\n"
                    "    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                    "  )"
                ),
                time_complexity  = time_complexity,
                space_complexity = space_complexity,
            ))

        # High number of UPPER(TRIM()) in joins
        upper_trim_count = len(RE_UPPER_TRIM.findall(sql))
        if upper_trim_count > 2:
            out.append(Finding(
                check_id        = "SQL-C004",
                title           = f"Repeated UPPER(TRIM()) on join keys ({upper_trim_count}×)",
                severity        = Severity.MEDIUM,
                category        = Category.SQL_COMPLEXITY,
                file            = path,
                code_snippet    = _extract_lines(sql, RE_UPPER_TRIM, context=4),
                description     = (
                    f"{upper_trim_count} UPPER(TRIM()) calls on join keys. "
                    "Per-row function application prevents micro-partition pruning "
                    "and prevents use of clustering keys on those columns."
                ),
                suggestion      = (
                    "Normalize case and whitespace in a staging model upstream:\n"
                    "  select upper(trim(facility_name)) as facility_name_normalized\n"
                    "Then join on the pre-normalized column without wrapping functions."
                ),
                time_complexity  = time_complexity,
                space_complexity = space_complexity,
            ))

        # Always surface the file's overall estimated complexity, even when
        # no specific complexity issue was flagged — so every scanned file
        # shows its Big-O in the PR review, not just the ones with findings.
        out.append(Finding(
            check_id         = "SQL-C000",
            title            = "Overall estimated complexity",
            severity         = Severity.INFO,
            category         = Category.SQL_COMPLEXITY,
            file             = path,
            description      = f"{time_rationale} {space_rationale}",
            time_complexity  = time_complexity,
            space_complexity = space_complexity,
        ))

        return out

    # ── Quality ────────────────────────────────────────────────────────────

    def _analyze_quality(self, sql: str, path: str, meta: dict) -> List[Finding]:
        out: List[Finding] = []

        # SELECT * in non-macro files
        if RE_SELECT_STAR.search(sql) and not meta.get("is_macro"):
            out.append(Finding(
                check_id        = "SQL-Q001",
                title           = "SELECT * prevents Snowflake column pruning",
                severity        = Severity.MEDIUM,
                category        = Category.SQL_QUALITY,
                file            = path,
                code_snippet    = _extract_lines(sql, RE_SELECT_STAR, context=4),
                description     = (
                    "SELECT * fetches every column from storage, even those not "
                    "needed by downstream models. Snowflake cannot prune columns "
                    "from micro-partitions when * is used."
                ),
                suggestion      = "Replace SELECT * with an explicit, named column list.",
                space_complexity = "O(n × all_columns) — no column pruning",
            ))

        # ORDER BY in a likely table-materialized model
        if RE_ORDER_BY.search(sql) and not meta.get("is_macro"):
            # Check if model is likely a table (has final ORDER BY after last CTE)
            # Simple heuristic: ORDER BY appears after the last CTE closing bracket
            stripped = sql.strip()
            last_order_by = [m.start() for m in RE_ORDER_BY.finditer(stripped)]
            if last_order_by:
                after_last = stripped[last_order_by[-1]:]
                # If nothing but whitespace/column names after ORDER BY — it's the final sort
                if not re.search(r"\)\s*$", after_last.strip()) and ")" not in after_last[:50]:
                    # Map the match position back to the un-stripped `sql` to get
                    # real line numbers, so this can be offered as a one-click
                    # GitHub suggestion — the whole clause is safe to delete
                    # outright since it's confirmed to be the query's final sort.
                    leading_ws = len(sql) - len(sql.lstrip())
                    start_pos  = leading_ws + last_order_by[-1]
                    start_line = sql[:start_pos].count("\n") + 1
                    end_line   = len(sql.rstrip().splitlines())
                    out.append(Finding(
                        check_id   = "SQL-Q002",
                        title      = "ORDER BY in table materialization — compute waste",
                        severity   = Severity.MEDIUM,
                        category   = Category.SQL_QUALITY,
                        file       = path,
                        line       = end_line,
                        code_snippet = _extract_lines(sql, re.compile(r'\border\s+by\b', re.I), context=5, last=True),
                        description= (
                            "Snowflake does not honor ORDER BY when writing to a TABLE. "
                            "The sort operation consumes compute credits and has zero "
                            "effect on how data is stored or queried."
                        ),
                        suggestion = (
                            "Remove the final ORDER BY clause from this model.\n"
                            "If sort order matters to consumers, document it in schema.yml "
                            "and apply ORDER BY in the consuming query instead."
                        ),
                        time_complexity = "Saves O(n log n) sort cost per run",
                        suggested_fix            = "",
                        suggested_fix_start_line = start_line if start_line != end_line else None,
                    ))

        # Stale file detection (old/backup files)
        if "_old" in path or "_backup" in path or "_bak" in path:
            out.append(Finding(
                check_id   = "SQL-Q003",
                title      = "Stale/backup SQL file in models directory",
                severity   = Severity.LOW,
                category   = Category.SQL_QUALITY,
                file       = path,
                description= (
                    "File name suggests it is a backup or old version. "
                    "Stale files are compiled by dbt and clutter the DAG."
                ),
                suggestion = "Delete this file or move it outside the models/ directory.",
            ))

        return out

    # ── Jinja macro injection ───────────────────────────────────────────────

    def _analyze_macro_injection(self, sql: str, path: str) -> List[Finding]:
        out: List[Finding] = []

        # Find macro definitions and their arguments
        macro_defs = re.findall(
            r"\{%-?\s*macro\s+(\w+)\s*\(([^)]*)\)",
            sql, re.IGNORECASE
        )

        for macro_name, args_str in macro_defs:
            args = [a.strip().split("=")[0].strip() for a in args_str.split(",") if a.strip()]
            for arg in args:
                # Check if arg is used directly in SQL (not inside ref/source/config)
                direct_use = re.search(
                    rf"['\"]?\s*\{{\{{\s*{re.escape(arg)}\s*\}}\}}\s*['\"]?",
                    sql, re.IGNORECASE
                )
                if direct_use:
                    snippet = sql[max(0, direct_use.start()-40): direct_use.end()+40].strip()
                    out.append(Finding(
                        check_id     = "SQL-S001",
                        title        = f"Jinja template injection risk in macro '{macro_name}'",
                        severity     = Severity.HIGH,
                        category     = Category.SQL_SECURITY,
                        file         = path,
                        code_snippet = snippet,
                        description  = (
                            f"Macro argument '{arg}' is interpolated directly into SQL "
                            "without an allowlist guard. If this macro is ever called with "
                            "externally-controlled input, it can be used to inject arbitrary SQL."
                        ),
                        suggestion   = (
                            f"Add an allowlist validation at the top of the macro:\n"
                            f"{{% set allowed = ['VAL1', 'VAL2'] %}}\n"
                            f"{{% if {arg} | upper not in allowed %}}\n"
                            f"  {{{{ exceptions.raise_compiler_error('Invalid {arg}: ' ~ {arg}) }}}}\n"
                            f"{{% endif %}}"
                        ),
                        cwe  = "CWE-89",
                        owasp= "A03:2021 Injection",
                    ))
                    break  # One finding per macro is enough

        return out


def is_incremental(sql: str) -> bool:
    return bool(re.search(r"is_incremental\(\)", sql, re.IGNORECASE))
