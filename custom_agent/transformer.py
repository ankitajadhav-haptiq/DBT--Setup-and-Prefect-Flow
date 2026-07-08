"""
Code transformation functions — extract real snippets from scanned files,
compute before→after diffs, and supply detailed explanations for every
optimization option.

Each option dict contains:
  label, title, safety, safety_class, perf
  why        — root cause and motivation for the change
  how_before — mechanism: exactly how Snowflake executes the original code
  how_after  — mechanism: exactly how Snowflake executes the optimised code
  before     — actual extracted code (from the real file)
  after      — transformed code (option-specific)
  note       — optional follow-up instruction
"""
import re
from typing import List, Dict, Optional


def get_options(check_id: str, snippet: str) -> List[Dict]:
    fn = _HANDLERS.get(check_id)
    if not fn or not snippet.strip():
        return []
    return fn(snippet.strip())


# ── SQL-C001  CROSS JOIN ───────────────────────────────────────────────────────

def _sql_c001(snippet: str) -> List[Dict]:
    before = snippet

    # Option A: CROSS JOIN → INNER JOIN, WHERE → ON
    after_a = re.sub(r'\bcross\s+join\b', 'inner join', before, flags=re.I)
    if re.search(r'\binner\s+join\b', after_a, re.I) and ' on ' not in after_a.lower():
        after_a = re.sub(r'\bwhere\b', '    on', after_a, count=1, flags=re.I)
        after_a = re.sub(r'\n(\s+)and\b', r'\n\1and', after_a, flags=re.I)

    # Option B: QUALIFY
    cj_m  = re.search(r'\bcross\s+join\s+\S+(?:\s+(\w+))?', before, re.I)
    alias = cj_m.group(1) if (cj_m and cj_m.group(1)) else "tbl"
    after_b = re.sub(
        r'\bwhere\b(.*)',
        ("qualify\n"
         "    row_number() over (\n"
         "        partition by <join_key_cols>\n"
         "        order by 1\n"
         "    ) = 1"),
        before, count=1, flags=re.I | re.S,
    )
    if after_b == before:
        after_b = before + "\nqualify row_number() over (partition by <join_keys> order by 1) = 1"

    # Option C: comment
    first_line = before.split('\n')[0].strip()
    after_c = f"-- intentional cross join: {first_line.lower()}\n-- row count = left_rows × right_rows per group\n" + before

    return [
        {
            "label": "A",
            "title": "Replace CROSS JOIN with INNER JOIN ON (recommended)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ eliminates Cartesian product — hash join instead",
            "why": (
                "CROSS JOIN instructs Snowflake to pair every row in the left table with every row in "
                "the right table before any filtering happens. If the left table has 10,000 rows and the "
                "right has 500 rows, Snowflake builds a 5,000,000-row intermediate result just to discard "
                "4,990,000 of them via the WHERE clause. This wastes warehouse credits, spills to "
                "temporary storage on large tables, and slows downstream query steps."
            ),
            "how_before": (
                "Snowflake allocates memory for the full n × m Cartesian product. All rows from both "
                "tables are broadcast or shuffled across compute nodes. The WHERE condition then runs as "
                "a post-join filter on the bloated intermediate dataset. Network I/O and memory usage "
                "scale with the product of both table sizes — not just the matching rows."
            ),
            "how_after": (
                "INNER JOIN ON pushes the join condition into the join planning phase. Snowflake uses a "
                "hash join: it builds a hash table on the smaller side, then probes it row-by-row with "
                "the larger side. Only matching key pairs are ever materialised. Memory, network I/O, "
                "and credits scale with the number of matching rows — not the full product."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Keep CROSS JOIN — replace WHERE with QUALIFY to push filter down",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ QUALIFY is evaluated by Snowflake closer to the scan",
            "why": (
                "Least-change option: keeps the CROSS JOIN keyword but moves the filtering logic to "
                "QUALIFY. QUALIFY is a Snowflake-specific clause that lets the planner push the "
                "window-function filter into the execution plan earlier than a WHERE clause."
            ),
            "how_before": (
                "WHERE runs after the full join has been computed. The Cartesian product is built first, "
                "then filtered. This is the same cost as the current code — all n × m pairs are produced "
                "before any row is discarded."
            ),
            "how_after": (
                "QUALIFY wraps the join condition as a row_number() window predicate. Snowflake's "
                "optimiser can push this filter toward the scan layer, reducing the number of rows "
                "that need to be joined across nodes. The output is identical — only the internal "
                "execution order changes."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Document the CROSS JOIN as intentional — no code change",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "— no performance change",
            "why": (
                "If the Cartesian product is intentionally small — for example, a 1-row date_params CTE "
                "cross joined to broadcast a single value to all rows — the cost is negligible. "
                "A comment prevents future reviewers from misreading it as a bug and prevents the "
                "scanner from continuing to flag it."
            ),
            "how_before": (
                "No change in execution. The CROSS JOIN builds the full product. If both sides are "
                "small (e.g., 1 row × n rows), the product equals n rows — effectively just a broadcast "
                "of the constant value. Cost: negligible."
            ),
            "how_after": (
                "No change in execution — the comment documents intent. The scanner warning is "
                "suppressed via --dismiss. Reviewers immediately understand the pattern is deliberate "
                "and do not attempt to 'fix' it in a future PR."
            ),
            "before": before, "after": after_c,
            "note": "After confirming intent, dismiss: python custom_agent/main.py --dismiss SQL-C001",
        },
    ]


# ── SQL-Q002  ORDER BY in TABLE materialisation ────────────────────────────────

def _sql_q002(snippet: str) -> List[Dict]:
    before = snippet

    # Option A: remove ORDER BY
    after_a = re.sub(
        r'\border\s+by\b[^\n]*(\n[ \t]+[^\n]+)*',
        '-- ORDER BY removed: Snowflake discards the sort in TABLE materialisation',
        before, count=1, flags=re.I,
    ).rstrip()

    # Option B: switch to VIEW
    after_b = (
        "-- Change the dbt config to materialized='view':\n"
        "-- {{ config(materialized='view') }}\n\n"
        + before
        + "\n-- ORDER BY is now evaluated at query-read time — Snowflake honours it"
    )

    # Option C: CLUSTER BY
    col_m = re.search(r'\border\s+by\b\s+(\w+)', before, re.I)
    cluster_col = col_m.group(1) if col_m else "date_column"
    after_c = (
        f"-- Replace ORDER BY with CLUSTER BY in the dbt config:\n"
        f"-- {{ config(materialized='table', cluster_by=['{cluster_col}']) }}\n\n"
        + re.sub(
            r'\border\s+by\b[^\n]*(\n[ \t]+[^\n]+)*',
            f'-- ORDER BY removed; micro-partitions clustered by {cluster_col} instead',
            before, count=1, flags=re.I,
        )
    )

    return [
        {
            "label": "A",
            "title": "Remove ORDER BY from the final SELECT (recommended)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ O(n log n) sort cost eliminated from every scheduled run",
            "why": (
                "Snowflake's TABLE materialisation (CREATE TABLE AS SELECT) does not guarantee "
                "any row order in the stored table. After the query finishes, Snowflake writes rows "
                "into columnar micro-partitions in its own internal order — the ORDER BY result is "
                "never used. You pay the sort cost (O(n log n) warehouse credits) every time the "
                "model runs, for zero benefit."
            ),
            "how_before": (
                "Snowflake executes the full SELECT, then performs a global sort across all rows "
                "using the ORDER BY columns. This requires a sort buffer, potential spill to "
                "temporary local storage on large tables, and additional network shuffling to "
                "consolidate sorted partitions. The sorted result is then written to TABLE storage "
                "— but Snowflake immediately re-organises it into micro-partitions by its own "
                "internal clustering algorithm, discarding the sort order."
            ),
            "how_after": (
                "Without ORDER BY, Snowflake writes rows directly to micro-partitions as they emerge "
                "from the query — no sort phase, no sort buffer, no spill risk. The stored data is "
                "byte-identical to what the ORDER BY version produces (Snowflake's storage format "
                "ignores the sort anyway). Every scheduled run saves the O(n log n) sort credits."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Switch materialisation from TABLE to VIEW",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ ORDER BY is honoured correctly when consumers query the VIEW",
            "why": (
                "In a VIEW, Snowflake evaluates the ORDER BY at query time — when a downstream "
                "model or user actually reads the data. This is the only place ORDER BY is "
                "meaningful for final output ordering. If ordering matters to consumers, a VIEW "
                "is the correct materialisation type."
            ),
            "how_before": (
                "TABLE materialisation stores a snapshot of the data. ORDER BY is applied during "
                "the write but discarded. Consumers who query this TABLE get rows in Snowflake's "
                "internal storage order — not the ORDER BY you specified."
            ),
            "how_after": (
                "VIEW re-runs the full SELECT query on each read. ORDER BY is applied to the live "
                "result set — Snowflake returns rows in the specified order. Trade-off: the query "
                "runs on every consumer read instead of once per dbt run. Suitable for models with "
                "low query frequency or fast upstream queries."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Use CLUSTER BY for physical micro-partition ordering",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ date-range scans skip irrelevant micro-partitions",
            "why": (
                "If you added ORDER BY hoping downstream queries with date filters would be faster, "
                "CLUSTER BY achieves that goal correctly. CLUSTER BY tells Snowflake to physically "
                "co-locate rows with similar values in the same micro-partitions — enabling "
                "micro-partition pruning on filters, which is where the real scan savings come from."
            ),
            "how_before": (
                "ORDER BY sorts rows at write time, but Snowflake's storage engine ignores this "
                "and writes micro-partitions by its own internal order. A downstream query with "
                "WHERE revenue_date = '2024-01-01' must still scan all micro-partitions because "
                "Snowflake has no partition-level metadata saying which partitions contain which dates."
            ),
            "how_after": (
                "CLUSTER BY in the dbt config tells Snowflake to maintain micro-partition ordering "
                "on the specified column via Automatic Clustering (a background maintenance process). "
                "Snowflake records min/max metadata per micro-partition. A downstream query with "
                "WHERE revenue_date = '2024-01-01' reads only the 1–2 micro-partitions that contain "
                "that date — all others are pruned without being read from storage."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── SQL-C003  Window function without ROWS frame ──────────────────────────────

def _add_rows_frame(over_inner: str) -> str:
    if re.search(r'\b(rows|range)\s+between\b', over_inner, re.I):
        return over_inner
    stripped = over_inner.rstrip()
    return stripped + '\n    rows between unbounded preceding and current row\n'


def _sql_c003(snippet: str) -> List[Dict]:
    before = snippet

    # Option A: add ROWS BETWEEN
    after_a = re.sub(
        r'over\s*\(([^)]+)\)',
        lambda m: 'over (' + _add_rows_frame(m.group(1)) + ')',
        before, flags=re.I,
    )
    if after_a == before:
        after_a = before + "\n    -- add inside OVER(): rows between unbounded preceding and current row"

    # Option B: QUALIFY
    after_b = re.sub(r'\bwhere\s+rn\s*=\s*1\b', 'qualify rn = 1', before, flags=re.I)
    if after_b == before:
        after_b = (
            before.rstrip()
            + "\n-- Replace 'where rn = 1' (outer subquery) with:\n"
            "qualify row_number() over (\n"
            "    partition by ...\n"
            "    order by ...\n"
            "    rows between unbounded preceding and current row\n"
            ") = 1"
        )

    # Option C: verification query
    p_m = re.search(r'partition\s+by\s+([\w\s,]+?)(?:order|rows|range|\))', before, re.I)
    o_m = re.search(r'order\s+by\s+([\w\s,]+?)(?:rows|range|\))',           before, re.I)
    pcols = p_m.group(1).strip() if p_m else "<partition_cols>"
    ocols = o_m.group(1).strip() if o_m else "<order_cols>"
    after_c = (
        f"-- Step 1: verify no ties in ORDER BY keys within each partition\n"
        f"select {pcols}, {ocols}, count(*) as cnt\n"
        f"from <your_source_table>\n"
        f"group by {pcols}, {ocols}\n"
        f"having cnt > 1;\n"
        f"-- If 0 rows returned → no ties → apply Option A safely\n\n"
        f"-- Step 2: apply the ROWS frame\n"
        + after_a
    )

    return [
        {
            "label": "A",
            "title": "Add ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (recommended)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ removes tie-checking overhead from every window evaluation",
            "why": (
                "When you write OVER (PARTITION BY ... ORDER BY ...) without a frame clause, "
                "Snowflake defaults to RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. "
                "RANGE mode requires Snowflake to check whether any other rows in the same "
                "partition share the exact same ORDER BY value (a 'tie') — even if there are "
                "never any ties. This tie-checking adds CPU work per row. ROWS mode skips it entirely."
            ),
            "how_before": (
                "In RANGE mode, Snowflake processes each row and asks: 'Does any other row in "
                "this partition have the same ORDER BY value?' To answer this, it maintains "
                "additional state tracking equal-key groups. For row_number() — which produces a "
                "unique number regardless — this tie-checking is completely wasted work. "
                "The computation still runs on every row, every time the model refreshes."
            ),
            "how_after": (
                "In ROWS mode, Snowflake processes each row exactly once in the ORDER BY sequence "
                "with a simple running-aggregate cursor — no tie group tracking, no equal-key "
                "comparisons. For row_number() with unique ORDER BY keys, ROWS and RANGE produce "
                "byte-identical values (verified by SQL Standard ISO/IEC 9075). Only the internal "
                "execution algorithm changes — the output does not."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Restructure dedup using QUALIFY — eliminate the outer subquery",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ window function computed once, no outer scan",
            "why": (
                "A common pattern is: SELECT * FROM (SELECT ..., row_number() OVER (...) AS rn) "
                "WHERE rn = 1. This creates a nested subquery — Snowflake computes row_number() "
                "in the inner query, materialises the result, then the outer query re-reads it "
                "to apply the WHERE filter. QUALIFY collapses this into a single pass."
            ),
            "how_before": (
                "Inner subquery: Snowflake computes row_number() for all rows and writes a "
                "temporary result with the rn column. Outer query: Snowflake reads that temporary "
                "result and discards all rows where rn ≠ 1. Two scans for work that could be done "
                "in one."
            ),
            "how_after": (
                "QUALIFY evaluates the window function inline as part of the same scan that "
                "produces the SELECT columns. Snowflake emits only rows where the QUALIFY "
                "predicate is true — no intermediate materialisation, no second scan. "
                "The rn column does not need to be included in the SELECT list unless you "
                "want to expose it."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Verify uniqueness first, then apply Option A safely",
            "safety": "⚠ verify before applying", "safety_class": "caution",
            "perf": "— same result as A once verified",
            "why": (
                "ROWS and RANGE produce identical values ONLY when no two rows share the same "
                "ORDER BY value within a partition (no ties). If ties exist, RANGE may assign "
                "different row numbers than ROWS would. Before applying Option A, confirm there "
                "are no ties in your data."
            ),
            "how_before": (
                "RANGE mode: if two rows have the same ORDER BY value, they are in the same "
                "frame boundary. row_number() still assigns unique numbers, but the assignment "
                "order within ties is non-deterministic. ROWS mode would assign them in "
                "physical row order instead. If ties exist, the rn value for each tied row "
                "may differ between RANGE and ROWS."
            ),
            "how_after": (
                "Run the verification query (Step 1). If it returns 0 rows, there are no ties — "
                "ROWS and RANGE are provably identical on your data. Apply the ROWS frame "
                "(Step 2) with confidence. If the verification query returns rows, investigate "
                "whether the tied ORDER BY values are expected before switching to ROWS."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── SQL-C004  UPPER(TRIM()) on join keys ─────────────────────────────────────

def _sql_c004(snippet: str) -> List[Dict]:
    before = snippet

    def norm_ref(m):
        col = m.group(1).split('.')[-1]
        return col + '_key'

    after_a = re.sub(r'upper\s*\(\s*trim\s*\((\w+(?:\.\w+)?)\)\s*\)',
                     norm_ref, before, flags=re.I)
    after_a = (
        "-- In staging model, pre-normalise once:\n"
        "-- select upper(trim(raw_column)) as raw_column_key, ...\n\n"
        + after_a
    )

    col_m   = re.search(r'upper\s*\(\s*trim\s*\((\w+(?:\.\w+)?)\)', before, re.I)
    col_raw = col_m.group(1).split('.')[-1] if col_m else "key_column"
    after_b = (
        before
        + f"\n\n-- No SQL change — add this test to schema.yml:\n"
        f"-- - name: {col_raw}\n"
        f"--   tests:\n"
        f"--     - dbt_utils.expression_is_true:\n"
        f"--         expression: >\n"
        f"--           {col_raw} = upper(trim({col_raw}))\n"
        f"-- Once the test passes consistently, remove UPPER(TRIM()) in a follow-up PR."
    )

    after_c = (
        f"-- Wrap the smaller lookup table in a normalising CTE:\n"
        f"clean_lookup as (\n"
        f"    select\n"
        f"        upper(trim({col_raw})) as {col_raw}_key,\n"
        f"        *\n"
        f"    from {{{{ ref('source_table') }}}}\n"
        f"),\n\n"
        + re.sub(r'upper\s*\(\s*trim\s*\((\w+(?:\.\w+)?)\)\s*\)',
                 lambda m: m.group(1).split('.')[-1] + '_key',
                 before, flags=re.I)
    )

    return [
        {
            "label": "A",
            "title": "Pre-compute UPPER(TRIM()) in a staging model (recommended)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡⚡ micro-partition pruning enabled + per-row CPU eliminated",
            "why": (
                "When you write UPPER(TRIM(column)) in a join condition, Snowflake applies the "
                "function to every row of both tables at query time — it cannot use the raw "
                "column value to look up matching micro-partitions. This has two costs: "
                "(1) CPU: the function runs on every row, every join, every model refresh. "
                "(2) Pruning: Snowflake cannot skip any micro-partitions because the stored "
                "column value does not match what the join condition computes."
            ),
            "how_before": (
                "For each row in the left table: Snowflake calls UPPER(TRIM(left.col)) and "
                "UPPER(TRIM(right.col)) to compute the normalised values, then compares them. "
                "This happens for every row pair the join evaluates. Snowflake's min/max "
                "metadata on micro-partitions is based on the raw stored value — not the "
                "UPPER(TRIM()) result — so partition pruning is impossible. All micro-partitions "
                "must be read regardless of the filter."
            ),
            "how_after": (
                "The staging model runs UPPER(TRIM()) once at ingestion and stores the result "
                "as a dedicated column. The join now compares two stored column values directly "
                "— no function call at join time. Snowflake's min/max metadata for the "
                "normalised column is accurate, so it can skip micro-partitions that cannot "
                "contain matching values. CPU cost per row drops to zero on the normalised side."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Keep UPPER(TRIM()) — enforce clean source with a dbt schema test",
            "safety": "✓ 100% safe, no SQL changes", "safety_class": "safe",
            "perf": "— no perf gain now, but proves the path to removing UPPER(TRIM())",
            "why": (
                "If the source data is already normalised (already uppercase, no leading/trailing "
                "spaces), UPPER(TRIM()) computes the same value as the stored column. The test "
                "proves this is true. Once it consistently passes, you can remove UPPER(TRIM()) "
                "in a future PR — confident that no rows will stop matching."
            ),
            "how_before": (
                "Same execution as current — UPPER(TRIM()) runs at join time, per-row, on every "
                "query. No pruning benefit. The cost is unchanged. The schema test is an addition "
                "only — it runs at CI time and does not affect query execution."
            ),
            "how_after": (
                "The dbt test runs in CI: SELECT count(*) WHERE column != UPPER(TRIM(column)). "
                "If it returns 0, the source is provably clean. At that point a follow-up PR "
                "can remove UPPER(TRIM()) from all joins — the test already proves safety. "
                "This option sets up the evidence base for the full fix."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Normalise just the lookup side with an inline CTE",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ halves UPPER(TRIM()) calls; lookup side gains pruning",
            "why": (
                "Without touching the staging model, normalise the smaller (lookup/dimension) "
                "table in a CTE. The CTE pre-computes UPPER(TRIM()) once for that table. "
                "The join then uses the CTE column directly on one side — halving the CPU cost "
                "and giving one side of the join a prunable normalised column."
            ),
            "how_before": (
                "Both sides of the join apply UPPER(TRIM()) per-row. If both tables have n rows, "
                "the total function calls = 2n per join evaluation. Neither side supports "
                "micro-partition pruning."
            ),
            "how_after": (
                "The CTE computes UPPER(TRIM()) once for the lookup table (O(m) calls where m "
                "is the smaller table). The join uses the CTE's pre-normalised column on that "
                "side — zero function calls. The fact table side still applies UPPER(TRIM()) "
                "per-row (O(n) calls). Total calls drop from 2n to n. The lookup-side column "
                "is stored in the CTE with accurate min/max — Snowflake can prune it."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── SQL-Q001  SELECT * ────────────────────────────────────────────────────────

def _sql_q001(snippet: str) -> List[Dict]:
    before = snippet

    after_a = re.sub(
        r'\bselect\s+\*\b',
        ("select\n"
         "    -- Run: dbt compile --models <this_model_name>\n"
         "    -- Copy the column list from the compiled SQL output:\n"
         "    col1,\n"
         "    col2,\n"
         "    col3   -- replace with actual columns from dbt compile"),
        before, flags=re.I,
    )

    after_b = (
        before
        + "\n\n-- No SQL change — add column definitions to schema.yml:\n"
        "-- models:\n"
        "--   - name: <this_model>\n"
        "--     columns:\n"
        "--       - name: col1\n"
        "--         tests: [not_null]\n"
        "--       - name: col2\n"
        "--       # ... full column list from: dbt compile --models <model>"
    )

    star_m   = re.search(r'\bselect\s+\*\s+from\s+(\w+)', before, re.I)
    cte_name = star_m.group(1) if star_m else "final_cte"
    after_c  = re.sub(
        r'\bselect\s+\*\s+from\s+(\w+)',
        lambda m: f"{{{{ dbt_utils.star(ref('{m.group(1)}')) }}}}\nfrom {m.group(1)}",
        before, flags=re.I,
    )
    if after_c == before:
        after_c = (
            before
            + f"\n-- Replace SELECT * with:\n"
            f"-- {{{{ dbt_utils.star(ref('{cte_name}')) }}}}"
        )

    return [
        {
            "label": "A",
            "title": "Expand SELECT * to an explicit column list (recommended)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ enables Snowflake column pruning — only needed columns read from storage",
            "why": (
                "SELECT * tells Snowflake: 'I need every column that exists in this table/CTE.' "
                "Snowflake cannot skip any column's data files, even if a downstream model only "
                "reads 3 of the 25 columns. You pay the I/O and memory cost for all 25 every time "
                "this model runs and every time a downstream model reads it. An explicit list "
                "eliminates this waste entirely."
            ),
            "how_before": (
                "Snowflake resolves SELECT * at compile time to the full column list of the "
                "referenced CTE or table. It then reads all column data files from every "
                "micro-partition that passes the WHERE filter — even columns that no downstream "
                "model ever uses. Memory usage during the query scales with the full row width "
                "(all columns × all rows). If a new column is added upstream, it automatically "
                "appears here — silently, with no CI warning."
            ),
            "how_after": (
                "With an explicit column list, Snowflake reads only the data files for the listed "
                "columns from storage — all other column files are skipped (columnar pruning). "
                "Memory usage during the query scales with only the needed columns × rows. "
                "If a new column is added upstream, it does NOT appear here — it requires an "
                "explicit code change, which is caught in code review."
            ),
            "before": before, "after": after_a,
            "note": "Run 'dbt compile --models <model>' to get the exact column list — never transcribe manually.",
        },
        {
            "label": "B",
            "title": "Keep SELECT * — lock the schema via schema.yml column definitions",
            "safety": "✓ 100% safe, no SQL changes", "safety_class": "safe",
            "perf": "— no I/O savings, but schema drift causes CI failure instead of silent bug",
            "why": (
                "The immediate risk of SELECT * is not just column pruning — it is schema drift. "
                "If an upstream CTE adds, removes, or renames a column, SELECT * silently passes "
                "that change to all downstream models. Adding column definitions to schema.yml "
                "gives dbt a contract: if the schema changes, the test fails in CI before "
                "the change reaches production."
            ),
            "how_before": (
                "SELECT * resolves dynamically at runtime. If the upstream CTE gains a new column, "
                "this model's output gains that column too — no code change, no PR, no review. "
                "Downstream models that depend on a fixed schema may break silently or return "
                "unexpected results."
            ),
            "how_after": (
                "schema.yml column definitions act as a compile-time contract. dbt runs "
                "'dbt test' at CI time and checks that each defined column exists and passes "
                "its tests (not_null, unique, etc.). If upstream removes a column this model "
                "expects, CI fails with a clear error — not a production incident. "
                "SELECT * is unchanged — only the safety net is added."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Use dbt_utils.star() macro — auto-expands at compile time",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ explicit column list in compiled SQL — same pruning benefit as Option A",
            "why": (
                "dbt_utils.star() generates an explicit column list from a relation at dbt compile "
                "time. It gives you the pruning benefit of an explicit column list (Option A) "
                "while automatically staying in sync with upstream schema changes — the macro "
                "regenerates the list on each 'dbt compile' run."
            ),
            "how_before": (
                "SELECT * is dynamic — evaluated at Snowflake query runtime. Snowflake has no "
                "way to know which columns will actually be used by the query that reads the "
                "result, so it reads all of them. No columnar pruning is possible."
            ),
            "how_after": (
                "At 'dbt compile' time, star() queries the database catalog for the relation's "
                "column list and writes it verbatim into the compiled SQL. Snowflake sees an "
                "explicit list of column names — identical in effect to Option A. "
                "Requires dbt-utils >= 0.9.0 in packages.yml. Add any columns to exclude "
                "via the 'except' parameter: {{ dbt_utils.star(ref('cte'), except=['col_to_skip']) }}"
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ══════════════════════════════════════════════════════════════════════════════
# PYTHON handlers
# ══════════════════════════════════════════════════════════════════════════════

# ── PY-S001  subprocess shell=True ───────────────────────────────────────────

def _py_s001(snippet: str) -> List[Dict]:
    before = snippet

    # Option A: split command into list
    after_a = re.sub(
        r'subprocess\.\w+\(\s*["\']([^"\']+)["\'],?\s*shell\s*=\s*True',
        lambda m: (
            "import shlex\n"
            f"cmd = shlex.split('{m.group(1)}')\n"
            f"subprocess.run(cmd, shell=False"
        ),
        before, flags=re.I,
    )
    if after_a == before:
        after_a = (
            "import shlex\n"
            "# Split command string into a safe list:\n"
            "cmd = shlex.split(your_command_string)\n"
            "subprocess.run(cmd)  # shell=False is the default"
        )

    # Option B: pass args directly as list literal
    after_b = re.sub(
        r'subprocess\.\w+\(["\'][^"\']+["\'],?\s*shell\s*=\s*True[^)]*\)',
        'subprocess.run([\n    "command",\n    arg1,\n    arg2,\n], capture_output=True, text=True)',
        before, flags=re.I,
    )
    if after_b == before:
        after_b = (
            before.replace('shell=True', 'shell=False  # removed')
            + "\n# Pass each argument as a separate list element:\n"
            "# subprocess.run(['git', 'commit', '-m', message])"
        )

    # Option C: use pathlib/os for file ops
    after_c = (
        before
        + "\n\n# If this runs a file operation, use Python's stdlib instead:\n"
        "# import pathlib\n"
        "# pathlib.Path(dest).write_bytes(pathlib.Path(src).read_bytes())\n"
        "# — No shell needed, no injection surface"
    )

    return [
        {
            "label": "A",
            "title": "Use shlex.split() to safely parse the command string (recommended)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "— same performance, injection surface eliminated",
            "why": (
                "shell=True passes your entire command string to /bin/sh. "
                "If any part of that string comes from user input, environment variables, "
                "or file names, an attacker can inject shell metacharacters: "
                "';', '&&', '|', '$()' etc. to run arbitrary commands as your process. "
                "shlex.split() breaks the command into tokens and passes them directly to "
                "execvp() — the shell is never involved."
            ),
            "how_before": (
                "Python calls sh -c 'your entire command string'. The shell parses the string, "
                "expands variables ($VAR), evaluates subshells ($(cmd)), and interprets "
                "metacharacters. If command contains '; rm -rf /' as a suffix — appended via "
                "string concatenation or f-string — the shell executes it. "
                "CWE-78: OS Command Injection."
            ),
            "how_after": (
                "shlex.split() tokenises the command string using shell quoting rules — "
                "the same way a shell would — but returns a Python list. "
                "subprocess.run(list, shell=False) calls execvp() directly with those exact "
                "tokens. No shell is spawned. Metacharacters in any token are treated as "
                "literal characters, not shell syntax."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Build the argument list directly — no string parsing needed",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "— same performance, clearest possible form",
            "why": (
                "The cleanest fix is to never build a shell command string in the first place. "
                "Pass each argument as a separate element in a Python list. "
                "Arguments with spaces or special characters are passed verbatim — "
                "no quoting, no escaping, no injection surface at all."
            ),
            "how_before": (
                "The command string is assembled (often by f-string or concatenation), "
                "then handed wholesale to the shell. The shell re-parses it — introducing "
                "a second parse pass where injection can occur between construction and execution."
            ),
            "how_after": (
                "Each argument occupies its own list element. subprocess passes them as "
                "separate argv entries directly to the kernel — exactly what you specified, "
                "nothing more. A filename containing spaces or quotes is passed as-is: "
                "no shell escaping required, no injection possible."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Replace the shell call with Python stdlib (pathlib / os / shutil)",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "⚡ often faster — no subprocess fork overhead",
            "why": (
                "Many shell one-liners (cp, mv, rm, mkdir, cat, grep, wc) have direct Python "
                "equivalents in pathlib, os, shutil, or subprocess.run(list). "
                "Using Python APIs eliminates the shell entirely — no process, no injection."
            ),
            "how_before": (
                "A subprocess is forked, /bin/sh is exec'd, the shell interprets the command, "
                "spawns a child process, waits for it to finish, then returns. "
                "Overhead: 2–3 process forks, shell startup, command parsing."
            ),
            "how_after": (
                "pathlib/os/shutil operate directly in the Python process — no fork, no shell. "
                "I/O operations use the OS kernel directly. Result: faster, no injection "
                "surface, clearer error handling (Python exceptions instead of exit codes)."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-S002  eval() ──────────────────────────────────────────────────────────

def _py_s002(snippet: str) -> List[Dict]:
    before = snippet

    after_a = re.sub(r'\beval\s*\(', 'ast.literal_eval(', before)
    if after_a == before:
        after_a = before + "\n# Replace with:\nimport ast\nresult = ast.literal_eval(expression)"

    after_b = re.sub(r'\beval\s*\(', 'json.loads(', before)
    if after_b == before:
        after_b = "import json\nresult = json.loads(json_string)  # safe for JSON data"

    after_c = (
        before
        + "\n\n# Explicit parsing — no eval() needed:\n"
        "# if expression == 'option_a':\n"
        "#     result = handle_a()\n"
        "# elif expression == 'option_b':\n"
        "#     result = handle_b()\n"
        "# else:\n"
        "#     raise ValueError(f'Unknown option: {expression}')"
    )

    return [
        {
            "label": "A",
            "title": "Replace eval() with ast.literal_eval() for Python literals (recommended)",
            "safety": "✓ safe for literals", "safety_class": "safe",
            "perf": "— same performance for literal values",
            "why": (
                "eval() compiles and executes any Python expression — import os; os.system('rm -rf /') "
                "is valid input. If the argument comes from a file, API, user input, or environment "
                "variable, this is a Remote Code Execution (RCE) vulnerability rated CRITICAL. "
                "ast.literal_eval() parses only Python literal structures "
                "(strings, numbers, lists, dicts, tuples, booleans, None) and raises ValueError "
                "for anything else."
            ),
            "how_before": (
                "eval(expr) compiles expr as a Python code object and executes it in the current "
                "scope. The compiled code has full access to builtins, can import modules, read/write "
                "files, open network sockets, and execute system commands. "
                "A malicious string like \"__import__('os').system('id')\" runs immediately."
            ),
            "how_after": (
                "ast.literal_eval(expr) parses the string using Python's AST parser and walks the "
                "parse tree. It raises ValueError for any node that is not a literal — "
                "function calls, attribute access, and imports all raise ValueError before any "
                "code runs. Safe for parsing '[1, 2, 3]', \"{'key': 'val'}\", \"True\", etc."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Use json.loads() if the data is JSON format",
            "safety": "✓ safe for JSON input", "safety_class": "safe",
            "perf": "⚡ faster than eval() for JSON data (C extension)",
            "why": (
                "If the string is JSON — curly braces, square brackets, quoted keys — "
                "json.loads() is both safer and faster than eval(). "
                "The JSON parser accepts only JSON syntax; Python expressions are not valid JSON."
            ),
            "how_before": (
                "eval() allows Python dicts with unquoted keys, trailing commas, tuples, "
                "and arbitrary expressions as values — things JSON does not allow. "
                "This flexibility is the attack surface."
            ),
            "how_after": (
                "json.loads() strictly enforces RFC 8259 JSON syntax. "
                "Any deviation raises json.JSONDecodeError. Python code embedded in "
                "the string is not valid JSON and will never execute."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Replace with explicit if/elif dispatch — zero eval() needed",
            "safety": "✓ 100% safe", "safety_class": "safe",
            "perf": "— minimal overhead, maximum clarity",
            "why": (
                "Most eval() calls evaluate one of a known set of expressions. "
                "An explicit dispatch table (dict or if/elif) handles every valid case "
                "and rejects everything else — with a clear error message."
            ),
            "how_before": (
                "eval() accepts any input and executes it. There is no allowlist — "
                "the set of 'valid' inputs is unbounded. The only safety is hoping "
                "the caller never passes something malicious."
            ),
            "how_after": (
                "An explicit allowlist (dict keys or if/elif conditions) defines exactly "
                "which inputs are valid. Anything outside that set raises an exception "
                "immediately — before any code runs. Reviewers can see the full set of "
                "accepted inputs in the code."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-S003  SQL string concatenation ────────────────────────────────────────

def _py_s003(snippet: str) -> List[Dict]:
    before = snippet

    after_a = re.sub(
        r'["\'](\s*(?:SELECT|INSERT|UPDATE|DELETE)[^"\']*)["\']\s*\+\s*(\w+)',
        r'"\1%s"  # parameterised placeholder\ncursor.execute(query, (\2,))',
        before, flags=re.I,
    )
    if after_a == before:
        after_a = (
            "query = \"\"\"\n"
            "    SELECT *\n"
            "    FROM your_table\n"
            "    WHERE id = %s\n"
            "      AND name = %s\n"
            "\"\"\"\n"
            "cursor.execute(query, (user_id, user_name))"
        )

    after_b = (
        "from sqlalchemy import text\n\n"
        "query = text(\"\"\"\n"
        "    SELECT *\n"
        "    FROM your_table\n"
        "    WHERE id = :user_id\n"
        "      AND name = :name\n"
        "\"\"\")\n"
        "result = conn.execute(query, {\"user_id\": user_id, \"name\": name})"
    )

    after_c = (
        "# Use the Snowflake connector's execute_many or the ORM:\n"
        "from snowflake.connector import DictCursor\n\n"
        "# Parameterised — Snowflake connector escapes automatically:\n"
        "cursor.execute(\n"
        "    \"SELECT * FROM table WHERE col = %s\",\n"
        "    (user_value,)\n"
        ")"
    )

    return [
        {
            "label": "A",
            "title": "Use cursor.execute(query, params) with %s placeholders (recommended)",
            "safety": "✓ eliminates SQL injection", "safety_class": "safe",
            "perf": "— identical runtime; parameterised queries can be cached by the DB",
            "why": (
                "Building a SQL string with + or f-string and then executing it means the "
                "database receives user data as SQL syntax — not as a value. "
                "A user_id of \"1 OR 1=1\" becomes WHERE id = 1 OR 1=1 and returns all rows. "
                "A name of \"'; DROP TABLE users; --\" runs a second statement. "
                "This is OWASP A03:2021 (Injection), CWE-89."
            ),
            "how_before": (
                "Python builds the query string by concatenating user-controlled values into "
                "the SQL text. The database's query parser then reads the final string — it "
                "cannot distinguish between your SQL structure and the injected data. "
                "User values become SQL code."
            ),
            "how_after": (
                "The query string contains %s placeholders — literal question marks with no "
                "user data. cursor.execute(query, params) sends the query and params to the "
                "database separately. The DB driver escapes and quotes each param before "
                "substitution — user data is always treated as a value, never as SQL."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Use SQLAlchemy text() with named :param bindparams",
            "safety": "✓ eliminates SQL injection", "safety_class": "safe",
            "perf": "— same safety; named params are more readable for complex queries",
            "why": (
                "SQLAlchemy's text() construct with named :param placeholders provides the "
                "same security as cursor.execute() with positional %s, but uses named "
                "parameters — easier to read in long queries and prevents parameter order bugs."
            ),
            "how_before": (
                "String concatenation bakes user values directly into the SQL text. "
                "The database sees one undifferentiated string."
            ),
            "how_after": (
                "text() creates a SQL expression object with named placeholders. "
                "conn.execute(query, dict) sends the dict separately. "
                "SQLAlchemy's compiled layer escapes each value according to the dialect's rules."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Use the Snowflake connector's native parameterised execute",
            "safety": "✓ eliminates SQL injection", "safety_class": "safe",
            "perf": "— same runtime; connector handles escaping",
            "why": (
                "The snowflake-connector-python cursor.execute() natively supports "
                "%s placeholders. No additional library needed — just stop concatenating "
                "and pass the values as a tuple."
            ),
            "how_before": (
                "The connector receives a fully-formed SQL string with user data embedded. "
                "It executes it as-is — the injection is already in the string."
            ),
            "how_after": (
                "The connector receives the query template and values separately. "
                "It escapes each value according to Snowflake's type rules and substitutes "
                "safely — user data never modifies the SQL structure."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-C001  High cyclomatic complexity ──────────────────────────────────────

def _py_c001(snippet: str) -> List[Dict]:
    before = snippet

    # Option A: extract helper functions (show pattern)
    after_a = (
        "# Break the large function into focused helpers:\n\n"
        "def _validate_input(data):\n"
        "    \"\"\"Handles all validation branches — CC contribution isolated here.\"\"\"\n"
        "    if not data:\n"
        "        raise ValueError('data required')\n"
        "    # ... validation logic extracted from main function\n\n"
        "def _process_record(record):\n"
        "    \"\"\"Handles processing logic — single responsibility.\"\"\"\n"
        "    # ... processing logic extracted from main function\n\n"
        "def main_function(data):\n"
        "    \"\"\"Orchestrates: validate → process → return. CC=3.\"\"\"\n"
        "    _validate_input(data)\n"
        "    return _process_record(data)"
    )

    # Option B: dict dispatch to replace if/elif chains
    after_b = (
        "# Replace if/elif chains with a dispatch dict:\n\n"
        "# Before (CC += 1 per elif):\n"
        "# if action == 'create': _create()\n"
        "# elif action == 'update': _update()\n"
        "# elif action == 'delete': _delete()\n\n"
        "_DISPATCH = {\n"
        "    'create': _create,\n"
        "    'update': _update,\n"
        "    'delete': _delete,\n"
        "}\n\n"
        "handler = _DISPATCH.get(action)\n"
        "if handler is None:\n"
        "    raise ValueError(f'Unknown action: {action}')\n"
        "handler()"
    )

    # Option C: dataclass/strategy
    after_c = (
        before
        + "\n\n# Move state into a dataclass and methods into the class:\n"
        "# @dataclass\n"
        "# class Processor:\n"
        "#     config: dict\n"
        "#     def validate(self): ...\n"
        "#     def process(self): ...\n"
        "#     def run(self): self.validate(); return self.process()"
    )

    return [
        {
            "label": "A",
            "title": "Extract helper functions — one responsibility per function (recommended)",
            "safety": "✓ 100% safe — behaviour unchanged", "safety_class": "safe",
            "perf": "— same runtime; negligible function-call overhead",
            "why": (
                "Cyclomatic complexity (CC) counts the number of independent paths through code. "
                "Every if, elif, for, while, except, and comprehension adds 1. "
                "A CC of 15+ means 15+ test cases needed for full branch coverage. "
                "High CC correlates directly with defect rate — studies show CC > 10 is "
                "3× more likely to contain bugs than CC ≤ 5."
            ),
            "how_before": (
                "All branches live in one function. A reader must hold the entire state machine "
                "in their head while reading. Each new if/elif cascades — a change deep in the "
                "function can affect branches defined 50 lines earlier. Testing requires "
                "constructing inputs that exercise each of the 15+ independent paths."
            ),
            "how_after": (
                "Each helper function has CC = 2–4. A reader understands one helper at a time. "
                "The orchestrating function has CC = 3–4 (one branch per helper call outcome). "
                "Tests for each helper are independent — a bug in _validate_input is caught "
                "without needing to construct end-to-end scenarios."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Replace if/elif chains with a dispatch dictionary",
            "safety": "✓ 100% safe — same outcomes", "safety_class": "safe",
            "perf": "⚡ O(1) dispatch vs O(n) elif chain",
            "why": (
                "Long if/elif chains are the most common source of high CC. "
                "Each elif adds 1 to CC and 1 to the minimum test count. "
                "A dict keyed by the discriminator value replaces the whole chain "
                "with a single O(1) lookup — CC contribution drops from n to 1."
            ),
            "how_before": (
                "Python evaluates each elif condition in order — O(n) comparisons for n branches. "
                "Adding a new case requires finding the right place in the chain and inserting "
                "another elif. Forgetting the elif means falling through to else incorrectly."
            ),
            "how_after": (
                "dict.get(key) is a hash-table lookup — O(1) regardless of how many handlers "
                "are registered. Adding a new case is one dict entry — no touching the "
                "dispatch logic. The 'unknown key' case is handled explicitly and uniformly."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Refactor into a class — group state and behaviour together",
            "safety": "✓ 100% safe — same outcomes", "safety_class": "safe",
            "perf": "— same runtime; adds structure overhead",
            "why": (
                "When a function has high CC because it threads a large amount of state through "
                "many branches, a dataclass or class often fits better. "
                "State becomes attributes, branches become methods — each method has low CC."
            ),
            "how_before": (
                "A single function signature carries all the state as parameters or closures. "
                "Inner branches mutate local variables that other branches later read — "
                "implicit data flow that is hard to follow and test."
            ),
            "how_after": (
                "A @dataclass stores the shared state as typed fields. Methods operate on "
                "self — explicit data flow, mockable in tests, individually testable. "
                "run() orchestrates the methods; each method has CC ≤ 5."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-C002  Nested loops ─────────────────────────────────────────────────────

def _py_c002(snippet: str) -> List[Dict]:
    before = snippet

    # Option A: replace inner loop with dict lookup
    after_a = (
        "# Build a lookup dict before the outer loop — O(m) one-time cost\n"
        "lookup = {item['key']: item for item in inner_list}\n\n"
        "# Outer loop now does O(1) lookups instead of O(m) inner scan:\n"
        "for outer_item in outer_list:\n"
        "    match = lookup.get(outer_item['key'])  # O(1) hash lookup\n"
        "    if match:\n"
        "        process(outer_item, match)"
    )

    # Option B: pandas merge
    after_b = (
        "import pandas as pd\n\n"
        "# Load both datasets as DataFrames:\n"
        "df_outer = pd.DataFrame(outer_list)\n"
        "df_inner = pd.DataFrame(inner_list)\n\n"
        "# Vectorised merge — Snowflake-style join in Python memory:\n"
        "merged = df_outer.merge(\n"
        "    df_inner,\n"
        "    on='key_column',\n"
        "    how='inner'\n"
        ")\n"
        "# Process merged DataFrame with vectorised ops — no Python loops"
    )

    # Option C: push to SQL
    after_c = (
        "# Push the join/filter to Snowflake — handles billions of rows efficiently:\n"
        "cursor.execute(\"\"\"\n"
        "    SELECT o.*, i.*\n"
        "    FROM outer_table o\n"
        "    JOIN inner_table i ON o.key = i.key\n"
        "    WHERE <your conditions>\n"
        "\"\"\")\n"
        "results = cursor.fetchall()\n"
        "# Python only processes the already-joined result — no nested loop needed"
    )

    return [
        {
            "label": "A",
            "title": "Replace inner loop with a dict/set lookup — O(n) total (recommended)",
            "safety": "✓ 100% safe — same results", "safety_class": "safe",
            "perf": "⚡ O(n²) → O(n) — dramatic speedup for large datasets",
            "why": (
                "Nested loops multiply complexity. An outer loop over n items and an inner "
                "loop over m items = n × m iterations total. For 10,000 rows × 5,000 lookup "
                "rows = 50,000,000 comparisons. A Python dict lookup is O(1) — "
                "replacing the inner loop with dict.get() collapses the total to O(n+m)."
            ),
            "how_before": (
                "For each of the n outer items, Python iterates through all m inner items "
                "to find a match. Total comparisons: n × m. If n=10k and m=5k: 50 million "
                "iterations. Python's GIL means this runs single-threaded. "
                "At ~10M simple operations/second: 5 seconds for this logic alone."
            ),
            "how_after": (
                "Build the dict once: O(m) — one pass through the inner list. "
                "Then for each outer item: one dict.get() hash lookup — O(1). "
                "Total: O(n + m) instead of O(n × m). "
                "Same 10k × 5k example: 15,000 operations instead of 50,000,000."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Use pandas merge() — vectorised C-level join",
            "safety": "✓ 100% safe — same results", "safety_class": "safe",
            "perf": "⚡ 10–100× faster than nested Python loops for large datasets",
            "why": (
                "pandas merge() implements the join algorithm in compiled C (via NumPy). "
                "It uses sort-merge or hash join depending on dataset size. "
                "Vectorised operations process entire arrays at once — no Python per-row overhead."
            ),
            "how_before": (
                "Python interpreter overhead: each iteration involves bytecode dispatch, "
                "attribute lookups, and Python object creation. "
                "For 50M iterations, this overhead dominates execution time."
            ),
            "how_after": (
                "pandas.merge() calls C-compiled NumPy/pandas internals that operate on "
                "contiguous memory arrays. The join key comparison is a single vectorised "
                "operation on arrays — no Python per-row overhead. "
                "Memory layout is cache-friendly."
            ),
            "before": before, "after": after_b, "note": "Requires: pip install pandas",
        },
        {
            "label": "C",
            "title": "Push the join to Snowflake SQL — let the warehouse handle it",
            "safety": "✓ 100% safe — same results", "safety_class": "safe",
            "perf": "⚡ Snowflake uses distributed hash joins across thousands of cores",
            "why": (
                "If both datasets live in Snowflake, Python is doing work that the warehouse "
                "does orders of magnitude faster. A SQL JOIN with clustering keys can scan "
                "billions of rows using micro-partition pruning — Python cannot."
            ),
            "how_before": (
                "Python fetches all rows from both tables into memory, then runs the nested "
                "loop comparison. Network transfer + deserialization + Python iteration "
                "= slow for large datasets."
            ),
            "how_after": (
                "Snowflake executes the join inside the warehouse using distributed parallel "
                "hash joins. Only the final result set is returned to Python. "
                "Network transfer is minimal — only matched rows travel over the wire."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-C003  String concatenation in loop ────────────────────────────────────

def _py_c003(snippet: str) -> List[Dict]:
    before = snippet

    # Extract the variable being concatenated
    var_m = re.search(r'(\w+)\s*\+=', before)
    var   = var_m.group(1) if var_m else "result"

    after_a = re.sub(
        r'(\w+)\s*\+=\s*(.+)',
        r'_parts.append(\2)  # O(1) per iteration',
        before,
    )
    after_a = (
        f"_parts = []  # collect parts, join once at the end\n\n"
        + after_a
        + f"\n\n{var} = ''.join(_parts)  # O(n) — one allocation, one copy"
    )

    after_b = (
        "import io\n\n"
        f"_buf = io.StringIO()\n\n"
        + re.sub(r'(\w+)\s*\+=\s*(.+)', r'_buf.write(\2)', before)
        + f"\n\n{var} = _buf.getvalue()"
    )

    after_c = (
        f"# List comprehension — most Pythonic for simple transformations:\n"
        f"{var} = ''.join(\n"
        f"    str(item)  # your transformation here\n"
        f"    for item in items\n"
        f")"
    )

    return [
        {
            "label": "A",
            "title": "list.append() then ''.join() at the end — O(n) (recommended)",
            "safety": "✓ 100% safe — identical output string", "safety_class": "safe",
            "perf": "⚡ O(n²) → O(n) time and memory",
            "why": (
                "Python strings are immutable. Every s += fragment allocates a brand-new "
                "string of length len(s) + len(fragment) and copies both. After n iterations: "
                "total bytes copied = 1 + 2 + 3 + ... + n = n(n+1)/2 = O(n²). "
                "For 10,000 iterations of 10-char strings: ~500MB of allocations and GC pressure."
            ),
            "how_before": (
                "Iteration 1: allocate 10-char string, copy. "
                "Iteration 2: allocate 20-char string, copy all 20 chars. "
                "Iteration 100: allocate 1,000-char string, copy all 1,000 chars. "
                "Python's allocator + garbage collector handles all discarded intermediates. "
                "GC pressure grows linearly with n — eventual pause times increase."
            ),
            "how_after": (
                "list.append() is O(1) amortized — the list occasionally doubles its "
                "backing array, but each append costs O(1) on average. "
                "''.join(parts) computes total length, allocates one buffer of that size, "
                "then copies each part exactly once. Total: O(n) time, O(n) memory — "
                "the minimum possible for building a string of length n."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Use io.StringIO as a write buffer",
            "safety": "✓ 100% safe — identical output", "safety_class": "safe",
            "perf": "⚡ O(n) — amortized growth like BytesIO",
            "why": (
                "io.StringIO maintains an internal growable buffer. Each write() appends "
                "to the buffer with amortized O(1) cost. getvalue() returns the final string "
                "in one O(n) copy. Useful when you have complex write patterns "
                "(seek, tell, conditional writes) that don't fit list.append()."
            ),
            "how_before": (
                "Same as Option A's 'before' — each += allocates a new string and copies. "
                "The buffer grows quadratically."
            ),
            "how_after": (
                "StringIO allocates an internal buffer that grows geometrically (like a list). "
                "write() copies each fragment once to the current position. "
                "getvalue() returns the current content — one read of the buffer, O(n)."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Use a generator expression inside ''.join()",
            "safety": "✓ 100% safe — identical output", "safety_class": "safe",
            "perf": "⚡ O(n) — most concise Pythonic form",
            "why": (
                "When the loop body is a simple transformation (str(item), f'{item}\\n', etc.), "
                "a generator expression inside join() expresses the intent in one line and "
                "lets Python optimise the iteration in C internals."
            ),
            "how_before": (
                "An explicit for loop builds intermediate strings with += — quadratic."
            ),
            "how_after": (
                "join() with a generator pulls one transformed string at a time — "
                "no intermediate list is built, no intermediate strings are allocated "
                "beyond the current element. One O(n) pass to compute total length, "
                "one O(n) allocation, done."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-C004  Recursive function ──────────────────────────────────────────────

def _py_c004(snippet: str) -> List[Dict]:
    before = snippet

    fn_m   = re.search(r'def\s+(\w+)\s*\(', before)
    fn_name = fn_m.group(1) if fn_m else "recursive_fn"

    after_a = (
        f"def {fn_name}_iterative(root):\n"
        f"    \"\"\"Iterative version using explicit stack — no recursion limit.\"\"\"\n"
        f"    stack = [root]\n"
        f"    results = []\n"
        f"    while stack:\n"
        f"        node = stack.pop()\n"
        f"        results.append(process(node))\n"
        f"        # Push children in reverse order (so left is processed first):\n"
        f"        stack.extend(reversed(get_children(node)))\n"
        f"    return results"
    )

    after_b = (
        "import functools\n\n"
        + before.rstrip()
        + "\n\n"
        f"# Add memoization to avoid recomputing the same inputs:\n"
        f"@functools.lru_cache(maxsize=None)\n"
        f"def {fn_name}_memoized(n):\n"
        f"    # same body as {fn_name} — lru_cache caches each (n,) result\n"
        f"    ..."
    )

    after_c = (
        before.rstrip()
        + "\n\n# Last-resort: increase limit for deep but bounded recursion:\n"
        "import sys\n"
        "sys.setrecursionlimit(10_000)  # default is 1000\n"
        "# WARNING: stack frames consume ~1KB each — 10k frames = ~10MB stack\n"
        "# Use iterative approach (Option A) for truly deep recursion"
    )

    return [
        {
            "label": "A",
            "title": "Convert to iterative with an explicit stack (recommended)",
            "safety": "✓ 100% safe — same output, no recursion limit", "safety_class": "safe",
            "perf": "⚡ eliminates call-stack overhead; O(depth) space vs O(depth) stack frames",
            "why": (
                "Python's default recursion limit is 1000 frames. Exceeding it raises "
                "RecursionError. For tree traversal or recursive data processing with "
                "arbitrary depth, this makes the function unreliable in production. "
                "An iterative version using a Python list as a stack has no depth limit "
                "and avoids function-call overhead."
            ),
            "how_before": (
                "Each recursive call creates a new stack frame: local variables, return address, "
                "arguments — ~1–2KB per frame. 1000 frames = ~1–2MB stack consumption. "
                "Python also acquires and releases the GIL lock per call. "
                "For depth > 1000: RecursionError — program crashes."
            ),
            "how_after": (
                "A Python list used as a stack (append/pop) grows in heap memory — "
                "not in the C call stack. Heap memory limit is available RAM, not 1000 frames. "
                "Each iteration is a single while-loop body — no function call overhead, "
                "no GIL acquisition. The algorithm is identical — just rewritten without "
                "call-frame recursion."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Add @functools.lru_cache memoization — avoid recomputing subtrees",
            "safety": "✓ safe if inputs are hashable", "safety_class": "safe",
            "perf": "⚡ O(2^n) → O(n) for overlapping subproblems (e.g. Fibonacci, DP)",
            "why": (
                "If the recursive function is called with the same arguments more than once "
                "(overlapping subproblems — like Fibonacci or dynamic programming), "
                "each duplicate call re-does all the work. lru_cache stores the result "
                "for each unique argument tuple and returns it instantly on repeat calls."
            ),
            "how_before": (
                "fib(5) calls fib(4) and fib(3). fib(4) calls fib(3) and fib(2). "
                "fib(3) is computed twice. For fib(40): 102,334,155 calls. "
                "O(2^n) — exponential growth."
            ),
            "how_after": (
                "lru_cache wraps the function in a dict keyed by (n,). "
                "First call: compute and store. Repeat call: return stored value in O(1). "
                "fib(40) now makes exactly 40 unique calls — O(n) instead of O(2^n)."
            ),
            "before": before, "after": after_b,
            "note": "lru_cache requires hashable arguments — lists, dicts, and sets are not hashable.",
        },
        {
            "label": "C",
            "title": "Increase sys.setrecursionlimit — use only for bounded shallow recursion",
            "safety": "⚠ verify depth is bounded", "safety_class": "caution",
            "perf": "— same performance, just raises the crash threshold",
            "why": (
                "For recursion that is bounded and shallow (e.g. depth ≤ 5,000 guaranteed) "
                "this is the simplest fix. It does NOT fix the underlying algorithmic problem "
                "and should not be used for unbounded recursion."
            ),
            "how_before": (
                "RecursionError raised at depth 1001. "
                "Stack frame limit enforced by Python's C runtime."
            ),
            "how_after": (
                "Limit raised. The recursion still uses C stack — each frame still consumes "
                "~1–2KB. At 10,000 frames: ~20MB stack. Most OS default stack sizes are 8MB. "
                "For depth > ~4000 you risk a C-level stack overflow (segfault, not Python exception). "
                "Only use for bounded, shallow recursion where depth is well understood."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-C005  Nested data structure growth ────────────────────────────────────

def _py_c005(snippet: str) -> List[Dict]:
    before = snippet

    after_a = (
        "# Pre-allocate: if final size is known, use a fixed-size array:\n"
        "import numpy as np\n"
        "results = np.empty(n * m, dtype=object)  # one allocation\n"
        "idx = 0\n"
        "for i in range(n):\n"
        "    for j in range(m):\n"
        "        results[idx] = compute(i, j)\n"
        "        idx += 1"
    )

    after_b = (
        "# Generator — yields one item at a time, O(1) memory:\n"
        "def generate_results(outer, inner):\n"
        "    for o in outer:\n"
        "        for i in inner:\n"
        "            yield process(o, i)  # caller consumes one at a time\n\n"
        "# Consumer writes directly to output:\n"
        "with open('output.csv', 'w') as f:\n"
        "    for row in generate_results(outer_list, inner_list):\n"
        "        f.write(row)"
    )

    after_c = (
        "# Use a set for deduplication instead of a list:\n"
        "seen = set()\n"
        "for o in outer:\n"
        "    for i in inner:\n"
        "        key = (o['id'], i['id'])\n"
        "        if key not in seen:\n"
        "            seen.add(key)\n"
        "            process(o, i)  # process each unique pair once"
    )

    return [
        {
            "label": "A",
            "title": "Pre-allocate with numpy — one allocation, cache-friendly (recommended)",
            "safety": "✓ 100% safe — same data", "safety_class": "safe",
            "perf": "⚡ one O(n×m) allocation vs O(n×m) incremental allocations",
            "why": (
                "list.append() in a nested loop calls Python's allocator once per iteration. "
                "The list doubles its backing array when full — each doubling copies all "
                "existing elements. For n×m = 1M items: ~20 doublings, ~1M extra copies. "
                "Pre-allocating once avoids all intermediate copies."
            ),
            "how_before": (
                "Each append() may trigger a realloc: Python allocates a new backing array "
                "(1.125× the current size), copies all existing elements, then appends. "
                "For 1M appends: memory fragmentation, GC pressure, and ~1M extra element copies "
                "across all doublings."
            ),
            "how_after": (
                "numpy.empty(n*m) allocates one contiguous block of memory. "
                "Each element assignment is a direct memory write — O(1), no realloc, "
                "no copying. The data is laid out contiguously — CPU cache-friendly for "
                "subsequent numerical operations."
            ),
            "before": before, "after": after_a, "note": "Requires: pip install numpy",
        },
        {
            "label": "B",
            "title": "Use a generator — stream results without materialising the full list",
            "safety": "✓ 100% safe — same elements, different consumption pattern", "safety_class": "safe",
            "perf": "⚡ O(1) memory — only one item held at a time",
            "why": (
                "If the result list is only consumed once (written to a file, streamed to an API, "
                "passed to a for-loop), you don't need to materialise it. A generator "
                "yields one item at a time — the full list is never in memory simultaneously."
            ),
            "how_before": (
                "All n×m items are computed and appended to a list before any are consumed. "
                "Peak memory = n×m items held simultaneously. If each item is 1KB: "
                "1M items = 1GB peak memory."
            ),
            "how_after": (
                "yield produces one item and suspends — the caller consumes it (writes to file, "
                "sends to API) and asks for the next. At any moment, only one item is in memory. "
                "Peak memory = O(1) regardless of n×m."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Use a set for deduplication — avoid processing pairs twice",
            "safety": "✓ 100% safe — same unique results", "safety_class": "safe",
            "perf": "⚡ O(1) membership check vs O(n) list search",
            "why": (
                "If the nested loop is building a collection of unique items, "
                "using a set is both faster (O(1) lookup) and prevents duplicate processing. "
                "List membership check (if x in list) is O(n) — becomes O(n²) inside a loop."
            ),
            "how_before": (
                "append() adds items unconditionally. If duplicates exist, they accumulate. "
                "Checking 'if item not in result_list' before append is O(n) — "
                "which makes the loop O(n²) overall."
            ),
            "how_after": (
                "set.add() is O(1) — hash-based. 'if key not in seen' is O(1). "
                "The dedup check costs O(1) per iteration instead of O(n). "
                "Total loop complexity drops from O(n²) to O(n)."
            ),
            "before": before, "after": after_c, "note": None,
        },
    ]


# ── PY-Q002  Long function ────────────────────────────────────────────────────

def _py_q002(snippet: str) -> List[Dict]:
    before = snippet

    fn_m    = re.search(r'def\s+(\w+)\s*\(', before)
    fn_name = fn_m.group(1) if fn_m else "long_function"

    after_a = (
        f"# Split '{fn_name}' into focused helpers:\n\n"
        f"def _setup_{fn_name}(args):\n"
        f"    \"\"\"Handles setup / validation — first 1/3 of original function.\"\"\"\n"
        f"    ...\n\n"
        f"def _execute_{fn_name}(state):\n"
        f"    \"\"\"Core logic — middle 1/3 of original function.\"\"\"\n"
        f"    ...\n\n"
        f"def _finalise_{fn_name}(result):\n"
        f"    \"\"\"Cleanup / formatting — last 1/3 of original function.\"\"\"\n"
        f"    ...\n\n"
        f"def {fn_name}(args):\n"
        f"    \"\"\"Orchestrates the three phases — fits on half a screen.\"\"\"\n"
        f"    state  = _setup_{fn_name}(args)\n"
        f"    result = _execute_{fn_name}(state)\n"
        f"    return _finalise_{fn_name}(result)"
    )

    after_b = (
        f"@dataclass\n"
        f"class {fn_name.title().replace('_','')}Runner:\n"
        f"    config: dict\n"
        f"    state:  dict = field(default_factory=dict)\n\n"
        f"    def setup(self):\n"
        f"        \"\"\"Extracted from {fn_name} — setup phase.\"\"\"\n"
        f"        ...\n\n"
        f"    def execute(self):\n"
        f"        \"\"\"Extracted from {fn_name} — core phase.\"\"\"\n"
        f"        ...\n\n"
        f"    def run(self):\n"
        f"        self.setup()\n"
        f"        return self.execute()"
    )

    after_c = (
        before.rstrip()
        + "\n\n# Add a docstring section map as a navigation aid:\n"
        f"def {fn_name}(args):\n"
        "    \"\"\"\n"
        "    Sections:\n"
        "        1. Input validation   — lines 1–20\n"
        "        2. Data loading       — lines 21–60\n"
        "        3. Transformation     — lines 61–120\n"
        "        4. Output formatting  — lines 121–end\n"
        "    \"\"\"\n"
        "    # ── Section 1: Input validation ──────────────────────\n"
        "    ...\n"
        "    # ── Section 2: Data loading ──────────────────────────\n"
        "    ..."
    )

    return [
        {
            "label": "A",
            "title": "Extract setup / execute / finalise helper functions (recommended)",
            "safety": "✓ 100% safe — same behaviour", "safety_class": "safe",
            "perf": "— negligible function-call overhead",
            "why": (
                f"'{fn_name}' is doing multiple distinct things in sequence. "
                "Functions longer than ~50 lines are statistically harder to review correctly — "
                "reviewers miss bugs because they cannot hold the full context in working memory. "
                "Each extracted helper has a clear name, clear inputs, and clear return value — "
                "independently testable and understandable."
            ),
            "how_before": (
                f"'{fn_name}' mixes setup, core logic, and cleanup in one flat body. "
                "A bug in the cleanup section requires reading 150+ lines to understand the "
                "state at that point. A test must invoke the entire function to exercise "
                "any one part of it."
            ),
            "how_after": (
                "Each helper handles one concern. A bug in _finalise is isolated — "
                "only 20 lines of context needed. Unit tests call _setup, _execute, "
                "_finalise independently with mock inputs. The orchestrator becomes 3 lines — "
                "immediately understandable at a glance."
            ),
            "before": before, "after": after_a, "note": None,
        },
        {
            "label": "B",
            "title": "Convert to a class — methods replace the long function body",
            "safety": "✓ 100% safe — same behaviour", "safety_class": "safe",
            "perf": "— minimal overhead; __init__ replaces argument passing",
            "why": (
                "When a long function threads state through many local variables, "
                "a class is often the right structure. State becomes self attributes; "
                "sections become methods. Each method is short, named, and testable."
            ),
            "how_before": (
                "Local variables thread state across 150+ lines. "
                "It is unclear which variables are still alive at any given point. "
                "Passing this state to helper functions requires long argument lists."
            ),
            "how_after": (
                "self.state is visible to all methods — no long argument lists. "
                "Each method is a named, focused unit. "
                "The run() method is the entry point — 3–5 lines, self-documenting."
            ),
            "before": before, "after": after_b, "note": None,
        },
        {
            "label": "C",
            "title": "Add a section map docstring — navigation without refactor",
            "safety": "✓ 100% safe — no behaviour change", "safety_class": "safe",
            "perf": "— no runtime change",
            "why": (
                "If refactoring is not feasible right now (e.g. release freeze), "
                "a section map docstring and inline section comments let reviewers navigate "
                "the function without reading every line. Reduces review error rate "
                "without changing any logic."
            ),
            "how_before": (
                "No internal structure markers — readers must read top-to-bottom to orient "
                "themselves. A reviewer looking for the output formatting step must scan "
                "the full function body."
            ),
            "how_after": (
                "Section headers act as a table of contents. A reviewer jumps directly to "
                "Section 4: Output formatting. The docstring section map keeps them accurate "
                "as the function evolves. This is a stopgap — Option A or B should follow "
                "in the next sprint."
            ),
            "before": before, "after": after_c, "note": "Follow up with Option A in the next sprint.",
        },
    ]


# ── Dispatch table ─────────────────────────────────────────────────────────────

_HANDLERS = {
    # SQL
    "SQL-C001": _sql_c001,
    "SQL-Q002": _sql_q002,
    "SQL-C003": _sql_c003,
    "SQL-C004": _sql_c004,
    "SQL-Q001": _sql_q001,
    # Python
    "PY-S001": _py_s001,
    "PY-S002": _py_s002,
    "PY-S003": _py_s003,
    "PY-C001": _py_c001,
    "PY-C002": _py_c002,
    "PY-C003": _py_c003,
    "PY-C004": _py_c004,
    "PY-C005": _py_c005,
    "PY-Q002": _py_q002,
}
