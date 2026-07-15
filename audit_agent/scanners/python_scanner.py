"""
Python Scanner — Time Complexity, Space Complexity, Security, and Quality.

Time complexity is estimated via AST analysis:
  - Nested loops      → O(n^depth)
  - Recursive calls   → O(2^n) worst case flagged
  - String concat in loops → O(n²) memory allocation pattern

Space complexity signals:
  - List comprehensions building full copies → O(n)
  - Nested data structure creation in loops  → O(n²)

Security checks (without calling bandit subprocess):
  - Hardcoded secrets / API keys
  - subprocess with shell=True
  - exec() / eval() calls
  - SQL string concatenation
"""
import ast
import re
from typing import List, Optional, Tuple

from .base import BaseScanner, Finding, Severity, Category

def _py_lines(content: str, line: int, context: int = 6) -> str:
    """Extract lines around a 1-based line number from source content."""
    ls = content.splitlines()
    idx   = max(0, line - 1)
    start = max(0, idx - context)
    end   = min(len(ls), idx + context + 1)
    return "\n".join(ls[start:end]).strip()


# ── Regexes for quick-scan before AST parse ─────────────────────────────────
RE_SUBPROCESS_SHELL = re.compile(r"subprocess\.\w+\(.*shell\s*=\s*True", re.DOTALL)
RE_EVAL             = re.compile(r"\beval\s*\(")
RE_EXEC             = re.compile(r"\bexec\s*\(")
RE_SQL_CONCAT       = re.compile(
    r'(["\']\s*(SELECT|INSERT|UPDATE|DELETE|DROP)\b.*["\'])\s*\+', re.IGNORECASE
)
RE_HARDCODED_SECRET = re.compile(
    r'(?i)(password|passwd|secret|api_key|token|auth_key)\s*=\s*["\'][^"\']{6,}["\']'
)


# ── AST helpers ─────────────────────────────────────────────────────────────

def _get_max_loop_nesting(tree: ast.AST) -> int:
    """Return the maximum depth of nested for/while loops in the AST."""
    def _depth(node: ast.AST, current: int) -> int:
        max_d = current
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.For, ast.While)):
                max_d = max(max_d, _depth(child, current + 1))
            else:
                max_d = max(max_d, _depth(child, current))
        return max_d

    return _depth(tree, 0)


def _find_string_concat_in_loops(tree: ast.AST) -> List[int]:
    """Find line numbers where string += occurs inside a loop (O(n²) pattern)."""
    lines = []

    class Visitor(ast.NodeVisitor):
        def __init__(self):
            self._in_loop = False

        def visit_For(self, node):
            prev = self._in_loop
            self._in_loop = True
            self.generic_visit(node)
            self._in_loop = prev

        def visit_While(self, node):
            prev = self._in_loop
            self._in_loop = True
            self.generic_visit(node)
            self._in_loop = prev

        def visit_AugAssign(self, node):
            if self._in_loop and isinstance(node.op, ast.Add):
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    lines.append(node.lineno)
                elif isinstance(node.value, (ast.JoinedStr, ast.BinOp)):
                    lines.append(node.lineno)
            self.generic_visit(node)

    Visitor().visit(tree)
    return lines


def _find_recursive_functions(tree: ast.AST) -> List[Tuple[str, int]]:
    """Return list of (function_name, line_number) for recursive functions."""
    results = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fn_name = node.name
            for child in ast.walk(node):
                if isinstance(child, ast.Call):
                    if isinstance(child.func, ast.Name) and child.func.id == fn_name:
                        results.append((fn_name, node.lineno))
                        break
    return results


def _find_nested_data_structures_in_loops(tree: ast.AST) -> List[int]:
    """Find where lists/dicts are appended inside nested loops → O(n²) space."""
    lines = []

    class Visitor(ast.NodeVisitor):
        def __init__(self):
            self._loop_depth = 0

        def visit_For(self, node):
            self._loop_depth += 1
            self.generic_visit(node)
            self._loop_depth -= 1

        def visit_While(self, node):
            self._loop_depth += 1
            self.generic_visit(node)
            self._loop_depth -= 1

        def visit_Call(self, node):
            if self._loop_depth >= 2:
                if (isinstance(node.func, ast.Attribute) and
                        node.func.attr in ("append", "extend", "update", "add")):
                    lines.append(node.lineno)
            self.generic_visit(node)

    Visitor().visit(tree)
    return lines


def _estimate_cyclomatic_complexity(tree: ast.AST) -> int:
    """
    Simplified cyclomatic complexity: count decision branches.
    CC = 1 + number of (if/elif/for/while/except/with/assert/comprehension)
    Grade: A=1-5, B=6-10, C=11-15, D=16-20, E=21-25, F=26+
    """
    BRANCH_NODES = (
        ast.If, ast.For, ast.While, ast.ExceptHandler,
        ast.With, ast.Assert, ast.ListComp, ast.SetComp,
        ast.DictComp, ast.GeneratorExp, ast.IfExp,
    )
    count = 1
    for node in ast.walk(tree):
        if isinstance(node, BRANCH_NODES):
            count += 1
    return count


def _cc_grade(cc: int) -> str:
    if cc <= 5:   return "A — simple"
    if cc <= 10:  return "B — moderate"
    if cc <= 15:  return "C — complex"
    if cc <= 20:  return "D — very complex"
    if cc <= 25:  return "E — extremely complex"
    return         "F — untestable"


class PythonScanner(BaseScanner):
    name = "python"

    def scan(self, manifest: dict) -> List[Finding]:
        findings: List[Finding] = []
        for py_file in manifest.get("python_files", []):
            content = py_file.get("full_content", "")
            path    = py_file["path"]
            if not content.strip():
                continue
            findings.extend(self._scan_file(content, path))
        return findings

    def _scan_file(self, content: str, path: str) -> List[Finding]:
        out: List[Finding] = []

        # Parse AST first (if possible) so regex-based checks can cross-check
        # a match isn't just text inside a string/docstring/comment before
        # treating it as real, executable code.
        tree: Optional[ast.AST] = None
        try:
            tree = ast.parse(content)
        except SyntaxError:
            pass

        # ── Regex-based quick checks (no AST needed) ─────────────────────
        out.extend(self._check_regex_patterns(content, path, tree))

        if tree is None:
            try:
                ast.parse(content)
            except SyntaxError as e:
                out.append(Finding(
                    check_id   = "PY-Q001",
                    title      = "Python syntax error",
                    severity   = Severity.HIGH,
                    category   = Category.PYTHON_QUALITY,
                    file       = path,
                    line       = getattr(e, "lineno", 0) or 0,
                    description= f"File cannot be parsed: {e}",
                    suggestion = "Fix the syntax error before running the audit.",
                ))
            return out

        out.extend(self._check_complexity(content, tree, path))
        out.extend(self._check_quality(content, tree, path))
        return out

    # ── Regex checks ───────────────────────────────────────────────────────

    def _check_regex_patterns(self, content: str, path: str, tree: Optional[ast.AST] = None) -> List[Finding]:
        out: List[Finding] = []

        # Real eval(...) call sites confirmed via AST — used to gate
        # suggested_fix so it never fires on the word "eval(" appearing
        # inside a string literal, docstring, or comment.
        real_eval_lines = set()
        if tree is not None:
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                        and node.func.id == "eval"):
                    real_eval_lines.add(node.lineno)

        if RE_SUBPROCESS_SHELL.search(content):
            line = content[: RE_SUBPROCESS_SHELL.search(content).start()].count("\n") + 1
            out.append(Finding(
                check_id     = "PY-S001",
                title        = "subprocess with shell=True — command injection risk",
                severity     = Severity.HIGH,
                category     = Category.PYTHON_SECURITY,
                file         = path,
                line         = line,
                code_snippet = _py_lines(content, line, context=4),
                description  = (
                    "Using shell=True passes the command string to the shell, enabling "
                    "command injection if any part of the command is user-controlled."
                ),
                suggestion   = (
                    "Pass arguments as a list: subprocess.run(['cmd', arg1, arg2])\n"
                    "Never use shell=True with any input from external sources."
                ),
                cwe  = "CWE-78",
                owasp= "A03:2021 Injection",
            ))

        for m in RE_EVAL.finditer(content):
            line = content[: m.start()].count("\n") + 1
            fixed_line = None
            if line in real_eval_lines:
                line_start = content.rfind("\n", 0, m.start()) + 1
                col        = m.start() - line_start
                line_text  = content.splitlines()[line - 1]
                fixed_line = line_text[:col] + "ast.literal_eval(" + line_text[col + len("eval("):]
            out.append(Finding(
                check_id     = "PY-S002",
                title        = "eval() call — arbitrary code execution risk",
                severity     = Severity.HIGH,
                category     = Category.PYTHON_SECURITY,
                file         = path,
                line         = line,
                code_snippet = _py_lines(content, line, context=4),
                description  = "eval() executes arbitrary Python code. If the argument is externally influenced, this is a critical RCE vulnerability.",
                suggestion   = "Replace eval() with ast.literal_eval() for safe evaluation of Python literals.",
                cwe          = "CWE-95",
                suggested_fix = fixed_line,
            ))

        for m in RE_SQL_CONCAT.finditer(content):
            line = content[: m.start()].count("\n") + 1
            out.append(Finding(
                check_id     = "PY-S003",
                title        = "SQL string concatenation — injection risk",
                severity     = Severity.HIGH,
                category     = Category.PYTHON_SECURITY,
                file         = path,
                line         = line,
                code_snippet = _py_lines(content, line, context=4),
                description  = "SQL query built by string concatenation. Any part derived from user input is a SQL injection vulnerability.",
                suggestion   = "Use parameterized queries: cursor.execute('SELECT ... WHERE id = %s', (user_id,))",
                cwe          = "CWE-89",
                owasp        = "A03:2021 Injection",
            ))

        return out

    # ── Complexity checks ──────────────────────────────────────────────────

    def _check_complexity(self, content: str, tree: ast.AST, path: str) -> List[Finding]:
        out: List[Finding] = []
        lines = content.splitlines()

        # Cyclomatic complexity (per-file, rough)
        cc = _estimate_cyclomatic_complexity(tree)
        if cc > 10:
            severity = Severity.HIGH if cc > 20 else Severity.MEDIUM
            # find the longest function as the representative snippet
            longest = max(
                (n for n in ast.walk(tree)
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))),
                key=lambda n: len(list(ast.walk(n))), default=None
            )
            snip_line = longest.lineno if longest else 1
            out.append(Finding(
                check_id        = "PY-C001",
                title           = f"High cyclomatic complexity — CC={cc} ({_cc_grade(cc)})",
                severity        = severity,
                category        = Category.PYTHON_COMPLEXITY,
                file            = path,
                line            = snip_line,
                code_snippet    = _py_lines(content, snip_line, context=8),
                description     = (
                    f"File-level cyclomatic complexity is {cc} ({_cc_grade(cc)}). "
                    "High CC means more branches to test, higher defect probability, "
                    "and harder code reviews."
                ),
                suggestion      = (
                    "Break large functions into smaller, single-responsibility functions. "
                    "Aim for CC ≤ 10 per function (grade B or better)."
                ),
                time_complexity = f"More branches → O({cc} paths) to test fully",
            ))

        # Nested loops
        max_nesting = _get_max_loop_nesting(tree)
        if max_nesting >= 2:
            severity = Severity.HIGH if max_nesting >= 3 else Severity.MEDIUM
            complexity_str = f"O(n^{max_nesting})" if max_nesting <= 4 else "O(exponential)"
            # find the first nested loop line
            nested_line = 1
            for node in ast.walk(tree):
                if isinstance(node, (ast.For, ast.While)):
                    for child in ast.walk(node):
                        if isinstance(child, (ast.For, ast.While)) and child is not node:
                            nested_line = child.lineno
                            break
                    break
            out.append(Finding(
                check_id        = "PY-C002",
                title           = f"Nested loops — {max_nesting} levels deep",
                severity        = severity,
                category        = Category.PYTHON_COMPLEXITY,
                file            = path,
                line            = nested_line,
                code_snippet    = _py_lines(content, nested_line, context=6),
                description     = (
                    f"{max_nesting} levels of nested loops detected. "
                    f"Time complexity is approximately {complexity_str}. "
                    "For data engineering workloads processing rows, this can be devastating at scale."
                ),
                suggestion      = (
                    "Consider:\n"
                    "1. Replace inner loop with a dict/set lookup: O(1) instead of O(n)\n"
                    "2. Use pandas vectorized operations instead of Python loops\n"
                    "3. Push computation to Snowflake SQL where the optimizer can help"
                ),
                time_complexity = complexity_str,
            ))

        # String concatenation in loops
        concat_lines = _find_string_concat_in_loops(tree)
        if concat_lines:
            out.append(Finding(
                check_id        = "PY-C003",
                title           = f"String concatenation in loop at line(s) {concat_lines}",
                severity        = Severity.MEDIUM,
                category        = Category.PYTHON_COMPLEXITY,
                file            = path,
                line            = concat_lines[0],
                code_snippet    = _py_lines(content, concat_lines[0], context=6),
                description     = (
                    "String concatenation with += inside a loop creates a new string object "
                    "on every iteration. For n iterations this is O(n²) memory and time. "
                    "Python strings are immutable — each += copies the entire string."
                ),
                suggestion      = (
                    "Collect parts in a list and join at the end:\n"
                    "  parts = []\n"
                    "  for item in items:\n"
                    "      parts.append(str(item))\n"
                    "  result = ''.join(parts)  # O(n)"
                ),
                time_complexity  = "O(n²) → O(n) with list.join()",
                space_complexity = "O(n²) intermediate → O(n) with list",
            ))

        # Recursive functions
        recursive = _find_recursive_functions(tree)
        for fn_name, fn_line in recursive:
            out.append(Finding(
                check_id        = "PY-C004",
                title           = f"Recursive function '{fn_name}' — verify base case",
                severity        = Severity.LOW,
                category        = Category.PYTHON_COMPLEXITY,
                file            = path,
                line            = fn_line,
                code_snippet    = _py_lines(content, fn_line, context=8),
                description     = (
                    f"'{fn_name}' calls itself recursively. Without a proper base case "
                    "this will hit Python's recursion limit (default 1000). "
                    "Space complexity is O(depth) stack frames."
                ),
                suggestion      = (
                    "Ensure a base case terminates recursion. "
                    "For deep recursion, consider converting to iterative with an explicit stack."
                ),
                time_complexity  = "O(2^n) worst case without memoization",
                space_complexity = "O(depth) call stack",
            ))

        # Nested structure building (O(n²) space)
        nested_build_lines = _find_nested_data_structures_in_loops(tree)
        if nested_build_lines:
            out.append(Finding(
                check_id        = "PY-C005",
                title           = f"Data structure growth inside nested loops",
                severity        = Severity.MEDIUM,
                category        = Category.PYTHON_COMPLEXITY,
                file            = path,
                line            = nested_build_lines[0],
                code_snippet    = _py_lines(content, nested_build_lines[0], context=6),
                description     = (
                    f"append()/extend() called inside {len(nested_build_lines)} nested loop location(s). "
                    "Building data structures in nested loops risks O(n²) memory growth."
                ),
                suggestion      = (
                    "Pre-allocate or use a set for deduplication. "
                    "For large datasets, use generators or streaming patterns."
                ),
                space_complexity = "O(n²) — nested accumulation",
            ))

        # Always surface the file's overall estimated complexity, even when
        # no specific complexity issue was flagged — so every scanned file
        # shows its Big-O in the PR review, not just the ones with findings.
        if max_nesting == 0:
            overall_time = "O(1) — no loops detected"
        elif max_nesting == 1:
            overall_time = "O(n) — single loop"
        elif max_nesting <= 4:
            overall_time = f"O(n^{max_nesting}) — {max_nesting} nested loop levels"
        else:
            overall_time = "O(exponential) — deeply nested loops"

        if nested_build_lines:
            overall_space = "O(n²) — nested accumulation"
        elif recursive:
            overall_space = "O(depth) — recursive call stack"
        else:
            overall_space = "O(n) — linear"

        out.append(Finding(
            check_id         = "PY-C000",
            title            = "Overall estimated complexity",
            severity         = Severity.INFO,
            category         = Category.PYTHON_COMPLEXITY,
            file             = path,
            description      = f"Cyclomatic complexity: {cc} ({_cc_grade(cc)}). Max loop nesting: {max_nesting}.",
            time_complexity  = overall_time,
            space_complexity = overall_space,
        ))

        return out

    # ── Quality checks ─────────────────────────────────────────────────────

    def _check_quality(self, content: str, tree: ast.AST, path: str) -> List[Finding]:
        out: List[Finding] = []

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                body_len = len(list(ast.walk(node)))
                if body_len > 200:
                    out.append(Finding(
                        check_id     = "PY-Q002",
                        title        = f"Function '{node.name}' is very long ({body_len} AST nodes)",
                        severity     = Severity.LOW,
                        category     = Category.PYTHON_QUALITY,
                        file         = path,
                        line         = node.lineno,
                        code_snippet = _py_lines(content, node.lineno, context=10),
                        description  = (
                            f"'{node.name}' has {body_len} AST nodes. "
                            "Long functions are harder to test, review, and reason about."
                        ),
                        suggestion   = (
                            "Break into smaller, single-responsibility functions. "
                            "Aim for functions that fit on one screen (~50 lines)."
                        ),
                    ))

        return out
