"""
Secrets & Credential Exposure Scanner
Detects: private keys in repo, .env credentials, hardcoded tokens,
         Snowflake account IDs, Bandit B608 suppression, profiles.yml in VCS.
"""
import re
from pathlib import Path
from typing import List

from .base import BaseScanner, Finding, Severity, Category

# ── Patterns ───────────────────────────────────────────────────────────────
_RE_SNOWFLAKE_ACCOUNT = re.compile(
    r"[A-Z0-9]+-[A-Z0-9]+\.[a-z]+snowflakecomputing\.com|"
    r"account\s*[=:]\s*['\"]?[A-Z0-9]+-[A-Z0-9]+",
    re.IGNORECASE,
)
_RE_GENERIC_SECRET = re.compile(
    r"(?i)(password|passwd|secret|api_key|token|auth_key)\s*[=:]\s*['\"][^'\"]{6,}['\"]"
)
_RE_PRIVATE_KEY_BLOCK = re.compile(
    r"-----BEGIN\s+(RSA|EC|OPENSSH|PRIVATE)\s+PRIVATE KEY-----"
)
_RE_ENV_ASSIGNMENT = re.compile(
    r"^export\s+\w+=['\"]?.+['\"]?$", re.MULTILINE
)
_RE_HARDCODED_UUID = re.compile(
    r"['\"]([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})['\"]",
    re.IGNORECASE,
)

PRIVATE_KEY_EXTENSIONS = {".p8", ".pem", ".key", ".pfx", ".pkcs12"}
SKIP_DIRS = {".git", ".venv", "__pycache__", "target", "dbt_packages", "node_modules"}


class SecretsScanner(BaseScanner):
    name = "secrets"

    def scan(self, manifest: dict) -> List[Finding]:
        findings: List[Finding] = []
        root = Path(manifest["root"])

        # ── 1. Private key files committed to repo ─────────────────────────
        for rel_path in manifest["summary"].get("private_key_files", []):
            findings.append(Finding(
                check_id   = "SEC-001",
                title      = "Private key file committed to repository",
                severity   = Severity.CRITICAL,
                category   = Category.SECRETS,
                file       = rel_path,
                description= (
                    f"A private key file '{rel_path}' exists in the repository. "
                    "Anyone with read access to this repo can use this key to "
                    "authenticate as the Snowflake service user."
                ),
                suggestion = (
                    "1. Rotate the key immediately in Snowflake:\n"
                    "   ALTER USER <user> SET RSA_PUBLIC_KEY='<new_pub_key>';\n"
                    "2. Add to .gitignore: *.p8  *.pem  *.key\n"
                    "3. Purge from git history:\n"
                    f"   git filter-repo --path {rel_path} --invert-paths"
                ),
                cwe  = "CWE-312",
                owasp= "A02:2021 Cryptographic Failures",
            ))

        # ── 2. .env file with active credentials ───────────────────────────
        for risk in manifest.get("credential_risks", []):
            if risk["risk"] == "active_credentials_in_env":
                findings.append(Finding(
                    check_id   = "SEC-002",
                    title      = ".env file contains active credentials",
                    severity   = Severity.CRITICAL,
                    category   = Category.SECRETS,
                    file       = risk["file"],
                    description= (
                        f"Found {risk.get('active_variable_count', '?')} active "
                        "environment variable assignments (non-commented) in this "
                        ".env file. Snowflake account IDs, usernames, and key "
                        "paths are visible to anyone with repo access."
                    ),
                    suggestion = (
                        "Add to .gitignore: .env\n"
                        "Use a secret manager (GitHub Secrets, Azure Key Vault, "
                        "or Prefect Secret Blocks) for all credentials.\n"
                        "Copy template: mv .env .env.example && git rm .env"
                    ),
                    cwe  = "CWE-798",
                    owasp= "A07:2021 Identification and Authentication Failures",
                ))

            elif risk["risk"] == "profiles_yml_in_repo":
                findings.append(Finding(
                    check_id   = "SEC-003",
                    title      = "profiles.yml committed to repository",
                    severity   = Severity.HIGH,
                    category   = Category.SECRETS,
                    file       = risk["file"],
                    description= (
                        "dbt profiles.yml should live in ~/.dbt/ and must never "
                        "be committed to version control. It contains warehouse, "
                        "role, and authentication path references."
                    ),
                    suggestion = (
                        "Move to ~/.dbt/profiles.yml and add to .gitignore:\n"
                        "  mv vision_dbt/profiles.yml ~/.dbt/profiles.yml\n"
                        "  echo 'vision_dbt/profiles.yml' >> .gitignore\n"
                        "  git rm --cached vision_dbt/profiles.yml"
                    ),
                    cwe  = "CWE-538",
                    owasp= "A05:2021 Security Misconfiguration",
                ))

        # ── 3. Scan file contents for embedded secrets ─────────────────────
        for py_file in manifest.get("python_files", []):
            self._scan_python_content(py_file, findings)

        # ── 4. Bandit B608 global suppression ──────────────────────────────
        bandit_path = root / ".bandit"
        if bandit_path.exists():
            content = bandit_path.read_text(errors="ignore")
            if "B608" in content:
                findings.append(Finding(
                    check_id   = "SEC-004",
                    title      = "Bandit B608 (SQL injection) globally suppressed",
                    severity   = Severity.HIGH,
                    category   = Category.SECRETS,
                    file       = ".bandit",
                    description= (
                        "B608 is suppressed globally — this disables ALL SQL "
                        "injection detection across the entire Python codebase. "
                        "This can hide real vulnerabilities in connector code."
                    ),
                    suggestion = (
                        "Scope the skip to specific tested files only:\n"
                        "  # In the specific file that triggers B608 falsely:\n"
                        "  result = conn.execute(query)  # nosec B608\n"
                        "Remove the global B608 skip from .bandit."
                    ),
                    cwe  = "CWE-89",
                    owasp= "A03:2021 Injection",
                ))

        # ── 5. Hardcoded UUIDs in SQL (data coupling risk) ─────────────────
        for sql_file in manifest.get("sql_files", []):
            uuids = sql_file.get("hardcoded_uuids", [])
            if uuids:
                findings.append(Finding(
                    check_id     = "SEC-005",
                    title        = f"Hardcoded UUID literals in SQL model",
                    severity     = Severity.LOW,
                    category     = Category.SQL_SECURITY,
                    file         = sql_file["path"],
                    description  = (
                        f"Found {len(uuids)} hardcoded UUID(s) in SQL. "
                        "These create brittle data coupling and may expose "
                        "internal entity IDs in VCS history. "
                        f"Example: {uuids[0]}"
                    ),
                    suggestion   = (
                        "Move exclusion IDs to a dbt seed or source reference:\n"
                        "  where park_id not in (select park_id from "
                        "{{ ref('excluded_parks') }})"
                    ),
                    cwe = "CWE-547",
                ))

        return findings

    def _scan_python_content(self, py_file: dict, findings: List[Finding]):
        content = py_file.get("full_content", "")
        path    = py_file["path"]

        # Check for private key blocks embedded in Python files
        if _RE_PRIVATE_KEY_BLOCK.search(content):
            findings.append(Finding(
                check_id   = "SEC-006",
                title      = "Private key block embedded in Python source",
                severity   = Severity.CRITICAL,
                category   = Category.SECRETS,
                file       = path,
                description= "A PEM-format private key block is hardcoded in Python source.",
                suggestion = "Load key from environment variable or secret manager at runtime.",
                cwe        = "CWE-312",
            ))

        # Check for generic hardcoded secrets
        for match in _RE_GENERIC_SECRET.finditer(content):
            line_no = content[: match.start()].count("\n") + 1
            findings.append(Finding(
                check_id     = "SEC-007",
                title        = "Potential hardcoded credential in Python",
                severity     = Severity.HIGH,
                category     = Category.PYTHON_SECURITY,
                file         = path,
                line         = line_no,
                code_snippet = match.group(0)[:80],
                description  = f"Found what appears to be a hardcoded credential at line {line_no}.",
                suggestion   = "Replace with os.environ.get('SECRET_NAME') or a secret block.",
                cwe          = "CWE-798",
            ))
