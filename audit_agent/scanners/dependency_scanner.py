"""
Dependency Scanner — checks installed packages for known CVEs via pip-audit.
Runs pip-audit as a subprocess and parses its JSON output.
Falls back gracefully if pip-audit is not installed.
"""
import json
import subprocess
import shutil
from typing import List

from .base import BaseScanner, Finding, Severity, Category


class DependencyScanner(BaseScanner):
    name = "dependency"

    def scan(self, manifest: dict) -> List[Finding]:
        findings: List[Finding] = []

        if not shutil.which("pip-audit"):
            findings.append(Finding(
                check_id   = "DEP-000",
                title      = "pip-audit not installed — dependency scan skipped",
                severity   = Severity.INFO,
                category   = Category.DEPENDENCY,
                file       = "requirements.txt",
                description= "Install pip-audit to enable CVE scanning: pip install pip-audit",
                suggestion = "pip install pip-audit",
            ))
            return findings

        # Run pip-audit against the environment
        try:
            result = subprocess.run(
                ["pip-audit", "--format", "json", "--skip-editable"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode not in (0, 1):
                findings.append(Finding(
                    check_id   = "DEP-000",
                    title      = "pip-audit failed to run",
                    severity   = Severity.INFO,
                    category   = Category.DEPENDENCY,
                    file       = "requirements.txt",
                    description= f"pip-audit stderr: {result.stderr[:300]}",
                    suggestion = "Ensure pip-audit is installed and the virtualenv is activated.",
                ))
                return findings

            data = json.loads(result.stdout or "{}")
            # pip-audit >=2.x wraps results as {"dependencies": [...]}; older
            # versions returned a bare list — handle both without assuming one.
            dependencies = data.get("dependencies", []) if isinstance(data, dict) else data
            for vuln in dependencies:
                pkg   = vuln.get("name", "unknown")
                ver   = vuln.get("version", "?")
                vulns = vuln.get("vulns", [])
                for v in vulns:
                    cve_id = v.get("id", "unknown")
                    desc   = v.get("description", "")
                    fix    = v.get("fix_versions", [])
                    findings.append(Finding(
                        check_id   = f"DEP-{cve_id}",
                        title      = f"Vulnerable dependency: {pkg}=={ver} ({cve_id})",
                        severity   = Severity.HIGH,
                        category   = Category.DEPENDENCY,
                        file       = "requirements.txt",
                        description= desc[:400],
                        suggestion = (
                            f"Upgrade {pkg} to {fix[0] if fix else 'the latest patched version'}:\n"
                            f"  pip install '{pkg}>={fix[0]}'" if fix else
                            f"  pip install --upgrade {pkg}"
                        ),
                        cwe = "CWE-1035",
                    ))

        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            findings.append(Finding(
                check_id   = "DEP-000",
                title      = f"Dependency scan error: {type(e).__name__}",
                severity   = Severity.INFO,
                category   = Category.DEPENDENCY,
                file       = "requirements.txt",
                description= str(e),
                suggestion = "Run pip-audit manually: pip-audit --format json",
            ))

        return findings
