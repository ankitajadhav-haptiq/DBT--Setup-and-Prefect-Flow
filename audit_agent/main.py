#!/usr/bin/env python3
"""
Audit Agent — Entry Point

Usage:
  python main.py <repo_path> [--mode static|full] [--output report.md]

Modes:
  static (default) — file parser + all scanners, no AI (~30 sec)
  full             — static + CrewAI agent report (~10-30 min with Ollama)

Exit codes:
  0  — clean (no CRITICAL findings)
  1  — CRITICAL findings found (used by git pre-commit hook to block commit)
  2  — error during scan
"""
import argparse
import json
import sys
from pathlib import Path

# Allow running from repo root OR from audit_agent/ directory
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

from file_parser import RepositoryParser
from scanners    import SecretsScanner, SQLScanner, PythonScanner, DependencyScanner
from scorer      import score_findings, has_blocking_findings
from reporter    import write_report


def _banner(repo_path: str, mode: str):
    print(f"\n{'─'*60}")
    print(f"  Audit Agent — dbt / Snowflake Repository Scanner")
    print(f"  Target : {repo_path}")
    print(f"  Mode   : {mode}")
    print(f"{'─'*60}\n")


def run_static_scan(repo_path: Path) -> dict:
    """Phase 1-3: parse → scan → score. Returns scored results dict."""

    # ── Phase 1: Parse ────────────────────────────────────────────────────
    print("[1/4] Parsing repository structure...")
    parser   = RepositoryParser(repo_path)
    manifest = parser.build_manifest()

    summary = manifest["summary"]
    print(f"      SQL files   : {summary['total_sql_files']}")
    print(f"      Python files: {summary['total_python_files']}")
    print(f"      Private keys: {len(summary['private_key_files'])}")
    print(f"      dbt domains : {list(manifest['dbt_domains'].keys())}")

    # Save manifest for debugging / AI agents
    workspace = Path("audit_workspace")
    workspace.mkdir(exist_ok=True)
    (workspace / "repo_manifest.json").write_text(
        json.dumps(manifest, indent=2, default=str)
    )

    # ── Phase 2: Scan ─────────────────────────────────────────────────────
    print("\n[2/4] Running scanners...")
    all_findings = []

    scanners = [
        SecretsScanner(),
        SQLScanner(),
        PythonScanner(),
        DependencyScanner(),
    ]

    for scanner in scanners:
        print(f"      → {scanner.name} scanner...")
        findings = scanner.scan(manifest)
        all_findings.extend(findings)
        print(f"        found {len(findings)} issue(s)")

    # ── Phase 3: Score ────────────────────────────────────────────────────
    print("\n[3/4] Scoring findings...")
    scored = score_findings(all_findings)

    buckets = scored["by_priority"]
    print(f"      P0 CRITICAL : {len(buckets['P0'])}")
    print(f"      P1 HIGH     : {len(buckets['P1'])}")
    print(f"      P2 MEDIUM   : {len(buckets['P2'])}")
    print(f"      P3 LOW/INFO : {len(buckets['P3'])}")
    print(f"      Risk Score  : {scored['risk_score']}/100 — {scored['risk_label']}")

    return scored, manifest


def run_full_scan(scored: dict, manifest: dict, repo_path: str, output: str):
    """Phase 4: run CrewAI agents to generate human-readable report."""
    try:
        from crew import DataEngineeringAuditCrew

        inputs = {
            "repo_path":          repo_path,
            "manifest_path":      str(Path("audit_workspace/repo_manifest.json").resolve()),
            "repo_manifest_json": json.dumps(manifest, indent=2, default=str),
            "dbt_project_name":   manifest.get("dbt_project", {}).get("name", "unknown"),
            "dbt_domains":        list(manifest.get("dbt_domains", {}).keys()),
            "risk_score":         str(scored["risk_score"]),
            "critical_count":     str(len(scored["by_priority"]["P0"])),
            "private_key_files":  manifest.get("summary", {}).get("private_key_files", []),
        }

        print("\n[4/4] Running AI agents (needs Ollama on localhost:11434)...")
        crew_instance = DataEngineeringAuditCrew()
        crew_instance.crew().kickoff(inputs=inputs)
        print(f"      AI report written to: {output}")

    except ImportError:
        print("      [SKIP] crewai not installed — writing static report instead")
        write_report(scored, repo_path, output)
    except Exception as e:
        print(f"      [WARN] AI agents failed ({e}) — writing static report")
        write_report(scored, repo_path, output)


def main():
    parser = argparse.ArgumentParser(description="Audit Agent — dbt/Snowflake scanner")
    parser.add_argument("repo_path",            help="Path to the git repository to audit")
    parser.add_argument("--mode",   default="static", choices=["static", "full"],
                        help="'static' = scanners only; 'full' = + AI agents")
    parser.add_argument("--output", default="audit_report.md",
                        help="Output Markdown report file path")
    parser.add_argument("--json",   action="store_true",
                        help="Also write findings.json to audit_workspace/")
    parser.add_argument("--ci",     action="store_true",
                        help="CI mode: exit 1 on CRITICAL, print summary only")
    args = parser.parse_args()

    repo = Path(args.repo_path)
    if not repo.exists():
        print(f"Error: '{repo}' does not exist.")
        sys.exit(2)

    _banner(str(repo.resolve()), args.mode)

    try:
        scored, manifest = run_static_scan(repo)
    except Exception as e:
        print(f"\nScan error: {e}")
        sys.exit(2)

    # Write findings JSON if requested
    if args.json:
        findings_path = Path("audit_workspace/findings.json")
        findings_path.write_text(
            json.dumps([f.to_dict() for f in scored["sorted"]], indent=2),
            encoding="utf-8",
        )
        print(f"\n      Findings JSON: {findings_path}")

    # AI or static report
    if args.mode == "full":
        run_full_scan(scored, manifest, str(repo.resolve()), args.output)
    else:
        print("\n[4/4] Writing static Markdown report...")
        write_report(scored, str(repo.resolve()), args.output)

    # Final summary
    blocking = has_blocking_findings(scored)
    print(f"\n{'─'*60}")
    if blocking:
        p0 = scored["by_priority"]["P0"]
        print(f"  ❌  {len(p0)} CRITICAL finding(s) — COMMIT BLOCKED")
        for f in p0:
            print(f"      [{f.check_id}] {f.file}: {f.title}")
    else:
        print("  ✅  No CRITICAL findings — safe to commit")
    print(f"  Report: {args.output}")
    print(f"{'─'*60}\n")

    sys.exit(1 if blocking else 0)


if __name__ == "__main__":
    main()
