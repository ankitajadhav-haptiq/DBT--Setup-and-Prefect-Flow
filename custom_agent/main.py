#!/usr/bin/env python3
"""
Custom Agent CLI

Usage:
  python custom_agent/main.py                       # review staged changes
  python custom_agent/main.py --task "explain complexity of photo_revenue_report_dbt.sql"
  python custom_agent/main.py --mode react          # use Ollama LLM
  python custom_agent/main.py --mode static         # rule-based, no LLM
  python custom_agent/main.py --model codellama:13b # choose Ollama model
  python custom_agent/main.py --trace               # print step-by-step reasoning
  python custom_agent/main.py --dismiss PY-C002     # mark a check as false positive
  python custom_agent/main.py --output report.md    # save output to file

Exit codes:
  0 — PASS (no critical or high issues)
  1 — BLOCK or WARN (critical/high issues found — used by pre-commit hook)
  2 — Error during run
"""
import argparse
import sys
from pathlib import Path

_HERE = Path(__file__).parent
_REPO = _HERE.parent
sys.path.insert(0, str(_REPO))

from custom_agent.agent  import CustomAgent
from custom_agent.memory import AgentMemory
from custom_agent.tools  import ALL_TOOLS
from custom_agent.llm    import build_llm, OllamaLLM, RuleBasedLLM


def _run_full_repo(agent, args):
    """Collect all repo files matching filters, then run the static scanner over them."""
    # --files takes precedence over full-repo discovery
    if getattr(args, "files", None):
        all_files = [f for f in args.files if f.strip()]
    else:
        from custom_agent.tools import _list_all_repo_files
        raw = _list_all_repo_files(args.ext)
        all_files = [f.strip() for f in raw.splitlines() if f.strip() and not f.startswith("No ")]

        # Domain filter
        if args.domain:
            domain_upper = args.domain.upper()
            all_files = [f for f in all_files if domain_upper in f.upper()]
            if not all_files:
                print(f"[warn] No files matched domain '{args.domain}'")

    if not all_files:
        from custom_agent.agent import AgentResult
        return AgentResult(
            output   = "## Code Review\nNo files found matching the given filters.",
            steps    = [],
            mode     = "static",
            tool_calls = [],
            verdict  = "PASS",
        )

    if not args.ci:
        ext_label = args.ext
        domain_label = f" domain={args.domain}" if args.domain else ""
        print(f"[agent] Full-repo scan: {len(all_files)} files ({ext_label}{domain_label})")

    # Override default task for full-repo context
    task = args.task
    if "staged" in task.lower():
        task = f"Scan all {len(all_files)} repository files for security vulnerabilities, time/space complexity issues, and code quality problems."

    return agent._run_static(task, files=all_files)


def main():
    parser = argparse.ArgumentParser(
        description="Custom ReAct Agent — security, complexity & quality review",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--task",    default="Review all staged changes for security vulnerabilities, "
                             "time/space complexity issues, and code quality problems.",
        help="Task description to give the agent",
    )
    parser.add_argument("--mode",    default="auto", choices=["auto", "react", "static"],
                        help="auto=prefer Ollama, react=force LLM, static=rule-based")
    parser.add_argument("--model",   default="llama3", help="Ollama model name")
    parser.add_argument("--output",  default=None,     help="Write Markdown report to this file")
    parser.add_argument("--trace",   action="store_true", help="Print reasoning steps")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--dismiss", metavar="CHECK_ID",  help="Dismiss a check as false positive")
    parser.add_argument("--ci",        action="store_true", help="CI mode: minimal output, use exit codes")
    parser.add_argument("--open",      action="store_true", help="Auto-open HTML report in browser after scan")
    parser.add_argument("--full-repo", action="store_true", dest="full_repo",
                        help="Scan the entire repo, not just staged files")
    parser.add_argument("--files", nargs="*", metavar="FILE",
                        help="Explicit list of files to scan (implies --full-repo mode, skips discovery)")
    parser.add_argument("--domain",    default=None,
                        help="Filter by domain when using --full-repo: GOPOD, PHOTO, UNIFI, macros")
    parser.add_argument("--ext",       default=".sql,.py",
                        help="File extensions to include (default: .sql,.py)")
    args = parser.parse_args()

    memory = AgentMemory()

    # Handle dismiss command
    if args.dismiss:
        memory.dismiss(args.dismiss)
        print(f"Dismissed {args.dismiss}. It will be ignored in future runs.")
        return

    # Build LLM
    if args.mode == "static":
        llm = RuleBasedLLM()
    elif args.mode == "react":
        llm = OllamaLLM(model=args.model)
        if not llm.is_available():
            print(f"[warn] Ollama not available at localhost:11434 — falling back to static mode")
            llm = RuleBasedLLM()
    else:
        llm = build_llm(model=args.model)

    # Run agent
    agent = CustomAgent(
        tools   = ALL_TOOLS,
        llm     = llm,
        memory  = memory,
        verbose = args.verbose or not args.ci,
    )

    try:
        if args.full_repo or args.files:
            result = _run_full_repo(agent, args)
        else:
            result = agent.run(args.task)
    except KeyboardInterrupt:
        print("\n[interrupted]")
        sys.exit(2)
    except Exception as e:
        print(f"[error] Agent failed: {e}")
        if args.verbose:
            import traceback; traceback.print_exc()
        sys.exit(2)

    # Print reasoning trace if requested
    if args.trace:
        result.print_trace()

    # Save report — .html gets the artifact-style visual report, anything else gets Markdown
    output_path = args.output or "agent_report.md"
    if output_path.endswith(".html") and result.html_content:
        Path(output_path).write_text(result.html_content, encoding="utf-8")
    else:
        Path(output_path).write_text(result.output, encoding="utf-8")
    if not args.ci:
        print(f"  Report saved → {output_path}")
    if getattr(args, "open", False) and output_path.endswith(".html"):
        import webbrowser, os
        webbrowser.open("file://" + os.path.abspath(output_path))

    # Record in memory
    summary = {}
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        summary[sev] = result.output.count(sev)

    changed_files = [
        s.action_input for s in result.steps
        if s.action in ("scan_sql_file", "scan_python_file")
    ]
    memory.record_run(
        risk_score       = 100 if result.has_critical() else (50 if result.has_high() else 10),
        findings_summary = summary,
        files            = changed_files,
    )

    # CI / hook exit code
    verdict = result.verdict
    if verdict == "BLOCK":
        if args.ci:
            print(f"BLOCK — {summary.get('CRITICAL',0)} CRITICAL, {summary.get('HIGH',0)} HIGH")
        sys.exit(1)
    elif verdict == "WARN":
        if args.ci:
            print(f"WARN — {summary.get('HIGH',0)} HIGH findings")
        sys.exit(0)
    else:
        if args.ci:
            print("PASS")
        sys.exit(0)


if __name__ == "__main__":
    main()
