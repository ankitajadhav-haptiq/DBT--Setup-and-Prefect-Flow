"""
File-based persistent memory for the custom agent.

The agent reads memory at the start of each run so it can reference:
  - Past findings (what issues keep recurring)
  - Files with repeated problems (where to look first)
  - False positives the user dismissed
  - Baseline risk score to detect regressions

Memory files are JSON, stored in custom_agent/memory/.
All paths are relative to the repo root.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


_MEMORY_DIR = Path(__file__).parent / "memory"
_MEMORY_DIR.mkdir(exist_ok=True)


class AgentMemory:

    def __init__(self, memory_dir: Path = _MEMORY_DIR):
        self.dir = Path(memory_dir)
        self.dir.mkdir(parents=True, exist_ok=True)

        self._findings_file    = self.dir / "past_findings.json"
        self._baseline_file    = self.dir / "baseline.json"
        self._dismissed_file   = self.dir / "dismissed.json"
        self._hot_files_file   = self.dir / "hot_files.json"

        # In-memory cache for the current session
        self._past_findings:  List[Dict]       = self._load(self._findings_file, [])
        self._baseline:       Dict             = self._load(self._baseline_file, {})
        self._dismissed:      List[str]        = self._load(self._dismissed_file, [])
        self._hot_files:      Dict[str, int]   = self._load(self._hot_files_file, {})

    # ── Read API ───────────────────────────────────────────────────────────

    def get_context_for_prompt(self) -> str:
        """
        Returns a concise text block injected into the agent's system prompt
        so it knows what was found in previous runs.
        """
        lines = []

        if self._baseline:
            lines.append(
                f"Baseline risk score: {self._baseline.get('risk_score', 'unknown')}/100 "
                f"(measured {self._baseline.get('date', 'unknown')})"
            )

        if self._hot_files:
            top = sorted(self._hot_files.items(), key=lambda x: -x[1])[:5]
            lines.append("Files with repeated issues: " +
                         ", ".join(f"{f} ({n}×)" for f, n in top))

        if self._dismissed:
            lines.append(f"User-dismissed check IDs (ignore): {', '.join(self._dismissed[:10])}")

        recent = self._past_findings[-3:] if self._past_findings else []
        if recent:
            lines.append("Recent run summaries:")
            for r in recent:
                lines.append(
                    f"  [{r.get('date','')}] {r.get('critical',0)} critical, "
                    f"{r.get('high',0)} high, {r.get('total',0)} total"
                )

        return "\n".join(lines) if lines else "No prior run data."

    # ── Write API ──────────────────────────────────────────────────────────

    def record_run(self, risk_score: int, findings_summary: Dict[str, int], files: List[str]):
        """Call this after every agent run to persist the outcome."""
        entry = {
            "date":     datetime.now().strftime("%Y-%m-%d %H:%M"),
            "risk_score": risk_score,
            "critical": findings_summary.get("CRITICAL", 0),
            "high":     findings_summary.get("HIGH",     0),
            "medium":   findings_summary.get("MEDIUM",   0),
            "low":      findings_summary.get("LOW",      0),
            "total":    sum(findings_summary.values()),
        }

        self._past_findings.append(entry)
        # Keep only last 50 runs
        self._past_findings = self._past_findings[-50:]
        self._save(self._findings_file, self._past_findings)

        # Update hot-files counter
        for f in files:
            self._hot_files[f] = self._hot_files.get(f, 0) + 1
        self._save(self._hot_files_file, self._hot_files)

        # Set baseline if none exists
        if not self._baseline:
            self.set_baseline(risk_score)

    def set_baseline(self, risk_score: int):
        self._baseline = {
            "risk_score": risk_score,
            "date":       datetime.now().strftime("%Y-%m-%d"),
        }
        self._save(self._baseline_file, self._baseline)

    def dismiss(self, check_id: str):
        """Mark a check_id as a false positive — agent will ignore it in future runs."""
        if check_id not in self._dismissed:
            self._dismissed.append(check_id)
            self._save(self._dismissed_file, self._dismissed)
            print(f"  [memory] Dismissed: {check_id}")

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _load(path: Path, default: Any) -> Any:
        if path.exists():
            try:
                return json.loads(path.read_text())
            except json.JSONDecodeError:
                pass
        return default

    @staticmethod
    def _save(path: Path, data: Any):
        path.write_text(json.dumps(data, indent=2))
