"""
Scorer — ranks findings into priority tiers and calculates a risk score.

Priority tiers:
  P0 (CRITICAL)  → Block deploy / pre-commit HARD STOP
  P1 (HIGH)      → Fix within current sprint
  P2 (MEDIUM)    → Fix within current quarter
  P3 (LOW/INFO)  → Address in tech debt backlog

Risk score: 0–100 (weighted sum of findings, capped at 100)
"""
from typing import List, Dict, Any
from scanners.base import Finding, Severity

_WEIGHTS = {
    Severity.CRITICAL: 25,
    Severity.HIGH:     10,
    Severity.MEDIUM:    3,
    Severity.LOW:       1,
    Severity.INFO:      0,
}

_PRIORITY = {
    Severity.CRITICAL: "P0",
    Severity.HIGH:     "P1",
    Severity.MEDIUM:   "P2",
    Severity.LOW:      "P3",
    Severity.INFO:     "P3",
}


def score_findings(findings: List[Finding]) -> Dict[str, Any]:
    """
    Returns a dict with:
      - 'by_priority': {P0: [...], P1: [...], P2: [...], P3: [...]}
      - 'risk_score':  int 0-100
      - 'risk_label':  CRITICAL / HIGH / MEDIUM / LOW
      - 'summary':     dict with counts per severity
      - 'sorted':      all findings sorted P0→P3
    """
    buckets: Dict[str, List[Finding]] = {"P0": [], "P1": [], "P2": [], "P3": []}
    raw_score = 0

    for f in findings:
        priority = _PRIORITY.get(f.severity, "P3")
        buckets[priority].append(f)
        raw_score += _WEIGHTS.get(f.severity, 0)

    risk_score = min(100, raw_score)

    if risk_score >= 50:
        risk_label = "CRITICAL"
    elif risk_score >= 25:
        risk_label = "HIGH"
    elif risk_score >= 10:
        risk_label = "MEDIUM"
    else:
        risk_label = "LOW"

    summary = {sev.value: 0 for sev in Severity}
    for f in findings:
        summary[f.severity.value] += 1

    sorted_findings = (
        buckets["P0"] + buckets["P1"] + buckets["P2"] + buckets["P3"]
    )

    return {
        "by_priority": buckets,
        "risk_score":  risk_score,
        "risk_label":  risk_label,
        "summary":     summary,
        "sorted":      sorted_findings,
        "total":       len(findings),
    }


def has_blocking_findings(scored: Dict[str, Any]) -> bool:
    """True if any P0 (CRITICAL) findings exist — used by pre-commit hook."""
    return len(scored["by_priority"]["P0"]) > 0
