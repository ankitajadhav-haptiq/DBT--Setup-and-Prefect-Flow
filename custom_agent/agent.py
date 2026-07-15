"""
Custom ReAct Agent — pure Python, no frameworks.

ReAct = Reasoning + Acting
Reference: Yao et al., 2022 (https://arxiv.org/abs/2210.03629)

The agent loop:
  1. Build a prompt: system prompt + task + memory context
  2. LLM generates:  Thought → Action → Action Input
  3. Agent executes the tool → gets Observation
  4. Observation is appended to the conversation
  5. Repeat until LLM writes FINAL ANSWER

The conversation grows step by step:
  [system prompt]
  Task: ...
  Thought: I should list staged files first.
  Action: list_staged_files
  Action Input:
  Observation: vision_dbt/macros/monthly_exchange_rates.sql
  Thought: I need to scan that SQL file.
  Action: scan_sql_file
  Action Input: vision_dbt/macros/monthly_exchange_rates.sql
  Observation: [HIGH] Jinja injection risk ...
  Thought: I have enough information.
  FINAL ANSWER: ## Code Review ...
"""
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .llm    import OllamaLLM, RuleBasedLLM, build_llm
from .memory import AgentMemory
from .prompts import build_prompt
from .tools  import Tool, ALL_TOOLS


# ── Result dataclass ───────────────────────────────────────────────────────

@dataclass
class AgentStep:
    step_number: int
    thought:     str
    action:      str
    action_input: str
    observation: str
    elapsed_ms:  int


@dataclass
class AgentResult:
    output:       str                 # The FINAL ANSWER Markdown text
    steps:        List[AgentStep]     # Trace of reasoning steps
    mode:         str                 # "react" or "static"
    tool_calls:   List[str]           # List of tool names called
    verdict:      str = "PASS"        # BLOCK / WARN / PASS (parsed from output)
    elapsed_s:    float = 0.0
    html_content: str = ""            # Artifact-style HTML report (populated by static mode)
    raw_findings: list = field(default_factory=list)  # Flat Finding list (populated by static mode)

    def has_critical(self) -> bool:
        return "CRITICAL" in self.output.upper() or self.verdict == "BLOCK"

    def has_high(self) -> bool:
        return "HIGH" in self.output.upper() or self.verdict in ("BLOCK", "WARN")

    def print_trace(self):
        """Print step-by-step reasoning for debugging."""
        for s in self.steps:
            print(f"\n── Step {s.step_number} ({s.elapsed_ms}ms) ──────────────")
            print(f"Thought: {s.thought[:200]}")
            print(f"Action:  {s.action}({s.action_input[:100]})")
            print(f"Result:  {s.observation[:300]}")


# ── Agent ──────────────────────────────────────────────────────────────────

class CustomAgent:
    """
    A minimal ReAct agent you can configure and extend.

    Usage:
        agent = CustomAgent(name="reviewer", tools=ALL_TOOLS)
        result = agent.run("Review the staged changes for security issues.")
        print(result.output)
    """

    def __init__(
        self,
        name:      str             = "code-review-agent",
        tools:     List[Tool]      = None,
        llm        = None,
        memory:    AgentMemory     = None,
        max_steps: int             = 10,
        trigger:   str             = "manual",
        verbose:   bool            = True,
    ):
        self.name      = name
        self.tools     = {t.name: t for t in (tools or ALL_TOOLS)}
        self.llm       = llm or build_llm()
        self.memory    = memory or AgentMemory()
        self.max_steps = max_steps
        self.trigger   = trigger
        self.verbose   = verbose

    # ── Public API ──────────────────────────────────────────────────────────

    def run(self, task: str) -> AgentResult:
        """
        Main entry point. Runs the agent on a task and returns a result.
        Automatically chooses ReAct (LLM) or static (rule-based) mode.
        """
        t0 = time.time()

        if self.verbose:
            print(f"\n[{self.name}] Starting — mode: {self._mode_label()}")
            print(f"[{self.name}] Task: {task}\n")

        if isinstance(self.llm, RuleBasedLLM):
            result = self._run_static(task)
        else:
            result = self._run_react(task)

        result.elapsed_s = round(time.time() - t0, 2)
        result.verdict   = self._extract_verdict(result.output)

        if self.verbose:
            print(f"\n[{self.name}] Done in {result.elapsed_s}s — "
                  f"mode={result.mode}, steps={len(result.steps)}, "
                  f"verdict={result.verdict}")

        return result

    # ── ReAct loop ──────────────────────────────────────────────────────────

    def _run_react(self, task: str) -> AgentResult:
        steps:        List[AgentStep] = []
        tool_calls:   List[str]       = []
        conversation: str             = build_prompt(
            task, list(self.tools.values()),
            self.memory.get_context_for_prompt(),
            self.trigger,
        )

        for step_num in range(1, self.max_steps + 1):
            t0 = time.time()

            # ── LLM generates Thought + Action + Action Input ──────────────
            # Stop at "Observation:" so the agent pauses and we inject the result
            response = self.llm.generate(conversation, stop=["Observation:"])
            elapsed  = int((time.time() - t0) * 1000)

            if self.verbose:
                print(f"  Step {step_num}: {response[:120].strip()}")

            # ── Check for FINAL ANSWER ──────────────────────────────────────
            if "FINAL ANSWER:" in response:
                final = response.split("FINAL ANSWER:", 1)[1].strip()
                return AgentResult(output=final, steps=steps, mode="react", tool_calls=tool_calls)

            # ── Parse Action + Action Input ─────────────────────────────────
            try:
                action, action_input = self._parse_action(response)
            except ValueError as e:
                # Ask the model to try again with the correct format
                observation = (
                    f"Format error: {e}. "
                    "Respond with EXACTLY:\nThought: ...\nAction: <tool_name>\nAction Input: <input>"
                )
                conversation += response + f"\nObservation: {observation}\n"
                continue

            # ── Execute tool ────────────────────────────────────────────────
            if action in self.tools:
                observation = self.tools[action].run(action_input)
                tool_calls.append(action)
            else:
                observation = (
                    f"Tool '{action}' not found. "
                    f"Available: {', '.join(self.tools.keys())}"
                )

            if self.verbose:
                print(f"         → {action}({action_input[:60]!r})")
                print(f"         ← {observation[:120]!r}")

            # ── Append to conversation ──────────────────────────────────────
            conversation += response + f"\nObservation: {observation}\n"

            steps.append(AgentStep(
                step_number  = step_num,
                thought      = self._extract_thought(response),
                action       = action,
                action_input = action_input,
                observation  = observation[:500],
                elapsed_ms   = elapsed,
            ))

        # Max steps reached — force a final answer
        conversation += (
            "\nThought: I have reached the maximum number of steps. "
            "I will summarize what I found.\nFINAL ANSWER:\n"
        )
        final = self.llm.generate(conversation)
        return AgentResult(output=final, steps=steps, mode="react", tool_calls=tool_calls)

    # ── Static (rule-based) mode ────────────────────────────────────────────

    def _run_static(self, task: str, files: List[str] = None, mode_override: str = None) -> AgentResult:
        """
        Runs all scanners in sequence, prints structured terminal output,
        and returns an AgentResult with a Markdown report as output.

        files: explicit list of paths to scan. If None, uses staged files.
        """
        from .reporter import scan_path, scan_secrets, print_report, build_markdown, print_progress
        from audit_agent.scanners.base import Finding

        steps:      List[AgentStep] = []
        tool_calls: List[str]       = []
        full_repo_mode = files is not None

        # ── Determine file list ────────────────────────────────────────────────
        if full_repo_mode:
            # CI passes every changed/added file — scan_path() falls back to a
            # generic secrets-only check for extensions with no dedicated scanner.
            scan_files = list(files)
        else:
            staged_raw = self.tools["list_staged_files"].run("")
            tool_calls.append("list_staged_files")
            scan_files = [
                line.strip() for line in staged_raw.splitlines()
                if line.strip() and not line.startswith("No ")
                and line.strip().endswith((".sql", ".py"))
            ]

        if self.verbose and not full_repo_mode and not scan_files:
            print("  No staged SQL or Python files to review.")

        # ── Scan each file ─────────────────────────────────────────────────────
        file_findings: List[tuple] = []
        total = len(scan_files)

        for idx, fp in enumerate(scan_files, 1):
            if self.verbose:
                print_progress(fp, idx, total)

            t0       = time.time()
            findings: List[Finding] = scan_path(fp)
            elapsed  = int((time.time() - t0) * 1000)

            file_findings.append((fp, findings))
            action_name = self._scan_action_name(fp)
            tool_calls.append(action_name)

            steps.append(AgentStep(
                step_number  = len(steps) + 1,
                thought      = f"Scan {fp}",
                action       = action_name,
                action_input = fp,
                observation  = f"{len(findings)} finding(s)",
                elapsed_ms   = elapsed,
            ))

        # ── Secrets scan ───────────────────────────────────────────────────────
        secret_findings: List[Finding] = scan_secrets(".")
        tool_calls.append("check_secrets")

        # ── Print structured terminal output ───────────────────────────────────
        mode_label = mode_override or ("full-repo" if full_repo_mode else "static")
        if self.verbose:
            verdict = print_report(
                file_findings   = file_findings,
                secret_findings = secret_findings,
                trigger         = self.trigger,
                mode            = mode_label,
                total_scanned   = total,
            )
        else:
            from .reporter import _derive_verdict, _count_by_sev
            from audit_agent.scanners.base import Severity
            all_f = [f for _, fs in file_findings for f in fs] + secret_findings
            counts = {s.value: 0 for s in Severity}
            for f in all_f:
                counts[f.severity.value] += 1
            verdict = _derive_verdict(counts)

        # ── Build Markdown for file output ─────────────────────────────────────
        markdown = build_markdown(
            file_findings   = file_findings,
            secret_findings = secret_findings,
            trigger         = self.trigger,
            mode            = mode_label,
            total_scanned   = total,
            verdict         = verdict,
        )

        # ── Build artifact-style HTML report ───────────────────────────────────
        from .reporter import build_html
        html = build_html(
            file_findings   = file_findings,
            secret_findings = secret_findings,
            trigger         = self.trigger,
            mode            = mode_label,
            total_scanned   = total,
            verdict         = verdict,
        )

        all_findings = [f for _, fs in file_findings for f in fs] + secret_findings

        return AgentResult(
            output       = markdown,
            steps        = steps,
            mode         = "static",
            tool_calls   = tool_calls,
            verdict      = verdict,
            html_content = html,
            raw_findings = all_findings,
        )

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _scan_action_name(fp: str) -> str:
        if fp.endswith(".sql"):
            return "scan_sql_file"
        if fp.endswith(".py"):
            return "scan_python_file"
        return "scan_generic_file"

    def _parse_action(self, text: str):
        """Extract (action, action_input) from LLM response."""
        action_match = re.search(r"Action:\s*(\w+)", text)
        input_match  = re.search(r"Action Input:\s*(.*?)(?=\n(?:Thought|Action|FINAL)|$)", text, re.DOTALL)

        if not action_match:
            raise ValueError("No 'Action:' found in response")

        action       = action_match.group(1).strip()
        action_input = input_match.group(1).strip() if input_match else ""
        return action, action_input

    @staticmethod
    def _extract_thought(text: str) -> str:
        m = re.search(r"Thought:\s*(.*?)(?=\nAction:|$)", text, re.DOTALL)
        return m.group(1).strip()[:300] if m else ""

    @staticmethod
    def _extract_verdict(output: str) -> str:
        if "**BLOCK**" in output or "BLOCK" in output[:500]:
            return "BLOCK"
        if "**WARN**" in output or "WARN" in output[:500]:
            return "WARN"
        return "PASS"

    def _mode_label(self) -> str:
        if isinstance(self.llm, RuleBasedLLM):
            return "static (no LLM)"
        return f"react ({getattr(self.llm, 'model', 'unknown')})"
