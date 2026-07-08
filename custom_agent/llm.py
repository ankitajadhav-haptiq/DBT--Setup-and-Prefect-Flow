"""
LLM backends for the custom agent.

OllamaLLM  — calls a local Ollama server (llama3, mistral, codellama, etc.)
RuleBasedLLM — zero-AI fallback; formats scanner output as agent output.
               This ensures git hooks work even when Ollama is not running.
"""
import json
import re
from typing import List, Optional

try:
    import requests
    _REQUESTS_OK = True
except ImportError:
    _REQUESTS_OK = False


class OllamaLLM:
    """
    Thin wrapper around the Ollama /api/generate endpoint.

    The agent calls:
        text = llm.generate(prompt, stop=["Observation:"])

    stop tokens tell the model to pause so the agent can inject the tool
    observation before asking the model to continue reasoning.
    """

    def __init__(
        self,
        model:       str   = "llama3",
        base_url:    str   = "http://localhost:11434",
        temperature: float = 0.1,
        num_ctx:     int   = 8192,
        num_predict: int   = 2048,
    ):
        self.model       = model
        self.base_url    = base_url.rstrip("/")
        self.temperature = temperature
        self.num_ctx     = num_ctx
        self.num_predict = num_predict

    def is_available(self) -> bool:
        if not _REQUESTS_OK:
            return False
        try:
            r = requests.get(f"{self.base_url}/api/tags", timeout=2)
            return r.status_code == 200
        except Exception:
            return False

    def generate(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        if not _REQUESTS_OK:
            raise RuntimeError("requests not installed: pip install requests")

        payload = {
            "model":       self.model,
            "prompt":      prompt,
            "temperature": self.temperature,
            "num_ctx":     self.num_ctx,
            "num_predict": self.num_predict,
            "stream":      False,
        }
        if stop:
            payload["stop"] = stop

        try:
            r = requests.post(
                f"{self.base_url}/api/generate",
                json    = payload,
                timeout = 300,
            )
            r.raise_for_status()
            return r.json().get("response", "")
        except requests.RequestException as e:
            raise RuntimeError(f"Ollama request failed: {e}") from e

    def __repr__(self) -> str:
        return f"OllamaLLM(model={self.model!r}, url={self.base_url!r})"


class RuleBasedLLM:
    """
    Zero-AI fallback that makes the agent work without any LLM.

    Instead of calling a language model, it looks for structured
    data injected into the prompt by the tools and formats it into
    a FINAL ANSWER block.  Git hooks stay fast and functional even
    without Ollama installed.
    """

    def is_available(self) -> bool:
        return True   # always works

    def generate(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        # Extract any observation blocks already in the prompt
        observations = re.findall(
            r"Observation:\s*(.*?)(?=\nThought:|$)",
            prompt,
            re.DOTALL,
        )

        if not observations:
            # First call — tell the agent to run its tools
            return (
                "Thought: I need to gather information about the changed files.\n"
                "Action: list_staged_files\n"
                "Action Input: \n"
            )

        # Enough observations — produce a final answer
        summary_lines = ["## Static Analysis Summary\n"]
        for obs in observations:
            obs = obs.strip()
            if obs and len(obs) > 20:
                summary_lines.append(obs[:2000])

        return "Thought: I have collected all scanner results.\nFINAL ANSWER:\n" + "\n\n---\n\n".join(summary_lines)

    def __repr__(self) -> str:
        return "RuleBasedLLM(no-AI fallback)"


def build_llm(model: str = "llama3", prefer_ollama: bool = True) -> object:
    """
    Return the best available LLM backend.
    Falls back to RuleBasedLLM if Ollama is unreachable.
    """
    if prefer_ollama:
        llm = OllamaLLM(model=model)
        if llm.is_available():
            print(f"  [LLM] Using Ollama — model: {model}")
            return llm
        print("  [LLM] Ollama not reachable — using rule-based fallback")

    return RuleBasedLLM()
