"""
CrewAI orchestration — optional AI layer on top of static scan results.
Requires: pip install crewai langchain-community  +  ollama running locally.
"""
from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from langchain_community.llms import Ollama


@CrewBase
class DataEngineeringAuditCrew:
    """Three-agent crew: Security Auditor → Performance Optimizer → Lead dbt Engineer."""

    agents_config = "config/agents.yaml"
    tasks_config  = "config/tasks.yaml"

    def _llm(self, model: str = "llama3") -> Ollama:
        return Ollama(
            model      = model,
            base_url   = "http://localhost:11434",
            temperature= 0.1,     # Low → deterministic, repeatable findings
            num_ctx    = 8192,    # Expand for large SQL file content
            num_predict= 4096,    # Allow full code-rewrite responses
        )

    # ── Agents ─────────────────────────────────────────────────────────────

    @agent
    def security_auditor(self) -> Agent:
        return Agent(
            config          = self.agents_config["security_auditor"],
            llm             = self._llm("llama3"),
            verbose         = True,
            allow_delegation= False,
            max_iter        = 5,
        )

    @agent
    def performance_optimizer(self) -> Agent:
        return Agent(
            config          = self.agents_config["performance_optimizer"],
            llm             = self._llm("codellama:13b"),  # Better SQL rewrites
            verbose         = True,
            allow_delegation= False,
            max_iter        = 5,
        )

    @agent
    def lead_dbt_engineer(self) -> Agent:
        return Agent(
            config          = self.agents_config["lead_dbt_engineer"],
            llm             = self._llm("llama3"),
            verbose         = True,
            allow_delegation= True,   # Can delegate clarifications
            max_iter        = 8,
        )

    # ── Tasks ──────────────────────────────────────────────────────────────

    @task
    def scan_repository_structure(self) -> Task:
        return Task(config=self.tasks_config["scan_repository_structure"])

    @task
    def audit_credentials_and_secrets(self) -> Task:
        return Task(config=self.tasks_config["audit_credentials_and_secrets"])

    @task
    def audit_jinja_injection_risks(self) -> Task:
        return Task(config=self.tasks_config["audit_jinja_injection_risks"])

    @task
    def audit_snowflake_rbac(self) -> Task:
        return Task(config=self.tasks_config["audit_snowflake_rbac"])

    @task
    def analyze_sql_performance(self) -> Task:
        return Task(config=self.tasks_config["analyze_sql_performance"])

    @task
    def analyze_materialization_strategy(self) -> Task:
        return Task(config=self.tasks_config["analyze_materialization_strategy"])

    @task
    def audit_dbt_quality(self) -> Task:
        return Task(config=self.tasks_config["audit_dbt_quality"])

    @task
    def synthesize_final_report(self) -> Task:
        return Task(
            config      = self.tasks_config["synthesize_final_report"],
            output_file = "audit_report.md",
        )

    # ── Crew ───────────────────────────────────────────────────────────────

    @crew
    def crew(self) -> Crew:
        return Crew(
            agents  = self.agents,
            tasks   = self.tasks,
            process = Process.sequential,
            verbose = True,
            memory  = True,    # Shared cross-task memory for synthesis
            max_rpm = 10,      # Respect local Ollama rate limits
        )
