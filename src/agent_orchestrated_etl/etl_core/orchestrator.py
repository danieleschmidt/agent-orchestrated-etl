"""
ETLOrchestrator: coordinates multiple ETLAgents in a pipeline.

Responsibilities
----------------
- Instantiate ETLAgents from a PipelineConfig
- Chain stage outputs into the next stage's inputs
- Handle stage failures with configurable retries
- Emit a structured PipelineRun report

Example
-------
>>> config = PipelineConfig.from_yaml("pipeline.yaml")
>>> orch = ETLOrchestrator(config)
>>> report = orch.run()
>>> print(report.summary())
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .agent import AgentRole, AgentResult, ETLAgent
from .pipeline import PipelineConfig, PipelineStage

logger = logging.getLogger(__name__)


@dataclass
class StageRun:
    """Record of a single stage execution (possibly with retries)."""
    stage_name: str
    role: str
    attempts: int
    result: AgentResult


@dataclass
class PipelineRun:
    """Complete record of a pipeline execution."""
    pipeline_name: str
    started_at: float
    finished_at: float
    status: str                          # "success" | "partial" | "failure"
    stages: List[StageRun] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        return self.finished_at - self.started_at

    @property
    def total_records_processed(self) -> int:
        return sum(s.result.records_out for s in self.stages if s.result.status == "success")

    def summary(self) -> str:
        lines = [
            f"Pipeline : {self.pipeline_name}",
            f"Status   : {self.status.upper()}",
            f"Duration : {self.duration_seconds:.3f}s",
            f"Records  : {self.total_records_processed}",
            "",
            "Stages:",
        ]
        for sr in self.stages:
            r = sr.result
            badge = "✓" if r.status == "success" else "✗"
            lines.append(
                f"  {badge} [{sr.role:12s}] {sr.stage_name:20s}  "
                f"{r.records_in:>6} → {r.records_out:<6}  "
                f"({r.duration_seconds:.3f}s, {sr.attempts} attempt(s))"
                + (f"  ERROR: {r.error}" if r.error else "")
            )
        return "\n".join(lines)


class ETLOrchestrator:
    """
    Orchestrates a multi-stage ETL pipeline using ETLAgents.

    Parameters
    ----------
    config:
        A PipelineConfig describing the stages to run.
    agent_factory:
        Optional callable ``(stage: PipelineStage) -> ETLAgent``.
        If omitted, the default factory creates an ETLAgent per stage.
    """

    def __init__(
        self,
        config: PipelineConfig,
        agent_factory: Optional[Callable[[PipelineStage], ETLAgent]] = None,
    ):
        self.config = config
        self._agent_factory = agent_factory or self._default_agent_factory
        self._log = logging.getLogger(f"etl.orchestrator.{config.name}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, initial_inputs: Optional[Dict[str, Any]] = None) -> PipelineRun:
        """
        Execute all pipeline stages in order.

        Parameters
        ----------
        initial_inputs:
            Extra key/value pairs merged into the first stage's inputs.
            Useful for runtime overrides (e.g. ``{"source": "live.csv"}``).

        Returns
        -------
        PipelineRun
            Full execution record including per-stage results.
        """
        started_at = time.time()
        stage_runs: List[StageRun] = []
        current_data: Any = None
        overall_status = "success"

        self._log.info(
            "Starting pipeline '%s' with %d stage(s)",
            self.config.name, len(self.config.stages),
        )

        for stage in self.config.stages:
            inputs = self._build_inputs(stage, current_data, initial_inputs)
            stage_run, current_data = self._run_stage(stage, inputs)
            stage_runs.append(stage_run)

            if stage_run.result.status != "success":
                overall_status = "failure"
                if self.config.stop_on_failure:
                    self._log.error(
                        "Stage '%s' failed — stopping pipeline (stop_on_failure=True)",
                        stage.name,
                    )
                    break
                else:
                    overall_status = "partial"
                    self._log.warning(
                        "Stage '%s' failed — continuing (stop_on_failure=False)",
                        stage.name,
                    )
                    current_data = None  # next stage gets no data from failed stage

        finished_at = time.time()
        run = PipelineRun(
            pipeline_name=self.config.name,
            started_at=started_at,
            finished_at=finished_at,
            status=overall_status,
            stages=stage_runs,
            metadata=self.config.metadata,
        )
        self._log.info("Pipeline '%s' finished: %s", self.config.name, overall_status)
        return run

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_stage(
        self, stage: PipelineStage, inputs: Dict[str, Any]
    ) -> tuple[StageRun, Any]:
        """Run a single stage with retry logic."""
        agent = self._agent_factory(stage)
        max_attempts = self.config.max_retries + 1
        last_result: Optional[AgentResult] = None

        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                wait = 2 ** (attempt - 2)   # 1s, 2s, 4s, …
                self._log.warning(
                    "Retrying stage '%s' (attempt %d/%d) in %ds",
                    stage.name, attempt, max_attempts, wait,
                )
                time.sleep(wait)

            last_result = agent.run(inputs)

            if last_result.status == "success":
                break

        assert last_result is not None
        sr = StageRun(
            stage_name=stage.name,
            role=stage.role,
            attempts=attempt,
            result=last_result,
        )
        return sr, last_result.data if last_result.status == "success" else None

    def _build_inputs(
        self,
        stage: PipelineStage,
        current_data: Any,
        initial_inputs: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Assemble the input dict for a stage:
        - start with the stage's own config
        - merge in any initial_inputs (for stage 0)
        - pipe in the previous stage's output as 'data'
        """
        inputs: Dict[str, Any] = dict(stage.config)

        if initial_inputs:
            inputs.update(initial_inputs)
            initial_inputs = None   # only apply to first stage

        if current_data is not None:
            inputs["data"] = current_data

        return inputs

    @staticmethod
    def _default_agent_factory(stage: PipelineStage) -> ETLAgent:
        """Create a plain ETLAgent from a PipelineStage."""
        try:
            role = AgentRole(stage.role)
        except ValueError:
            raise ValueError(
                f"Unknown agent role '{stage.role}' in stage '{stage.name}'. "
                f"Valid roles: {[r.value for r in AgentRole]}"
            )
        return ETLAgent(role=role, name=stage.name, config=stage.config)
