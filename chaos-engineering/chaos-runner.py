#!/usr/bin/env python3
"""
Chaos Engineering Runner for Agent-Orchestrated-ETL

Automated chaos engineering test runner that executes predefined experiments
and generates comprehensive reports on system resilience.
"""

import asyncio
import json
import subprocess
import time
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import aiohttp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ExperimentResult:
    """Result of a chaos experiment."""
    name: str
    start_time: str
    end_time: str
    duration_seconds: float
    success: bool
    steady_state_before: bool
    steady_state_after: bool
    error_message: Optional[str]
    metrics: Dict[str, Any]


@dataclass
class ChaosReport:
    """Comprehensive chaos engineering report."""
    suite_name: str
    timestamp: str
    total_experiments: int
    successful_experiments: int
    failed_experiments: int
    overall_resilience_score: float
    experiments: List[ExperimentResult]
    recommendations: List[str]


class ChaosRunner:
    """Chaos engineering experiment runner."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.experiments_file = project_root / "chaos-engineering" / "chaos-experiments.yaml"
        self.reports_dir = project_root / "chaos-reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        # Configuration
        self.orchestrator_url = "http://localhost:8080"  # Default, can be overridden
        self.experiment_timeout = 600  # 10 minutes max per experiment
        
    def load_experiments(self) -> List[Dict[str, Any]]:
        """Load chaos experiments from YAML file."""
        try:
            with open(self.experiments_file, 'r') as f:
                content = f.read()
                # Parse multiple YAML documents
                experiments = list(yaml.safe_load_all(content))
                return [exp for exp in experiments if exp is not None]
        except Exception as e:
            logger.error(f"Failed to load experiments: {e}")
            return []
    
    async def check_steady_state(self, experiment: Dict[str, Any]) -> bool:
        """Check if system is in steady state according to experiment definition."""
        hypothesis = experiment.get("steady-state-hypothesis", {})
        probes = hypothesis.get("probes", [])
        
        logger.info(f"Checking steady state with {len(probes)} probes")
        
        for probe in probes:
            try:
                result = await self._execute_probe(probe)
                if not result:
                    logger.warning(f"Probe {probe['name']} failed steady state check")
                    return False
            except Exception as e:
                logger.error(f"Probe {probe['name']} error: {e}")
                return False
        
        return True
    
    async def _execute_probe(self, probe: Dict[str, Any]) -> bool:
        """Execute a probe and check its tolerance."""
        provider = probe.get("provider", {})
        tolerance = probe.get("tolerance", {})
        
        try:
            if provider.get("type") == "http":
                return await self._execute_http_probe(provider, tolerance)
            elif provider.get("type") == "process":
                return await self._execute_process_probe(provider, tolerance)
            else:
                logger.warning(f"Unknown probe provider type: {provider.get('type')}")
                return False
        except Exception as e:
            logger.error(f"Probe execution failed: {e}")
            return False
    
    async def _execute_http_probe(self, provider: Dict[str, Any], tolerance: Dict[str, Any]) -> bool:
        """Execute an HTTP probe."""
        url = provider.get("url")
        timeout = provider.get("timeout", 30)
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            try:
                async with session.get(url) as response:
                    if tolerance.get("type") == "status":
                        expected_status = tolerance.get("expect", 200)
                        return response.status == expected_status
                    
                    elif tolerance.get("type") == "range":
                        # For simplicity, assuming numeric response
                        text = await response.text()
                        try:
                            value = float(text.strip())
                            range_min, range_max = tolerance.get("range", [0, 100])
                            return range_min <= value <= range_max
                        except ValueError:
                            return False
                    
                    elif tolerance.get("type") == "jsonpath":
                        # Simplified JSONPath handling
                        data = await response.json()
                        path = tolerance.get("path", "")
                        expected = tolerance.get("expect")
                        
                        # Basic path parsing (supports simple $.field syntax)
                        if path.startswith("$."):
                            field = path[2:]
                            actual_value = data.get(field)
                            return actual_value == expected
                    
                    return True
            except Exception as e:
                logger.error(f"HTTP probe failed: {e}")
                return False
    
    async def _execute_process_probe(self, provider: Dict[str, Any], tolerance: Dict[str, Any]) -> bool:
        """Execute a process probe."""
        path = provider.get("path")
        arguments = provider.get("arguments", [])
        
        try:
            result = subprocess.run(
                [path] + arguments,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if tolerance.get("type") == "status":
                expected_status = tolerance.get("expect", 0)
                return result.returncode == expected_status
            
            elif tolerance.get("type") == "range":
                # Extract numeric value from stdout
                import re
                numbers = re.findall(r'\d+', result.stdout)
                if numbers:
                    value = int(numbers[0])
                    range_min, range_max = tolerance.get("range", [0, 100])
                    return range_min <= value <= range_max
            
            return result.returncode == 0
            
        except subprocess.TimeoutExpired:
            logger.error("Process probe timed out")
            return False
        except Exception as e:
            logger.error(f"Process probe failed: {e}")
            return False
    
    async def execute_actions(self, actions: List[Dict[str, Any]]) -> bool:
        """Execute chaos experiment actions."""
        logger.info(f"Executing {len(actions)} chaos actions")
        
        for action in actions:
            try:
                success = await self._execute_action(action)
                if not success:
                    logger.error(f"Action {action.get('name', 'unnamed')} failed")
                    return False
                
                # Handle pauses
                pause_config = action.get("pauses", {})
                if "after" in pause_config:
                    pause_duration = pause_config["after"]
                    logger.info(f"Pausing for {pause_duration} seconds")
                    await asyncio.sleep(pause_duration)
                    
            except Exception as e:
                logger.error(f"Action execution failed: {e}")
                return False
        
        return True
    
    async def _execute_action(self, action: Dict[str, Any]) -> bool:
        """Execute a single chaos action."""
        provider = action.get("provider", {})
        action_type = action.get("type", "action")
        
        if provider.get("type") == "process":
            path = provider.get("path")
            arguments = provider.get("arguments", [])
            background = action.get("background", False)
            
            try:
                if background:
                    # Start process in background
                    process = subprocess.Popen(
                        [path] + arguments,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    logger.info(f"Started background process: PID {process.pid}")
                    return True
                else:
                    # Run process and wait
                    result = subprocess.run(
                        [path] + arguments,
                        capture_output=True,
                        text=True,
                        timeout=60
                    )
                    
                    if result.returncode != 0:
                        logger.error(f"Action failed: {result.stderr}")
                        return False
                    
                    return True
                    
            except subprocess.TimeoutExpired:
                logger.error("Action timed out")
                return False
            except Exception as e:
                logger.error(f"Action execution error: {e}")
                return False
        
        elif provider.get("type") == "http":
            # HTTP-based actions (for future extension)
            url = provider.get("url")
            method = provider.get("method", "POST")
            
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.request(method, url) as response:
                        return response.status < 400
                except Exception as e:
                    logger.error(f"HTTP action failed: {e}")
                    return False
        
        logger.warning(f"Unknown action provider type: {provider.get('type')}")
        return False
    
    async def execute_rollbacks(self, rollbacks: List[Dict[str, Any]]) -> bool:
        """Execute rollback actions to restore system state."""
        logger.info(f"Executing {len(rollbacks)} rollback actions")
        
        for rollback in rollbacks:
            try:
                await self._execute_action(rollback)
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
                # Continue with other rollbacks even if one fails
        
        # Give system time to stabilize after rollbacks
        await asyncio.sleep(10)
        return True
    
    async def collect_metrics(self) -> Dict[str, Any]:
        """Collect system metrics during experiment."""
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "system_load": None,
            "memory_usage": None,
            "response_times": [],
            "error_rates": {}
        }
        
        try:
            # Collect basic system metrics
            result = subprocess.run(
                ["kubectl", "top", "pods", "--namespace=agent-etl"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                metrics["kubectl_top_output"] = result.stdout
            
            # Try to get application metrics if orchestrator is available
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{self.orchestrator_url}/metrics") as response:
                        if response.status == 200:
                            app_metrics = await response.json()
                            metrics["application_metrics"] = app_metrics
            except Exception:
                pass  # Metrics collection is best-effort
                
        except Exception as e:
            logger.warning(f"Metrics collection failed: {e}")
        
        return metrics
    
    async def run_experiment(self, experiment: Dict[str, Any]) -> ExperimentResult:
        """Run a single chaos experiment."""
        name = experiment.get("title", "Unnamed Experiment")
        logger.info(f"🔥 Starting experiment: {name}")
        
        start_time = datetime.utcnow()
        start_metrics = await self.collect_metrics()
        
        try:
            # Check initial steady state
            logger.info("Checking initial steady state...")
            steady_state_before = await self.check_steady_state(experiment)
            
            if not steady_state_before:
                logger.error("System not in steady state before experiment")
                return ExperimentResult(
                    name=name,
                    start_time=start_time.isoformat(),
                    end_time=datetime.utcnow().isoformat(),
                    duration_seconds=0,
                    success=False,
                    steady_state_before=False,
                    steady_state_after=False,
                    error_message="System not in steady state before experiment",
                    metrics={"start": start_metrics}
                )
            
            # Execute chaos actions
            logger.info("Executing chaos actions...")
            method_actions = experiment.get("method", [])
            actions_success = await self.execute_actions(method_actions)
            
            if not actions_success:
                logger.error("Chaos actions failed")
            
            # Collect metrics during chaos
            chaos_metrics = await self.collect_metrics()
            
            # Execute rollbacks
            logger.info("Executing rollbacks...")
            rollbacks = experiment.get("rollbacks", [])
            await self.execute_rollbacks(rollbacks)
            
            # Check final steady state
            logger.info("Checking final steady state...")
            steady_state_after = await self.check_steady_state(experiment)
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            success = actions_success and steady_state_after
            
            logger.info(f"✅ Experiment completed: {name} ({'SUCCESS' if success else 'FAILED'})")
            
            return ExperimentResult(
                name=name,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                success=success,
                steady_state_before=steady_state_before,
                steady_state_after=steady_state_after,
                error_message=None if success else "Experiment failed",
                metrics={
                    "start": start_metrics,
                    "chaos": chaos_metrics,
                    "end": await self.collect_metrics()
                }
            )
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"❌ Experiment failed with error: {e}")
            
            # Attempt rollbacks even on failure
            try:
                rollbacks = experiment.get("rollbacks", [])
                await self.execute_rollbacks(rollbacks)
            except Exception as rollback_error:
                logger.error(f"Rollback also failed: {rollback_error}")
            
            return ExperimentResult(
                name=name,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                success=False,
                steady_state_before=False,
                steady_state_after=False,
                error_message=str(e),
                metrics={"start": start_metrics, "error": str(e)}
            )
    
    def calculate_resilience_score(self, results: List[ExperimentResult]) -> float:
        """Calculate overall system resilience score."""
        if not results:
            return 0.0
        
        total_score = 0.0
        max_score = 0.0
        
        for result in results:
            # Base score components
            experiment_score = 0.0
            experiment_max = 100.0
            
            # Steady state before (20 points)
            if result.steady_state_before:
                experiment_score += 20
            
            # Actions executed successfully (30 points)
            if result.success:
                experiment_score += 30
            
            # Steady state after (50 points - most important)
            if result.steady_state_after:
                experiment_score += 50
            
            total_score += experiment_score
            max_score += experiment_max
        
        return (total_score / max_score) * 100 if max_score > 0 else 0.0
    
    def generate_recommendations(self, results: List[ExperimentResult]) -> List[str]:
        """Generate recommendations based on experiment results."""
        recommendations = []
        
        failed_experiments = [r for r in results if not r.success]
        
        if not failed_experiments:
            recommendations.append("✅ All chaos experiments passed! System shows excellent resilience.")
            recommendations.append("Consider increasing experiment complexity or duration.")
            recommendations.append("Schedule regular chaos engineering sessions.")
        else:
            recommendations.append(f"🔧 {len(failed_experiments)} experiments failed - review and fix issues:")
            
            for result in failed_experiments:
                recommendations.append(f"  - Fix issues in: {result.name}")
                
                if not result.steady_state_before:
                    recommendations.append(f"    • System health checks need improvement")
                
                if not result.steady_state_after:
                    recommendations.append(f"    • Recovery mechanisms need strengthening")
        
        # General recommendations
        recommendations.extend([
            "📊 Implement comprehensive monitoring and alerting",
            "🔄 Add automated recovery procedures",
            "📚 Create runbooks for common failure scenarios",
            "🎯 Gradually increase chaos experiment complexity",
            "👥 Include chaos engineering in incident response training"
        ])
        
        return recommendations
    
    async def run_chaos_suite(self) -> ChaosReport:
        """Run complete chaos engineering test suite."""
        logger.info("🚀 Starting chaos engineering test suite")
        
        experiments = self.load_experiments()
        if not experiments:
            logger.error("No experiments loaded")
            return ChaosReport(
                suite_name="Agent-Orchestrated-ETL Chaos Suite",
                timestamp=datetime.utcnow().isoformat(),
                total_experiments=0,
                successful_experiments=0,
                failed_experiments=0,
                overall_resilience_score=0.0,
                experiments=[],
                recommendations=["❌ No experiments found - check chaos-experiments.yaml"]
            )
        
        results = []
        
        for experiment in experiments:
            try:
                result = await self.run_experiment(experiment)
                results.append(result)
                
                # Brief pause between experiments
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Failed to run experiment: {e}")
                continue
        
        # Generate report
        successful = len([r for r in results if r.success])
        failed = len([r for r in results if not r.success])
        resilience_score = self.calculate_resilience_score(results)
        recommendations = self.generate_recommendations(results)
        
        report = ChaosReport(
            suite_name="Agent-Orchestrated-ETL Chaos Suite",
            timestamp=datetime.utcnow().isoformat(),
            total_experiments=len(results),
            successful_experiments=successful,
            failed_experiments=failed,
            overall_resilience_score=resilience_score,
            experiments=results,
            recommendations=recommendations
        )
        
        return report
    
    def save_report(self, report: ChaosReport) -> Path:
        """Save chaos engineering report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = self.reports_dir / f"chaos-report-{timestamp}.json"
        
        with open(report_path, 'w') as f:
            json.dump(asdict(report), f, indent=2)
        
        return report_path
    
    def print_summary(self, report: ChaosReport) -> None:
        """Print chaos engineering report summary."""
        print("\n" + "="*70)
        print("🔥 CHAOS ENGINEERING REPORT")
        print("="*70)
        print(f"Suite: {report.suite_name}")
        print(f"Timestamp: {report.timestamp}")
        print(f"Total Experiments: {report.total_experiments}")
        print(f"Successful: {report.successful_experiments}")
        print(f"Failed: {report.failed_experiments}")
        print(f"Resilience Score: {report.overall_resilience_score:.1f}/100")
        
        if report.overall_resilience_score >= 80:
            grade = "A"
        elif report.overall_resilience_score >= 70:
            grade = "B"
        elif report.overall_resilience_score >= 60:
            grade = "C"
        else:
            grade = "F"
        
        print(f"Resilience Grade: {grade}")
        
        print("\n📋 Experiment Results:")
        print("-" * 70)
        for exp in report.experiments:
            status = "✅ PASS" if exp.success else "❌ FAIL"
            print(f"{exp.name:.<45} {status}")
        
        print("\n💡 Recommendations:")
        print("-" * 70)
        for rec in report.recommendations[:5]:  # Show top 5
            print(f"  {rec}")
        
        print("="*70)


async def main():
    """Main entry point for chaos engineering runner."""
    project_root = Path(__file__).parent.parent
    runner = ChaosRunner(project_root)
    
    # Run chaos suite
    report = await runner.run_chaos_suite()
    
    # Save report
    report_path = runner.save_report(report)
    print(f"\n💾 Report saved to: {report_path}")
    
    # Print summary
    runner.print_summary(report)
    
    # Set exit code based on resilience score
    if report.overall_resilience_score >= 70:
        exit_code = 0
    elif report.overall_resilience_score >= 50:
        exit_code = 1
    else:
        exit_code = 2
    
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)