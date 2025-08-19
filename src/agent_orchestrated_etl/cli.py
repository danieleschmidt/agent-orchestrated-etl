from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from . import data_source_analysis, orchestrator
from .dag_generator import dag_to_airflow_code, generate_dag
from .data_source_analysis import SUPPORTED_SOURCES, analyze_source
from .validation import (
    ValidationError,
    safe_dag_id_type,
    safe_path_type,
    safe_source_type,
    sanitize_json_output,
)
from .logging_config import get_logger
from .core import DataQualityValidator


def generate_dag_cmd(args: list[str] | None = None) -> int:
    """Generate a DAG from a data source and output to a Python file.

    Parameters
    ----------
    args:
        Optional list of CLI arguments. If ``--list-sources`` is supplied,
        the positional ``source`` and ``output`` arguments are ignored.
    """
    parser = argparse.ArgumentParser(prog="generate_dag")
    parser.add_argument(
        "source",
        nargs="?",
        type=safe_source_type(SUPPORTED_SOURCES),
        help="Data source type, e.g. s3, postgresql, or api",
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=safe_path_type,
        help="Output DAG file path",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="Print supported source types and exit",
    )
    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="Print tasks in execution order and exit",
    )
    parser.add_argument(
        "--dag-id",
        type=safe_dag_id_type,
        default="generated",
        help="Airflow DAG ID to use in the generated file",
    )
    parser.add_argument(
        "--quantum-optimize",
        action="store_true",
        help="Use quantum-inspired optimization for task scheduling",
    )
    parser.add_argument(
        "--adaptive-resources",
        action="store_true",
        help="Enable adaptive resource management",
    )
    ns = parser.parse_args(args)

    if ns.list_sources:
        print(data_source_analysis.supported_sources_text())
        return 0

    if not ns.source or not ns.output:
        parser.error("the following arguments are required: source output")

    try:
        metadata = analyze_source(ns.source)
    except (ValueError, ValidationError) as e:
        parser.exit(2, f"Source analysis failed: {e}\n")

    dag = generate_dag(metadata)

    if ns.list_tasks:
        for task_id in dag.topological_sort():
            print(task_id)
        return 0

    out_path = Path(ns.output)
    out_path.write_text(dag_to_airflow_code(dag, dag_id=ns.dag_id))
    return 0


def run_pipeline_cmd(args: list[str] | None = None) -> int:
    """Create and execute a pipeline from the given source.

    Parameters
    ----------
    args:
        Optional list of CLI arguments. ``--list-sources`` can be used to
        display supported sources without providing the ``source`` argument.
        ``--monitor`` writes task events to the specified file.
        Events are appended as they occur and recorded even if pipeline
        creation fails.
    """
    parser = argparse.ArgumentParser(prog="run_pipeline")
    parser.add_argument(
        "source",
        nargs="?",
        type=safe_source_type(SUPPORTED_SOURCES),
        help="Data source type, e.g. s3, postgresql, or api",
    )
    parser.add_argument(
        "--output",
        type=safe_path_type,
        help=(
            "Optional path to write JSON results; prints to stdout "
            "if omitted"
        ),
    )
    parser.add_argument(
        "--dag-id",
        type=safe_dag_id_type,
        default="generated",
        help="DAG ID to use if also emitting Airflow code",
    )
    parser.add_argument(
        "--airflow",
        type=safe_path_type,
        help="Optional path to also write the generated Airflow DAG file",
    )
    parser.add_argument(
        "--monitor",
        type=safe_path_type,
        help="Optional path to write pipeline event log",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="Print supported source types and exit",
    )
    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="Print tasks in execution order and exit",
    )
    parser.add_argument(
        "--quantum-optimize",
        action="store_true",
        help="Use quantum-inspired optimization for pipeline execution",
    )
    parser.add_argument(
        "--adaptive-resources",
        action="store_true",
        help="Enable adaptive resource management",
    )
    parser.add_argument(
        "--data-quality",
        action="store_true",
        help="Run comprehensive data quality analysis",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for data processing",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Number of parallel workers for processing",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        help="Pipeline execution timeout in seconds",
    )
    parser.add_argument(
        "--config-file",
        type=safe_path_type,
        help="Path to pipeline configuration file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate pipeline without executing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity (can be used multiple times)",
    )
    ns = parser.parse_args(args)

    if ns.list_sources:
        print(data_source_analysis.supported_sources_text())
        return 0

    if not ns.source:
        parser.error("the following arguments are required: source")

    # Setup logging based on verbosity
    logger = get_logger("agent_etl.cli")
    if ns.verbose >= 2:
        import logging
        logging.getLogger("agent_etl").setLevel(logging.DEBUG)
    elif ns.verbose == 1:
        import logging
        logging.getLogger("agent_etl").setLevel(logging.INFO)
    
    # Load configuration from file if provided
    config = {}
    if ns.config_file:
        try:
            with open(ns.config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {ns.config_file}")
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            return 1
    
    monitor = None
    if ns.monitor:
        monitor = orchestrator.MonitorAgent(ns.monitor)

    # Source validation is now handled by argparse type function
    orch = orchestrator.DataOrchestrator(
        enable_quantum_planning=ns.quantum_optimize,
        enable_adaptive_resources=ns.adaptive_resources
    )
    
    try:
        # Create pipeline with enhanced options
        pipeline_options = {
            'dag_id': ns.dag_id,
            'timeout': ns.timeout,
        }
        
        # Override with config file settings
        pipeline_options.update(config.get('pipeline', {}))
        
        pipeline = orch.create_pipeline(ns.source, **pipeline_options)
        
        logger.info(f"Created pipeline '{ns.dag_id}' for source '{ns.source}'")
        
    except ValueError as exc:
        if monitor:
            monitor.error(str(exc))
        logger.error(f"Pipeline creation failed: {exc}")
        print(exc, file=sys.stderr)
        return 1

    if ns.list_tasks:
        for task_id in pipeline.dag.topological_sort():
            print(task_id)
        return 0

    # Dry run mode - validate without executing
    if ns.dry_run:
        logger.info("Dry run mode: validating pipeline without execution")
        pipeline_status = pipeline.get_status()
        
        validation_results = {
            "status": "validation_complete",
            "pipeline": pipeline_status,
            "tasks": pipeline.dag.topological_sort(),
            "estimated_runtime": "unknown",  # Would estimate based on data size
            "resource_requirements": {
                "cpu": "low",
                "memory": "medium",
                "disk": "low"
            }
        }
        
        output_text = json.dumps(validation_results, indent=2)
        if ns.output:
            Path(ns.output).write_text(output_text)
        else:
            print(output_text)
            
        return 0
    
    # Execute pipeline with enhanced monitoring
    try:
        start_time = time.time()
        logger.info(f"Starting pipeline execution: {ns.dag_id}")
        
        # Execute pipeline
        if hasattr(orch, 'execute_pipeline_async') and ns.parallel_workers > 1:
            import asyncio
            results = asyncio.run(orch.execute_pipeline_async(pipeline, monitor))
        else:
            results = pipeline.execute(monitor=monitor)
            
        execution_time = time.time() - start_time
        logger.info(f"Pipeline execution completed in {execution_time:.2f} seconds")
        
        # Add execution metadata
        results['_metadata'] = {
            'execution_time': execution_time,
            'pipeline_id': ns.dag_id,
            'source_type': ns.source,
            'timestamp': time.time(),
            'options': {
                'quantum_optimize': ns.quantum_optimize,
                'adaptive_resources': ns.adaptive_resources,
                'data_quality': ns.data_quality
            }
        }
        
        # Run data quality analysis if requested
        if ns.data_quality:
            logger.info("Running data quality analysis")
            validator = DataQualityValidator()
            
            # Extract data for quality analysis
            for task_id, task_result in results.items():
                if task_id.startswith('extract') and isinstance(task_result, list):
                    quality_result = validator.validate_data_quality(task_result)
                    results[f'{task_id}_quality'] = quality_result
                    logger.info(f"Quality score for {task_id}: {quality_result['quality_score']:.2f}")
        
    except Exception as exc:  # pragma: no cover - defensive
        if monitor:
            monitor.error(str(exc))
        logger.error(f"Pipeline execution failed: {exc}", exc_info=True)
        print(exc, file=sys.stderr)
        return 1

    # Sanitize output to prevent information leakage
    sanitized_results = sanitize_json_output(results)
    output_text = json.dumps(sanitized_results, indent=2)

    if ns.output:
        Path(ns.output).write_text(output_text)
        logger.info(f"Results written to {ns.output}")
    else:
        print(output_text)

    if ns.airflow:
        airflow_code = orch.dag_to_airflow(pipeline)
        Path(ns.airflow).write_text(airflow_code)
        logger.info(f"Airflow DAG written to {ns.airflow}")

    return 0


# Advanced CLI Commands

def benchmark_cmd(args: list[str] | None = None) -> int:
    """Benchmark pipeline performance with different configurations."""
    parser = argparse.ArgumentParser(prog="benchmark_pipeline")
    parser.add_argument(
        "source",
        type=safe_source_type(SUPPORTED_SOURCES),
        help="Data source type to benchmark",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of benchmark iterations",
    )
    parser.add_argument(
        "--output",
        type=safe_path_type,
        help="Output file for benchmark results",
    )
    parser.add_argument(
        "--compare-modes",
        action="store_true",
        help="Compare quantum vs standard vs adaptive modes",
    )
    ns = parser.parse_args(args)
    
    logger = get_logger("agent_etl.benchmark")
    results = {
        "source": ns.source,
        "iterations": ns.iterations,
        "timestamp": time.time(),
        "benchmarks": []
    }
    
    modes = []
    if ns.compare_modes:
        modes = [
            {"name": "standard", "quantum": False, "adaptive": False},
            {"name": "quantum", "quantum": True, "adaptive": False},
            {"name": "adaptive", "quantum": False, "adaptive": True},
            {"name": "quantum_adaptive", "quantum": True, "adaptive": True}
        ]
    else:
        modes = [{"name": "default", "quantum": False, "adaptive": False}]
    
    for mode in modes:
        logger.info(f"Benchmarking mode: {mode['name']}")
        mode_results = {
            "mode": mode["name"],
            "config": mode,
            "iterations": [],
            "statistics": {}
        }
        
        execution_times = []
        
        for i in range(ns.iterations):
            logger.info(f"Running iteration {i+1}/{ns.iterations}")
            
            orch = orchestrator.DataOrchestrator(
                enable_quantum_planning=mode["quantum"],
                enable_adaptive_resources=mode["adaptive"]
            )
            
            start_time = time.time()
            try:
                pipeline = orch.create_pipeline(ns.source)
                results_data = pipeline.execute()
                execution_time = time.time() - start_time
                
                iteration_result = {
                    "iteration": i + 1,
                    "execution_time": execution_time,
                    "status": "success",
                    "records_processed": len(results_data.get('extract', [])) if 'extract' in results_data else 0
                }
                
                execution_times.append(execution_time)
                mode_results["iterations"].append(iteration_result)
                
            except Exception as e:
                logger.error(f"Iteration {i+1} failed: {e}")
                mode_results["iterations"].append({
                    "iteration": i + 1,
                    "status": "failed",
                    "error": str(e)
                })
        
        # Calculate statistics
        if execution_times:
            mode_results["statistics"] = {
                "avg_execution_time": sum(execution_times) / len(execution_times),
                "min_execution_time": min(execution_times),
                "max_execution_time": max(execution_times),
                "success_rate": (len(execution_times) / ns.iterations) * 100,
                "throughput_per_minute": 60 / (sum(execution_times) / len(execution_times)) if execution_times else 0
            }
        
        results["benchmarks"].append(mode_results)
    
    # Output results
    output_text = json.dumps(results, indent=2)
    if ns.output:
        Path(ns.output).write_text(output_text)
        logger.info(f"Benchmark results written to {ns.output}")
    else:
        print(output_text)
    
    return 0


def validate_cmd(args: list[str] | None = None) -> int:
    """Validate data source and pipeline configuration."""
    parser = argparse.ArgumentParser(prog="validate_pipeline")
    parser.add_argument(
        "source",
        type=safe_source_type(SUPPORTED_SOURCES),
        help="Data source to validate",
    )
    parser.add_argument(
        "--config-file",
        type=safe_path_type,
        help="Pipeline configuration file to validate",
    )
    parser.add_argument(
        "--output",
        type=safe_path_type,
        help="Output file for validation results",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Enable strict validation mode",
    )
    ns = parser.parse_args(args)
    
    logger = get_logger("agent_etl.validate")
    validation_results = {
        "source": ns.source,
        "timestamp": time.time(),
        "validation_status": "passed",
        "checks": []
    }
    
    # Validate data source
    try:
        logger.info(f"Validating data source: {ns.source}")
        metadata = analyze_source(ns.source)
        validation_results["checks"].append({
            "check": "data_source_analysis",
            "status": "passed",
            "message": f"Successfully analyzed {ns.source}",
            "metadata": metadata
        })
    except Exception as e:
        validation_results["checks"].append({
            "check": "data_source_analysis",
            "status": "failed",
            "message": str(e)
        })
        validation_results["validation_status"] = "failed"
    
    # Validate configuration file
    if ns.config_file:
        try:
            logger.info(f"Validating config file: {ns.config_file}")
            with open(ns.config_file, 'r') as f:
                config = json.load(f)
            
            # Basic config validation
            required_sections = ['pipeline', 'source', 'destination']
            missing_sections = [s for s in required_sections if s not in config]
            
            if missing_sections:
                validation_results["checks"].append({
                    "check": "config_structure",
                    "status": "warning",
                    "message": f"Missing optional sections: {missing_sections}"
                })
            else:
                validation_results["checks"].append({
                    "check": "config_structure",
                    "status": "passed",
                    "message": "Configuration structure is valid"
                })
                
        except Exception as e:
            validation_results["checks"].append({
                "check": "config_file_validation",
                "status": "failed",
                "message": str(e)
            })
            validation_results["validation_status"] = "failed"
    
    # Try to create pipeline
    try:
        logger.info("Validating pipeline creation")
        orch = orchestrator.DataOrchestrator()
        pipeline = orch.create_pipeline(ns.source)
        
        # Validate task dependencies
        task_order = pipeline.dag.topological_sort()
        validation_results["checks"].append({
            "check": "pipeline_creation",
            "status": "passed",
            "message": f"Pipeline created with {len(task_order)} tasks",
            "task_order": task_order
        })
        
    except Exception as e:
        validation_results["checks"].append({
            "check": "pipeline_creation",
            "status": "failed",
            "message": str(e)
        })
        validation_results["validation_status"] = "failed"
    
    # Summary
    passed_checks = sum(1 for check in validation_results["checks"] if check["status"] == "passed")
    total_checks = len(validation_results["checks"])
    
    validation_results["summary"] = {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": sum(1 for check in validation_results["checks"] if check["status"] == "failed"),
        "warnings": sum(1 for check in validation_results["checks"] if check["status"] == "warning")
    }
    
    # Output results
    output_text = json.dumps(validation_results, indent=2)
    if ns.output:
        Path(ns.output).write_text(output_text)
        logger.info(f"Validation results written to {ns.output}")
    else:
        print(output_text)
    
    return 0 if validation_results["validation_status"] == "passed" else 1


def status_cmd(args: list[str] | None = None) -> int:
    """Get system status and health information."""
    parser = argparse.ArgumentParser(prog="pipeline_status")
    parser.add_argument(
        "--format",
        choices=["json", "yaml", "table"],
        default="json",
        help="Output format",
    )
    parser.add_argument(
        "--output",
        type=safe_path_type,
        help="Output file for status information",
    )
    ns = parser.parse_args(args)
    
    logger = get_logger("agent_etl.status")
    
    try:
        # Get orchestrator status
        orch = orchestrator.DataOrchestrator()
        orchestrator_status = orch.get_orchestrator_status()
        
        # System information
        import psutil
        import platform
        
        system_info = {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count": psutil.cpu_count(),
            "memory_gb": round(psutil.virtual_memory().total / (1024**3), 2),
            "disk_free_gb": round(psutil.disk_usage('/').free / (1024**3), 2)
        }
        
        status_info = {
            "timestamp": time.time(),
            "system": system_info,
            "orchestrator": orchestrator_status,
            "health_status": "healthy",  # Would determine based on various factors
            "version": "0.1.0"
        }
        
        # Format output
        if ns.format == "json":
            output_text = json.dumps(status_info, indent=2)
        elif ns.format == "yaml":
            try:
                import yaml
                output_text = yaml.dump(status_info, default_flow_style=False)
            except ImportError:
                logger.warning("PyYAML not available, falling back to JSON")
                output_text = json.dumps(status_info, indent=2)
        elif ns.format == "table":
            # Simple table format
            output_lines = ["System Status:", "=" * 50]
            output_lines.append(f"Health: {status_info['health_status']}")
            output_lines.append(f"Version: {status_info['version']}")
            output_lines.append(f"Platform: {system_info['platform']}")
            output_lines.append(f"CPU Cores: {system_info['cpu_count']}")
            output_lines.append(f"Memory: {system_info['memory_gb']} GB")
            output_lines.append(f"Disk Free: {system_info['disk_free_gb']} GB")
            output_text = "\n".join(output_lines)
        
        if ns.output:
            Path(ns.output).write_text(output_text)
            logger.info(f"Status information written to {ns.output}")
        else:
            print(output_text)
            
    except Exception as e:
        logger.error(f"Failed to get status: {e}")
        return 1
    
    return 0


def main() -> int:
    """Main CLI entry point with command routing."""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "benchmark":
            return benchmark_cmd(sys.argv[2:])
        elif command == "validate":
            return validate_cmd(sys.argv[2:])
        elif command == "status":
            return status_cmd(sys.argv[2:])
        elif command == "run":
            return run_pipeline_cmd(sys.argv[2:])
        elif command in ["-h", "--help"]:
            print("Available commands:")
            print("  generate_dag    Generate Airflow DAG from data source")
            print("  run            Run pipeline from data source")
            print("  benchmark      Benchmark pipeline performance")
            print("  validate       Validate data source and configuration")
            print("  status         Get system status and health")
            print("")
            print("Use 'command --help' for command-specific options")
            return 0
    
    return generate_dag_cmd()


# Export CLI functions for programmatic use
__all__ = [
    "main",
    "generate_dag_cmd",
    "run_pipeline_cmd",
    "benchmark_cmd",
    "validate_cmd",
    "status_cmd"
]

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
