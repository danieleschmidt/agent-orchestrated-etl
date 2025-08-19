#!/usr/bin/env python3
"""
Advanced Pipeline Orchestration Examples
========================================

This module demonstrates advanced usage patterns of the Agent-Orchestrated-ETL system,
showcasing quantum optimization, adaptive resource management, and real-world scenarios.

Examples:
- Multi-source data integration
- Real-time streaming pipelines
- Complex transformation workflows
- Error recovery and resilience patterns
- Performance optimization techniques
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent_orchestrated_etl import (
    DataOrchestrator,
    EnhancedDataOrchestrator,
    MonitorAgent,
    QuantumPipelineOrchestrator,
    AdaptiveResourceManager,
    PerformanceOptimizer
)
from agent_orchestrated_etl.core import DataQualityValidator
from agent_orchestrated_etl.logging_config import get_logger

logger = get_logger("examples.advanced")


class AdvancedPipelineExamples:
    """Collection of advanced pipeline orchestration examples."""

    def __init__(self):
        self.logger = logger
        self.orchestrator = EnhancedDataOrchestrator(
            enable_quantum_planning=True,
            enable_adaptive_resources=True
        )

    async def multi_source_integration_example(self) -> Dict[str, Any]:
        """
        Example: Multi-source data integration with conflict resolution.
        
        This example demonstrates how to:
        - Process data from multiple heterogeneous sources
        - Apply conflict resolution strategies
        - Maintain data lineage and quality metrics
        """
        self.logger.info("Starting multi-source integration example")
        
        # Define multiple data sources
        sources = [
            {"type": "database", "connection": "postgresql://localhost/sales"},
            {"type": "api", "endpoint": "https://api.crm.example.com/customers"},
            {"type": "s3", "bucket": "data-lake", "prefix": "customer-events/"},
            {"type": "file", "path": "/data/customer-profiles.csv"}
        ]
        
        # Create integration pipeline with conflict resolution
        integration_config = {
            "conflict_resolution": {
                "strategy": "latest_wins",
                "tie_breaker": "source_priority",
                "source_priorities": ["database", "api", "s3", "file"]
            },
            "data_quality_rules": [
                {"field": "customer_id", "rule": "not_null", "required": True},
                {"field": "email", "rule": "email_format", "required": False},
                {"field": "last_updated", "rule": "recent_timestamp", "days": 30}
            ],
            "transformation_pipeline": [
                {"step": "standardize_schema", "config": {"target_schema": "unified_customer"}},
                {"step": "resolve_conflicts", "config": {"method": "merge_with_priority"}},
                {"step": "enrich_data", "config": {"external_apis": ["geocoding", "demographics"]}},
                {"step": "validate_quality", "config": {"min_score": 0.85}}
            ]
        }
        
        results = []
        validator = DataQualityValidator()
        
        # Process each source with adaptive resource allocation
        for i, source in enumerate(sources):
            source_id = f"source_{i+1}"
            
            try:
                # Create specialized pipeline for this source
                pipeline = await self.orchestrator.create_adaptive_pipeline(
                    source_config=source,
                    pipeline_id=source_id,
                    optimization_target="throughput"
                )
                
                # Execute with monitoring
                monitor = MonitorAgent(f"/tmp/{source_id}_monitor.log")
                source_results = await self.orchestrator.execute_pipeline_async(
                    pipeline, monitor
                )
                
                # Apply source-specific transformations
                transformed_data = await self._apply_source_transformations(
                    source_results, source, integration_config
                )
                
                # Validate data quality
                quality_metrics = validator.validate_data_quality(
                    transformed_data, integration_config["data_quality_rules"]
                )
                
                results.append({
                    "source": source,
                    "source_id": source_id,
                    "data": transformed_data,
                    "quality_metrics": quality_metrics,
                    "processing_time": time.time()
                })
                
            except Exception as e:
                self.logger.error(f"Failed to process {source_id}: {e}")
                results.append({
                    "source": source,
                    "source_id": source_id,
                    "error": str(e),
                    "status": "failed"
                })
        
        # Perform data integration and conflict resolution
        integrated_data = await self._integrate_multi_source_data(
            results, integration_config
        )
        
        return {
            "integration_status": "completed",
            "sources_processed": len(sources),
            "sources_successful": len([r for r in results if "error" not in r]),
            "total_records": len(integrated_data.get("unified_records", [])),
            "conflicts_resolved": integrated_data.get("conflicts_resolved", 0),
            "quality_score": integrated_data.get("overall_quality_score", 0),
            "execution_time": time.time(),
            "lineage": integrated_data.get("data_lineage", {}),
            "sample_records": integrated_data.get("unified_records", [])[:5]
        }

    async def streaming_pipeline_example(self) -> Dict[str, Any]:
        """
        Example: Real-time streaming data pipeline with event processing.
        
        This example demonstrates:
        - Real-time data ingestion and processing
        - Event-driven transformations
        - Sliding window analytics
        - Alerting and anomaly detection
        """
        self.logger.info("Starting streaming pipeline example")
        
        # Configure streaming pipeline
        streaming_config = {
            "input_streams": [
                {"source": "kafka", "topic": "user-events", "format": "json"},
                {"source": "rabbitmq", "queue": "order-events", "format": "avro"},
                {"source": "websocket", "endpoint": "ws://events.example.com", "format": "json"}
            ],
            "processing_windows": [
                {"type": "tumbling", "duration": "5m", "aggregations": ["count", "sum", "avg"]},
                {"type": "sliding", "duration": "15m", "slide": "1m", "aggregations": ["percentiles"]},
                {"type": "session", "gap": "30m", "aggregations": ["unique_users", "session_duration"]}
            ],
            "output_sinks": [
                {"type": "elasticsearch", "index": "processed-events", "batch_size": 1000},
                {"type": "redis", "key_pattern": "metrics:{window}:{timestamp}", "ttl": 3600},
                {"type": "webhook", "url": "https://alerts.example.com/webhook", "condition": "anomaly_detected"}
            ],
            "anomaly_detection": {
                "enabled": True,
                "algorithms": ["isolation_forest", "statistical_outlier", "time_series_decomposition"],
                "sensitivity": 0.95,
                "alert_threshold": 3
            }
        }
        
        # Create streaming orchestrator with quantum optimization
        streaming_orchestrator = await self.orchestrator.create_streaming_pipeline(
            config=streaming_config,
            enable_quantum_scheduling=True
        )
        
        # Simulate streaming data processing
        simulation_results = []
        anomalies_detected = []
        
        # Run simulation for 30 seconds (normally this would run continuously)
        start_time = time.time()
        simulation_duration = 30
        
        while time.time() - start_time < simulation_duration:
            # Simulate batch of events
            simulated_events = self._generate_simulated_events(batch_size=100)
            
            # Process events through streaming pipeline
            batch_results = await streaming_orchestrator.process_event_batch(
                events=simulated_events,
                timestamp=time.time()
            )
            
            # Check for anomalies
            anomaly_results = await self._detect_anomalies(
                batch_results, streaming_config["anomaly_detection"]
            )
            
            if anomaly_results["anomalies_found"]:
                anomalies_detected.extend(anomaly_results["anomalies"])
                self.logger.warning(f"Anomalies detected: {len(anomaly_results['anomalies'])}")
            
            simulation_results.append({
                "timestamp": time.time(),
                "events_processed": len(simulated_events),
                "processing_latency_ms": batch_results.get("processing_latency_ms", 0),
                "throughput_per_second": batch_results.get("throughput_per_second", 0),
                "anomalies": len(anomaly_results.get("anomalies", []))
            })
            
            # Wait before next batch
            await asyncio.sleep(1)
        
        # Calculate streaming metrics
        total_events = sum(r["events_processed"] for r in simulation_results)
        avg_latency = sum(r["processing_latency_ms"] for r in simulation_results) / len(simulation_results)
        avg_throughput = sum(r["throughput_per_second"] for r in simulation_results) / len(simulation_results)
        
        return {
            "streaming_status": "simulation_completed",
            "simulation_duration_seconds": simulation_duration,
            "total_events_processed": total_events,
            "average_latency_ms": avg_latency,
            "average_throughput_per_second": avg_throughput,
            "anomalies_detected": len(anomalies_detected),
            "processing_windows": len(streaming_config["processing_windows"]),
            "output_sinks": len(streaming_config["output_sinks"]),
            "sample_anomalies": anomalies_detected[:3],
            "performance_metrics": simulation_results[-5:]  # Last 5 batches
        }

    async def quantum_optimization_example(self) -> Dict[str, Any]:
        """
        Example: Quantum-inspired optimization for complex ETL workflows.
        
        This example demonstrates:
        - Quantum annealing for optimal task scheduling
        - Quantum-inspired algorithms for resource allocation
        - Performance comparison between classical and quantum approaches
        """
        self.logger.info("Starting quantum optimization example")
        
        # Create complex workflow with interdependencies
        complex_workflow = {
            "stages": [
                {
                    "name": "data_ingestion",
                    "tasks": ["extract_customers", "extract_orders", "extract_products", "extract_inventory"],
                    "parallelizable": True,
                    "resource_requirements": {"cpu": 2, "memory": 4, "io": "high"}
                },
                {
                    "name": "data_validation",
                    "tasks": ["validate_customers", "validate_orders", "validate_products", "validate_inventory"],
                    "parallelizable": True,
                    "depends_on": ["data_ingestion"],
                    "resource_requirements": {"cpu": 1, "memory": 2, "io": "low"}
                },
                {
                    "name": "data_transformation",
                    "tasks": ["transform_customer_profiles", "calculate_order_metrics", "enrich_product_data"],
                    "parallelizable": False,  # Sequential due to data dependencies
                    "depends_on": ["data_validation"],
                    "resource_requirements": {"cpu": 4, "memory": 8, "io": "medium"}
                },
                {
                    "name": "data_aggregation",
                    "tasks": ["aggregate_sales_metrics", "calculate_customer_lifetime_value", "generate_recommendations"],
                    "parallelizable": True,
                    "depends_on": ["data_transformation"],
                    "resource_requirements": {"cpu": 3, "memory": 6, "io": "medium"}
                },
                {
                    "name": "data_loading",
                    "tasks": ["load_to_warehouse", "update_cache", "trigger_notifications"],
                    "parallelizable": True,
                    "depends_on": ["data_aggregation"],
                    "resource_requirements": {"cpu": 1, "memory": 2, "io": "high"}
                }
            ],
            "constraints": {
                "max_concurrent_tasks": 8,
                "total_cpu_limit": 16,
                "total_memory_limit_gb": 32,
                "max_execution_time_minutes": 60
            }
        }
        
        # Create quantum-optimized orchestrator
        quantum_orchestrator = QuantumPipelineOrchestrator(self.orchestrator)
        
        # Compare classical vs quantum scheduling
        optimization_results = {}
        
        # Classical optimization
        self.logger.info("Running classical optimization")
        classical_start = time.time()
        classical_schedule = await quantum_orchestrator.create_classical_schedule(
            workflow=complex_workflow,
            optimization_target="minimize_execution_time"
        )
        classical_time = time.time() - classical_start
        
        # Quantum-inspired optimization
        self.logger.info("Running quantum-inspired optimization")
        quantum_start = time.time()
        quantum_schedule = await quantum_orchestrator.create_quantum_schedule(
            workflow=complex_workflow,
            optimization_target="minimize_execution_time_and_resources",
            quantum_algorithm="qaoa"  # Quantum Approximate Optimization Algorithm
        )
        quantum_time = time.time() - quantum_start
        
        # Simulate execution with both schedules
        classical_execution = await self._simulate_workflow_execution(
            classical_schedule, complex_workflow
        )
        
        quantum_execution = await self._simulate_workflow_execution(
            quantum_schedule, complex_workflow
        )
        
        optimization_results = {
            "workflow_complexity": {
                "total_stages": len(complex_workflow["stages"]),
                "total_tasks": sum(len(stage["tasks"]) for stage in complex_workflow["stages"]),
                "dependency_chains": 5,
                "resource_constraints": len(complex_workflow["constraints"])
            },
            "classical_optimization": {
                "optimization_time_seconds": classical_time,
                "schedule": classical_schedule,
                "simulated_execution": classical_execution
            },
            "quantum_optimization": {
                "optimization_time_seconds": quantum_time,
                "schedule": quantum_schedule,
                "simulated_execution": quantum_execution,
                "quantum_algorithm": "qaoa"
            },
            "performance_comparison": {
                "execution_time_improvement_percent": (
                    (classical_execution["total_time"] - quantum_execution["total_time"]) 
                    / classical_execution["total_time"] * 100
                ),
                "resource_utilization_improvement_percent": (
                    (quantum_execution["resource_efficiency"] - classical_execution["resource_efficiency"])
                    / classical_execution["resource_efficiency"] * 100
                ),
                "scheduling_overhead_ratio": quantum_time / classical_time
            }
        }
        
        return optimization_results

    async def resilience_testing_example(self) -> Dict[str, Any]:
        """
        Example: Comprehensive resilience and error recovery testing.
        
        This example demonstrates:
        - Circuit breaker patterns
        - Retry mechanisms with exponential backoff
        - Graceful degradation strategies
        - Chaos engineering principles
        """
        self.logger.info("Starting resilience testing example")
        
        # Define resilience test scenarios
        test_scenarios = [
            {
                "name": "database_connection_failure",
                "description": "Simulate database connection timeout and recovery",
                "failure_type": "connection_timeout",
                "failure_duration": 30,  # seconds
                "expected_behavior": "circuit_breaker_activation"
            },
            {
                "name": "api_rate_limiting",
                "description": "Simulate API rate limiting and backoff behavior",
                "failure_type": "rate_limit_exceeded",
                "failure_rate": 0.3,  # 30% of requests fail
                "expected_behavior": "exponential_backoff_retry"
            },
            {
                "name": "memory_pressure",
                "description": "Simulate high memory usage and graceful degradation",
                "failure_type": "resource_exhaustion",
                "resource": "memory",
                "threshold": 0.9,  # 90% memory usage
                "expected_behavior": "graceful_degradation"
            },
            {
                "name": "network_partition",
                "description": "Simulate network partition and recovery",
                "failure_type": "network_partition",
                "affected_services": ["external_api", "database"],
                "expected_behavior": "fallback_to_cache"
            }
        ]
        
        resilience_results = []
        
        for scenario in test_scenarios:
            self.logger.info(f"Testing scenario: {scenario['name']}")
            
            # Create pipeline with resilience features
            resilient_pipeline = await self.orchestrator.create_resilient_pipeline(
                source="test_data",
                resilience_config={
                    "circuit_breaker": {
                        "failure_threshold": 5,
                        "recovery_timeout": 30,
                        "half_open_max_calls": 3
                    },
                    "retry": {
                        "max_attempts": 5,
                        "base_delay": 1,
                        "max_delay": 60,
                        "exponential_base": 2
                    },
                    "graceful_degradation": {
                        "enabled": True,
                        "fallback_strategies": ["cache", "simplified_processing", "skip_non_critical"]
                    }
                }
            )
            
            # Inject failures based on scenario
            failure_injector = await self._create_failure_injector(scenario)
            
            scenario_start = time.time()
            
            try:
                # Execute pipeline with failure injection
                with failure_injector:
                    execution_results = await resilient_pipeline.execute_with_monitoring()
                
                scenario_duration = time.time() - scenario_start
                
                # Analyze resilience metrics
                resilience_metrics = await self._analyze_resilience_metrics(
                    execution_results, scenario, scenario_duration
                )
                
                resilience_results.append({
                    "scenario": scenario["name"],
                    "status": "completed",
                    "duration_seconds": scenario_duration,
                    "resilience_metrics": resilience_metrics,
                    "behavior_analysis": await self._analyze_expected_behavior(
                        execution_results, scenario["expected_behavior"]
                    )
                })
                
            except Exception as e:
                resilience_results.append({
                    "scenario": scenario["name"],
                    "status": "failed",
                    "error": str(e),
                    "duration_seconds": time.time() - scenario_start
                })
        
        # Calculate overall resilience score
        successful_scenarios = [r for r in resilience_results if r["status"] == "completed"]
        overall_resilience_score = (
            len(successful_scenarios) / len(test_scenarios) * 100
            if test_scenarios else 0
        )
        
        return {
            "resilience_testing_status": "completed",
            "total_scenarios": len(test_scenarios),
            "successful_scenarios": len(successful_scenarios),
            "overall_resilience_score": overall_resilience_score,
            "scenario_results": resilience_results,
            "summary": {
                "circuit_breaker_activations": sum(
                    r.get("resilience_metrics", {}).get("circuit_breaker_activations", 0)
                    for r in successful_scenarios
                ),
                "total_retries": sum(
                    r.get("resilience_metrics", {}).get("total_retries", 0)
                    for r in successful_scenarios
                ),
                "graceful_degradations": sum(
                    r.get("resilience_metrics", {}).get("graceful_degradations", 0)
                    for r in successful_scenarios
                )
            }
        }

    # Helper methods
    
    async def _apply_source_transformations(
        self, data: Any, source: Dict[str, Any], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Apply source-specific transformations."""
        # Mock transformation logic
        if isinstance(data, dict) and 'extract' in data:
            return data['extract']
        elif isinstance(data, list):
            return data
        else:
            return [{"data": data, "source": source["type"]}]
    
    async def _integrate_multi_source_data(
        self, results: List[Dict[str, Any]], config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Integrate data from multiple sources with conflict resolution."""
        unified_records = []
        conflicts_resolved = 0
        data_lineage = {}
        
        # Mock integration logic
        for result in results:
            if "data" in result:
                for record in result["data"]:
                    record["_source"] = result["source_id"]
                    record["_processing_time"] = result.get("processing_time", time.time())
                    unified_records.append(record)
                    
                data_lineage[result["source_id"]] = {
                    "source": result["source"],
                    "record_count": len(result["data"]),
                    "quality_score": result.get("quality_metrics", {}).get("quality_score", 0)
                }
        
        return {
            "unified_records": unified_records,
            "conflicts_resolved": conflicts_resolved,
            "overall_quality_score": 0.85,  # Mock score
            "data_lineage": data_lineage
        }
    
    def _generate_simulated_events(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate simulated events for streaming example."""
        import random
        
        events = []
        for i in range(batch_size):
            event_type = random.choice(["user_action", "order_placed", "system_event"])
            events.append({
                "id": f"event_{i}_{int(time.time())}",
                "type": event_type,
                "timestamp": time.time(),
                "user_id": f"user_{random.randint(1, 1000)}",
                "value": random.uniform(10, 1000),
                "metadata": {"source": "simulation"}
            })
        return events
    
    async def _detect_anomalies(
        self, batch_results: Dict[str, Any], anomaly_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Detect anomalies in streaming data."""
        # Mock anomaly detection
        import random
        
        anomalies = []
        if random.random() < 0.1:  # 10% chance of anomaly
            anomalies.append({
                "id": f"anomaly_{int(time.time())}",
                "type": "statistical_outlier",
                "severity": "medium",
                "description": "Unusual spike in event volume",
                "timestamp": time.time()
            })
        
        return {
            "anomalies_found": len(anomalies) > 0,
            "anomalies": anomalies
        }
    
    async def _simulate_workflow_execution(
        self, schedule: Dict[str, Any], workflow: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Simulate workflow execution based on schedule."""
        # Mock execution simulation
        total_tasks = sum(len(stage["tasks"]) for stage in workflow["stages"])
        
        return {
            "total_time": 450 + random.uniform(-50, 50),  # Mock execution time
            "resource_efficiency": random.uniform(0.7, 0.95),  # Mock efficiency
            "parallel_utilization": random.uniform(0.6, 0.9),
            "tasks_completed": total_tasks
        }
    
    async def _create_failure_injector(self, scenario: Dict[str, Any]):
        """Create failure injector for resilience testing."""
        # Mock failure injector
        class MockFailureInjector:
            def __init__(self, scenario):
                self.scenario = scenario
                
            async def __aenter__(self):
                return self
                
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        return MockFailureInjector(scenario)
    
    async def _analyze_resilience_metrics(
        self, results: Dict[str, Any], scenario: Dict[str, Any], duration: float
    ) -> Dict[str, Any]:
        """Analyze resilience metrics from execution results."""
        # Mock metrics analysis
        import random
        
        return {
            "circuit_breaker_activations": random.randint(0, 3),
            "total_retries": random.randint(2, 10),
            "graceful_degradations": random.randint(0, 2),
            "recovery_time_seconds": random.uniform(5, 30),
            "success_rate": random.uniform(0.8, 1.0)
        }
    
    async def _analyze_expected_behavior(
        self, results: Dict[str, Any], expected_behavior: str
    ) -> Dict[str, Any]:
        """Analyze whether the system behaved as expected."""
        # Mock behavior analysis
        return {
            "expected_behavior": expected_behavior,
            "actual_behavior": expected_behavior,  # Mock - assume it worked
            "behavior_match": True,
            "analysis": f"System correctly exhibited {expected_behavior}"
        }


async def run_all_examples():
    """Run all advanced pipeline orchestration examples."""
    examples = AdvancedPipelineExamples()
    logger.info("Starting advanced pipeline orchestration examples")
    
    results = {}
    
    try:
        # Multi-source integration
        results["multi_source_integration"] = await examples.multi_source_integration_example()
        
        # Streaming pipeline
        results["streaming_pipeline"] = await examples.streaming_pipeline_example()
        
        # Quantum optimization
        results["quantum_optimization"] = await examples.quantum_optimization_example()
        
        # Resilience testing
        results["resilience_testing"] = await examples.resilience_testing_example()
        
    except Exception as e:
        logger.error(f"Example execution failed: {e}")
        results["error"] = str(e)
    
    # Save results to file
    results_path = Path("examples_results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"Example results saved to {results_path}")
    return results


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_all_examples())