"""Performance benchmark tests for services."""

import asyncio
import statistics
import time
from unittest.mock import AsyncMock, patch

import pytest

from src.agent_orchestrated_etl.services.integration_service import IntegrationService
from src.agent_orchestrated_etl.services.intelligence_service import IntelligenceService
from src.agent_orchestrated_etl.services.optimization_service import OptimizationService
from src.agent_orchestrated_etl.services.pipeline_service import PipelineService


@pytest.mark.performance
@pytest.mark.slow
class TestPipelineServicePerformance:
    """Performance benchmarks for PipelineService."""

    @pytest.fixture
    def pipeline_service_bench(self, mock_db_manager):
        """Pipeline service for benchmarking."""
        return PipelineService(db_manager=mock_db_manager)

    @pytest.mark.asyncio
    async def test_pipeline_creation_performance(self, pipeline_service_bench):
        """Benchmark pipeline creation performance."""
        source_config = {
            "type": "file",
            "path": "/data/test.csv",
            "format": "csv"
        }
        transformation_rules = [
            {"type": "data_cleaning", "parameters": {"remove_nulls": True}}
        ]
        load_config = {
            "type": "database",
            "table": "processed_data"
        }

        # Warm-up
        await pipeline_service_bench.create_pipeline(
            source_config, transformation_rules, load_config
        )

        # Benchmark multiple pipeline creations
        iterations = 100
        times = []

        for i in range(iterations):
            start_time = time.perf_counter()

            await pipeline_service_bench.create_pipeline(
                source_config,
                transformation_rules,
                load_config,
                pipeline_id=f"perf-test-{i}"
            )

            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)  # Convert to milliseconds

        # Performance assertions
        avg_time = statistics.mean(times)
        p95_time = statistics.quantiles(times, n=20)[18]  # 95th percentile

        print("\nPipeline Creation Performance:")
        print(f"  Average time: {avg_time:.2f} ms")
        print(f"  95th percentile: {p95_time:.2f} ms")
        print(f"  Min time: {min(times):.2f} ms")
        print(f"  Max time: {max(times):.2f} ms")

        # Performance requirements
        assert avg_time < 100.0, f"Average pipeline creation time {avg_time:.2f}ms exceeds 100ms"
        assert p95_time < 200.0, f"P95 pipeline creation time {p95_time:.2f}ms exceeds 200ms"

    @pytest.mark.asyncio
    async def test_concurrent_pipeline_execution(self, pipeline_service_bench):
        """Benchmark concurrent pipeline executions."""
        # Setup mock to simulate realistic execution times
        async def mock_create_execution(*args, **kwargs):
            await asyncio.sleep(0.01)  # Simulate 10ms database operation
            return f"exec-{time.time()}"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution = mock_create_execution
            mock_repo_class.return_value = mock_repo

            # Benchmark concurrent executions
            concurrent_executions = 50
            start_time = time.perf_counter()

            tasks = [
                pipeline_service_bench.execute_pipeline(f"pipeline-{i}")
                for i in range(concurrent_executions)
            ]

            results = await asyncio.gather(*tasks)

            end_time = time.perf_counter()
            total_time = (end_time - start_time) * 1000

            print("\nConcurrent Pipeline Execution Performance:")
            print(f"  {concurrent_executions} concurrent executions")
            print(f"  Total time: {total_time:.2f} ms")
            print(f"  Average time per execution: {total_time / concurrent_executions:.2f} ms")
            print(f"  Throughput: {concurrent_executions / (total_time / 1000):.1f} executions/second")

            # All executions should succeed
            assert len(results) == concurrent_executions

            # Performance requirements
            assert total_time < 5000.0, f"Concurrent execution time {total_time:.2f}ms exceeds 5000ms"
            throughput = concurrent_executions / (total_time / 1000)
            assert throughput > 20.0, f"Throughput {throughput:.1f} executions/sec is below 20/sec"

    @pytest.mark.asyncio
    async def test_pipeline_status_query_performance(self, pipeline_service_bench):
        """Benchmark pipeline status query performance."""
        # Setup mock data
        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_execution = AsyncMock()
            mock_execution.pipeline_id = "test-pipeline"
            mock_execution.status.value = "completed"
            mock_repo.get_latest_execution.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            # Benchmark status queries
            iterations = 500
            times = []

            for i in range(iterations):
                start_time = time.perf_counter()

                await pipeline_service_bench.get_pipeline_status("test-pipeline")

                end_time = time.perf_counter()
                times.append((end_time - start_time) * 1000)

            avg_time = statistics.mean(times)
            p95_time = statistics.quantiles(times, n=20)[18]

            print("\nPipeline Status Query Performance:")
            print(f"  {iterations} status queries")
            print(f"  Average time: {avg_time:.2f} ms")
            print(f"  95th percentile: {p95_time:.2f} ms")

            # Performance requirements
            assert avg_time < 50.0, f"Average status query time {avg_time:.2f}ms exceeds 50ms"
            assert p95_time < 100.0, f"P95 status query time {p95_time:.2f}ms exceeds 100ms"


@pytest.mark.performance
@pytest.mark.slow
class TestIntelligenceServicePerformance:
    """Performance benchmarks for IntelligenceService."""

    @pytest.fixture
    def intelligence_service_bench(self, mock_db_manager):
        """Intelligence service for benchmarking."""
        return IntelligenceService(db_manager=mock_db_manager)

    @pytest.mark.asyncio
    async def test_pipeline_analysis_performance(self, intelligence_service_bench):
        """Benchmark pipeline requirements analysis."""
        source_config = {
            "type": "database",
            "table": "large_table",
            "estimated_records": 1000000
        }
        business_requirements = {
            "data_quality": "high",
            "performance": "critical",
            "compliance": ["GDPR", "SOX", "HIPAA"]
        }

        # Warm-up
        await intelligence_service_bench.analyze_pipeline_requirements(
            source_config, business_requirements
        )

        # Benchmark analysis performance
        iterations = 50
        times = []

        for i in range(iterations):
            # Vary the source config slightly to avoid caching
            varied_config = source_config.copy()
            varied_config["estimated_records"] = 1000000 + i

            start_time = time.perf_counter()

            await intelligence_service_bench.analyze_pipeline_requirements(
                varied_config, business_requirements
            )

            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)

        avg_time = statistics.mean(times)
        p95_time = statistics.quantiles(times, n=20)[18]

        print("\nPipeline Analysis Performance:")
        print(f"  Average time: {avg_time:.2f} ms")
        print(f"  95th percentile: {p95_time:.2f} ms")

        # Performance requirements
        assert avg_time < 500.0, f"Average analysis time {avg_time:.2f}ms exceeds 500ms"
        assert p95_time < 1000.0, f"P95 analysis time {p95_time:.2f}ms exceeds 1000ms"

    @pytest.mark.asyncio
    async def test_prediction_model_performance(self, intelligence_service_bench):
        """Benchmark prediction model performance."""
        pipeline_config = {
            "source": {"type": "database", "records": 100000},
            "transformations": [
                {"type": "data_cleaning", "complexity": "medium"},
                {"type": "aggregation", "complexity": "high"}
            ],
            "destination": {"type": "file"}
        }
        execution_context = {
            "resources": {"memory_gb": 8, "cpu_cores": 4},
            "priority": "high"
        }

        # Mock historical data
        intelligence_service_bench.db_manager.execute_query.return_value = [
            {"avg_execution_time": 25.5, "success_rate": 0.95, "avg_memory_usage": 4.2}
        ]

        # Benchmark prediction performance
        iterations = 100
        times = []

        for i in range(iterations):
            start_time = time.perf_counter()

            await intelligence_service_bench.predict_pipeline_behavior(
                pipeline_config, execution_context
            )

            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)

        avg_time = statistics.mean(times)
        throughput = iterations / (sum(times) / 1000)

        print("\nPrediction Model Performance:")
        print(f"  Average time: {avg_time:.2f} ms")
        print(f"  Throughput: {throughput:.1f} predictions/second")

        # Performance requirements
        assert avg_time < 100.0, f"Average prediction time {avg_time:.2f}ms exceeds 100ms"
        assert throughput > 50.0, f"Throughput {throughput:.1f} predictions/sec is below 50/sec"


@pytest.mark.performance
@pytest.mark.slow
class TestOptimizationServicePerformance:
    """Performance benchmarks for OptimizationService."""

    @pytest.fixture
    def optimization_service_bench(self, mock_db_manager):
        """Optimization service for benchmarking."""
        return OptimizationService(db_manager=mock_db_manager)

    @pytest.mark.asyncio
    async def test_optimization_algorithm_performance(self, optimization_service_bench):
        """Benchmark optimization algorithm performance."""
        current_config = {
            "batch_size": 1000,
            "parallel_workers": 2,
            "memory_limit_gb": 4,
            "timeout_minutes": 60
        }
        performance_data = {
            "avg_execution_time_minutes": 30,
            "avg_memory_usage_gb": 3.2,
            "avg_cpu_utilization": 0.6,
            "throughput_records_per_second": 1000
        }
        optimization_goals = {
            "primary": "reduce_execution_time",
            "target_reduction_percent": 25
        }

        # Benchmark optimization performance
        iterations = 20
        times = []

        for i in range(iterations):
            start_time = time.perf_counter()

            await optimization_service_bench.optimize_pipeline_configuration(
                f"pipeline-{i}", current_config, performance_data, optimization_goals
            )

            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)

        avg_time = statistics.mean(times)
        p95_time = statistics.quantiles(times, n=20)[18]

        print("\nOptimization Algorithm Performance:")
        print(f"  Average time: {avg_time:.2f} ms")
        print(f"  95th percentile: {p95_time:.2f} ms")

        # Performance requirements
        assert avg_time < 1000.0, f"Average optimization time {avg_time:.2f}ms exceeds 1000ms"
        assert p95_time < 2000.0, f"P95 optimization time {p95_time:.2f}ms exceeds 2000ms"

    @pytest.mark.asyncio
    async def test_real_time_optimization_performance(self, optimization_service_bench):
        """Benchmark real-time optimization performance."""
        live_metrics = {
            "current_throughput": 800,
            "memory_usage_percent": 75,
            "cpu_utilization": 0.8,
            "error_rate": 0.05
        }

        # Benchmark real-time optimization
        iterations = 200
        times = []

        for i in range(iterations):
            start_time = time.perf_counter()

            await optimization_service_bench.real_time_optimization(
                f"pipeline-{i}", live_metrics, 0.2
            )

            end_time = time.perf_counter()
            times.append((end_time - start_time) * 1000)

        avg_time = statistics.mean(times)
        p99_time = statistics.quantiles(times, n=100)[98]  # 99th percentile

        print("\nReal-time Optimization Performance:")
        print(f"  Average time: {avg_time:.2f} ms")
        print(f"  99th percentile: {p99_time:.2f} ms")

        # Real-time optimization should be very fast
        assert avg_time < 50.0, f"Average real-time optimization {avg_time:.2f}ms exceeds 50ms"
        assert p99_time < 200.0, f"P99 real-time optimization {p99_time:.2f}ms exceeds 200ms"


@pytest.mark.performance
@pytest.mark.slow
class TestIntegrationServicePerformance:
    """Performance benchmarks for IntegrationService."""

    @pytest.fixture
    def integration_service_bench(self, mock_db_manager):
        """Integration service for benchmarking."""
        return IntegrationService(db_manager=mock_db_manager)

    @pytest.mark.asyncio
    async def test_api_integration_performance(self, integration_service_bench):
        """Benchmark API integration performance."""
        integration_id = "api-perf-test"
        operation = "fetch_data"
        params = {
            "url": "https://api.example.com/data",
            "method": "GET"
        }

        # Mock HTTP responses with realistic delay
        async def mock_http_call(*args, **kwargs):
            await asyncio.sleep(0.1)  # Simulate 100ms API call
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"data": "test"}
            return mock_response

        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value.__aenter__ = mock_http_call

            # Benchmark API integration performance
            iterations = 50
            times = []

            for i in range(iterations):
                start_time = time.perf_counter()

                await integration_service_bench.execute_integration(
                    integration_id, operation, params, {}
                )

                end_time = time.perf_counter()
                times.append((end_time - start_time) * 1000)

            avg_time = statistics.mean(times)
            overhead_time = avg_time - 100  # Subtract simulated API time

            print("\nAPI Integration Performance:")
            print(f"  Average total time: {avg_time:.2f} ms")
            print(f"  Integration overhead: {overhead_time:.2f} ms")
            print(f"  Throughput: {iterations / (sum(times) / 1000):.1f} calls/second")

            # Performance requirements
            assert overhead_time < 50.0, f"Integration overhead {overhead_time:.2f}ms exceeds 50ms"

    @pytest.mark.asyncio
    async def test_concurrent_integrations_performance(self, integration_service_bench):
        """Benchmark concurrent integration executions."""
        integrations = [
            {
                "id": f"concurrent-{i}",
                "operation": "fetch_data",
                "params": {"url": f"https://api{i}.example.com/data"}
            }
            for i in range(25)
        ]

        # Mock HTTP responses
        with patch('aiohttp.ClientSession.get') as mock_get:
            async def mock_response(*args, **kwargs):
                await asyncio.sleep(0.05)  # 50ms simulated response time
                mock_resp = AsyncMock()
                mock_resp.status = 200
                mock_resp.json.return_value = {"data": "test"}
                return mock_resp

            mock_get.return_value.__aenter__ = mock_response

            start_time = time.perf_counter()

            results = await integration_service_bench.execute_batch_integrations(
                integrations, {"parallel": True}
            )

            end_time = time.perf_counter()
            total_time = (end_time - start_time) * 1000

            print("\nConcurrent Integrations Performance:")
            print(f"  {len(integrations)} concurrent integrations")
            print(f"  Total time: {total_time:.2f} ms")
            print(f"  Expected sequential time: ~{len(integrations) * 50:.0f} ms")
            print(f"  Concurrency benefit: {(len(integrations) * 50) / total_time:.1f}x")

            # All integrations should succeed
            assert len(results) == len(integrations)
            assert all(result["success"] for result in results)

            # Should be significantly faster than sequential execution
            expected_sequential = len(integrations) * 50
            assert total_time < expected_sequential * 0.5, "Concurrency benefit insufficient"

    @pytest.mark.asyncio
    async def test_circuit_breaker_performance(self, integration_service_bench):
        """Benchmark circuit breaker performance impact."""
        integration_id = "circuit-breaker-perf"

        # Test with circuit breaker closed (normal operation)
        times_normal = []
        for i in range(100):
            start_time = time.perf_counter()
            integration_service_bench._check_circuit_breaker(integration_id)
            end_time = time.perf_counter()
            times_normal.append((end_time - start_time) * 1000000)  # microseconds

        # Test with circuit breaker open (fast failure)
        integration_service_bench.circuit_breakers[integration_id] = {
            "state": "open",
            "failure_count": 10,
            "last_failure": time.time(),
            "failure_threshold": 5,
            "timeout_seconds": 60
        }

        times_open = []
        for i in range(100):
            start_time = time.perf_counter()
            integration_service_bench._check_circuit_breaker(integration_id)
            end_time = time.perf_counter()
            times_open.append((end_time - start_time) * 1000000)  # microseconds

        avg_normal = statistics.mean(times_normal)
        avg_open = statistics.mean(times_open)

        print("\nCircuit Breaker Performance:")
        print(f"  Normal operation: {avg_normal:.1f} μs")
        print(f"  Open circuit: {avg_open:.1f} μs")
        print(f"  Performance impact: {((avg_normal - avg_open) / avg_normal * 100):+.1f}%")

        # Circuit breaker should have minimal performance impact
        assert avg_normal < 100.0, f"Circuit breaker check too slow: {avg_normal:.1f}μs"
        assert avg_open < 100.0, f"Open circuit check too slow: {avg_open:.1f}μs"


@pytest.mark.performance
@pytest.mark.slow
class TestOverallSystemPerformance:
    """End-to-end system performance benchmarks."""

    @pytest.mark.asyncio
    async def test_complete_pipeline_flow_performance(
        self,
        mock_db_manager,
        mock_intelligence_service,
        mock_optimization_service
    ):
        """Benchmark complete pipeline creation and execution flow."""
        pipeline_service = PipelineService(
            db_manager=mock_db_manager,
            intelligence_service=mock_intelligence_service,
            optimization_service=mock_optimization_service
        )

        source_config = {
            "type": "file",
            "path": "/data/performance_test.csv",
            "format": "csv"
        }
        transformation_rules = [
            {"type": "data_cleaning", "parameters": {"remove_nulls": True}},
            {"type": "data_validation", "parameters": {"schema": "test_schema.json"}}
        ]
        load_config = {
            "type": "database",
            "table": "performance_results"
        }

        # Mock repository for execution
        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.return_value = "exec-perf-test"
            mock_repo_class.return_value = mock_repo

            # Benchmark complete flow
            iterations = 10
            times = []

            for i in range(iterations):
                start_time = time.perf_counter()

                # Step 1: Create pipeline
                pipeline_result = await pipeline_service.create_pipeline(
                    source_config, transformation_rules, load_config,
                    pipeline_id=f"perf-pipeline-{i}"
                )

                # Step 2: Execute pipeline
                execution_result = await pipeline_service.execute_pipeline(
                    pipeline_result["pipeline_id"]
                )

                # Step 3: Check status
                status_result = await pipeline_service.get_pipeline_status(
                    pipeline_result["pipeline_id"]
                )

                end_time = time.perf_counter()
                times.append((end_time - start_time) * 1000)

            avg_time = statistics.mean(times)
            p95_time = statistics.quantiles(times, n=20)[18]

            print("\nComplete Pipeline Flow Performance:")
            print(f"  Average time: {avg_time:.2f} ms")
            print(f"  95th percentile: {p95_time:.2f} ms")
            print(f"  Throughput: {iterations / (sum(times) / 1000):.1f} pipelines/second")

            # Performance requirements for complete flow
            assert avg_time < 2000.0, f"Average complete flow time {avg_time:.2f}ms exceeds 2000ms"
            assert p95_time < 5000.0, f"P95 complete flow time {p95_time:.2f}ms exceeds 5000ms"

    @pytest.mark.asyncio
    async def test_memory_usage_performance(self, mock_db_manager):
        """Test memory usage under load."""
        import gc
        import tracemalloc

        # Start memory tracing
        tracemalloc.start()

        pipeline_service = PipelineService(db_manager=mock_db_manager)

        # Create many pipeline objects
        pipelines = []
        for i in range(1000):
            source_config = {"type": "file", "path": f"/data/test_{i}.csv"}
            transformation_rules = [{"type": "cleaning", "params": {"id": i}}]
            load_config = {"type": "database", "table": f"table_{i}"}

            pipeline_result = await pipeline_service.create_pipeline(
                source_config, transformation_rules, load_config
            )
            pipelines.append(pipeline_result)

            # Trigger garbage collection every 100 iterations
            if i % 100 == 0:
                gc.collect()

        # Get memory usage snapshot
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        current_mb = current / 1024 / 1024
        peak_mb = peak / 1024 / 1024

        print("\nMemory Usage Performance:")
        print(f"  Current memory: {current_mb:.2f} MB")
        print(f"  Peak memory: {peak_mb:.2f} MB")
        print(f"  Memory per pipeline: {current_mb / len(pipelines):.3f} MB")

        # Memory usage should be reasonable
        assert current_mb < 100.0, f"Current memory usage {current_mb:.2f}MB exceeds 100MB"
        assert peak_mb < 200.0, f"Peak memory usage {peak_mb:.2f}MB exceeds 200MB"
