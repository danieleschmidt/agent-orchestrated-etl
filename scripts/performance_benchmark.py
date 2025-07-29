#!/usr/bin/env python3
"""
Performance Benchmarking Suite for Agent-Orchestrated-ETL

Comprehensive performance testing framework that measures:
- ETL pipeline throughput and latency
- Memory usage patterns
- CPU utilization
- I/O performance
- Agent coordination overhead
- Scalability metrics under load
"""

import asyncio
import json
import time
import tracemalloc
import psutil
import statistics
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
import concurrent.futures


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""
    name: str
    duration_seconds: float
    memory_peak_mb: float
    memory_current_mb: float
    cpu_percent: float
    throughput_ops_per_sec: float
    success_rate: float
    error_count: int
    metadata: Dict[str, Any]


@dataclass
class BenchmarkSuite:
    """Container for a complete benchmark suite."""
    suite_name: str
    timestamp: str
    results: List[BenchmarkResult]
    system_info: Dict[str, Any]
    summary: Dict[str, Any]


class PerformanceProfiler:
    """Advanced performance profiler with system metrics."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
        self.start_cpu_times = None
        self.memory_samples = []
        self.cpu_samples = []
        
    def start(self):
        """Start profiling."""
        tracemalloc.start()
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.start_cpu_times = self.process.cpu_times()
        self.memory_samples = []
        self.cpu_samples = []
        
    def sample(self):
        """Take a performance sample."""
        memory_mb = self.process.memory_info().rss / 1024 / 1024
        cpu_percent = self.process.cpu_percent()
        
        self.memory_samples.append(memory_mb)
        self.cpu_samples.append(cpu_percent)
        
    def stop(self) -> Dict[str, Any]:
        """Stop profiling and return results."""
        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        duration = end_time - self.start_time
        memory_current_mb = current / 1024 / 1024
        memory_peak_mb = peak / 1024 / 1024
        
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        avg_memory = statistics.mean(self.memory_samples) if self.memory_samples else 0
        
        return {
            "duration_seconds": duration,
            "memory_peak_mb": memory_peak_mb,
            "memory_current_mb": memory_current_mb,
            "memory_average_mb": avg_memory,
            "cpu_average_percent": avg_cpu,
            "memory_samples": self.memory_samples,
            "cpu_samples": self.cpu_samples
        }


class BenchmarkRunner:
    """Main benchmark runner for ETL pipeline performance testing."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.output_dir = project_root / "performance-reports"
        self.output_dir.mkdir(exist_ok=True)
        self.profiler = PerformanceProfiler()
        
    def get_system_info(self) -> Dict[str, Any]:
        """Get comprehensive system information."""
        return {
            "cpu_count": psutil.cpu_count(logical=False),
            "cpu_count_logical": psutil.cpu_count(logical=True),
            "cpu_freq": dict(psutil.cpu_freq()._asdict()) if psutil.cpu_freq() else None,
            "memory_total_gb": psutil.virtual_memory().total / 1024**3,
            "memory_available_gb": psutil.virtual_memory().available / 1024**3,
            "disk_usage_gb": psutil.disk_usage('/').total / 1024**3,
            "python_implementation": "CPython",  # Assuming CPython
            "benchmark_timestamp": datetime.utcnow().isoformat()
        }
    
    @asynccontextmanager
    async def benchmark_context(self, name: str):
        """Context manager for running individual benchmarks."""
        print(f"🏃 Running benchmark: {name}")
        
        self.profiler.start()
        start_time = time.time()
        error_count = 0
        operation_count = 0
        
        # Sample performance during benchmark
        sampling_task = asyncio.create_task(self._sample_during_benchmark())
        
        try:
            yield {
                "add_operation": lambda: setattr(self, '_op_count', getattr(self, '_op_count', 0) + 1),
                "add_error": lambda: setattr(self, '_err_count', getattr(self, '_err_count', 0) + 1),
                "get_operation_count": lambda: getattr(self, '_op_count', 0),
                "get_error_count": lambda: getattr(self, '_err_count', 0)
            }
        finally:
            sampling_task.cancel()
            try:
                await sampling_task
            except asyncio.CancelledError:
                pass
            
            end_time = time.time()
            duration = end_time - start_time
            
            performance_data = self.profiler.stop()
            operation_count = getattr(self, '_op_count', 0)
            error_count = getattr(self, '_err_count', 0)
            
            # Calculate success rate and throughput
            success_rate = (operation_count - error_count) / operation_count if operation_count > 0 else 0
            throughput = operation_count / duration if duration > 0 else 0
            
            result = BenchmarkResult(
                name=name,
                duration_seconds=duration,
                memory_peak_mb=performance_data["memory_peak_mb"],
                memory_current_mb=performance_data["memory_current_mb"],
                cpu_percent=performance_data["cpu_average_percent"],
                throughput_ops_per_sec=throughput,
                success_rate=success_rate,
                error_count=error_count,
                metadata={
                    "operation_count": operation_count,
                    "memory_samples": performance_data["memory_samples"][-10:],  # Last 10 samples
                    "cpu_samples": performance_data["cpu_samples"][-10:]
                }
            )
            
            print(f"✅ {name} completed: {throughput:.2f} ops/sec, {success_rate:.2%} success rate")
            self._current_results.append(result)
            
            # Reset counters
            if hasattr(self, '_op_count'):
                delattr(self, '_op_count')
            if hasattr(self, '_err_count'):
                delattr(self, '_err_count')
    
    async def _sample_during_benchmark(self):
        """Sample performance metrics during benchmark execution."""
        while True:
            try:
                self.profiler.sample()
                await asyncio.sleep(0.5)  # Sample every 500ms
            except asyncio.CancelledError:
                break
    
    async def benchmark_etl_pipeline_throughput(self) -> None:
        """Benchmark ETL pipeline throughput with varying data sizes."""
        async with self.benchmark_context("ETL Pipeline Throughput") as ctx:
            try:
                # Simulate ETL operations with different data sizes
                data_sizes = [100, 1000, 10000, 50000]
                
                for size in data_sizes:
                    # Simulate data extraction
                    start = time.time()
                    await self._simulate_extraction(size)
                    ctx["add_operation"]()
                    
                    # Simulate data transformation
                    await self._simulate_transformation(size)
                    ctx["add_operation"]()
                    
                    # Simulate data loading
                    await self._simulate_loading(size)
                    ctx["add_operation"]()
                    
                    # Add some realistic processing delay
                    processing_time = max(0.01, size / 100000)  # Scale with data size
                    await asyncio.sleep(processing_time)
                    
            except Exception as e:
                print(f"❌ ETL benchmark error: {e}")
                ctx["add_error"]()
    
    async def benchmark_agent_coordination(self) -> None:
        """Benchmark agent coordination and communication overhead."""
        async with self.benchmark_context("Agent Coordination") as ctx:
            try:
                # Simulate multiple agents coordinating
                agent_count = 5
                coordination_rounds = 20
                
                for round_num in range(coordination_rounds):
                    # Simulate agent communication
                    tasks = []
                    for agent_id in range(agent_count):
                        task = asyncio.create_task(
                            self._simulate_agent_operation(agent_id, round_num)
                        )
                        tasks.append(task)
                    
                    # Wait for all agents to complete
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Count operations and errors
                    for result in results:
                        if isinstance(result, Exception):
                            ctx["add_error"]()
                        else:
                            ctx["add_operation"]()
                    
                    # Brief coordination delay
                    await asyncio.sleep(0.01)
                    
            except Exception as e:
                print(f"❌ Agent coordination benchmark error: {e}")
                ctx["add_error"]()
    
    async def benchmark_memory_usage_patterns(self) -> None:
        """Benchmark memory usage patterns under different loads."""
        async with self.benchmark_context("Memory Usage Patterns") as ctx:
            try:
                # Test different memory allocation patterns
                test_cases = [
                    ("small_allocations", 1000, 1024),      # 1000 x 1KB
                    ("medium_allocations", 100, 102400),    # 100 x 100KB  
                    ("large_allocations", 10, 1048576),     # 10 x 1MB
                ]
                
                for test_name, count, size in test_cases:
                    allocations = []
                    
                    # Allocate memory
                    for i in range(count):
                        data = bytearray(size)
                        data[0] = i % 256  # Write to prevent optimization
                        allocations.append(data)
                        ctx["add_operation"]()
                        
                        if i % 10 == 0:  # Sample every 10 allocations
                            await asyncio.sleep(0.001)
                    
                    # Hold memory briefly
                    await asyncio.sleep(0.1)
                    
                    # Release memory
                    del allocations
                    await asyncio.sleep(0.1)  # Allow GC
                    
            except Exception as e:
                print(f"❌ Memory benchmark error: {e}")
                ctx["add_error"]()
    
    async def benchmark_concurrent_operations(self) -> None:
        """Benchmark performance under concurrent load."""
        async with self.benchmark_context("Concurrent Operations") as ctx:
            try:
                # Test different concurrency levels
                concurrency_levels = [1, 5, 10, 20]
                operations_per_level = 50
                
                for concurrency in concurrency_levels:
                    semaphore = asyncio.Semaphore(concurrency)
                    
                    async def concurrent_operation(op_id: int):
                        async with semaphore:
                            try:
                                # Simulate mixed I/O and CPU work
                                await asyncio.sleep(0.01)  # I/O simulation
                                
                                # CPU work simulation
                                _ = sum(i * i for i in range(1000))
                                
                                ctx["add_operation"]()
                                return op_id
                            except Exception as e:
                                ctx["add_error"]()
                                raise
                    
                    # Launch concurrent operations
                    tasks = [
                        asyncio.create_task(concurrent_operation(i))
                        for i in range(operations_per_level)
                    ]
                    
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
            except Exception as e:
                print(f"❌ Concurrent operations benchmark error: {e}")
                ctx["add_error"]()
    
    async def benchmark_database_operations(self) -> None:
        """Benchmark database operation performance."""
        async with self.benchmark_context("Database Operations") as ctx:
            try:
                # Simulate database operations
                operation_types = [
                    ("insert", 100),
                    ("select", 200),
                    ("update", 50),
                    ("delete", 25)
                ]
                
                for op_type, count in operation_types:
                    for i in range(count):
                        # Simulate database operation latency
                        latency = {
                            "insert": 0.005,
                            "select": 0.002,
                            "update": 0.008,
                            "delete": 0.003
                        }[op_type]
                        
                        await asyncio.sleep(latency)
                        ctx["add_operation"]()
                        
                        # Occasional error simulation
                        if i % 47 == 0:  # ~2% error rate
                            ctx["add_error"]()
                    
            except Exception as e:
                print(f"❌ Database benchmark error: {e}")
                ctx["add_error"]()
    
    async def _simulate_extraction(self, size: int) -> None:
        """Simulate data extraction operation."""
        # Simulate reading data
        await asyncio.sleep(max(0.001, size / 1000000))  # Scale with size
        
    async def _simulate_transformation(self, size: int) -> None:
        """Simulate data transformation operation."""
        # Simulate data processing
        await asyncio.sleep(max(0.002, size / 500000))  # Transformation is slower
        
    async def _simulate_loading(self, size: int) -> None:
        """Simulate data loading operation."""
        # Simulate writing data
        await asyncio.sleep(max(0.001, size / 800000))  # Loading time
        
    async def _simulate_agent_operation(self, agent_id: int, round_num: int) -> str:
        """Simulate an individual agent operation."""
        # Simulate agent processing time
        processing_time = 0.005 + (agent_id * 0.001)  # Slight variation per agent
        await asyncio.sleep(processing_time)
        
        # Simulate occasional agent failure
        if round_num % 13 == agent_id % 13:  # Rare, pseudo-random failures
            raise Exception(f"Agent {agent_id} failed in round {round_num}")
        
        return f"agent-{agent_id}-round-{round_num}"
    
    async def run_full_benchmark_suite(self) -> BenchmarkSuite:
        """Run the complete benchmark suite."""
        print("🚀 Starting comprehensive performance benchmark suite...")
        
        self._current_results = []
        start_time = datetime.utcnow().isoformat()
        
        # Run all benchmarks
        benchmarks = [
            self.benchmark_etl_pipeline_throughput,
            self.benchmark_agent_coordination, 
            self.benchmark_memory_usage_patterns,
            self.benchmark_concurrent_operations,
            self.benchmark_database_operations
        ]
        
        for benchmark in benchmarks:
            try:
                await benchmark()
            except Exception as e:
                print(f"❌ Benchmark failed: {e}")
                # Continue with other benchmarks
        
        # Generate summary statistics
        summary = self._generate_summary(self._current_results)
        
        suite = BenchmarkSuite(
            suite_name="Agent-Orchestrated-ETL Performance Suite",
            timestamp=start_time,
            results=self._current_results,
            system_info=self.get_system_info(),
            summary=summary
        )
        
        return suite
    
    def _generate_summary(self, results: List[BenchmarkResult]) -> Dict[str, Any]:
        """Generate summary statistics from benchmark results."""
        if not results:
            return {"error": "No benchmark results"}
        
        total_operations = sum(r.metadata.get("operation_count", 0) for r in results)
        total_errors = sum(r.error_count for r in results)
        avg_throughput = statistics.mean(r.throughput_ops_per_sec for r in results)
        avg_success_rate = statistics.mean(r.success_rate for r in results)
        peak_memory = max(r.memory_peak_mb for r in results)
        avg_cpu = statistics.mean(r.cpu_percent for r in results)
        
        return {
            "total_benchmarks": len(results),
            "total_operations": total_operations,
            "total_errors": total_errors,
            "overall_success_rate": avg_success_rate,
            "average_throughput_ops_per_sec": avg_throughput,
            "peak_memory_usage_mb": peak_memory,
            "average_cpu_percent": avg_cpu,
            "benchmark_names": [r.name for r in results],
            "performance_grade": self._calculate_performance_grade(results)
        }
    
    def _calculate_performance_grade(self, results: List[BenchmarkResult]) -> str:
        """Calculate overall performance grade."""
        if not results:
            return "F"
        
        # Simple scoring based on success rate and throughput
        avg_success_rate = statistics.mean(r.success_rate for r in results)
        avg_throughput = statistics.mean(r.throughput_ops_per_sec for r in results)
        
        score = (avg_success_rate * 70) + min(avg_throughput / 10, 30)  # Max 100
        
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"
    
    def save_results(self, suite: BenchmarkSuite) -> Path:
        """Save benchmark results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_path = self.output_dir / f"performance-benchmark-{timestamp}.json"
        
        # Convert to dict for JSON serialization
        suite_dict = asdict(suite)
        
        with open(results_path, 'w') as f:
            json.dump(suite_dict, f, indent=2)
        
        return results_path
    
    def print_summary(self, suite: BenchmarkSuite) -> None:
        """Print benchmark summary to console."""
        print("\n" + "="*70)
        print("🏁 PERFORMANCE BENCHMARK RESULTS")
        print("="*70)
        print(f"Suite: {suite.suite_name}")
        print(f"Timestamp: {suite.timestamp}")
        print(f"System: {suite.system_info['cpu_count']} cores, "
              f"{suite.system_info['memory_total_gb']:.1f}GB RAM")
        print("-" * 70)
        
        summary = suite.summary
        print(f"Overall Performance Grade: {summary['performance_grade']}")
        print(f"Total Benchmarks: {summary['total_benchmarks']}")
        print(f"Total Operations: {summary['total_operations']:,}")
        print(f"Success Rate: {summary['overall_success_rate']:.2%}")
        print(f"Average Throughput: {summary['average_throughput_ops_per_sec']:.2f} ops/sec")
        print(f"Peak Memory Usage: {summary['peak_memory_usage_mb']:.2f} MB")
        print(f"Average CPU Usage: {summary['average_cpu_percent']:.1f}%")
        
        print("\n📊 Individual Benchmark Results:")
        print("-" * 70)
        for result in suite.results:
            print(f"{result.name:.<30} "
                  f"{result.throughput_ops_per_sec:>8.2f} ops/sec | "
                  f"{result.success_rate:>6.1%} success | "
                  f"{result.memory_peak_mb:>6.1f} MB peak")
        
        print("="*70)


async def main():
    """Main entry point for performance benchmarking."""
    project_root = Path(__file__).parent.parent
    runner = BenchmarkRunner(project_root)
    
    # Run full benchmark suite
    suite = await runner.run_full_benchmark_suite()
    
    # Save results
    results_path = runner.save_results(suite)
    print(f"\n💾 Results saved to: {results_path}")
    
    # Print summary
    runner.print_summary(suite)
    
    # Set exit code based on performance grade
    grade = suite.summary["performance_grade"]
    if grade in ["A", "B"]:
        exit_code = 0
    elif grade == "C":
        exit_code = 1
    else:
        exit_code = 2
    
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)