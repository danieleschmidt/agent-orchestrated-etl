#!/usr/bin/env python3
"""
Advanced Performance Benchmark Suite
Comprehensive performance analysis and optimization for agent-orchestrated-etl
"""

import asyncio
import time
import psutil
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import json
import sys
from pathlib import Path

@dataclass
class BenchmarkResult:
    """Performance benchmark result data structure."""
    name: str
    duration: float
    memory_peak: float
    cpu_percent: float
    throughput: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None

class PerformanceBenchmarker:
    """Advanced performance benchmarking and optimization analysis."""
    
    def __init__(self, output_file: str = "performance-report.json"):
        self.output_file = output_file
        self.results: List[BenchmarkResult] = []
        self.baseline_metrics: Dict[str, float] = {}
    
    async def benchmark_function(
        self, 
        func: Callable, 
        name: str, 
        *args, 
        iterations: int = 10,
        **kwargs
    ) -> BenchmarkResult:
        """Benchmark a function with comprehensive metrics."""
        process = psutil.Process()
        durations = []
        memory_peaks = []
        cpu_percentages = []
        
        # Warmup run
        if asyncio.iscoroutinefunction(func):
            await func(*args, **kwargs)
        else:
            func(*args, **kwargs)
        
        for _ in range(iterations):
            # Reset metrics
            process.cpu_percent()  # First call for baseline
            memory_before = process.memory_info().rss / 1024 / 1024  # MB
            
            start_time = time.perf_counter()
            
            if asyncio.iscoroutinefunction(func):
                await func(*args, **kwargs)
            else:
                func(*args, **kwargs)
            
            duration = time.perf_counter() - start_time
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            cpu_percent = process.cpu_percent()
            
            durations.append(duration)
            memory_peaks.append(memory_after - memory_before)
            cpu_percentages.append(cpu_percent)
        
        result = BenchmarkResult(
            name=name,
            duration=statistics.mean(durations),
            memory_peak=statistics.mean(memory_peaks),
            cpu_percent=statistics.mean(cpu_percentages),
            metadata={
                "duration_std": statistics.stdev(durations) if len(durations) > 1 else 0,
                "memory_std": statistics.stdev(memory_peaks) if len(memory_peaks) > 1 else 0,
                "iterations": iterations,
                "min_duration": min(durations),
                "max_duration": max(durations)
            }
        )
        
        self.results.append(result)
        return result
    
    def benchmark_concurrent_performance(
        self, 
        func: Callable, 
        name: str, 
        concurrency_levels: List[int] = [1, 2, 4, 8, 16]
    ) -> Dict[int, BenchmarkResult]:
        """Benchmark function performance under different concurrency levels."""
        results = {}
        
        for level in concurrency_levels:
            start_time = time.perf_counter()
            
            with ThreadPoolExecutor(max_workers=level) as executor:
                futures = [executor.submit(func) for _ in range(level)]
                for future in futures:
                    future.result()
            
            duration = time.perf_counter() - start_time
            throughput = level / duration if duration > 0 else 0
            
            result = BenchmarkResult(
                name=f"{name}_concurrent_{level}",
                duration=duration,
                memory_peak=0,  # Would need more sophisticated tracking
                cpu_percent=0,  # Would need more sophisticated tracking
                throughput=throughput,
                metadata={"concurrency_level": level}
            )
            
            results[level] = result
            self.results.append(result)
        
        return results
    
    def analyze_performance_regression(self, baseline_file: str) -> Dict[str, float]:
        """Compare current performance against baseline metrics."""
        try:
            with open(baseline_file, 'r') as f:
                baseline = json.load(f)
                self.baseline_metrics = baseline.get('baseline_metrics', {})
        except FileNotFoundError:
            print(f"Baseline file {baseline_file} not found, creating new baseline")
            return {}
        
        regressions = {}
        for result in self.results:
            baseline_duration = self.baseline_metrics.get(result.name)
            if baseline_duration:
                regression = (result.duration - baseline_duration) / baseline_duration * 100
                regressions[result.name] = regression
        
        return regressions
    
    def generate_optimization_recommendations(self) -> List[str]:
        """Generate actionable performance optimization recommendations."""
        recommendations = []
        
        # Analyze results for optimization opportunities
        high_memory_functions = [r for r in self.results if r.memory_peak > 100]  # >100MB
        slow_functions = [r for r in self.results if r.duration > 1.0]  # >1 second
        high_cpu_functions = [r for r in self.results if r.cpu_percent > 80]  # >80% CPU
        
        if high_memory_functions:
            recommendations.append(
                f"🔍 Memory Optimization: {len(high_memory_functions)} functions show high memory usage. "
                f"Consider implementing memory pooling or streaming for: {', '.join([f.name for f in high_memory_functions])}"
            )
        
        if slow_functions:
            recommendations.append(
                f"⚡ Performance Optimization: {len(slow_functions)} functions exceed 1s execution time. "
                f"Consider async/await patterns or caching for: {', '.join([f.name for f in slow_functions])}"
            )
        
        if high_cpu_functions:
            recommendations.append(
                f"🚀 CPU Optimization: {len(high_cpu_functions)} functions show high CPU usage. "
                f"Consider multiprocessing or algorithm optimization for: {', '.join([f.name for f in high_cpu_functions])}"
            )
        
        # Concurrency analysis
        concurrent_results = [r for r in self.results if 'concurrent' in r.name]
        if concurrent_results:
            best_throughput = max(concurrent_results, key=lambda r: r.throughput or 0)
            recommendations.append(
                f"🔄 Optimal Concurrency: Best performance at {best_throughput.metadata.get('concurrency_level', 'unknown')} concurrent workers "
                f"with {best_throughput.throughput:.2f} ops/sec throughput"
            )
        
        return recommendations
    
    def export_results(self) -> Dict[str, Any]:
        """Export comprehensive performance analysis results."""
        # Update baseline metrics with current results
        for result in self.results:
            self.baseline_metrics[result.name] = result.duration
        
        report = {
            "timestamp": time.time(),
            "system_info": {
                "cpu_count": psutil.cpu_count(),
                "memory_total": psutil.virtual_memory().total / 1024 / 1024 / 1024,  # GB
                "python_version": sys.version
            },
            "benchmark_results": [
                {
                    "name": r.name,
                    "duration": r.duration,
                    "memory_peak": r.memory_peak,
                    "cpu_percent": r.cpu_percent,
                    "throughput": r.throughput,
                    "metadata": r.metadata
                }
                for r in self.results
            ],
            "baseline_metrics": self.baseline_metrics,
            "recommendations": self.generate_optimization_recommendations(),
            "performance_summary": {
                "total_functions_tested": len(self.results),
                "average_duration": statistics.mean([r.duration for r in self.results]),
                "total_memory_usage": sum([r.memory_peak for r in self.results]),
                "highest_cpu_usage": max([r.cpu_percent for r in self.results]) if self.results else 0
            }
        }
        
        with open(self.output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report

# Example usage and integration patterns
async def main():
    """Example performance benchmarking execution."""
    benchmarker = PerformanceBenchmarker()
    
    # Example function to benchmark
    def cpu_intensive_task():
        return sum(i * i for i in range(10000))
    
    async def io_intensive_task():
        await asyncio.sleep(0.1)
        return "completed"
    
    # Benchmark individual functions
    await benchmarker.benchmark_function(cpu_intensive_task, "cpu_intensive", iterations=5)
    await benchmarker.benchmark_function(io_intensive_task, "io_intensive", iterations=5)
    
    # Benchmark concurrency
    benchmarker.benchmark_concurrent_performance(cpu_intensive_task, "cpu_task")
    
    # Generate report
    report = benchmarker.export_results()
    
    print("Performance Benchmark Complete!")
    print(f"Report saved to: {benchmarker.output_file}")
    print("\nRecommendations:")
    for rec in report["recommendations"]:
        print(f"  {rec}")

if __name__ == "__main__":
    asyncio.run(main())