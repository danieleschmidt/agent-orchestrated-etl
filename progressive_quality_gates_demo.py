#!/usr/bin/env python3
"""
Progressive Quality Gates Demonstration
Showcases the adaptive quality validation system in action.
"""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import our progressive quality gates
try:
    from src.agent_orchestrated_etl.progressive_quality_gates import (
        ProgressiveQualityGates, 
        QualityMetrics, 
        QualityLevel
    )
    from src.agent_orchestrated_etl.quality_gate_integration import QualityGateIntegration
except ImportError as e:
    logger.error(f"Import error: {e}")
    print("Please ensure you're running from the repository root directory")
    exit(1)


async def demo_progressive_quality_gates():
    """Demonstrate progressive quality gates in action."""
    logger.info("🚀 Starting Progressive Quality Gates Demonstration")
    
    # Initialize quality gates system
    quality_gates = ProgressiveQualityGates()
    integration = QualityGateIntegration()
    
    # Demo scenarios with increasing complexity
    scenarios = [
        {
            "name": "Simple File Processing",
            "source": "file:///data/simple.csv",
            "operations": {"transform": "basic_clean"},
            "metadata": {
                "estimated_data_volume": 1000,
                "external_dependencies": [],
                "business_criticality": "low"
            },
            "metrics": QualityMetrics(
                code_coverage=88.0,
                performance_score=92.0,
                security_score=95.0,
                reliability_score=90.0,
                maintainability_score=85.0,
                complexity_score=90.0,
                test_success_rate=96.0
            )
        },
        {
            "name": "API Data Integration",
            "source": "api://external-service.com/data",
            "operations": {
                "extract": "api_fetch",
                "transform": "normalize_json",
                "validate": "schema_check"
            },
            "metadata": {
                "estimated_data_volume": 50000,
                "external_dependencies": ["api", "auth_service"],
                "business_criticality": "medium"
            },
            "metrics": QualityMetrics(
                code_coverage=82.0,
                performance_score=78.0,
                security_score=88.0,
                reliability_score=85.0,
                maintainability_score=80.0,
                complexity_score=75.0,
                test_success_rate=92.0
            )
        },
        {
            "name": "Enterprise Data Warehouse",
            "source": "s3://enterprise-data/warehouse/*",
            "operations": {
                "extract": "s3_batch_read",
                "transform": "complex_aggregation",
                "enrich": "lookup_tables",
                "validate": "business_rules",
                "load": "data_warehouse"
            },
            "metadata": {
                "estimated_data_volume": 5000000,
                "external_dependencies": ["aws_s3", "redshift", "data_catalog", "auth_service"],
                "business_criticality": "critical"
            },
            "metrics": QualityMetrics(
                code_coverage=79.0,
                performance_score=85.0,
                security_score=92.0,
                reliability_score=88.0,
                maintainability_score=82.0,
                complexity_score=78.0,
                test_success_rate=89.0
            )
        }
    ]
    
    results_summary = []
    
    for i, scenario in enumerate(scenarios, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 SCENARIO {i}: {scenario['name']}")
        logger.info(f"{'='*60}")
        
        # Execute quality gates for this scenario
        try:
            results = await quality_gates.execute_progressive_gates(
                scenario["metadata"], 
                scenario["metrics"]
            )
            
            # Display results
            logger.info(f"✅ Quality Level Determined: {quality_gates.quality_level.value.upper()}")
            logger.info(f"🎯 Total Gates Executed: {len(results)}")
            
            passed = sum(1 for r in results if r.status.value == "passed")
            failed = len(results) - passed
            
            logger.info(f"✅ Passed: {passed}")
            logger.info(f"❌ Failed: {failed}")
            logger.info(f"📈 Success Rate: {(passed/len(results)*100):.1f}%")
            
            # Show detailed results
            for result in results:
                status_emoji = "✅" if result.status.value == "passed" else "❌"
                logger.info(f"  {status_emoji} {result.name}: {result.score:.1f}/{result.threshold:.1f}")
                
                if result.suggestions:
                    for suggestion in result.suggestions[:2]:  # Show top 2 suggestions
                        logger.info(f"    💡 {suggestion}")
            
            # Store scenario results
            results_summary.append({
                "scenario": scenario["name"],
                "quality_level": quality_gates.quality_level.value,
                "total_gates": len(results),
                "passed_gates": passed,
                "success_rate": passed/len(results)*100,
                "execution_time": sum(r.execution_time for r in results),
                "results": [
                    {
                        "gate": r.name,
                        "status": r.status.value,
                        "score": r.score,
                        "threshold": r.threshold,
                        "suggestions": r.suggestions
                    }
                    for r in results
                ]
            })
            
        except Exception as e:
            logger.error(f"❌ Scenario execution failed: {e}")
            results_summary.append({
                "scenario": scenario["name"],
                "error": str(e)
            })
    
    # Generate comprehensive report
    await generate_demo_report(results_summary, quality_gates)
    
    # Show quality dashboard
    logger.info(f"\n{'='*60}")
    logger.info("📊 QUALITY DASHBOARD")
    logger.info(f"{'='*60}")
    
    dashboard = quality_gates.get_quality_dashboard()
    
    if dashboard["status"] == "active":
        logger.info(f"🎯 Current Quality Level: {dashboard['quality_level'].upper()}")
        logger.info(f"📈 Overall Success Rate: {dashboard['overall_success_rate']:.1f}%")
        logger.info(f"🔄 Total Executions: {dashboard['total_executions']}")
        logger.info(f"📊 Recent Executions: {dashboard['recent_executions']}")
        
        logger.info(f"\n📊 Metric Averages:")
        for metric, average in dashboard['metric_averages'].items():
            logger.info(f"  • {metric.replace('_', ' ').title()}: {average:.1f}")
        
        logger.info(f"\n🎯 Adaptive Thresholds:")
        for metric, threshold in dashboard['adaptive_thresholds'].items():
            logger.info(f"  • {metric.replace('_', ' ').title()}: {threshold:.1f}")
    
    logger.info(f"\n🎉 Progressive Quality Gates Demonstration Complete!")
    logger.info(f"📊 Report saved to: reports/progressive_quality_gates_demo.json")


async def generate_demo_report(results_summary, quality_gates):
    """Generate comprehensive demonstration report."""
    report = {
        "demo_execution": {
            "timestamp": datetime.now().isoformat(),
            "total_scenarios": len(results_summary),
            "quality_gates_version": "1.0.0"
        },
        "scenarios": results_summary,
        "quality_dashboard": quality_gates.get_quality_dashboard(),
        "adaptive_insights": {
            "threshold_adaptations": "Thresholds automatically adjust based on performance trends",
            "quality_level_progression": "Quality levels increase automatically with pipeline complexity",
            "learning_patterns": "System learns from execution patterns to optimize future runs"
        },
        "recommendations": [
            "Review failed quality gates and implement suggested improvements",
            "Monitor adaptive threshold changes to understand system learning",
            "Use quality dashboard for ongoing pipeline health assessment",
            "Consider implementing continuous quality monitoring for production pipelines"
        ]
    }
    
    # Save report
    report_path = Path("reports/progressive_quality_gates_demo.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)


async def demo_integration():
    """Demonstrate quality gate integration with pipeline execution."""
    logger.info("\n🔗 Demonstrating Quality Gate Integration")
    
    integration = QualityGateIntegration()
    
    # Simulate pipeline execution with quality gates
    result = await integration.execute_pipeline_with_quality_gates(
        source="s3://demo-bucket/data/",
        operations={"transform": "demo_transform"},
        quality_config={
            "criticality": "high",
            "execution_timeout": 300
        }
    )
    
    logger.info(f"Integration Result: {result['status']}")
    logger.info(f"Quality Success Rate: {result['quality_summary']['success_rate']:.1f}%")
    
    return result


if __name__ == "__main__":
    try:
        # Run the demonstration
        asyncio.run(demo_progressive_quality_gates())
        
        # Also demonstrate integration
        print("\n" + "="*60)
        integration_result = asyncio.run(demo_integration())
        print(f"Integration demonstration completed: {integration_result['status']}")
        
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise