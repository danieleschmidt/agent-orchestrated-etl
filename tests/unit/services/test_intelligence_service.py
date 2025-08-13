"""Unit tests for IntelligenceService."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from src.agent_orchestrated_etl.services.intelligence_service import IntelligenceService


class TestIntelligenceService:
    """Test cases for IntelligenceService."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = AsyncMock()
        self.intelligence_service = IntelligenceService(db_manager=self.mock_db_manager)

    @pytest.mark.asyncio
    async def test_analyze_pipeline_requirements_basic(self):
        """Test basic pipeline requirements analysis."""
        source_config = {
            "type": "file",
            "path": "/data/users.csv",
            "format": "csv",
            "size_bytes": 1024000
        }
        business_requirements = {
            "data_quality": "high",
            "performance": "medium",
            "compliance": ["GDPR", "SOX"]
        }

        result = await self.intelligence_service.analyze_pipeline_requirements(
            source_config, business_requirements
        )

        assert "recommended_transformations" in result
        assert "estimated_complexity" in result
        assert "resource_requirements" in result
        assert "compliance_recommendations" in result

        # Should recommend data validation for high quality requirement
        assert any("validation" in t.lower() for t in result["recommended_transformations"])

        # Should include GDPR compliance recommendations
        assert any("gdpr" in r.lower() for r in result["compliance_recommendations"])

    @pytest.mark.asyncio
    async def test_analyze_pipeline_requirements_large_dataset(self):
        """Test analysis for large datasets."""
        source_config = {
            "type": "database",
            "table": "transactions",
            "estimated_records": 10000000,
            "size_bytes": 5000000000  # 5GB
        }
        business_requirements = {
            "performance": "high",
            "scalability": "required"
        }

        result = await self.intelligence_service.analyze_pipeline_requirements(
            source_config, business_requirements
        )

        assert result["estimated_complexity"] == "high"
        assert "parallel_processing" in result["recommended_optimizations"]
        assert "batch_processing" in result["recommended_optimizations"]

        # Should recommend high resource allocation
        memory_req = result["resource_requirements"]["memory_gb"]
        assert memory_req >= 8  # At least 8GB for large datasets

    @pytest.mark.asyncio
    async def test_optimize_execution_strategy_performance_focused(self):
        """Test execution strategy optimization for performance."""
        pipeline_id = "perf-pipeline-001"
        current_metrics = {
            "execution_time_minutes": 45,
            "memory_usage_gb": 4,
            "cpu_utilization": 0.3,
            "throughput_records_per_second": 1000
        }
        historical_data = [
            {
                "execution_time_minutes": 50,
                "memory_usage_gb": 3.5,
                "throughput_records_per_second": 800,
                "timestamp": datetime.now() - timedelta(days=1)
            },
            {
                "execution_time_minutes": 48,
                "memory_usage_gb": 4.2,
                "throughput_records_per_second": 950,
                "timestamp": datetime.now() - timedelta(days=2)
            }
        ]

        result = await self.intelligence_service.optimize_execution_strategy(
            pipeline_id, current_metrics, historical_data
        )

        assert "optimization_recommendations" in result
        assert "predicted_improvements" in result
        assert "confidence_score" in result

        # Should recommend increasing parallelism due to low CPU utilization
        recommendations = result["optimization_recommendations"]
        assert any("parallel" in r.lower() or "cpu" in r.lower() for r in recommendations)

    @pytest.mark.asyncio
    async def test_optimize_execution_strategy_memory_constrained(self):
        """Test optimization for memory-constrained scenarios."""
        pipeline_id = "memory-pipeline-001"
        current_metrics = {
            "execution_time_minutes": 30,
            "memory_usage_gb": 15,
            "memory_limit_gb": 16,
            "cpu_utilization": 0.8,
            "oom_errors": 2
        }
        historical_data = []

        result = await self.intelligence_service.optimize_execution_strategy(
            pipeline_id, current_metrics, historical_data
        )

        recommendations = result["optimization_recommendations"]

        # Should recommend memory optimization strategies
        assert any("memory" in r.lower() or "batch" in r.lower() for r in recommendations)
        assert any("streaming" in r.lower() or "chunk" in r.lower() for r in recommendations)

    @pytest.mark.asyncio
    async def test_predict_pipeline_behavior_success_scenario(self):
        """Test pipeline behavior prediction for successful execution."""
        pipeline_config = {
            "source": {"type": "database", "table": "users", "records": 100000},
            "transformations": [
                {"type": "data_cleaning", "complexity": "medium"},
                {"type": "aggregation", "complexity": "low"}
            ],
            "destination": {"type": "file", "format": "json"}
        }
        execution_context = {
            "priority": "high",
            "resources": {"memory_gb": 8, "cpu_cores": 4},
            "timeout_minutes": 60
        }

        # Mock historical performance data
        self.mock_db_manager.execute_query.return_value = [
            {
                "avg_execution_time": 25.5,
                "success_rate": 0.95,
                "avg_memory_usage": 4.2
            }
        ]

        result = await self.intelligence_service.predict_pipeline_behavior(
            pipeline_config, execution_context
        )

        assert result["predicted_success_probability"] > 0.8
        assert result["estimated_execution_time_minutes"] > 0
        assert result["estimated_resource_usage"]["memory_gb"] > 0
        assert "risk_factors" in result
        assert "optimization_opportunities" in result

    @pytest.mark.asyncio
    async def test_predict_pipeline_behavior_high_risk(self):
        """Test pipeline behavior prediction for high-risk execution."""
        pipeline_config = {
            "source": {"type": "api", "url": "external-api", "records": 1000000},
            "transformations": [
                {"type": "complex_ml", "complexity": "very_high"},
                {"type": "data_enrichment", "complexity": "high"}
            ],
            "destination": {"type": "database", "table": "large_table"}
        }
        execution_context = {
            "priority": "low",
            "resources": {"memory_gb": 2, "cpu_cores": 1},
            "timeout_minutes": 30
        }

        # Mock historical data showing poor performance
        self.mock_db_manager.execute_query.return_value = [
            {
                "avg_execution_time": 45.0,
                "success_rate": 0.6,
                "avg_memory_usage": 6.8
            }
        ]

        result = await self.intelligence_service.predict_pipeline_behavior(
            pipeline_config, execution_context
        )

        assert result["predicted_success_probability"] < 0.7
        assert len(result["risk_factors"]) > 0
        assert "insufficient_resources" in str(result["risk_factors"]).lower()

    @pytest.mark.asyncio
    async def test_learn_from_execution_success(self):
        """Test learning from successful pipeline execution."""
        execution_data = {
            "pipeline_id": "learn-pipeline-001",
            "execution_id": "exec-123",
            "status": "completed",
            "execution_time_minutes": 22,
            "memory_usage_gb": 3.5,
            "cpu_utilization": 0.7,
            "records_processed": 50000,
            "transformations": [
                {"type": "data_cleaning", "duration_minutes": 5},
                {"type": "validation", "duration_minutes": 3}
            ]
        }

        await self.intelligence_service.learn_from_execution(execution_data)

        # Should have stored learning data in database
        self.mock_db_manager.execute_query.assert_called()

        # Verify the learning data was processed correctly
        call_args = self.mock_db_manager.execute_query.call_args
        assert "INSERT" in call_args[0][0] or "UPDATE" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_learn_from_execution_failure(self):
        """Test learning from failed pipeline execution."""
        execution_data = {
            "pipeline_id": "learn-pipeline-002",
            "execution_id": "exec-124",
            "status": "failed",
            "execution_time_minutes": 10,
            "memory_usage_gb": 7.8,
            "error_type": "OutOfMemoryError",
            "error_message": "Java heap space",
            "failure_stage": "transformation"
        }

        await self.intelligence_service.learn_from_execution(execution_data)

        # Should have recorded failure patterns
        self.mock_db_manager.execute_query.assert_called()

    @pytest.mark.asyncio
    async def test_recommend_transformations_data_quality(self):
        """Test transformation recommendations based on data quality issues."""
        data_profile = {
            "null_percentage": 15.0,
            "duplicate_percentage": 8.0,
            "data_types": {"inconsistent": ["date_column", "numeric_column"]},
            "outliers": {"count": 250, "columns": ["price", "quantity"]}
        }
        business_rules = {
            "data_quality": "high",
            "null_tolerance": 5.0,
            "duplicate_tolerance": 2.0
        }

        result = await self.intelligence_service._recommend_transformations(
            data_profile, business_rules
        )

        transformations = result["transformations"]

        # Should recommend null handling
        assert any("null" in t["type"].lower() for t in transformations)

        # Should recommend duplicate removal
        assert any("duplicate" in t["type"].lower() for t in transformations)

        # Should recommend outlier detection
        assert any("outlier" in t["type"].lower() for t in transformations)

    @pytest.mark.asyncio
    async def test_estimate_complexity_simple(self):
        """Test complexity estimation for simple pipelines."""
        pipeline_config = {
            "source": {"type": "file", "size_mb": 10},
            "transformations": [
                {"type": "column_mapping", "complexity": "low"}
            ],
            "destination": {"type": "database"}
        }

        complexity = await self.intelligence_service._estimate_complexity(pipeline_config)

        assert complexity == "low"

    @pytest.mark.asyncio
    async def test_estimate_complexity_complex(self):
        """Test complexity estimation for complex pipelines."""
        pipeline_config = {
            "source": {"type": "multiple_apis", "size_gb": 50},
            "transformations": [
                {"type": "machine_learning", "complexity": "very_high"},
                {"type": "complex_aggregation", "complexity": "high"},
                {"type": "data_enrichment", "complexity": "medium"}
            ],
            "destination": {"type": "multiple_targets"}
        }

        complexity = await self.intelligence_service._estimate_complexity(pipeline_config)

        assert complexity in ["high", "very_high"]

    @pytest.mark.asyncio
    async def test_calculate_resource_requirements(self):
        """Test resource requirements calculation."""
        data_size_gb = 5.0
        complexity_score = 7.5
        transformations = [
            {"type": "aggregation", "memory_multiplier": 1.5},
            {"type": "join", "memory_multiplier": 2.0}
        ]

        result = await self.intelligence_service._calculate_resource_requirements(
            data_size_gb, complexity_score, transformations
        )

        assert result["memory_gb"] >= data_size_gb  # At least as much as data size
        assert result["cpu_cores"] >= 1
        assert result["storage_gb"] >= data_size_gb
        assert "network_bandwidth_mbps" in result

    @pytest.mark.asyncio
    async def test_get_similar_pipelines(self):
        """Test finding similar pipelines for learning."""
        pipeline_config = {
            "source_type": "database",
            "data_size_gb": 2.5,
            "transformation_types": ["cleaning", "validation"],
            "destination_type": "file"
        }

        # Mock similar pipelines from database
        self.mock_db_manager.execute_query.return_value = [
            {
                "pipeline_id": "similar-001",
                "avg_execution_time": 18.5,
                "success_rate": 0.92,
                "similarity_score": 0.85
            },
            {
                "pipeline_id": "similar-002",
                "avg_execution_time": 22.0,
                "success_rate": 0.88,
                "similarity_score": 0.78
            }
        ]

        result = await self.intelligence_service._get_similar_pipelines(pipeline_config)

        assert len(result) == 2
        assert result[0]["similarity_score"] > result[1]["similarity_score"]
        assert all("pipeline_id" in p for p in result)

    @pytest.mark.asyncio
    async def test_analyze_historical_patterns(self):
        """Test historical pattern analysis."""
        pipeline_id = "pattern-pipeline-001"

        # Mock historical execution data
        historical_data = [
            {"execution_time": 25, "success": True, "memory_usage": 4.0, "timestamp": "2023-01-01"},
            {"execution_time": 28, "success": True, "memory_usage": 4.2, "timestamp": "2023-01-02"},
            {"execution_time": 45, "success": False, "memory_usage": 7.8, "timestamp": "2023-01-03"},
            {"execution_time": 24, "success": True, "memory_usage": 3.9, "timestamp": "2023-01-04"}
        ]

        result = await self.intelligence_service._analyze_historical_patterns(
            pipeline_id, historical_data
        )

        assert "success_rate" in result
        assert "avg_execution_time" in result
        assert "performance_trend" in result
        assert "failure_patterns" in result
        assert result["success_rate"] == 0.75  # 3 out of 4 successful

    def test_calculate_similarity_score(self):
        """Test pipeline similarity calculation."""
        pipeline1 = {
            "source_type": "database",
            "data_size_category": "medium",
            "transformation_count": 3,
            "complexity": "medium"
        }
        pipeline2 = {
            "source_type": "database",
            "data_size_category": "large",
            "transformation_count": 3,
            "complexity": "high"
        }

        score = self.intelligence_service._calculate_similarity_score(pipeline1, pipeline2)

        assert 0.0 <= score <= 1.0
        assert score > 0.5  # Should be somewhat similar due to matching source_type and transformation_count

    @pytest.mark.asyncio
    async def test_error_handling_in_analysis(self):
        """Test error handling during pipeline analysis."""
        source_config = {"type": "invalid_source"}
        business_requirements = {}

        # Mock database error
        self.mock_db_manager.execute_query.side_effect = Exception("Database connection failed")

        result = await self.intelligence_service.analyze_pipeline_requirements(
            source_config, business_requirements
        )

        # Should gracefully handle errors and return basic recommendations
        assert "recommended_transformations" in result
        assert "estimated_complexity" in result
        assert result["estimated_complexity"] == "unknown"
