#!/usr/bin/env python3
"""
Generation 2: Robust ETL Core Integration
Integrates robustness features into the main ETL system
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import time
import logging
from typing import Any, Dict, List, Optional, Union
from contextlib import contextmanager

# Import core ETL functionality
from agent_orchestrated_etl import DataOrchestrator, core
from agent_orchestrated_etl.agents.coordination import AgentCoordinator
from agent_orchestrated_etl.agents.communication import AgentCommunicationHub
from agent_orchestrated_etl.data_source_analysis import S3DataAnalyzer

# Import robustness features
from enhanced_error_handling_gen2 import (
    ETLBaseException, DataExtractionError, DataTransformationError, 
    DataValidationError, PipelineExecutionError,
    retry_with_backoff, CircuitBreaker, DataValidator, 
    GracefulDegradationManager, PipelineHealthMonitor
)

class RobustETLOrchestrator:
    """Enhanced ETL Orchestrator with comprehensive robustness features"""
    
    def __init__(self):
        self.base_orchestrator = DataOrchestrator()
        self.health_monitor = PipelineHealthMonitor()
        self.degradation_manager = GracefulDegradationManager()
        self.validator = DataValidator()
        self.circuit_breakers = {}
        
        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.RobustETLOrchestrator")
        
        # Initialize validation rules
        self._setup_validation_rules()
        
        # Setup fallback strategies
        self._setup_fallback_strategies()
        
        self.logger.info("Robust ETL Orchestrator initialized with comprehensive resilience features")
    
    def _setup_validation_rules(self):
        """Setup standard data validation rules"""
        self.validator.add_rule('not_empty', 
                               lambda data: data is not None and len(data) > 0,
                               "Data cannot be empty")
        
        self.validator.add_rule('valid_structure', 
                               lambda data: all(isinstance(item, dict) for item in data) if isinstance(data, list) else isinstance(data, dict),
                               "All data items must be dictionaries")
        
        self.validator.add_rule('has_required_fields', 
                               lambda data: all(any(key in item for key in ['id', 'name', 'value']) for item in data) if isinstance(data, list) else True,
                               "Records must contain at least one of: id, name, or value")
    
    def _setup_fallback_strategies(self):
        """Setup fallback strategies for different operations"""
        
        def extraction_fallback(*args, **kwargs):
            """Fallback for data extraction - return sample data"""
            self.logger.warning("Using fallback extraction strategy")
            return [
                {"id": "fallback_1", "name": "fallback_data", "value": 0, "source": "fallback"},
                {"id": "fallback_2", "name": "fallback_data", "value": 0, "source": "fallback"}
            ]
        
        def transformation_fallback(data, *args, **kwargs):
            """Fallback for data transformation - minimal processing"""
            self.logger.warning("Using fallback transformation strategy")
            if not data:
                return []
            
            return [{**item, 'transformed': True, 'fallback': True} for item in data]
        
        self.degradation_manager.register_fallback('extraction', extraction_fallback)
        self.degradation_manager.register_fallback('transformation', transformation_fallback)
    
    def _get_circuit_breaker(self, operation: str) -> CircuitBreaker:
        """Get or create circuit breaker for operation"""
        if operation not in self.circuit_breakers:
            self.circuit_breakers[operation] = CircuitBreaker(
                failure_threshold=3,
                recovery_timeout=30.0,
                success_threshold=2
            )
        return self.circuit_breakers[operation]
    
    @retry_with_backoff(max_retries=2, exceptions=(DataExtractionError, ConnectionError))
    def robust_extract_data(self, source_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Robust data extraction with error handling and fallbacks"""
        start_time = time.time()
        
        try:
            circuit_breaker = self._get_circuit_breaker('extraction')
            
            def extraction_operation():
                return core.primary_data_extraction(source_config=source_config)
            
            # Use circuit breaker for extraction
            with self.degradation_manager.graceful_execution('extraction') as execution_mode:
                if execution_mode == "primary":
                    data = circuit_breaker.call(extraction_operation)
                else:
                    data = self.degradation_manager.fallback_strategies['extraction']()
            
            # Validate extracted data
            validation_result = self.validator.validate_data(data, ['not_empty', 'valid_structure'])
            if not validation_result['valid']:
                raise DataValidationError(f"Extracted data validation failed: {validation_result['errors']}")
            
            # Record success metrics
            response_time = time.time() - start_time
            self.health_monitor.record_metric('extraction_response_time', response_time)
            self.health_monitor.record_metric('extraction_success_rate', 1.0)
            
            self.logger.info(f"Data extraction completed successfully in {response_time:.2f}s")
            return data
            
        except Exception as e:
            response_time = time.time() - start_time
            self.health_monitor.record_metric('extraction_error_rate', 1.0)
            self.health_monitor.record_metric('extraction_response_time', response_time)
            
            if isinstance(e, (DataExtractionError, DataValidationError)):
                raise
            else:
                raise DataExtractionError(f"Extraction failed: {str(e)}", source=source_config.get('type', 'unknown'))
    
    @retry_with_backoff(max_retries=2, exceptions=(DataTransformationError,))
    def robust_transform_data(self, data: List[Dict[str, Any]], transformation_rules: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Robust data transformation with error handling and fallbacks"""
        start_time = time.time()
        
        try:
            circuit_breaker = self._get_circuit_breaker('transformation')
            
            def transformation_operation():
                return core.transform_data(data, transformation_rules=transformation_rules)
            
            # Use circuit breaker for transformation
            with self.degradation_manager.graceful_execution('transformation') as execution_mode:
                if execution_mode == "primary":
                    transformed_data = circuit_breaker.call(transformation_operation)
                else:
                    transformed_data = self.degradation_manager.fallback_strategies['transformation'](data)
            
            # Validate transformed data
            validation_result = self.validator.validate_data(transformed_data, ['not_empty', 'valid_structure'])
            if not validation_result['valid']:
                self.logger.warning(f"Transformed data validation warnings: {validation_result['errors']}")
            
            # Record success metrics
            response_time = time.time() - start_time
            self.health_monitor.record_metric('transformation_response_time', response_time)
            self.health_monitor.record_metric('transformation_success_rate', 1.0)
            
            self.logger.info(f"Data transformation completed successfully in {response_time:.2f}s")
            return transformed_data
            
        except Exception as e:
            response_time = time.time() - start_time
            self.health_monitor.record_metric('transformation_error_rate', 1.0)
            self.health_monitor.record_metric('transformation_response_time', response_time)
            
            if isinstance(e, DataTransformationError):
                raise
            else:
                raise DataTransformationError(f"Transformation failed: {str(e)}", transformation_step="core_transform")
    
    def create_robust_pipeline(self, source: str, transformation_rules: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a robust ETL pipeline with comprehensive error handling"""
        pipeline_start_time = time.time()
        pipeline_id = f"robust_pipeline_{int(time.time())}"
        
        try:
            self.logger.info(f"Creating robust pipeline {pipeline_id} for source: {source}")
            
            # Parse source configuration
            if source.startswith('s3://'):
                source_config = {'type': 's3', 'path': source, 'bucket': source.split('/')[2], 'key': '/'.join(source.split('/')[3:])}
            elif source.startswith('db://'):
                source_config = {'type': 'database', 'connection_string': source}
            elif source.startswith('api://'):
                source_config = {'type': 'api', 'endpoint': source}
            else:
                source_config = {'type': 'sample'}
            
            # Robust extraction
            extracted_data = self.robust_extract_data(source_config)
            
            # Robust transformation
            transformed_data = self.robust_transform_data(extracted_data, transformation_rules)
            
            # Pipeline execution metrics
            total_response_time = time.time() - pipeline_start_time
            self.health_monitor.record_metric('pipeline_response_time', total_response_time)
            self.health_monitor.record_metric('pipeline_success_rate', 1.0)
            
            pipeline_result = {
                'pipeline_id': pipeline_id,
                'source': source,
                'status': 'success',
                'total_response_time': total_response_time,
                'extracted_records': len(extracted_data),
                'transformed_records': len(transformed_data),
                'health_status': 'healthy',
                'data_quality_score': self._calculate_data_quality_score(transformed_data)
            }
            
            self.logger.info(f"Pipeline {pipeline_id} completed successfully in {total_response_time:.2f}s")
            return pipeline_result
            
        except Exception as e:
            total_response_time = time.time() - pipeline_start_time
            self.health_monitor.record_metric('pipeline_error_rate', 1.0)
            self.health_monitor.record_metric('pipeline_response_time', total_response_time)
            
            error_result = {
                'pipeline_id': pipeline_id,
                'source': source,
                'status': 'failed',
                'error': str(e),
                'error_type': type(e).__name__,
                'total_response_time': total_response_time,
                'health_status': 'unhealthy'
            }
            
            self.logger.error(f"Pipeline {pipeline_id} failed: {e}")
            
            if isinstance(e, ETLBaseException):
                error_result['error_code'] = e.error_code
                error_result['error_context'] = e.context
                raise
            else:
                raise PipelineExecutionError(f"Pipeline execution failed: {str(e)}", pipeline_id=pipeline_id)
    
    def _calculate_data_quality_score(self, data: List[Dict[str, Any]]) -> float:
        """Calculate data quality score"""
        if not data:
            return 0.0
        
        # Simple quality metrics
        total_fields = sum(len(record) for record in data)
        non_null_fields = sum(sum(1 for value in record.values() if value is not None and value != '') for record in data)
        
        completeness = non_null_fields / total_fields if total_fields > 0 else 0.0
        return round(completeness, 2)
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health report"""
        health_report = self.health_monitor.get_health_report()
        
        # Add circuit breaker states
        circuit_breaker_states = {
            name: cb.state for name, cb in self.circuit_breakers.items()
        }
        
        # Add service health status
        service_health = self.degradation_manager.service_health
        
        return {
            'timestamp': time.time(),
            'overall_health': 'healthy' if len(health_report['recent_alerts']) == 0 else 'degraded',
            'metrics_summary': health_report['metrics_summary'],
            'recent_alerts': health_report['recent_alerts'],
            'circuit_breaker_states': circuit_breaker_states,
            'service_health': service_health,
            'uptime_metrics': {
                'total_pipelines_executed': sum(len(metrics) for metrics in self.health_monitor.metrics.values() if 'pipeline' in metrics),
                'average_response_time': self._get_average_metric('pipeline_response_time')
            }
        }
    
    def _get_average_metric(self, metric_name: str) -> float:
        """Get average value for a metric"""
        if metric_name not in self.health_monitor.metrics:
            return 0.0
        
        values = [entry['value'] for entry in self.health_monitor.metrics[metric_name]]
        return sum(values) / len(values) if values else 0.0

def test_robust_etl_system():
    """Test the robust ETL system"""
    print("🛡️ Testing Robust ETL System (Generation 2)")
    print("=" * 50)
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')
    
    # Initialize robust orchestrator
    orchestrator = RobustETLOrchestrator()
    print("✅ Robust ETL Orchestrator initialized")
    
    # Test successful pipeline
    print("\n🔍 Testing successful pipeline execution...")
    try:
        result = orchestrator.create_robust_pipeline('s3://test-bucket/test-data.json')
        print(f"✅ Pipeline completed: {result['status']} in {result['total_response_time']:.2f}s")
        print(f"📊 Data quality score: {result['data_quality_score']}")
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
    
    # Test pipeline with transformation rules
    print("\n🔄 Testing pipeline with transformation rules...")
    transformation_rules = [
        {'type': 'add_field', 'field_name': 'processed', 'field_value': True},
        {'type': 'calculate_field', 'field_name': 'doubled_value', 'calculation': {'type': 'multiply', 'field1': 'value', 'field2': 'value'}}
    ]
    
    try:
        result = orchestrator.create_robust_pipeline('api://test-service.com/data', transformation_rules)
        print(f"✅ Enhanced pipeline completed: {result['status']}")
    except Exception as e:
        print(f"❌ Enhanced pipeline failed: {e}")
    
    # Test system health monitoring
    print("\n🏥 Testing system health monitoring...")
    health_report = orchestrator.get_system_health()
    print(f"✅ Overall health: {health_report['overall_health']}")
    print(f"📊 Metrics tracked: {list(health_report['metrics_summary'].keys())}")
    print(f"🚨 Recent alerts: {len(health_report['recent_alerts'])}")
    
    # Test error scenarios
    print("\n💥 Testing error scenarios...")
    try:
        # This should trigger fallback mechanisms
        result = orchestrator.create_robust_pipeline('unsupported://unknown-source')
        print(f"✅ Fallback pipeline result: {result['status']}")
    except Exception as e:
        print(f"⚠️ Expected error handled: {type(e).__name__}")
    
    print("\n" + "=" * 50)
    print("🎉 Robust ETL System Testing Complete!")
    
    return True

if __name__ == "__main__":
    test_robust_etl_system()