"""
Advanced cost optimization automation for agent-orchestrated-etl.
Analyzes resource usage patterns and implements intelligent cost reduction strategies.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import boto3
import pandas as pd
from prometheus_client.parser import text_string_to_metric_families


class OptimizationStrategy(Enum):
    """Cost optimization strategy types."""
    RIGHTSIZING = "rightsizing"
    SCHEDULING = "scheduling"
    SPOT_INSTANCES = "spot_instances"
    RESERVED_CAPACITY = "reserved_capacity"
    DATA_LIFECYCLE = "data_lifecycle"


@dataclass
class CostRecommendation:
    """Cost optimization recommendation."""
    strategy: OptimizationStrategy
    resource_id: str
    current_cost: float
    projected_savings: float
    confidence: float
    implementation_risk: str
    description: str
    action_required: List[str]


class ResourceAnalyzer:
    """Analyzes resource usage patterns for cost optimization."""
    
    def __init__(self, aws_profile: Optional[str] = None):
        """Initialize with AWS session."""
        session = boto3.Session(profile_name=aws_profile)
        self.cloudwatch = session.client('cloudwatch')
        self.ce = session.client('ce')  # Cost Explorer
        self.ec2 = session.client('ec2')
        self.rds = session.client('rds')
        self.s3 = session.client('s3')
        
        self.logger = logging.getLogger(__name__)
    
    async def analyze_compute_usage(self, days: int = 30) -> List[CostRecommendation]:
        """Analyze compute resource usage patterns."""
        recommendations = []
        
        # Get EC2 instance metrics
        instances = self.ec2.describe_instances()
        
        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:
                if instance['State']['Name'] != 'running':
                    continue
                    
                instance_id = instance['InstanceId']
                instance_type = instance['InstanceType']
                
                # Get CPU utilization
                cpu_metrics = await self._get_cloudwatch_metrics(
                    namespace='AWS/EC2',
                    metric_name='CPUUtilization',
                    dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                    days=days
                )
                
                # Analyze usage patterns
                avg_cpu = cpu_metrics['average'] if cpu_metrics else 0
                max_cpu = cpu_metrics['maximum'] if cpu_metrics else 0
                
                # Generate rightsizing recommendations
                if avg_cpu < 20 and max_cpu < 50:
                    smaller_instance = self._suggest_smaller_instance(instance_type)
                    if smaller_instance:
                        cost_savings = await self._calculate_instance_savings(
                            instance_type, smaller_instance
                        )
                        
                        recommendations.append(CostRecommendation(
                            strategy=OptimizationStrategy.RIGHTSIZING,
                            resource_id=instance_id,
                            current_cost=cost_savings['current'],
                            projected_savings=cost_savings['savings'],
                            confidence=0.8 if avg_cpu < 10 else 0.6,
                            implementation_risk="low",
                            description=f"Rightsize {instance_type} to {smaller_instance}",
                            action_required=[
                                "Schedule maintenance window",
                                "Take AMI snapshot",
                                "Stop instance and change type",
                                "Test application performance"
                            ]
                        ))
                
                # Generate spot instance recommendations
                if self._is_spot_suitable(instance):
                    spot_savings = await self._calculate_spot_savings(instance_type)
                    
                    recommendations.append(CostRecommendation(
                        strategy=OptimizationStrategy.SPOT_INSTANCES,
                        resource_id=instance_id,
                        current_cost=spot_savings['current'],
                        projected_savings=spot_savings['savings'],
                        confidence=0.7,
                        implementation_risk="medium",
                        description=f"Convert to Spot instance",
                        action_required=[
                            "Implement spot instance handling",
                            "Add interruption notifications",
                            "Test failover scenarios",
                            "Update auto-scaling groups"
                        ]
                    ))
        
        return recommendations
    
    async def analyze_storage_usage(self, days: int = 30) -> List[CostRecommendation]:
        """Analyze storage usage and lifecycle policies."""
        recommendations = []
        
        # Analyze S3 storage
        buckets = self.s3.list_buckets()
        
        for bucket in buckets['Buckets']:
            bucket_name = bucket['Name']
            
            # Get storage metrics
            storage_metrics = await self._get_cloudwatch_metrics(
                namespace='AWS/S3',
                metric_name='BucketSizeBytes',
                dimensions=[
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'StorageType', 'Value': 'StandardStorage'}
                ],
                days=days
            )
            
            # Check lifecycle policies
            try:
                lifecycle = self.s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                has_lifecycle = len(lifecycle.get('Rules', [])) > 0
            except:
                has_lifecycle = False
            
            if not has_lifecycle and storage_metrics and storage_metrics['average'] > 1e9:  # > 1GB
                # Recommend lifecycle policy
                projected_savings = storage_metrics['average'] * 0.4 * 0.023 / (1024**3)  # ~40% to IA
                
                recommendations.append(CostRecommendation(
                    strategy=OptimizationStrategy.DATA_LIFECYCLE,
                    resource_id=bucket_name,
                    current_cost=storage_metrics['average'] * 0.023 / (1024**3),  # Standard storage cost
                    projected_savings=projected_savings,
                    confidence=0.9,
                    implementation_risk="low",
                    description="Implement S3 lifecycle policy",
                    action_required=[
                        "Analyze access patterns",
                        "Create lifecycle rules",
                        "Test data retrieval",
                        "Monitor cost impact"
                    ]
                ))
        
        return recommendations
    
    async def analyze_scheduling_opportunities(self) -> List[CostRecommendation]:
        """Identify resources that can be scheduled for cost savings."""
        recommendations = []
        
        # Analyze development/staging environments
        instances = self.ec2.describe_instances()
        
        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:
                if instance['State']['Name'] != 'running':
                    continue
                
                # Check tags for environment
                tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                environment = tags.get('Environment', '').lower()
                
                if environment in ['dev', 'development', 'staging', 'test']:
                    instance_id = instance['InstanceId']
                    instance_type = instance['InstanceType']
                    
                    # Calculate potential savings from scheduling
                    monthly_cost = await self._get_instance_monthly_cost(instance_type)
                    
                    # Assume 12 hours/day, 5 days/week for dev environments
                    hours_saved = (24 - 12) * 5 + 24 * 2  # 88 hours per week
                    weekly_savings = monthly_cost * (hours_saved / (24 * 7))
                    monthly_savings = weekly_savings * 4.33
                    
                    recommendations.append(CostRecommendation(
                        strategy=OptimizationStrategy.SCHEDULING,
                        resource_id=instance_id,
                        current_cost=monthly_cost,
                        projected_savings=monthly_savings,
                        confidence=0.95,
                        implementation_risk="low",
                        description=f"Schedule {environment} environment",
                        action_required=[
                            "Implement auto-start/stop scripts",
                            "Configure CloudWatch Events",
                            "Add environment-specific schedules",
                            "Test scheduled operations"
                        ]
                    ))
        
        return recommendations
    
    async def _get_cloudwatch_metrics(
        self, 
        namespace: str, 
        metric_name: str, 
        dimensions: List[Dict], 
        days: int
    ) -> Optional[Dict]:
        """Get CloudWatch metrics for analysis."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)
            
            response = self.cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour
                Statistics=['Average', 'Maximum']
            )
            
            if response['Datapoints']:
                datapoints = response['Datapoints']
                return {
                    'average': sum(dp['Average'] for dp in datapoints) / len(datapoints),
                    'maximum': max(dp['Maximum'] for dp in datapoints)
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting CloudWatch metrics: {e}")
            return None
    
    def _suggest_smaller_instance(self, current_type: str) -> Optional[str]:
        """Suggest a smaller instance type based on current usage."""
        # Simplified instance type mapping
        downsize_map = {
            't3.large': 't3.medium',
            't3.xlarge': 't3.large',
            'c5.large': 'c5.medium',
            'c5.xlarge': 'c5.large',
            'm5.large': 'm5.medium',
            'm5.xlarge': 'm5.large',
        }
        
        return downsize_map.get(current_type)
    
    async def _calculate_instance_savings(
        self, 
        current_type: str, 
        target_type: str
    ) -> Dict[str, float]:
        """Calculate cost savings from instance rightsizing."""
        # Simplified pricing (would use AWS Pricing API in production)
        pricing = {
            't3.medium': 0.0416,
            't3.large': 0.0832,
            't3.xlarge': 0.1664,
            'c5.medium': 0.085,
            'c5.large': 0.17,
            'c5.xlarge': 0.34,
            'm5.medium': 0.096,
            'm5.large': 0.192,
            'm5.xlarge': 0.384,
        }
        
        current_hourly = pricing.get(current_type, 0.1)
        target_hourly = pricing.get(target_type, 0.05)
        
        monthly_current = current_hourly * 24 * 30
        monthly_target = target_hourly * 24 * 30
        
        return {
            'current': monthly_current,
            'savings': monthly_current - monthly_target
        }
    
    def _is_spot_suitable(self, instance: Dict) -> bool:
        """Determine if instance is suitable for Spot pricing."""
        # Check if instance is not critical based on tags
        tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
        
        # Avoid spot for production databases or critical services
        if tags.get('Environment', '').lower() == 'production':
            if any(keyword in tags.get('Name', '').lower() 
                   for keyword in ['database', 'db', 'critical', 'primary']):
                return False
        
        return True
    
    async def _calculate_spot_savings(self, instance_type: str) -> Dict[str, float]:
        """Calculate potential Spot instance savings."""
        # Spot instances typically save 70-90% off On-Demand pricing
        current_cost = await self._get_instance_monthly_cost(instance_type)
        spot_discount = 0.75  # 75% savings
        
        return {
            'current': current_cost,
            'savings': current_cost * spot_discount
        }
    
    async def _get_instance_monthly_cost(self, instance_type: str) -> float:
        """Get monthly cost for instance type."""
        # Simplified pricing lookup (would use AWS Pricing API)
        base_pricing = {
            't3.micro': 8.5,
            't3.small': 17.0,
            't3.medium': 30.0,
            't3.large': 60.0,
            't3.xlarge': 120.0,
        }
        
        return base_pricing.get(instance_type, 50.0)


class CostOptimizer:
    """Main cost optimization orchestrator."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize cost optimizer."""
        self.analyzer = ResourceAnalyzer()
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        self.config = self._load_config(config_path)
    
    async def generate_recommendations(self) -> List[CostRecommendation]:
        """Generate comprehensive cost optimization recommendations."""
        all_recommendations = []
        
        # Analyze different resource types
        compute_recs = await self.analyzer.analyze_compute_usage()
        storage_recs = await self.analyzer.analyze_storage_usage()
        scheduling_recs = await self.analyzer.analyze_scheduling_opportunities()
        
        all_recommendations.extend(compute_recs)
        all_recommendations.extend(storage_recs)
        all_recommendations.extend(scheduling_recs)
        
        # Sort by projected savings
        all_recommendations.sort(key=lambda x: x.projected_savings, reverse=True)
        
        return all_recommendations
    
    def export_recommendations(
        self, 
        recommendations: List[CostRecommendation], 
        format: str = 'json'
    ) -> str:
        """Export recommendations to specified format."""
        if format == 'json':
            return json.dumps([
                {
                    'strategy': rec.strategy.value,
                    'resource_id': rec.resource_id,
                    'current_cost': rec.current_cost,
                    'projected_savings': rec.projected_savings,
                    'confidence': rec.confidence,
                    'implementation_risk': rec.implementation_risk,
                    'description': rec.description,
                    'action_required': rec.action_required
                }
                for rec in recommendations
            ], indent=2)
        
        elif format == 'csv':
            df = pd.DataFrame([
                {
                    'Strategy': rec.strategy.value,
                    'Resource': rec.resource_id,
                    'Current Cost': rec.current_cost,
                    'Projected Savings': rec.projected_savings,
                    'Confidence': rec.confidence,
                    'Risk': rec.implementation_risk,
                    'Description': rec.description
                }
                for rec in recommendations
            ])
            return df.to_csv(index=False)
        
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _load_config(self, config_path: Optional[str]) -> Dict:
        """Load configuration from file."""
        default_config = {
            'analysis_days': 30,
            'min_confidence_threshold': 0.5,
            'max_implementation_risk': 'high',
            'excluded_resources': [],
            'notification_threshold': 100.0  # Minimum savings to notify
        }
        
        if config_path:
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                default_config.update(user_config)
            except Exception as e:
                self.logger.warning(f"Could not load config: {e}")
        
        return default_config


async def main():
    """Main entry point for cost optimization analysis."""
    optimizer = CostOptimizer()
    
    print("🔍 Analyzing resource usage for cost optimization...")
    recommendations = await optimizer.generate_recommendations()
    
    if recommendations:
        total_savings = sum(rec.projected_savings for rec in recommendations)
        print(f"💰 Found {len(recommendations)} optimization opportunities")
        print(f"💵 Total projected monthly savings: ${total_savings:.2f}")
        
        # Export recommendations
        json_output = optimizer.export_recommendations(recommendations, 'json')
        with open('cost_optimization_recommendations.json', 'w') as f:
            f.write(json_output)
        
        print("\n📊 Top 5 recommendations:")
        for i, rec in enumerate(recommendations[:5], 1):
            print(f"{i}. {rec.description}")
            print(f"   💰 Savings: ${rec.projected_savings:.2f}/month")
            print(f"   🎯 Confidence: {rec.confidence:.0%}")
            print(f"   ⚠️  Risk: {rec.implementation_risk}")
            print()
    
    else:
        print("✅ No cost optimization opportunities found!")


if __name__ == "__main__":
    asyncio.run(main())