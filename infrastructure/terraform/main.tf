# Infrastructure as Code for Agent-Orchestrated-ETL
# Terraform configuration for deploying ETL infrastructure on AWS

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4"
    }
  }
  
  backend "s3" {
    # Configure your S3 backend
    # bucket = "your-terraform-state-bucket"
    # key    = "agent-etl/terraform.tfstate"
    # region = "us-east-1"
  }
}

# Local variables
locals {
  project_name = "agent-orchestrated-etl"
  environment  = var.environment
  
  common_tags = {
    Project     = local.project_name
    Environment = local.environment
    ManagedBy   = "Terraform"
    Repository  = "github.com/terragon-labs/agent-orchestrated-etl"
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Random suffix for unique resource names
resource "random_id" "suffix" {
  byte_length = 4
}

# VPC Configuration
module "vpc" {
  source = "./modules/vpc"
  
  project_name = local.project_name
  environment  = local.environment
  
  vpc_cidr             = var.vpc_cidr
  availability_zones   = var.availability_zones
  private_subnet_cidrs = var.private_subnet_cidrs
  public_subnet_cidrs  = var.public_subnet_cidrs
  
  tags = local.common_tags
}

# ECS Cluster for containerized ETL agents
module "ecs" {
  source = "./modules/ecs"
  
  project_name = local.project_name
  environment  = local.environment
  
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  
  # Task configuration
  etl_agent_cpu       = var.etl_agent_cpu
  etl_agent_memory    = var.etl_agent_memory
  etl_agent_count     = var.etl_agent_count
  
  # Container image
  container_image     = var.container_image
  
  tags = local.common_tags
}

# RDS PostgreSQL for metadata and coordination
module "database" {
  source = "./modules/rds"
  
  project_name = local.project_name
  environment  = local.environment
  
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  
  # Database configuration
  db_instance_class   = var.db_instance_class
  db_allocated_storage = var.db_allocated_storage
  db_max_allocated_storage = var.db_max_allocated_storage
  
  # Database credentials (stored in Secrets Manager)
  db_username         = var.db_username
  
  tags = local.common_tags
}

# S3 buckets for data storage
module "storage" {
  source = "./modules/s3"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Bucket configuration
  enable_versioning   = var.enable_s3_versioning
  enable_encryption   = true
  lifecycle_enabled   = true
  
  tags = local.common_tags
}

# Lambda functions for serverless processing
module "lambda" {
  source = "./modules/lambda"
  
  project_name = local.project_name
  environment  = local.environment
  
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  
  # Function configuration
  python_runtime      = var.lambda_python_runtime
  lambda_timeout      = var.lambda_timeout
  lambda_memory_size  = var.lambda_memory_size
  
  # Data bucket for triggers
  data_bucket_arn     = module.storage.data_bucket_arn
  data_bucket_name    = module.storage.data_bucket_name
  
  tags = local.common_tags
}

# ElastiCache Redis for caching and coordination
module "cache" {
  source = "./modules/elasticache"
  
  project_name = local.project_name
  environment  = local.environment
  
  vpc_id              = module.vpc.vpc_id
  private_subnet_ids  = module.vpc.private_subnet_ids
  
  # Cache configuration
  node_type           = var.redis_node_type
  num_cache_nodes     = var.redis_num_nodes
  
  tags = local.common_tags
}

# CloudWatch monitoring and logging
module "monitoring" {
  source = "./modules/monitoring"
  
  project_name = local.project_name
  environment  = local.environment
  
  # ECS cluster name for monitoring
  ecs_cluster_name    = module.ecs.cluster_name
  
  # Database instance identifier
  db_instance_id      = module.database.db_instance_id
  
  # S3 bucket names
  data_bucket_name    = module.storage.data_bucket_name
  logs_bucket_name    = module.storage.logs_bucket_name
  
  # Notification configuration
  alert_email         = var.alert_email
  
  tags = local.common_tags
}

# IAM roles and policies
module "iam" {
  source = "./modules/iam"
  
  project_name = local.project_name
  environment  = local.environment
  
  # Resource ARNs for policy creation
  data_bucket_arn     = module.storage.data_bucket_arn
  logs_bucket_arn     = module.storage.logs_bucket_arn
  database_secret_arn = module.database.db_secret_arn
  
  tags = local.common_tags
}

# Security groups and network ACLs
module "security" {
  source = "./modules/security"
  
  project_name = local.project_name
  environment  = local.environment
  
  vpc_id              = module.vpc.vpc_id
  vpc_cidr            = module.vpc.vpc_cidr
  
  tags = local.common_tags
}