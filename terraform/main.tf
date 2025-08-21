# Terraform configuration for multi-cloud deployment of Agent-Orchestrated-ETL

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "agent-orchestrated-etl"
}

variable "regions" {
  description = "Deployment regions"
  type = map(object({
    aws_region    = string
    gcp_region    = string
    azure_region  = string
  }))
  default = {
    primary = {
      aws_region   = "us-east-1"
      gcp_region   = "us-central1"
      azure_region = "East US"
    }
    secondary = {
      aws_region   = "eu-west-1"
      gcp_region   = "europe-west1"
      azure_region = "West Europe"
    }
    tertiary = {
      aws_region   = "ap-southeast-1"
      gcp_region   = "asia-southeast1"
      azure_region = "Southeast Asia"
    }
  }
}

# AWS Resources
module "aws_deployment" {
  source = "./modules/aws"
  
  for_each = var.regions
  
  region       = each.value.aws_region
  environment  = var.environment
  project_name = var.project_name
  
  # ECS Configuration
  ecs_cluster_name = "${var.project_name}-${each.key}"
  task_cpu         = 2048
  task_memory      = 4096
  desired_count    = 3
  
  # RDS Configuration
  db_instance_class = "db.t3.medium"
  db_allocated_storage = 100
  
  # ElastiCache Configuration
  cache_node_type = "cache.t3.micro"
  cache_num_nodes = 2
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
    Region      = each.key
    ManagedBy   = "terraform"
  }
}

# Google Cloud Resources
module "gcp_deployment" {
  source = "./modules/gcp"
  
  for_each = var.regions
  
  region       = each.value.gcp_region
  environment  = var.environment
  project_name = var.project_name
  
  # GKE Configuration
  cluster_name     = "${var.project_name}-${each.key}"
  node_count       = 3
  machine_type     = "e2-standard-2"
  disk_size_gb     = 50
  
  # Cloud SQL Configuration
  db_tier = "db-f1-micro"
  
  # Redis Configuration
  redis_memory_size_gb = 1
}

# Azure Resources
module "azure_deployment" {
  source = "./modules/azure"
  
  for_each = var.regions
  
  region       = each.value.azure_region
  environment  = var.environment
  project_name = var.project_name
  
  # AKS Configuration
  cluster_name         = "${var.project_name}-${each.key}"
  node_count          = 3
  vm_size             = "Standard_B2s"
  os_disk_size_gb     = 50
  
  # Azure Database for PostgreSQL
  db_sku_name = "B_Gen5_1"
  db_storage_mb = 51200
  
  # Azure Cache for Redis
  redis_family   = "C"
  redis_sku_name = "Basic"
  redis_capacity = 0
}

# Global Load Balancer and CDN
resource "aws_cloudfront_distribution" "global_distribution" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = "index.html"
  
  dynamic "origin" {
    for_each = module.aws_deployment
    content {
      domain_name = origin.value.alb_dns_name
      origin_id   = "AWS-${origin.key}"
      
      custom_origin_config {
        http_port              = 80
        https_port             = 443
        origin_protocol_policy = "https-only"
        origin_ssl_protocols   = ["TLSv1.2"]
      }
    }
  }
  
  default_cache_behavior {
    allowed_methods        = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "AWS-primary"
    compress               = true
    viewer_protocol_policy = "redirect-to-https"
    
    forwarded_values {
      query_string = true
      headers      = ["Authorization", "Content-Type"]
      cookies {
        forward = "none"
      }
    }
    
    min_ttl     = 0
    default_ttl = 3600
    max_ttl     = 86400
  }
  
  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }
  
  viewer_certificate {
    cloudfront_default_certificate = true
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
  }
}

# Monitoring and Observability
resource "aws_cloudwatch_dashboard" "global_dashboard" {
  dashboard_name = "${var.project_name}-global-monitoring"
  
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        width  = 12
        height = 6
        properties = {
          metrics = [
            ["AWS/ApplicationELB", "RequestCount"],
            ["AWS/ApplicationELB", "ResponseTime"]
          ]
          period = 300
          stat   = "Average"
          region = "us-east-1"
          title  = "Load Balancer Metrics"
        }
      }
    ]
  })
}

# Outputs
output "deployment_info" {
  description = "Global deployment information"
  value = {
    aws_deployments = {
      for k, v in module.aws_deployment : k => {
        region      = v.region
        cluster_arn = v.ecs_cluster_arn
        alb_dns     = v.alb_dns_name
      }
    }
    gcp_deployments = {
      for k, v in module.gcp_deployment : k => {
        region       = v.region
        cluster_name = v.gke_cluster_name
        endpoint     = v.gke_endpoint
      }
    }
    azure_deployments = {
      for k, v in module.azure_deployment : k => {
        region       = v.region
        cluster_name = v.aks_cluster_name
        fqdn         = v.aks_fqdn
      }
    }
    global_cdn = aws_cloudfront_distribution.global_distribution.domain_name
  }
}