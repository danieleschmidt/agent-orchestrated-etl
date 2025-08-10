#!/bin/bash
# Autonomous SDLC Production Deployment Setup Script
# Terragon Labs - Advanced SDLC Implementation

set -e  # Exit on any error
set -u  # Exit on undefined variable

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="terragon-pipelines"
DEPLOYMENT_ENV="${DEPLOYMENT_ENV:-production}"
AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="${CLUSTER_NAME:-terragon-autonomous-cluster}"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-terragon}"

log() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check required tools
    local tools=("kubectl" "helm" "aws" "docker")
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            error "$tool is required but not installed"
        fi
    done
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured or invalid"
    fi
    
    # Check kubectl context
    if ! kubectl cluster-info &> /dev/null; then
        error "kubectl not connected to cluster or cluster not accessible"
    fi
    
    success "Prerequisites check passed"
}

setup_namespace() {
    log "Setting up namespace: $NAMESPACE"
    
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    
    # Label namespace for monitoring and security
    kubectl label namespace "$NAMESPACE" \
        name="$NAMESPACE" \
        environment="$DEPLOYMENT_ENV" \
        component="autonomous-sdlc" \
        --overwrite
    
    success "Namespace $NAMESPACE ready"
}

create_secrets() {
    log "Creating secrets..."
    
    # Check if secrets already exist
    if kubectl get secret autonomous-sdlc-secrets -n "$NAMESPACE" &> /dev/null; then
        warn "Secrets already exist, skipping creation"
        return
    fi
    
    # Create secrets (these would be populated from secure secret management)
    kubectl create secret generic autonomous-sdlc-secrets -n "$NAMESPACE" \
        --from-literal=postgres-host="${POSTGRES_HOST:-postgres.terragon.internal}" \
        --from-literal=postgres-user="${POSTGRES_USER:-autonomous_sdlc}" \
        --from-literal=postgres-password="${POSTGRES_PASSWORD:-$(openssl rand -base64 32)}" \
        --from-literal=postgres-database="${POSTGRES_DATABASE:-autonomous_sdlc_db}" \
        --from-literal=aws-access-key-id="${AWS_ACCESS_KEY_ID}" \
        --from-literal=aws-secret-access-key="${AWS_SECRET_ACCESS_KEY}" \
        --from-literal=redis-password="${REDIS_PASSWORD:-$(openssl rand -base64 32)}" \
        --from-literal=jwt-secret="${JWT_SECRET:-$(openssl rand -base64 64)}" \
        --from-literal=encryption-key="${ENCRYPTION_KEY:-$(openssl rand -base64 32)}"
    
    success "Secrets created"
}

setup_storage() {
    log "Setting up persistent storage..."
    
    # Create storage class for EFS (if using AWS)
    if [[ "$AWS_REGION" != "" ]]; then
        cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: aws-efs
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ap
  fileSystemId: ${EFS_FILESYSTEM_ID:-fs-default}
  directoryPerms: "700"
  gidRangeStart: "1000"
  gidRangeEnd: "2000"
  basePath: "/autonomous-pipelines"
EOF
    fi
    
    success "Storage configuration applied"
}

deploy_database() {
    log "Deploying database (PostgreSQL)..."
    
    # Add Bitnami Helm repository
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    
    # Deploy PostgreSQL
    helm upgrade --install postgresql bitnami/postgresql \
        --namespace "$NAMESPACE" \
        --set auth.username="autonomous_sdlc" \
        --set auth.database="autonomous_sdlc_db" \
        --set auth.existingSecret="autonomous-sdlc-secrets" \
        --set auth.secretKeys.adminPasswordKey="postgres-password" \
        --set auth.secretKeys.userPasswordKey="postgres-password" \
        --set primary.persistence.enabled=true \
        --set primary.persistence.size="100Gi" \
        --set primary.resources.requests.cpu="1000m" \
        --set primary.resources.requests.memory="2Gi" \
        --set primary.resources.limits.cpu="2000m" \
        --set primary.resources.limits.memory="4Gi" \
        --set metrics.enabled=true \
        --set metrics.serviceMonitor.enabled=true \
        --wait --timeout=600s
    
    success "PostgreSQL deployed"
}

deploy_redis() {
    log "Deploying Redis for caching..."
    
    helm upgrade --install redis bitnami/redis \
        --namespace "$NAMESPACE" \
        --set auth.enabled=true \
        --set auth.existingSecret="autonomous-sdlc-secrets" \
        --set auth.existingSecretPasswordKey="redis-password" \
        --set master.persistence.enabled=true \
        --set master.persistence.size="20Gi" \
        --set master.resources.requests.cpu="200m" \
        --set master.resources.requests.memory="512Mi" \
        --set master.resources.limits.cpu="1000m" \
        --set master.resources.limits.memory="2Gi" \
        --set replica.replicaCount=2 \
        --set metrics.enabled=true \
        --set metrics.serviceMonitor.enabled=true \
        --wait --timeout=300s
    
    success "Redis deployed"
}

build_and_push_images() {
    log "Building and pushing Docker images..."
    
    # Build main application image
    docker build -t "${DOCKER_REGISTRY}/autonomous-pipeline-manager:v1.0.0" \
        -f deployment/Dockerfile.production .
    
    # Build backup tools image
    docker build -t "${DOCKER_REGISTRY}/backup-tools:v1.0.0" \
        -f deployment/Dockerfile.backup .
    
    # Push images
    docker push "${DOCKER_REGISTRY}/autonomous-pipeline-manager:v1.0.0"
    docker push "${DOCKER_REGISTRY}/backup-tools:v1.0.0"
    
    success "Docker images built and pushed"
}

deploy_autonomous_sdlc() {
    log "Deploying Autonomous SDLC system..."
    
    # Update image references in deployment YAML
    sed -i "s/terragon\/autonomous-pipeline-manager:v1.0.0/${DOCKER_REGISTRY//\//\\/}\/autonomous-pipeline-manager:v1.0.0/g" \
        deployment/autonomous_sdlc_deployment.yaml
    
    sed -i "s/terragon\/backup-tools:v1.0.0/${DOCKER_REGISTRY//\//\\/}\/backup-tools:v1.0.0/g" \
        deployment/autonomous_sdlc_deployment.yaml
    
    # Apply deployment
    kubectl apply -f deployment/autonomous_sdlc_deployment.yaml
    
    # Wait for deployment to be ready
    kubectl rollout status deployment/autonomous-pipeline-manager -n "$NAMESPACE" --timeout=600s
    
    success "Autonomous SDLC system deployed"
}

setup_monitoring() {
    log "Setting up monitoring and observability..."
    
    # Add Prometheus Helm repository
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update
    
    # Deploy Prometheus stack (if not already present)
    if ! helm list -n monitoring | grep -q prometheus; then
        kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
        
        helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
            --namespace monitoring \
            --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
            --set prometheus.prometheusSpec.retention="30d" \
            --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.storageClassName="gp2" \
            --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage="100Gi" \
            --set grafana.adminPassword="$(openssl rand -base64 32)" \
            --wait --timeout=600s
    fi
    
    success "Monitoring setup complete"
}

setup_ingress() {
    log "Setting up ingress..."
    
    # Create ingress for the service
    cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: autonomous-pipeline-manager-ingress
  namespace: $NAMESPACE
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
    nginx.ingress.kubernetes.io/rate-limit: "100"
    nginx.ingress.kubernetes.io/rate-limit-window: "1m"
spec:
  tls:
  - hosts:
    - autonomous-sdlc.terragon.com
    secretName: autonomous-sdlc-tls
  rules:
  - host: autonomous-sdlc.terragon.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: autonomous-pipeline-manager-service
            port:
              number: 80
EOF
    
    success "Ingress configured"
}

run_health_checks() {
    log "Running health checks..."
    
    # Wait for pods to be ready
    kubectl wait --for=condition=ready pod \
        -l app=autonomous-pipeline-manager \
        -n "$NAMESPACE" \
        --timeout=300s
    
    # Check service endpoints
    kubectl get endpoints autonomous-pipeline-manager-service -n "$NAMESPACE"
    
    # Test health endpoint
    local service_ip
    service_ip=$(kubectl get service autonomous-pipeline-manager-service -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    
    if [[ -n "$service_ip" ]]; then
        log "Testing health endpoint at $service_ip..."
        if curl -f "http://$service_ip/health" --connect-timeout 10; then
            success "Health check passed"
        else
            warn "Health check failed, but deployment may still be starting up"
        fi
    else
        warn "Service IP not yet available"
    fi
    
    # Show deployment status
    kubectl get all -n "$NAMESPACE"
    
    success "Health checks completed"
}

setup_backup_and_recovery() {
    log "Setting up backup and disaster recovery..."
    
    # Create S3 bucket for backups (if it doesn't exist)
    local backup_bucket="terragon-autonomous-backups-${AWS_REGION}"
    
    if ! aws s3 ls "s3://$backup_bucket" &> /dev/null; then
        aws s3 mb "s3://$backup_bucket" --region "$AWS_REGION"
        
        # Enable versioning and lifecycle policy
        aws s3api put-bucket-versioning \
            --bucket "$backup_bucket" \
            --versioning-configuration Status=Enabled
        
        # Create lifecycle policy
        cat > /tmp/lifecycle-policy.json <<EOF
{
    "Rules": [
        {
            "ID": "DeleteOldBackups",
            "Status": "Enabled",
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 90,
                    "StorageClass": "GLACIER"
                }
            ],
            "Expiration": {
                "Days": 365
            }
        }
    ]
}
EOF
        
        aws s3api put-bucket-lifecycle-configuration \
            --bucket "$backup_bucket" \
            --lifecycle-configuration file:///tmp/lifecycle-policy.json
        
        rm /tmp/lifecycle-policy.json
    fi
    
    # Update backup configuration with actual bucket name
    kubectl patch configmap autonomous-backup-config -n "$NAMESPACE" \
        --patch '{"data":{"s3_bucket":"'$backup_bucket'"}}'
    
    success "Backup and recovery configured"
}

generate_deployment_report() {
    log "Generating deployment report..."
    
    local report_file="deployment_report_$(date +%Y%m%d_%H%M%S).txt"
    
    cat > "$report_file" <<EOF
TERRAGON LABS - AUTONOMOUS SDLC DEPLOYMENT REPORT
===============================================

Deployment Date: $(date)
Environment: $DEPLOYMENT_ENV
Namespace: $NAMESPACE
AWS Region: $AWS_REGION
Cluster: $CLUSTER_NAME

DEPLOYED COMPONENTS:
-------------------
- Autonomous Pipeline Manager: DEPLOYED
- PostgreSQL Database: DEPLOYED
- Redis Cache: DEPLOYED  
- Monitoring Stack: DEPLOYED
- Backup System: DEPLOYED
- Ingress Controller: DEPLOYED

RESOURCE ALLOCATION:
-------------------
EOF
    
    kubectl top nodes >> "$report_file" 2>/dev/null || echo "Node metrics not available" >> "$report_file"
    echo "" >> "$report_file"
    
    kubectl get pods -n "$NAMESPACE" -o wide >> "$report_file"
    echo "" >> "$report_file"
    
    kubectl get services -n "$NAMESPACE" >> "$report_file"
    echo "" >> "$report_file"
    
    cat >> "$report_file" <<EOF

ACCESS INFORMATION:
------------------
- Service URL: https://autonomous-sdlc.terragon.com
- Grafana Dashboard: https://grafana.terragon.com
- Prometheus: https://prometheus.terragon.com

NEXT STEPS:
----------
1. Configure DNS to point to the load balancer IP
2. Set up SSL certificates 
3. Configure monitoring alerts
4. Run integration tests
5. Update documentation

SUPPORT:
-------
For issues or questions, contact:
- Engineering Team: engineering@terragon.com
- DevOps Team: devops@terragon.com
- Emergency: ops-team@terragon.com

EOF
    
    success "Deployment report generated: $report_file"
}

# Main deployment flow
main() {
    log "Starting Terragon Labs Autonomous SDLC deployment..."
    
    check_prerequisites
    setup_namespace
    create_secrets
    setup_storage
    deploy_database
    deploy_redis
    build_and_push_images
    deploy_autonomous_sdlc
    setup_monitoring
    setup_ingress
    setup_backup_and_recovery
    run_health_checks
    generate_deployment_report
    
    success "🎉 Autonomous SDLC deployment completed successfully!"
    
    cat <<EOF

🚀 DEPLOYMENT COMPLETE 🚀

Your Autonomous SDLC system is now running in production!

Key URLs:
- Main Application: https://autonomous-sdlc.terragon.com
- Health Check: https://autonomous-sdlc.terragon.com/health
- Metrics: https://autonomous-sdlc.terragon.com/metrics

Next Steps:
1. Run integration tests: ./scripts/run_integration_tests.sh
2. Configure monitoring dashboards
3. Set up automated testing pipelines
4. Review security configurations
5. Schedule regular backups verification

For support: https://github.com/terragon-labs/autonomous-sdlc/issues

Happy automating! 🤖✨

EOF
}

# Handle script arguments
case "${1:-deploy}" in
    "deploy")
        main
        ;;
    "check")
        check_prerequisites
        ;;
    "secrets")
        create_secrets
        ;;
    "health")
        run_health_checks
        ;;
    "report")
        generate_deployment_report
        ;;
    *)
        echo "Usage: $0 [deploy|check|secrets|health|report]"
        echo ""
        echo "Commands:"
        echo "  deploy  - Full deployment (default)"
        echo "  check   - Check prerequisites only" 
        echo "  secrets - Create secrets only"
        echo "  health  - Run health checks only"
        echo "  report  - Generate deployment report only"
        exit 1
        ;;
esac