#!/bin/bash

# =============================================================================
# Agent-Orchestrated-ETL Build Script
# =============================================================================
# This script provides comprehensive build capabilities for different environments
# Usage: ./scripts/build.sh [COMMAND] [OPTIONS]

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT=${ENVIRONMENT:-development}
REGISTRY=${REGISTRY:-ghcr.io}
IMAGE_NAME=${IMAGE_NAME:-agent-orchestrated-etl}
TAG=${TAG:-latest}
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
VCS_REF=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
VERSION=$(grep -E '^version = ' pyproject.toml | sed -E 's/version = "(.*)"/\1/' || echo "0.0.1")
PUSH=${PUSH:-false}
NO_CACHE=${NO_CACHE:-false}
PARALLEL=${PARALLEL:-true}

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
${CYAN}Agent-Orchestrated-ETL Build Script${NC}

${YELLOW}USAGE:${NC}
    ./scripts/build.sh [COMMAND] [OPTIONS]

${YELLOW}COMMANDS:${NC}
    build       Build Docker images (default)
    test        Run tests in containers
    push        Build and push images to registry
    deploy      Deploy to specified environment
    clean       Clean up Docker artifacts
    validate    Validate build configuration
    sbom        Generate Software Bill of Materials
    security    Run security scans
    help        Show this help message

${YELLOW}OPTIONS:${NC}
    -e, --env ENV           Environment (development|staging|production) [default: development]
    -r, --registry REG      Container registry [default: ghcr.io]
    -i, --image NAME        Image name [default: agent-orchestrated-etl]
    -t, --tag TAG           Image tag [default: latest]
    -p, --push              Push images to registry
    --no-cache              Build without Docker cache
    --no-parallel           Disable parallel builds
    -v, --verbose           Verbose output
    -h, --help              Show this help

${YELLOW}EXAMPLES:${NC}
    ./scripts/build.sh build -e production -t v1.0.0
    ./scripts/build.sh push -e staging --no-cache
    ./scripts/build.sh test -e development
    ./scripts/build.sh deploy -e production
    ./scripts/build.sh clean

${YELLOW}ENVIRONMENT VARIABLES:${NC}
    ENVIRONMENT    Target environment
    REGISTRY       Container registry URL
    IMAGE_NAME     Docker image name
    TAG            Docker image tag
    PUSH           Whether to push images (true/false)
    NO_CACHE       Disable Docker cache (true/false)
    PARALLEL       Enable parallel builds (true/false)

EOF
}

# Validation functions
validate_requirements() {
    log_step "Validating requirements..."
    
    local missing_tools=()
    
    command -v docker >/dev/null 2>&1 || missing_tools+=("docker")
    command -v docker-compose >/dev/null 2>&1 || missing_tools+=("docker-compose")
    command -v git >/dev/null 2>&1 || missing_tools+=("git")
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_error "Please install the missing tools and try again."
        exit 1
    fi
    
    # Check Docker daemon
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    log_success "All requirements validated"
}

validate_environment() {
    log_step "Validating environment configuration..."
    
    case $ENVIRONMENT in
        development|staging|production)
            log_info "Environment: $ENVIRONMENT"
            ;;
        *)
            log_error "Invalid environment: $ENVIRONMENT"
            log_error "Valid environments: development, staging, production"
            exit 1
            ;;
    esac
    
    # Check for required files
    local required_files=("Dockerfile" "docker-compose.yml" "pyproject.toml")
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            log_error "Required file not found: $file"
            exit 1
        fi
    done
    
    log_success "Environment configuration validated"
}

# Build functions
build_images() {
    log_step "Building Docker images for $ENVIRONMENT environment..."
    
    local build_args=(
        "--build-arg" "BUILD_DATE=$BUILD_DATE"
        "--build-arg" "VCS_REF=$VCS_REF"
        "--build-arg" "VERSION=$VERSION"
    )
    
    if [ "$NO_CACHE" = "true" ]; then
        build_args+=("--no-cache")
    fi
    
    # Build development image
    if [ "$ENVIRONMENT" = "development" ] || [ "$ENVIRONMENT" = "staging" ]; then
        log_info "Building development image..."
        docker build \
            "${build_args[@]}" \
            --target development \
            -t "$REGISTRY/$IMAGE_NAME:dev" \
            -t "$REGISTRY/$IMAGE_NAME:$TAG-dev" \
            .
    fi
    
    # Build production image
    if [ "$ENVIRONMENT" = "staging" ] || [ "$ENVIRONMENT" = "production" ]; then
        log_info "Building production image..."
        docker build \
            "${build_args[@]}" \
            --target production \
            -t "$REGISTRY/$IMAGE_NAME:$TAG" \
            -t "$REGISTRY/$IMAGE_NAME:latest" \
            .
    fi
    
    log_success "Docker images built successfully"
}

run_tests() {
    log_step "Running tests in containers..."
    
    # Create test network if it doesn't exist
    docker network create agent-etl-test-network 2>/dev/null || true
    
    # Start test dependencies
    log_info "Starting test dependencies..."
    docker-compose -f docker-compose.yml up -d postgres redis
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 10
    
    # Run tests
    log_info "Executing test suite..."
    docker-compose -f docker-compose.yml run --rm test
    
    # Cleanup
    log_info "Cleaning up test environment..."
    docker-compose -f docker-compose.yml down
    
    log_success "Tests completed successfully"
}

push_images() {
    log_step "Pushing images to registry..."
    
    if [ "$ENVIRONMENT" = "development" ] || [ "$ENVIRONMENT" = "staging" ]; then
        log_info "Pushing development image..."
        docker push "$REGISTRY/$IMAGE_NAME:dev"
        docker push "$REGISTRY/$IMAGE_NAME:$TAG-dev"
    fi
    
    if [ "$ENVIRONMENT" = "staging" ] || [ "$ENVIRONMENT" = "production" ]; then
        log_info "Pushing production image..."
        docker push "$REGISTRY/$IMAGE_NAME:$TAG"
        
        if [ "$TAG" != "latest" ]; then
            docker push "$REGISTRY/$IMAGE_NAME:latest"
        fi
    fi
    
    log_success "Images pushed to registry successfully"
}

deploy_environment() {
    log_step "Deploying to $ENVIRONMENT environment..."
    
    case $ENVIRONMENT in
        development)
            log_info "Starting development environment..."
            docker-compose up -d
            ;;
        staging)
            log_info "Starting staging environment..."
            docker-compose -f docker-compose.yml -f docker-compose.staging.yml up -d
            ;;
        production)
            log_info "Starting production environment..."
            docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
            ;;
    esac
    
    log_info "Waiting for services to be healthy..."
    sleep 30
    
    # Health check
    if check_health; then
        log_success "Deployment successful - all services healthy"
    else
        log_error "Deployment failed - some services are unhealthy"
        exit 1
    fi
}

check_health() {
    log_step "Checking service health..."
    
    local healthy=true
    
    # Check application health
    if curl -f http://localhost:8080/health >/dev/null 2>&1; then
        log_info "✓ Application service is healthy"
    else
        log_warn "✗ Application service is not responding"
        healthy=false
    fi
    
    # Check database health
    if docker-compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
        log_info "✓ Database service is healthy"
    else
        log_warn "✗ Database service is not responding"
        healthy=false
    fi
    
    # Check Redis health
    if docker-compose exec -T redis redis-cli ping >/dev/null 2>&1; then
        log_info "✓ Redis service is healthy"
    else
        log_warn "✗ Redis service is not responding"
        healthy=false
    fi
    
    $healthy
}

clean_docker() {
    log_step "Cleaning up Docker artifacts..."
    
    # Stop and remove containers
    docker-compose down --remove-orphans 2>/dev/null || true
    
    # Remove dangling images
    docker image prune -f
    
    # Remove unused volumes (be careful with this in production)
    if [ "$ENVIRONMENT" = "development" ]; then
        docker volume prune -f
    fi
    
    # Remove build cache
    docker builder prune -f
    
    log_success "Docker cleanup completed"
}

generate_sbom() {
    log_step "Generating Software Bill of Materials (SBOM)..."
    
    # Create SBOM directory
    mkdir -p build/sbom
    
    # Generate Python dependencies SBOM
    docker run --rm -v "$(pwd):/app" \
        "$REGISTRY/$IMAGE_NAME:$TAG" \
        pip list --format=json > build/sbom/python-packages.json
    
    # Generate container SBOM using syft
    if command -v syft >/dev/null 2>&1; then
        syft "$REGISTRY/$IMAGE_NAME:$TAG" -o json > build/sbom/container-sbom.json
        syft "$REGISTRY/$IMAGE_NAME:$TAG" -o spdx-json > build/sbom/container-sbom.spdx.json
    else
        log_warn "syft not found - container SBOM generation skipped"
    fi
    
    log_success "SBOM generated in build/sbom/"
}

run_security_scan() {
    log_step "Running security scans..."
    
    mkdir -p build/security
    
    # Scan container image with trivy
    if command -v trivy >/dev/null 2>&1; then
        trivy image \
            --format json \
            --output build/security/trivy-report.json \
            "$REGISTRY/$IMAGE_NAME:$TAG"
        
        trivy image \
            --format table \
            --severity HIGH,CRITICAL \
            "$REGISTRY/$IMAGE_NAME:$TAG"
    else
        log_warn "trivy not found - container security scan skipped"
    fi
    
    # Scan Python dependencies
    docker run --rm -v "$(pwd):/app" \
        "$REGISTRY/$IMAGE_NAME:$TAG" \
        safety check --json > build/security/safety-report.json || true
    
    log_success "Security scan completed - reports in build/security/"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -r|--registry)
                REGISTRY="$2"
                shift 2
                ;;
            -i|--image)
                IMAGE_NAME="$2"
                shift 2
                ;;
            -t|--tag)
                TAG="$2"
                shift 2
                ;;
            -p|--push)
                PUSH=true
                shift
                ;;
            --no-cache)
                NO_CACHE=true
                shift
                ;;
            --no-parallel)
                PARALLEL=false
                shift
                ;;
            -v|--verbose)
                set -x
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                COMMAND="$1"
                shift
                ;;
        esac
    done
}

# Main execution
main() {
    local command="${COMMAND:-build}"
    
    log_info "Agent-Orchestrated-ETL Build Script"
    log_info "Environment: $ENVIRONMENT"
    log_info "Registry: $REGISTRY"
    log_info "Image: $IMAGE_NAME:$TAG"
    log_info "Command: $command"
    echo
    
    validate_requirements
    validate_environment
    
    case $command in
        build)
            build_images
            if [ "$PUSH" = "true" ]; then
                push_images
            fi
            ;;
        test)
            run_tests
            ;;
        push)
            build_images
            push_images
            ;;
        deploy)
            build_images
            if [ "$PUSH" = "true" ]; then
                push_images
            fi
            deploy_environment
            ;;
        clean)
            clean_docker
            ;;
        validate)
            log_success "Configuration is valid"
            ;;
        sbom)
            generate_sbom
            ;;
        security)
            run_security_scan
            ;;
        help)
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            log_error "Run './scripts/build.sh help' for usage information"
            exit 1
            ;;
    esac
    
    log_success "Build script completed successfully!"
}

# Handle script arguments
if [ $# -eq 0 ]; then
    COMMAND="build"
else
    parse_args "$@"
fi

# Execute main function
main