#!/bin/bash

# FLEET-Q Docker Development Helper Script
# This script provides convenient commands for Docker-based development

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="docker-compose.yml"
ENV_FILE=".env"
IMAGE_NAME="fleet-q"

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if .env file exists
check_env_file() {
    if [ ! -f "$ENV_FILE" ]; then
        log_warning ".env file not found. Creating from example..."
        if [ -f ".env.docker.example" ]; then
            cp .env.docker.example .env
            log_info "Created .env from .env.docker.example"
            log_warning "Please edit .env with your Snowflake credentials before continuing"
            return 1
        else
            log_error ".env.docker.example not found. Please create .env manually"
            return 1
        fi
    fi
    return 0
}

# Build the Docker image
build() {
    log_info "Building FLEET-Q Docker image..."
    docker build -t "$IMAGE_NAME" .
    log_success "Docker image built successfully"
}

# Start all services
start() {
    check_env_file || return 1
    log_info "Starting FLEET-Q services..."
    docker-compose up -d
    log_success "Services started successfully"
    
    log_info "Waiting for services to be healthy..."
    sleep 10
    
    log_info "Service status:"
    docker-compose ps
    
    log_info "Access points:"
    echo "  - Leader Pod:  http://localhost:8000"
    echo "  - Worker 1:    http://localhost:8001"
    echo "  - Worker 2:    http://localhost:8002"
}

# Stop all services
stop() {
    log_info "Stopping FLEET-Q services..."
    docker-compose down
    log_success "Services stopped successfully"
}

# Restart all services
restart() {
    stop
    start
}

# View logs
logs() {
    local service=${1:-""}
    if [ -n "$service" ]; then
        log_info "Showing logs for $service..."
        docker-compose logs -f "$service"
    else
        log_info "Showing logs for all services..."
        docker-compose logs -f
    fi
}

# Show service status
status() {
    log_info "Service status:"
    docker-compose ps
    
    log_info "Health check status:"
    for service in fleet-q-leader fleet-q-worker-1 fleet-q-worker-2; do
        container_id=$(docker-compose ps -q "$service" 2>/dev/null)
        if [ -n "$container_id" ]; then
            health=$(docker inspect --format='{{.State.Health.Status}}' "$container_id" 2>/dev/null || echo "no-healthcheck")
            echo "  $service: $health"
        else
            echo "  $service: not running"
        fi
    done
}

# Test API endpoints
test() {
    log_info "Testing API endpoints..."
    
    # Test health endpoints
    for port in 8000 8001 8002; do
        log_info "Testing health endpoint on port $port..."
        if curl -s -f "http://localhost:$port/health" > /dev/null; then
            log_success "Port $port: healthy"
        else
            log_error "Port $port: unhealthy or not responding"
        fi
    done
    
    # Test leader endpoint
    log_info "Testing leader election endpoint..."
    if curl -s -f "http://localhost:8000/admin/leader" > /dev/null; then
        log_success "Leader endpoint: accessible"
        curl -s "http://localhost:8000/admin/leader" | jq . 2>/dev/null || curl -s "http://localhost:8000/admin/leader"
    else
        log_error "Leader endpoint: not accessible"
    fi
    
    # Test queue endpoint
    log_info "Testing queue statistics endpoint..."
    if curl -s -f "http://localhost:8000/admin/queue" > /dev/null; then
        log_success "Queue endpoint: accessible"
        curl -s "http://localhost:8000/admin/queue" | jq . 2>/dev/null || curl -s "http://localhost:8000/admin/queue"
    else
        log_error "Queue endpoint: not accessible"
    fi
}

# Submit a test step
submit_test() {
    local step_type=${1:-"test"}
    local step_args=${2:-'{"message": "Hello from Docker test"}'}
    
    log_info "Submitting test step..."
    
    payload=$(cat <<EOF
{
    "type": "$step_type",
    "args": $step_args,
    "metadata": {
        "submitted_by": "docker-dev-script",
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    }
}
EOF
)
    
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "http://localhost:8000/submit")
    
    if [ $? -eq 0 ]; then
        log_success "Step submitted successfully"
        echo "Response: $response"
        
        # Extract step ID and check status
        step_id=$(echo "$response" | jq -r '.step_id' 2>/dev/null || echo "$response")
        if [ "$step_id" != "null" ] && [ -n "$step_id" ]; then
            log_info "Checking step status..."
            sleep 2
            curl -s "http://localhost:8000/status/$step_id" | jq . 2>/dev/null || curl -s "http://localhost:8000/status/$step_id"
        fi
    else
        log_error "Failed to submit step"
    fi
}

# Clean up Docker resources
clean() {
    log_info "Cleaning up Docker resources..."
    
    # Stop and remove containers
    docker-compose down -v
    
    # Remove image
    if docker images | grep -q "$IMAGE_NAME"; then
        docker rmi "$IMAGE_NAME" 2>/dev/null || true
    fi
    
    # Remove unused volumes
    docker volume prune -f
    
    log_success "Cleanup completed"
}

# Development mode with hot reloading
dev() {
    check_env_file || return 1
    log_info "Starting FLEET-Q in development mode with hot reloading..."
    
    # Use override file for development
    docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d
    
    log_success "Development environment started"
    log_info "Source code is mounted for hot reloading"
    logs
}

# Scale services
scale() {
    local workers=${1:-2}
    log_info "Scaling worker services to $workers instances each..."
    
    docker-compose up -d --scale fleet-q-worker-1="$workers" --scale fleet-q-worker-2="$workers"
    
    log_success "Services scaled successfully"
    status
}

# Show help
help() {
    echo "FLEET-Q Docker Development Helper"
    echo ""
    echo "Usage: $0 <command> [arguments]"
    echo ""
    echo "Commands:"
    echo "  build                 Build the Docker image"
    echo "  start                 Start all services"
    echo "  stop                  Stop all services"
    echo "  restart               Restart all services"
    echo "  logs [service]        Show logs (optionally for specific service)"
    echo "  status                Show service status and health"
    echo "  test                  Test API endpoints"
    echo "  submit [type] [args]  Submit a test step"
    echo "  clean                 Clean up Docker resources"
    echo "  dev                   Start in development mode with hot reloading"
    echo "  scale <count>         Scale worker services"
    echo "  help                  Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start              # Start all services"
    echo "  $0 logs leader        # Show logs for leader service"
    echo "  $0 submit test        # Submit a test step"
    echo "  $0 scale 3            # Scale workers to 3 instances each"
    echo "  $0 dev                # Start with hot reloading"
}

# Main command dispatcher
case "${1:-help}" in
    build)
        build
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    logs)
        logs "$2"
        ;;
    status)
        status
        ;;
    test)
        test
        ;;
    submit)
        submit_test "$2" "$3"
        ;;
    clean)
        clean
        ;;
    dev)
        dev
        ;;
    scale)
        scale "$2"
        ;;
    help|--help|-h)
        help
        ;;
    *)
        log_error "Unknown command: $1"
        help
        exit 1
        ;;
esac