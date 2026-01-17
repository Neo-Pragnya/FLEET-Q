# FLEET-Q Docker Deployment Guide

This guide covers Docker deployment and local development setup for FLEET-Q, including container configuration, environment setup, and operational workflows.

## Overview

FLEET-Q is designed to run as a single-process container that coordinates all operations internally. The Docker configuration supports:

- Multi-stage builds with uv for fast dependency management
- Single-process execution with proper signal handling
- Health check configuration for container orchestration
- Local development environment with multiple pods
- Environment variable configuration for all settings

## Quick Start

### Prerequisites

1. **Docker and Docker Compose** installed
2. **Snowflake account** with appropriate permissions
3. **Environment variables** configured (see Configuration section)

### Basic Deployment

1. **Clone and configure**:
   ```bash
   git clone <repository-url>
   cd fleet-q
   cp .env.example .env
   # Edit .env with your Snowflake credentials
   ```

2. **Build and run single pod**:
   ```bash
   docker build -t fleet-q .
   docker run -p 8000:8000 --env-file .env fleet-q
   ```

3. **Or use docker-compose for multi-pod setup**:
   ```bash
   docker-compose up -d
   ```

## Docker Configuration

### Dockerfile Features

The multi-stage Dockerfile implements:

- **Builder stage**: Installs dependencies with uv for fast builds
- **Production stage**: Minimal runtime image with security best practices
- **Non-root user**: Runs as `fleetq` user for security
- **Health checks**: Built-in health endpoint monitoring
- **Signal handling**: Proper SIGTERM/SIGINT handling for graceful shutdown

### Container Architecture

```
┌─────────────────────────────────────┐
│ FLEET-Q Container                   │
├─────────────────────────────────────┤
│ • FastAPI Server (port 8000)        │
│ • Heartbeat Loop (background)       │
│ • Claim Loop (background)           │
│ • Recovery Loop (leader only)       │
│ • SQLite DLQ (ephemeral)           │
└─────────────────────────────────────┘
```

### Health Check Configuration

The container includes comprehensive health checks:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1
```

- **Interval**: Check every 30 seconds
- **Timeout**: 10 second timeout per check
- **Start period**: 40 seconds for application startup
- **Retries**: 3 consecutive failures before marking unhealthy

## Environment Configuration

### Required Variables

```bash
# Snowflake Connection (Required)
FLEET_Q_SNOWFLAKE_ACCOUNT=your-account
FLEET_Q_SNOWFLAKE_USER=your-username
FLEET_Q_SNOWFLAKE_PASSWORD=your-password
FLEET_Q_SNOWFLAKE_DATABASE=your-database
FLEET_Q_SNOWFLAKE_WAREHOUSE=your-warehouse
FLEET_Q_SNOWFLAKE_ROLE=your-role
```

### Optional Configuration

```bash
# Pod Configuration
FLEET_Q_POD_ID=auto-generated          # Unique pod identifier
FLEET_Q_MAX_PARALLELISM=10             # Maximum concurrent steps
FLEET_Q_CAPACITY_THRESHOLD=0.8         # Use 80% of max capacity

# Timing Configuration (seconds)
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=30
FLEET_Q_CLAIM_INTERVAL_SECONDS=5
FLEET_Q_RECOVERY_INTERVAL_SECONDS=300
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=180

# API Configuration
FLEET_Q_API_HOST=0.0.0.0
FLEET_Q_API_PORT=8000

# Logging Configuration
FLEET_Q_LOG_LEVEL=INFO                 # DEBUG, INFO, WARNING, ERROR
FLEET_Q_LOG_FORMAT=json                # json, text

# SQLite Configuration
FLEET_Q_SQLITE_DB_PATH=/app/data/fleet_q.db
```

## Local Development

### Multi-Pod Development Setup

The docker-compose.yml provides a complete multi-pod environment:

```bash
# Start all pods
docker-compose up -d

# View logs
docker-compose logs -f

# Scale workers
docker-compose up -d --scale fleet-q-worker-1=2

# Stop all pods
docker-compose down
```

### Pod Configuration

| Service | Port | Role | Parallelism |
|---------|------|------|-------------|
| fleet-q-leader | 8000 | Leader + Worker | 10 |
| fleet-q-worker-1 | 8001 | Worker | 8 |
| fleet-q-worker-2 | 8002 | Worker | 6 |

### Development with SQLite Simulation

For development without Snowflake access:

```bash
# Start SQLite simulation mode
docker-compose --profile dev-sqlite up fleet-q-dev-sqlite

# Access at http://localhost:8003
```

### Accessing Services

- **Leader Pod**: http://localhost:8000
- **Worker 1**: http://localhost:8001  
- **Worker 2**: http://localhost:8002
- **Dev SQLite**: http://localhost:8003 (with profile)

### API Endpoints

Each pod exposes the same API endpoints:

- `GET /health` - Pod health status
- `POST /submit` - Submit new step
- `GET /status/{step_id}` - Step status
- `GET /admin/leader` - Leader election info
- `GET /admin/queue` - Queue statistics
- `POST /admin/recovery/run` - Manual recovery (leader only)

## Production Deployment

### Kubernetes Deployment

Example Kubernetes deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-q
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fleet-q
  template:
    metadata:
      labels:
        app: fleet-q
    spec:
      containers:
      - name: fleet-q
        image: fleet-q:latest
        ports:
        - containerPort: 8000
        env:
        - name: FLEET_Q_SNOWFLAKE_ACCOUNT
          valueFrom:
            secretKeyRef:
              name: fleet-q-secrets
              key: snowflake-account
        # ... other environment variables
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 40
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
```

### EKS Specific Configuration

For AWS EKS deployments:

1. **IAM Roles**: Configure appropriate IAM roles for Snowflake access
2. **Secrets Management**: Use AWS Secrets Manager or Kubernetes secrets
3. **Load Balancer**: Configure ALB/NLB for API access
4. **Monitoring**: Integrate with CloudWatch or Prometheus
5. **Autoscaling**: Configure HPA based on queue depth metrics

### Security Considerations

- **Non-root execution**: Container runs as `fleetq` user
- **Secret management**: Never include credentials in images
- **Network policies**: Restrict pod-to-pod communication (FLEET-Q doesn't need it)
- **Resource limits**: Set appropriate CPU/memory limits
- **Image scanning**: Regularly scan images for vulnerabilities

## Monitoring and Observability

### Health Monitoring

Monitor container health using:

```bash
# Docker health status
docker ps --format "table {{.Names}}\t{{.Status}}"

# Docker Compose health status
docker-compose ps

# Kubernetes health status
kubectl get pods -l app=fleet-q
```

### Log Collection

FLEET-Q logs to stdout/stderr for container log collection:

```bash
# Docker logs
docker logs fleet-q-container

# Docker Compose logs
docker-compose logs -f fleet-q-leader

# Kubernetes logs
kubectl logs -l app=fleet-q -f
```

### Metrics Collection

Access metrics through API endpoints:

```bash
# Queue statistics
curl http://localhost:8000/admin/queue

# Leader information
curl http://localhost:8000/admin/leader

# Pod health
curl http://localhost:8000/health
```

## Troubleshooting

### Common Issues

1. **Container won't start**:
   ```bash
   # Check logs
   docker logs container-name
   
   # Verify environment variables
   docker exec container-name env | grep FLEET_Q
   ```

2. **Health check failures**:
   ```bash
   # Test health endpoint manually
   docker exec container-name curl http://localhost:8000/health
   
   # Check application logs
   docker logs container-name | grep -i error
   ```

3. **Snowflake connection issues**:
   ```bash
   # Verify credentials
   docker exec container-name python -c "
   from fleet_q.config import load_config
   config = load_config()
   print(f'Account: {config.snowflake_account}')
   print(f'Database: {config.snowflake_database}')
   "
   ```

4. **Pod not becoming leader**:
   ```bash
   # Check leader election
   curl http://localhost:8000/admin/leader
   
   # Check pod health records
   # (requires database access)
   ```

### Performance Tuning

1. **Resource allocation**:
   - CPU: 0.5-1 core per 10 max parallelism
   - Memory: 256MB base + 32MB per max parallelism
   - Storage: Minimal (SQLite is ephemeral)

2. **Configuration tuning**:
   - Reduce claim interval for higher throughput
   - Increase parallelism for CPU-bound tasks
   - Adjust heartbeat interval based on failure tolerance

3. **Container optimization**:
   - Use multi-stage builds to minimize image size
   - Enable BuildKit for faster builds
   - Use .dockerignore to exclude unnecessary files

## Development Workflow

### Building and Testing

```bash
# Build image
docker build -t fleet-q:dev .

# Run tests in container
docker run --rm fleet-q:dev python -m pytest

# Run with development configuration
docker run -p 8000:8000 --env-file .env.dev fleet-q:dev
```

### Debugging

```bash
# Run with debug logging
docker run -e FLEET_Q_LOG_LEVEL=DEBUG fleet-q:dev

# Access container shell
docker exec -it container-name /bin/bash

# Run Python REPL in container
docker exec -it container-name python
```

### Hot Reloading (Development)

For development with hot reloading:

```bash
# Mount source code as volume
docker run -p 8000:8000 \
  -v $(pwd)/fleet_q:/app/fleet_q \
  --env-file .env \
  fleet-q:dev
```

## Best Practices

### Container Best Practices

1. **Use specific base image tags** (not `latest`)
2. **Minimize layer count** with multi-stage builds
3. **Run as non-root user** for security
4. **Set resource limits** to prevent resource exhaustion
5. **Use health checks** for proper orchestration
6. **Handle signals properly** for graceful shutdown

### Configuration Best Practices

1. **Use environment variables** for all configuration
2. **Provide sensible defaults** for optional settings
3. **Validate configuration** at startup
4. **Use secrets management** for sensitive data
5. **Document all variables** with examples

### Operational Best Practices

1. **Monitor health endpoints** continuously
2. **Collect and analyze logs** centrally
3. **Set up alerting** for failures
4. **Plan for scaling** based on queue depth
5. **Test disaster recovery** procedures
6. **Keep images updated** for security

This guide provides comprehensive coverage of Docker deployment for FLEET-Q, supporting both development and production use cases while maintaining operational simplicity and security best practices.