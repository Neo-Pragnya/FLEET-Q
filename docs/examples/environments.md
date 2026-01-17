# Environment Configurations

This guide shows how to configure FLEET-Q for different deployment environments, from development to production.

## Development Environment

### Local Development with SQLite

Perfect for development without Snowflake access:

```bash
# .env.development
FLEET_Q_STORAGE_MODE=sqlite
FLEET_Q_SQLITE_DB_PATH=./fleet_q_dev.db
FLEET_Q_LOG_LEVEL=DEBUG
FLEET_Q_LOG_FORMAT=console
FLEET_Q_MAX_PARALLELISM=3
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=10
FLEET_Q_CLAIM_INTERVAL_SECONDS=2
FLEET_Q_RECOVERY_INTERVAL_SECONDS=30
FLEET_Q_API_HOST=127.0.0.1
FLEET_Q_API_PORT=8000
```

Start development server:
```bash
source .env.development
fleet-q
```

### Local Development with Snowflake

For testing with real Snowflake integration:

```bash
# .env.development.snowflake
FLEET_Q_STORAGE_MODE=snowflake
FLEET_Q_SNOWFLAKE_ACCOUNT=your-dev-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=dev_user
FLEET_Q_SNOWFLAKE_PASSWORD=dev_password
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_DEV
FLEET_Q_SNOWFLAKE_SCHEMA=PUBLIC
FLEET_Q_SNOWFLAKE_WAREHOUSE=DEV_WH
FLEET_Q_SNOWFLAKE_ROLE=DEV_ROLE

# Development-friendly settings
FLEET_Q_LOG_LEVEL=DEBUG
FLEET_Q_LOG_FORMAT=console
FLEET_Q_MAX_PARALLELISM=5
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=15
FLEET_Q_CLAIM_INTERVAL_SECONDS=3
FLEET_Q_RECOVERY_INTERVAL_SECONDS=60
```

## Testing Environment

### Automated Testing Configuration

For CI/CD pipelines and automated testing:

```bash
# .env.testing
FLEET_Q_STORAGE_MODE=sqlite
FLEET_Q_SQLITE_DB_PATH=:memory:  # In-memory database
FLEET_Q_LOG_LEVEL=WARNING
FLEET_Q_LOG_FORMAT=json
FLEET_Q_MAX_PARALLELISM=2
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=5
FLEET_Q_CLAIM_INTERVAL_SECONDS=1
FLEET_Q_RECOVERY_INTERVAL_SECONDS=10
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=15
FLEET_Q_API_HOST=127.0.0.1
FLEET_Q_API_PORT=0  # Random available port
```

### Integration Testing with Snowflake

For integration tests that require real Snowflake:

```bash
# .env.integration
FLEET_Q_STORAGE_MODE=snowflake
FLEET_Q_SNOWFLAKE_ACCOUNT=test-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=test_user
FLEET_Q_SNOWFLAKE_PASSWORD=test_password
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_TEST
FLEET_Q_SNOWFLAKE_SCHEMA=INTEGRATION_TESTS
FLEET_Q_SNOWFLAKE_WAREHOUSE=TEST_WH

# Fast intervals for testing
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_MAX_PARALLELISM=3
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=5
FLEET_Q_CLAIM_INTERVAL_SECONDS=1
FLEET_Q_RECOVERY_INTERVAL_SECONDS=15
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=20
```

## Staging Environment

### Pre-production Configuration

Staging environment that mirrors production:

```bash
# .env.staging
FLEET_Q_STORAGE_MODE=snowflake
FLEET_Q_SNOWFLAKE_ACCOUNT=staging-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=fleet_q_staging
FLEET_Q_SNOWFLAKE_PASSWORD=${STAGING_SNOWFLAKE_PASSWORD}
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_STAGING
FLEET_Q_SNOWFLAKE_SCHEMA=PUBLIC
FLEET_Q_SNOWFLAKE_WAREHOUSE=STAGING_WH
FLEET_Q_SNOWFLAKE_ROLE=FLEET_Q_STAGING_ROLE

# Production-like settings
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_LOG_FORMAT=json
FLEET_Q_MAX_PARALLELISM=15
FLEET_Q_CAPACITY_THRESHOLD=0.8
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=30
FLEET_Q_CLAIM_INTERVAL_SECONDS=5
FLEET_Q_RECOVERY_INTERVAL_SECONDS=300
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=180

# API configuration
FLEET_Q_API_HOST=0.0.0.0
FLEET_Q_API_PORT=8000

# Backoff configuration
FLEET_Q_DEFAULT_MAX_ATTEMPTS=5
FLEET_Q_DEFAULT_BASE_DELAY_MS=100
FLEET_Q_DEFAULT_MAX_DELAY_MS=30000
```

## Production Environment

### High-Availability Production

Production configuration optimized for reliability and performance:

```bash
# .env.production
FLEET_Q_STORAGE_MODE=snowflake
FLEET_Q_SNOWFLAKE_ACCOUNT=prod-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=fleet_q_prod
FLEET_Q_SNOWFLAKE_PASSWORD=${PROD_SNOWFLAKE_PASSWORD}
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_PROD
FLEET_Q_SNOWFLAKE_SCHEMA=PUBLIC
FLEET_Q_SNOWFLAKE_WAREHOUSE=PROD_WH
FLEET_Q_SNOWFLAKE_ROLE=FLEET_Q_PROD_ROLE

# Production logging
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_LOG_FORMAT=json

# Optimized for high throughput
FLEET_Q_MAX_PARALLELISM=25
FLEET_Q_CAPACITY_THRESHOLD=0.8
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=30
FLEET_Q_CLAIM_INTERVAL_SECONDS=5
FLEET_Q_RECOVERY_INTERVAL_SECONDS=300
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=180
FLEET_Q_LEADER_CHECK_INTERVAL_SECONDS=60

# API configuration
FLEET_Q_API_HOST=0.0.0.0
FLEET_Q_API_PORT=8000

# Conservative backoff for production stability
FLEET_Q_DEFAULT_MAX_ATTEMPTS=5
FLEET_Q_DEFAULT_BASE_DELAY_MS=200
FLEET_Q_DEFAULT_MAX_DELAY_MS=30000
```

### High-Throughput Production

For environments with very high task volumes:

```bash
# .env.production.high-throughput
FLEET_Q_STORAGE_MODE=snowflake
FLEET_Q_SNOWFLAKE_ACCOUNT=prod-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=fleet_q_prod
FLEET_Q_SNOWFLAKE_PASSWORD=${PROD_SNOWFLAKE_PASSWORD}
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_PROD
FLEET_Q_SNOWFLAKE_SCHEMA=PUBLIC
FLEET_Q_SNOWFLAKE_WAREHOUSE=XLARGE_WH  # Larger warehouse
FLEET_Q_SNOWFLAKE_ROLE=FLEET_Q_PROD_ROLE

# High-throughput settings
FLEET_Q_MAX_PARALLELISM=50
FLEET_Q_CAPACITY_THRESHOLD=0.9  # Use more capacity
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=20  # More frequent heartbeats
FLEET_Q_CLAIM_INTERVAL_SECONDS=2  # More frequent claiming
FLEET_Q_RECOVERY_INTERVAL_SECONDS=180  # More frequent recovery

# Aggressive backoff for high throughput
FLEET_Q_DEFAULT_MAX_ATTEMPTS=3
FLEET_Q_DEFAULT_BASE_DELAY_MS=50
FLEET_Q_DEFAULT_MAX_DELAY_MS=10000

# Logging optimized for performance
FLEET_Q_LOG_LEVEL=WARNING  # Reduce log volume
FLEET_Q_LOG_FORMAT=json
```

## Container Environments

### Docker Configuration

Basic Docker environment configuration:

```dockerfile
# Dockerfile.production
FROM python:3.11-slim

# Install FLEET-Q
RUN pip install fleet-q

# Set production defaults
ENV FLEET_Q_LOG_FORMAT=json
ENV FLEET_Q_LOG_LEVEL=INFO
ENV FLEET_Q_API_HOST=0.0.0.0
ENV FLEET_Q_API_PORT=8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run as non-root user
RUN useradd --create-home --shell /bin/bash fleetq
USER fleetq

EXPOSE 8000
CMD ["fleet-q"]
```

### Kubernetes ConfigMap

Production Kubernetes configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fleet-q-config
  namespace: fleet-q
data:
  FLEET_Q_STORAGE_MODE: "snowflake"
  FLEET_Q_SNOWFLAKE_DATABASE: "FLEET_Q_PROD"
  FLEET_Q_SNOWFLAKE_SCHEMA: "PUBLIC"
  FLEET_Q_SNOWFLAKE_WAREHOUSE: "PROD_WH"
  FLEET_Q_SNOWFLAKE_ROLE: "FLEET_Q_PROD_ROLE"
  FLEET_Q_LOG_LEVEL: "INFO"
  FLEET_Q_LOG_FORMAT: "json"
  FLEET_Q_MAX_PARALLELISM: "20"
  FLEET_Q_CAPACITY_THRESHOLD: "0.8"
  FLEET_Q_HEARTBEAT_INTERVAL_SECONDS: "30"
  FLEET_Q_CLAIM_INTERVAL_SECONDS: "5"
  FLEET_Q_RECOVERY_INTERVAL_SECONDS: "300"
  FLEET_Q_DEAD_POD_THRESHOLD_SECONDS: "180"
  FLEET_Q_API_HOST: "0.0.0.0"
  FLEET_Q_API_PORT: "8000"
---
apiVersion: v1
kind: Secret
metadata:
  name: fleet-q-secrets
  namespace: fleet-q
type: Opaque
stringData:
  FLEET_Q_SNOWFLAKE_ACCOUNT: "prod-account.us-east-1"
  FLEET_Q_SNOWFLAKE_USER: "fleet_q_prod"
  FLEET_Q_SNOWFLAKE_PASSWORD: "secure-password-here"
```

### EKS Deployment

Complete EKS deployment with multiple environments:

```yaml
# fleet-q-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fleet-q
  namespace: fleet-q
  labels:
    app: fleet-q
    environment: production
spec:
  replicas: 5
  selector:
    matchLabels:
      app: fleet-q
  template:
    metadata:
      labels:
        app: fleet-q
        environment: production
    spec:
      serviceAccountName: fleet-q-service-account
      containers:
      - name: fleet-q
        image: fleet-q:1.0.0
        ports:
        - containerPort: 8000
          name: http
        envFrom:
        - configMapRef:
            name: fleet-q-config
        - secretRef:
            name: fleet-q-secrets
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 40
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 5
          failureThreshold: 3
        startupProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 6
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
```

## Environment-Specific Optimizations

### Development Optimizations

```bash
# Fast feedback for development
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=5
FLEET_Q_CLAIM_INTERVAL_SECONDS=1
FLEET_Q_RECOVERY_INTERVAL_SECONDS=15
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=10

# Verbose logging for debugging
FLEET_Q_LOG_LEVEL=DEBUG
FLEET_Q_LOG_FORMAT=console

# Small capacity for resource efficiency
FLEET_Q_MAX_PARALLELISM=3
```

### Testing Optimizations

```bash
# Very fast intervals for test speed
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=1
FLEET_Q_CLAIM_INTERVAL_SECONDS=0.1
FLEET_Q_RECOVERY_INTERVAL_SECONDS=5
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=3

# Minimal logging for test performance
FLEET_Q_LOG_LEVEL=ERROR
FLEET_Q_LOG_FORMAT=json

# Small capacity for test isolation
FLEET_Q_MAX_PARALLELISM=2
```

### Production Optimizations

```bash
# Balanced intervals for stability and performance
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=30
FLEET_Q_CLAIM_INTERVAL_SECONDS=5
FLEET_Q_RECOVERY_INTERVAL_SECONDS=300
FLEET_Q_DEAD_POD_THRESHOLD_SECONDS=180

# Structured logging for monitoring
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_LOG_FORMAT=json

# High capacity for throughput
FLEET_Q_MAX_PARALLELISM=25
FLEET_Q_CAPACITY_THRESHOLD=0.8
```

## Configuration Validation

### Environment Validation Script

```python
#!/usr/bin/env python3
"""Validate FLEET-Q environment configuration"""

import os
import sys
from fleet_q.config import load_config

def validate_environment():
    """Validate current environment configuration"""
    try:
        config = load_config()
        print("✅ Configuration validation passed")
        
        print(f"Storage mode: {config.storage_mode}")
        print(f"Max parallelism: {config.max_parallelism}")
        print(f"Log level: {config.log_level}")
        
        if config.storage_mode == "snowflake":
            print(f"Snowflake account: {config.snowflake_account}")
            print(f"Snowflake database: {config.snowflake_database}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

if __name__ == "__main__":
    success = validate_environment()
    sys.exit(0 if success else 1)
```

### Docker Environment Validation

```bash
#!/bin/bash
# validate-docker-env.sh

echo "🔍 Validating Docker environment..."

# Check required environment variables
required_vars=()

if [ "$FLEET_Q_STORAGE_MODE" = "snowflake" ]; then
    required_vars+=(
        "FLEET_Q_SNOWFLAKE_ACCOUNT"
        "FLEET_Q_SNOWFLAKE_USER" 
        "FLEET_Q_SNOWFLAKE_PASSWORD"
        "FLEET_Q_SNOWFLAKE_DATABASE"
    )
fi

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Missing required environment variable: $var"
        exit 1
    fi
done

echo "✅ All required environment variables present"

# Test configuration loading
docker run --rm --env-file .env fleet-q python -c "
from fleet_q.config import load_config
config = load_config()
print('✅ Configuration loaded successfully')
print(f'Storage mode: {config.storage_mode}')
print(f'Max parallelism: {config.max_parallelism}')
"

echo "✅ Docker environment validation passed"
```

## Monitoring Configuration

### Production Monitoring

```bash
# Enable comprehensive monitoring
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_LOG_FORMAT=json

# Add monitoring-specific metadata
FLEET_Q_POD_ID=${HOSTNAME}
FLEET_Q_ENVIRONMENT=production
FLEET_Q_VERSION=1.0.0
```

### Development Monitoring

```bash
# Human-readable logs for development
FLEET_Q_LOG_LEVEL=DEBUG
FLEET_Q_LOG_FORMAT=console

# Add development context
FLEET_Q_POD_ID=dev-${USER}
FLEET_Q_ENVIRONMENT=development
```

## Security Configuration

### Production Security

```bash
# Secure defaults
FLEET_Q_API_HOST=0.0.0.0  # Bind to all interfaces (behind firewall)

# Use secrets management
FLEET_Q_SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD_SECRET}

# Audit logging
FLEET_Q_LOG_LEVEL=INFO  # Ensure all operations are logged
```

### Development Security

```bash
# Local development
FLEET_Q_API_HOST=127.0.0.1  # Bind to localhost only

# Development credentials (not in version control)
FLEET_Q_SNOWFLAKE_PASSWORD=dev_password
```

These environment configurations provide a solid foundation for deploying FLEET-Q across different environments while maintaining security, performance, and operational best practices.

## Next Steps

- **[Basic Usage](basic-usage.md)** - Learn how to use FLEET-Q in different environments
- **[Step Implementation](step-implementation.md)** - Implement custom step processors
- **[Idempotency Patterns](idempotency.md)** - Ensure safe task re-execution