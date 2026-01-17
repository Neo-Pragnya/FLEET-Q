# Configuration

FLEET-Q is configured entirely through environment variables, making it easy to deploy in containerized environments.

## Configuration Overview

All configuration variables use the `FLEET_Q_` prefix and can be set via:

- Environment variables
- `.env` files
- Container environment configuration
- Kubernetes ConfigMaps and Secrets

## Required Configuration

### Snowflake Connection

These variables are required for Snowflake connectivity:

| Variable | Description | Example |
|----------|-------------|---------|
| `FLEET_Q_SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `abc12345.us-east-1` |
| `FLEET_Q_SNOWFLAKE_USER` | Snowflake username | `fleet_q_user` |
| `FLEET_Q_SNOWFLAKE_PASSWORD` | Snowflake password | `secure_password` |
| `FLEET_Q_SNOWFLAKE_DATABASE` | Database name | `FLEET_Q_DB` |

### Optional Snowflake Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_SNOWFLAKE_SCHEMA` | `PUBLIC` | Schema name |
| `FLEET_Q_SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` | Warehouse name |
| `FLEET_Q_SNOWFLAKE_ROLE` | User's default role | Role to use |

## Pod Configuration

### Identity and Capacity

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_POD_ID` | Auto-generated | Unique pod identifier |
| `FLEET_Q_MAX_PARALLELISM` | `10` | Maximum concurrent steps |
| `FLEET_Q_CAPACITY_THRESHOLD` | `0.8` | Percentage of max capacity to use |

The pod ID is automatically generated using the hostname and a random suffix if not specified.

### Timing Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_HEARTBEAT_INTERVAL_SECONDS` | `30` | Heartbeat frequency |
| `FLEET_Q_CLAIM_INTERVAL_SECONDS` | `5` | How often to claim new work |
| `FLEET_Q_RECOVERY_INTERVAL_SECONDS` | `300` | Recovery cycle frequency (leader only) |
| `FLEET_Q_DEAD_POD_THRESHOLD_SECONDS` | `180` | When to consider pods dead |
| `FLEET_Q_LEADER_CHECK_INTERVAL_SECONDS` | `60` | Leader election check frequency |

## API Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_API_HOST` | `0.0.0.0` | API server host |
| `FLEET_Q_API_PORT` | `8000` | API server port |

## Logging Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `FLEET_Q_LOG_FORMAT` | `json` | Log format (json, console) |

### Log Levels

- **DEBUG**: Detailed debugging information
- **INFO**: General operational information
- **WARNING**: Warning messages for potential issues
- **ERROR**: Error messages for failures

### Log Formats

- **json**: Structured JSON logs (recommended for production)
- **console**: Human-readable console logs (good for development)

## Backoff Configuration

Configure exponential backoff behavior for retries:

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_DEFAULT_MAX_ATTEMPTS` | `5` | Maximum retry attempts |
| `FLEET_Q_DEFAULT_BASE_DELAY_MS` | `100` | Base delay in milliseconds |
| `FLEET_Q_DEFAULT_MAX_DELAY_MS` | `30000` | Maximum delay in milliseconds |

## Storage Configuration

### SQLite Configuration (Development)

For development and testing with SQLite simulation:

| Variable | Default | Description |
|----------|---------|-------------|
| `FLEET_Q_STORAGE_MODE` | `snowflake` | Storage backend (snowflake, sqlite) |
| `FLEET_Q_SQLITE_DB_PATH` | `/tmp/fleet_q.db` | SQLite database path |

## Environment-Specific Configurations

### Development Environment

```bash
# Development configuration
FLEET_Q_STORAGE_MODE=sqlite
FLEET_Q_SQLITE_DB_PATH=./fleet_q_dev.db
FLEET_Q_LOG_LEVEL=DEBUG
FLEET_Q_LOG_FORMAT=console
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=10
FLEET_Q_CLAIM_INTERVAL_SECONDS=2
FLEET_Q_RECOVERY_INTERVAL_SECONDS=60
FLEET_Q_MAX_PARALLELISM=5
```

### Production Environment

```bash
# Production configuration
FLEET_Q_SNOWFLAKE_ACCOUNT=prod-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=fleet_q_prod
FLEET_Q_SNOWFLAKE_PASSWORD=secure_prod_password
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_PROD
FLEET_Q_SNOWFLAKE_WAREHOUSE=PROD_WH
FLEET_Q_LOG_LEVEL=INFO
FLEET_Q_LOG_FORMAT=json
FLEET_Q_MAX_PARALLELISM=20
FLEET_Q_CAPACITY_THRESHOLD=0.8
```

### Testing Environment

```bash
# Testing configuration
FLEET_Q_SNOWFLAKE_ACCOUNT=test-account.us-east-1
FLEET_Q_SNOWFLAKE_USER=fleet_q_test
FLEET_Q_SNOWFLAKE_PASSWORD=test_password
FLEET_Q_SNOWFLAKE_DATABASE=FLEET_Q_TEST
FLEET_Q_LOG_LEVEL=DEBUG
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=5
FLEET_Q_CLAIM_INTERVAL_SECONDS=1
FLEET_Q_RECOVERY_INTERVAL_SECONDS=30
FLEET_Q_MAX_PARALLELISM=3
```

## Configuration Files

### .env File

Create a `.env` file in your project root:

```bash
# .env
FLEET_Q_SNOWFLAKE_ACCOUNT=your-account
FLEET_Q_SNOWFLAKE_USER=your-user
FLEET_Q_SNOWFLAKE_PASSWORD=your-password
FLEET_Q_SNOWFLAKE_DATABASE=your-database
FLEET_Q_MAX_PARALLELISM=10
FLEET_Q_LOG_LEVEL=INFO
```

Load with:

```bash
# Load environment file
source .env
fleet-q

# Or use with Docker
docker run --env-file .env fleet-q
```

### Docker Environment

```dockerfile
# Dockerfile
ENV FLEET_Q_SNOWFLAKE_ACCOUNT=your-account
ENV FLEET_Q_SNOWFLAKE_USER=your-user
ENV FLEET_Q_MAX_PARALLELISM=10
ENV FLEET_Q_LOG_LEVEL=INFO
```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fleet-q-config
data:
  FLEET_Q_SNOWFLAKE_ACCOUNT: "your-account"
  FLEET_Q_SNOWFLAKE_DATABASE: "FLEET_Q_PROD"
  FLEET_Q_MAX_PARALLELISM: "20"
  FLEET_Q_LOG_LEVEL: "INFO"
  FLEET_Q_LOG_FORMAT: "json"
---
apiVersion: v1
kind: Secret
metadata:
  name: fleet-q-secrets
type: Opaque
stringData:
  FLEET_Q_SNOWFLAKE_USER: "your-user"
  FLEET_Q_SNOWFLAKE_PASSWORD: "your-password"
```

## Configuration Validation

FLEET-Q validates configuration at startup and provides clear error messages:

```bash
# Example validation error
2024-01-01T12:00:00.000Z [ERROR] Configuration validation failed:
  - FLEET_Q_SNOWFLAKE_ACCOUNT is required
  - FLEET_Q_MAX_PARALLELISM must be a positive integer
  - FLEET_Q_HEARTBEAT_INTERVAL_SECONDS must be at least 5
```

### Validation Rules

- **Required fields**: Must be present and non-empty
- **Numeric fields**: Must be valid numbers within acceptable ranges
- **Enum fields**: Must match allowed values
- **Connection fields**: Validated during startup connection test

## Performance Tuning

### High Throughput Configuration

For high-throughput workloads:

```bash
FLEET_Q_MAX_PARALLELISM=50
FLEET_Q_CAPACITY_THRESHOLD=0.9
FLEET_Q_CLAIM_INTERVAL_SECONDS=2
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=15
```

### Low Latency Configuration

For low-latency workloads:

```bash
FLEET_Q_CLAIM_INTERVAL_SECONDS=1
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=10
FLEET_Q_DEFAULT_BASE_DELAY_MS=50
FLEET_Q_DEFAULT_MAX_DELAY_MS=5000
```

### Resource Constrained Configuration

For resource-constrained environments:

```bash
FLEET_Q_MAX_PARALLELISM=3
FLEET_Q_CAPACITY_THRESHOLD=0.7
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=60
FLEET_Q_CLAIM_INTERVAL_SECONDS=10
```

## Security Considerations

### Credential Management

- **Never hardcode credentials** in configuration files
- **Use environment variables** or secret management systems
- **Rotate credentials regularly**
- **Use least-privilege access** for Snowflake users

### Network Security

- **Bind to specific interfaces** using `FLEET_Q_API_HOST`
- **Use HTTPS** in production environments
- **Implement network policies** to restrict access

### Logging Security

- **Avoid logging sensitive data** in debug mode
- **Use structured logging** for better security monitoring
- **Implement log rotation** to manage disk usage

## Troubleshooting Configuration

### Common Configuration Issues

**Invalid Snowflake credentials**
```bash
# Test connection manually
python -c "
from fleet_q.config import load_config
from fleet_q.snowflake_storage import SnowflakeStorage
config = load_config()
storage = SnowflakeStorage(config)
# Will raise exception if credentials are invalid
"
```

**Port already in use**
```bash
# Check what's using the port
lsof -i :8000

# Use a different port
export FLEET_Q_API_PORT=8001
```

**Invalid numeric values**
```bash
# Check configuration parsing
python -c "
from fleet_q.config import load_config
config = load_config()
print(f'Max parallelism: {config.max_parallelism}')
print(f'Capacity threshold: {config.capacity_threshold}')
"
```

For more troubleshooting help, see the [Troubleshooting Guide](../operations/troubleshooting.md).