# FLEET-Q Development Workflow

This guide covers the complete development workflow for FLEET-Q, including local development setup, testing procedures, and deployment workflows using Docker.

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Git for version control
- Optional: Snowflake account for production testing

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd fleet-q

# Copy environment configuration
cp .env.docker.example .env

# Make development script executable
chmod +x scripts/docker-dev.sh
```

### 2. Choose Development Mode

#### Option A: SQLite Simulation (No Snowflake Required)

```bash
# Start SQLite simulation mode
./scripts/docker-dev.sh dev-sqlite

# Or manually with docker-compose
docker-compose --profile dev-sqlite up -d fleet-q-dev-sqlite
```

#### Option B: Full Snowflake Development

```bash
# Edit .env with your Snowflake credentials
vim .env

# Start all services
./scripts/docker-dev.sh start
```

### 3. Verify Setup

```bash
# Check service status
./scripts/docker-dev.sh status

# Test API endpoints
./scripts/docker-dev.sh test

# Submit a test step
./scripts/docker-dev.sh submit test '{"message": "Hello World"}'
```

## Development Modes

### SQLite Simulation Mode

Perfect for development without Snowflake access:

**Features:**
- Complete FLEET-Q functionality using SQLite
- No external dependencies
- Fast startup and teardown
- Ideal for feature development and testing

**Usage:**
```bash
# Start simulation mode
docker-compose --profile dev-sqlite up -d

# Access at http://localhost:8003
curl http://localhost:8003/health

# Submit test steps
curl -X POST http://localhost:8003/submit \
  -H "Content-Type: application/json" \
  -d '{"type": "test", "args": {"message": "Hello SQLite"}}'
```

**Configuration:**
- Uses `FLEET_Q_STORAGE_MODE=sqlite`
- Stores data in `/app/data/fleet_q_simulation.db`
- Faster intervals for development
- Debug logging enabled

### Multi-Pod Snowflake Mode

Full production-like environment with multiple pods:

**Features:**
- Real Snowflake coordination
- Multiple pods with leader election
- Production-like behavior
- Full recovery testing

**Usage:**
```bash
# Start all pods
docker-compose up -d

# Scale workers
docker-compose up -d --scale fleet-q-worker-1=3

# Monitor logs
docker-compose logs -f fleet-q-leader
```

**Pod Configuration:**
| Service | Port | Role | Parallelism | Purpose |
|---------|------|------|-------------|---------|
| fleet-q-leader | 8000 | Leader + Worker | 10 | Primary pod with recovery |
| fleet-q-worker-1 | 8001 | Worker | 8 | Secondary worker |
| fleet-q-worker-2 | 8002 | Worker | 6 | Tertiary worker |

## Development Workflow

### 1. Feature Development

```bash
# Start development environment
./scripts/docker-dev.sh dev

# Make code changes (hot reloading enabled)
# Edit files in fleet_q/ directory

# Test changes
./scripts/docker-dev.sh test

# View logs
./scripts/docker-dev.sh logs
```

### 2. Testing Workflow

#### Unit Testing
```bash
# Run tests in container
docker run --rm -v $(pwd):/app fleet-q python -m pytest tests/

# Run specific test file
docker run --rm -v $(pwd):/app fleet-q python -m pytest tests/test_storage.py -v
```

#### Integration Testing
```bash
# Start test environment
./scripts/docker-dev.sh start

# Run integration tests
./scripts/docker-dev.sh test

# Test leader election
curl http://localhost:8000/admin/leader

# Test step submission and processing
./scripts/docker-dev.sh submit test '{"data": "test"}'
```

#### Multi-Pod Testing
```bash
# Start multiple pods
./scripts/docker-dev.sh start

# Scale workers
./scripts/docker-dev.sh scale 3

# Submit multiple steps
for i in {1..10}; do
  ./scripts/docker-dev.sh submit "test-$i" "{\"id\": $i}"
done

# Monitor processing
watch -n 1 'curl -s http://localhost:8000/admin/queue | jq'
```

### 3. Debugging Workflow

#### Container Debugging
```bash
# Access container shell
docker exec -it fleet-q-leader /bin/bash

# Check logs
./scripts/docker-dev.sh logs fleet-q-leader

# Debug with Python REPL
docker exec -it fleet-q-leader python
```

#### Database Debugging (SQLite Mode)
```bash
# Access SQLite database
docker exec -it fleet-q-dev-sqlite sqlite3 /app/data/fleet_q_simulation.db

# Check tables
.tables

# Query pod health
SELECT * FROM POD_HEALTH;

# Query steps
SELECT step_id, status, claimed_by FROM STEP_TRACKER;
```

#### Network Debugging
```bash
# Check container networking
docker network ls
docker network inspect fleet-q_fleet-q-network

# Test connectivity between containers
docker exec fleet-q-leader ping fleet-q-worker-1
```

## Configuration Management

### Environment Variables

All configuration is managed through environment variables with the `FLEET_Q_` prefix:

```bash
# Core configuration
FLEET_Q_STORAGE_MODE=sqlite|snowflake
FLEET_Q_MAX_PARALLELISM=10
FLEET_Q_LOG_LEVEL=DEBUG|INFO|WARNING|ERROR

# Timing configuration
FLEET_Q_HEARTBEAT_INTERVAL_SECONDS=30
FLEET_Q_CLAIM_INTERVAL_SECONDS=5
FLEET_Q_RECOVERY_INTERVAL_SECONDS=300

# Snowflake configuration (production)
FLEET_Q_SNOWFLAKE_ACCOUNT=your-account
FLEET_Q_SNOWFLAKE_USER=your-user
FLEET_Q_SNOWFLAKE_PASSWORD=your-password
FLEET_Q_SNOWFLAKE_DATABASE=your-database
```

### Configuration Profiles

#### Development Profile (docker-compose.override.yml)
- Faster intervals for quick iteration
- Debug logging enabled
- Hot reloading with volume mounts
- Reduced parallelism for resource efficiency

#### Production Profile (docker-compose.yml)
- Production timing intervals
- JSON logging for structured output
- No volume mounts for security
- Full parallelism configuration

### Environment File Management

```bash
# Development with SQLite
cp .env.docker.example .env.dev
echo "FLEET_Q_STORAGE_MODE=sqlite" >> .env.dev

# Production with Snowflake
cp .env.docker.example .env.prod
# Edit .env.prod with real Snowflake credentials

# Use specific environment
docker-compose --env-file .env.dev up -d
```

## Performance Testing

### Load Testing

```bash
# Start environment
./scripts/docker-dev.sh start

# Submit many steps
for i in {1..100}; do
  curl -X POST http://localhost:8000/submit \
    -H "Content-Type: application/json" \
    -d "{\"type\": \"load-test\", \"args\": {\"id\": $i}}" &
done

# Monitor performance
watch -n 1 'curl -s http://localhost:8000/admin/queue'
```

### Stress Testing

```bash
# Scale up workers
./scripts/docker-dev.sh scale 5

# Monitor resource usage
docker stats

# Test recovery under load
# Kill a worker pod while processing
docker kill fleet-q-worker-1

# Watch recovery
./scripts/docker-dev.sh logs fleet-q-leader | grep recovery
```

## Troubleshooting

### Common Issues

#### 1. Container Won't Start
```bash
# Check logs
./scripts/docker-dev.sh logs

# Verify environment
docker exec container-name env | grep FLEET_Q

# Check configuration
docker exec container-name python -c "
from fleet_q.config import load_config
config = load_config()
print(f'Storage mode: {config.storage_mode}')
"
```

#### 2. SQLite Simulation Issues
```bash
# Check database file
docker exec fleet-q-dev-sqlite ls -la /app/data/

# Verify schema
docker exec fleet-q-dev-sqlite sqlite3 /app/data/fleet_q_simulation.db ".schema"

# Reset database
docker volume rm fleet-q_fleet-q-dev-data
./scripts/docker-dev.sh restart
```

#### 3. Snowflake Connection Issues
```bash
# Test credentials
docker exec fleet-q-leader python -c "
from fleet_q.config import load_config
from fleet_q.snowflake_storage import SnowflakeStorage
config = load_config()
storage = SnowflakeStorage(config)
# This will fail if credentials are wrong
"

# Check network connectivity
docker exec fleet-q-leader nslookup your-account.snowflakecomputing.com
```

#### 4. Leader Election Issues
```bash
# Check pod health
curl http://localhost:8000/admin/leader

# Verify heartbeats
./scripts/docker-dev.sh logs | grep heartbeat

# Force leader election
docker restart fleet-q-leader
```

### Performance Issues

#### High CPU Usage
```bash
# Check container stats
docker stats

# Reduce parallelism
# Edit .env: FLEET_Q_MAX_PARALLELISM=5
./scripts/docker-dev.sh restart
```

#### Memory Issues
```bash
# Check memory usage
docker exec container-name free -h

# Use in-memory SQLite
# Edit .env: FLEET_Q_SQLITE_DB_PATH=:memory:
```

#### Database Lock Issues (SQLite)
```bash
# Check for long-running transactions
docker exec fleet-q-dev-sqlite sqlite3 /app/data/fleet_q_simulation.db "PRAGMA busy_timeout=30000;"

# Restart if locked
./scripts/docker-dev.sh restart
```

## Best Practices

### Development Best Practices

1. **Use SQLite simulation** for feature development
2. **Test with Snowflake** before production deployment
3. **Use hot reloading** for faster iteration
4. **Monitor logs** continuously during development
5. **Test multi-pod scenarios** for distributed behavior

### Configuration Best Practices

1. **Use environment files** for different environments
2. **Never commit credentials** to version control
3. **Use meaningful pod IDs** for debugging
4. **Set appropriate timeouts** for your environment
5. **Use structured logging** in production

### Testing Best Practices

1. **Test both storage modes** (SQLite and Snowflake)
2. **Verify leader election** behavior
3. **Test recovery scenarios** with pod failures
4. **Load test** with realistic workloads
5. **Monitor resource usage** during tests

### Deployment Best Practices

1. **Use multi-stage builds** for smaller images
2. **Set resource limits** in production
3. **Use health checks** for orchestration
4. **Monitor application metrics** continuously
5. **Plan for scaling** based on queue depth

## Advanced Workflows

### Custom Step Types

```python
# Create custom step processor
class CustomStepProcessor:
    async def process(self, step_payload):
        # Your custom logic here
        return {"result": "processed"}

# Submit custom steps
curl -X POST http://localhost:8000/submit \
  -H "Content-Type: application/json" \
  -d '{
    "type": "custom",
    "args": {"data": "custom data"},
    "metadata": {"priority": 10}
  }'
```

### Monitoring and Observability

```bash
# Set up log aggregation
docker-compose logs -f | grep -E "(ERROR|WARNING|leader|recovery)"

# Monitor queue depth
watch -n 5 'curl -s http://localhost:8000/admin/queue | jq .pending_steps'

# Track step processing rate
# Custom script to monitor throughput
```

### CI/CD Integration

```yaml
# .github/workflows/test.yml
name: Test FLEET-Q
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Test with SQLite simulation
        run: |
          docker-compose --profile dev-sqlite up -d
          sleep 30
          ./scripts/docker-dev.sh test
          docker-compose down
```

This comprehensive development workflow guide provides everything needed to develop, test, and deploy FLEET-Q effectively using Docker containers.