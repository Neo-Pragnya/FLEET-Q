# FLEET-Q Test Environments

**Version:** 1.0  
**Last Updated:** 2026-02-08  
**Status:** Active  
**Owner:** FLEET-Q Testing Team

---

## Table of Contents

1. [Overview](#overview)
2. [Environment Matrix](#environment-matrix)
3. [Local Development](#local-development)
4. [CI Environment](#ci-environment)
5. [Integration Environment](#integration-environment)
6. [Staging Environment](#staging-environment)
7. [Production Environment](#production-environment)
8. [Environment Configuration](#environment-configuration)
9. [Access Control](#access-control)
10. [Deployment & Refresh](#deployment--refresh)

---

## Overview

### Purpose

This document defines the **test environment strategy** for FLEET-Q, ensuring consistent, isolated, and production-like environments for testing.

### Principles

1. **Isolation:** Each environment is independent
2. **Parity:** Environments mirror production architecture
3. **Automation:** Environment setup is fully automated
4. **Reproducibility:** Environments can be recreated deterministically
5. **Security:** Appropriate access controls per environment

### Environment Progression

```
Local → CI → Integration → Staging → Production
  ↓      ↓         ↓          ↓           ↓
Fast   Fast    Realistic  Identical   Real
Test   CI/CD    Integration  Prod     Users
```

---

## Environment Matrix

### Comparison Table

| Aspect | Local | CI | Integration | Staging | Production |
|--------|-------|----|----- --|---------|------------|
| **Purpose** | Development | PR validation | Cross-team testing | Pre-release validation | Real workload |
| **Data** | Synthetic | Synthetic | Synthetic | Anonymized | Real |
| **Scale** | Minimal | Minimal | Moderate | Full | Full |
| **External Systems** | Mocked | Mocked | Real | Real | Real |
| **Bedrock** | ❌ Mocked | ❌ Mocked | ✅ Real (dev) | ✅ Real (prod) | ✅ Real (prod) |
| **Database** | SQLite | SQLite | PostgreSQL | PostgreSQL | PostgreSQL |
| **ZeroMQ** | In-process | In-process | TCP | TCP | TCP |
| **Workers** | 1-3 | 3 | 5-10 | 20 | 50+ |
| **Uptime** | On-demand | Per-test | 24/7 | 24/7 | 24/7 |
| **Cost** | Free | Low | Medium | High | Highest |
| **Access** | Developer | CI system | Team | QA + DevOps | DevOps only |

### Test Coverage by Environment

| Test Type | Local | CI | Integration | Staging | Production |
|-----------|-------|----|-------------|---------|------------|
| **Unit Tests** | ✅ All | ✅ All | ❌ None | ❌ None | ❌ None |
| **Integration Tests** | ✅ Subset | ✅ All | ✅ All | ✅ Smoke | ❌ None |
| **BDD Scenarios** | ✅ Subset | ✅ All | ✅ All | ✅ All | ❌ None |
| **State Machine** | ✅ All | ✅ All | ✅ Verification | ❌ None | ❌ None |
| **Data Invariants** | ✅ All | ✅ All | ✅ All | ✅ All | ✅ Monitor |
| **Contracts** | ✅ All | ✅ All | ✅ Verification | ✅ Verification | ❌ None |
| **Resilience** | ⚠️ Subset | ✅ All | ✅ All | ⚠️ Subset | ❌ None |
| **Load Tests** | ❌ None | ❌ None | ✅ Yes | ✅ Yes | ❌ None |
| **Chaos Tests** | ❌ None | ❌ None | ✅ Yes | ⚠️ Controlled | ❌ None |

---

## Local Development

### Purpose

Fast feedback loop for developers working on features.

### Architecture

```
┌─────────────────────────────────────┐
│   Developer Machine                 │
│                                     │
│  ┌──────────────┐  ┌─────────────┐ │
│  │ Control Plane│  │   Workers   │ │
│  │  (FastAPI)   │  │   (3 pods)  │ │
│  └──────────────┘  └─────────────┘ │
│         │                 │         │
│  ┌──────────────┐  ┌─────────────┐ │
│  │   SQLite     │  │   ZeroMQ    │ │
│  │  (in-memory) │  │ (in-process)│ │
│  └──────────────┘  └─────────────┘ │
│         │                           │
│  ┌──────────────────────────────┐  │
│  │   Mock Bedrock Service       │  │
│  │   (returns canned responses) │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Setup

**Prerequisites:**
- Python 3.11+
- Poetry or pip
- Docker (optional, for PostgreSQL)

**Quick Start:**

```bash
# Clone repo
git clone https://github.com/your-org/FLEET-Q.git
cd FLEET-Q

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Start local server
poetry run python -m fleet_q.api --env=local
```

**Configuration:**

```yaml
# config/local.yaml
environment: local

control_plane:
  host: localhost
  port: 8000
  workers: 3

database:
  type: sqlite
  path: ":memory:"  # In-memory for speed

iohub:
  transport: inproc://iohub  # In-process ZeroMQ
  hwm: 1000

bedrock:
  mock: true
  mock_latency: 0.1  # 100ms simulated latency

logging:
  level: DEBUG
  format: json

metrics:
  enabled: true
  port: 9090
```

**Mock Bedrock Service:**

```python
# tests/mocks/bedrock_mock.py
from fastapi import FastAPI, HTTPException
import time
import random

app = FastAPI()

@app.post("/model/invoke")
def invoke_model(request: dict):
    """Mock Bedrock invocation."""
    
    # Simulate latency
    time.sleep(random.uniform(0.05, 0.2))
    
    # Mock throttling (10% of requests)
    if random.random() < 0.1:
        raise HTTPException(status_code=429, detail="ThrottlingException")
    
    # Mock errors (5% of requests)
    if random.random() < 0.05:
        raise HTTPException(status_code=500, detail="InternalServerError")
    
    # Return mock response
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": "Mock response to: " + request['messages'][0]['content']
            }
        },
        "usage": {
            "input_tokens": 10,
            "output_tokens": 20
        }
    }

# Run with: uvicorn bedrock_mock:app --port 8888
```

**Running Tests:**

```bash
# Run all tests
pytest

# Run specific category
pytest -m bdd
pytest -m state_machine

# Run specific scenario
pytest -k "FLEETQ-BDD-001"

# Run with coverage
pytest --cov=fleet_q --cov-report=html

# Run fast tests only (skip slow integration tests)
pytest -m "not slow"
```

### Development Workflow

1. **Write Test:** Create/modify test in `tests/`
2. **Implement:** Update code in `fleet_q/`
3. **Run Locally:** `pytest tests/test_new_feature.py`
4. **Check Coverage:** Ensure new code is covered
5. **Commit:** Push to feature branch
6. **CI Validation:** Automated tests run on PR

---

## CI Environment

### Purpose

Automated validation of pull requests before merge.

### Architecture

```
┌─────────────────────────────────────┐
│   GitHub Actions Runner             │
│                                     │
│  ┌──────────────┐  ┌─────────────┐ │
│  │   Docker     │  │   Docker    │ │
│  │ Control Plane│  │   Workers   │ │
│  └──────────────┘  └─────────────┘ │
│         │                 │         │
│  ┌──────────────┐  ┌─────────────┐ │
│  │   SQLite     │  │   ZeroMQ    │ │
│  │    (file)    │  │   (TCP)     │ │
│  └──────────────┘  └─────────────┘ │
│         │                           │
│  ┌──────────────────────────────┐  │
│  │   Mock Bedrock (Docker)      │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

### GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Test Suite

on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    
    strategy:
      matrix:
        python-version: ['3.11', '3.12']
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install dependencies
        run: |
          pip install poetry
          poetry install
      
      - name: Start mock services
        run: |
          docker-compose -f docker-compose.test.yml up -d
          sleep 5  # Wait for services
      
      - name: Run tests
        env:
          TEST_ENV: ci
          RUN_ID: run_${{ github.run_number }}_ci
        run: |
          poetry run pytest \
            --cov=fleet_q \
            --cov-report=xml \
            --cov-report=html \
            --junit-xml=test-results.xml \
            -v
      
      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          files: ./coverage.xml
      
      - name: Upload test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-results-${{ matrix.python-version }}
          path: |
            test-results.xml
            htmlcov/
            evidence/
      
      - name: Check coverage threshold
        run: |
          poetry run pytest --cov=fleet_q --cov-fail-under=80
      
      - name: Shutdown services
        if: always()
        run: |
          docker-compose -f docker-compose.test.yml down

  contract-tests:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Run contract validation
        run: |
          poetry run pytest -m contract
      
      - name: Check for breaking changes
        run: |
          poetry run python scripts/detect_breaking_changes.py

  quality-gates:
    runs-on: ubuntu-latest
    needs: [test, contract-tests]
    
    steps:
      - name: Check P0 coverage
        run: |
          # Fail if P0 tests don't pass 100%
          poetry run python scripts/check_p0_coverage.py
```

**Docker Compose for CI:**

```yaml
# docker-compose.test.yml
version: '3.8'

services:
  bedrock-mock:
    build:
      context: .
      dockerfile: tests/mocks/Dockerfile.bedrock
    ports:
      - "8888:8888"
    environment:
      - MOCK_ERROR_RATE=0.05
      - MOCK_THROTTLE_RATE=0.1
  
  control-plane:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      - ENV=ci
      - DATABASE_PATH=/tmp/fleetq.db
      - BEDROCK_ENDPOINT=http://bedrock-mock:8888
    depends_on:
      - bedrock-mock
  
  worker-1:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m fleet_q.worker --id=worker-1
    environment:
      - ENV=ci
      - CONTROL_PLANE_URL=http://control-plane:8000
    depends_on:
      - control-plane
  
  worker-2:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m fleet_q.worker --id=worker-2
    environment:
      - ENV=ci
      - CONTROL_PLANE_URL=http://control-plane:8000
    depends_on:
      - control-plane
  
  worker-3:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m fleet_q.worker --id=worker-3
    environment:
      - ENV=ci
      - CONTROL_PLANE_URL=http://control-plane:8000
    depends_on:
      - control-plane
```

### Quality Gates

Tests must pass these gates to merge:

| Gate | Threshold | Action if Failed |
|------|-----------|------------------|
| **P0 Test Pass Rate** | 100% | ❌ Block merge |
| **Overall Pass Rate** | ≥ 95% | ❌ Block merge |
| **Code Coverage** | ≥ 80% | ❌ Block merge |
| **Contract Validation** | PASS | ❌ Block merge |
| **No Breaking Changes** | PASS | ⚠️ Warn (manual review) |
| **State Coverage** | 100% | ❌ Block merge |

---

## Integration Environment

### Purpose

Continuous integration testing with real AWS services.

### Architecture

```
┌─────────────────────────────────────┐
│        AWS Account (Dev)            │
│                                     │
│  ┌──────────────┐  ┌─────────────┐ │
│  │     ECS      │  │     ECS     │ │
│  │ Control Plane│  │  Workers(10)│ │
│  └──────────────┘  └─────────────┘ │
│         │                 │         │
│  ┌──────────────┐  ┌─────────────┐ │
│  │     RDS      │  │   ElastiC   │ │
│  │ PostgreSQL   │  │   (Redis)   │ │
│  └──────────────┘  └─────────────┘ │
│         │                           │
│  ┌──────────────────────────────┐  │
│  │   Real Bedrock (us-west-2)   │  │
│  │   (Dev quota, test models)   │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Infrastructure

**Terraform Configuration:**

```hcl
# infra/integration/main.tf
resource "aws_ecs_cluster" "fleetq_integration" {
  name = "fleetq-integration"
}

resource "aws_ecs_task_definition" "control_plane" {
  family = "fleetq-control-plane-integration"
  
  container_definitions = jsonencode([{
    name  = "control-plane"
    image = "fleetq/control-plane:${var.version}"
    
    environment = [
      { name = "ENV", value = "integration" },
      { name = "DATABASE_URL", value = aws_db_instance.postgres.endpoint },
      { name = "BEDROCK_REGION", value = "us-west-2" }
    ]
    
    portMappings = [
      { containerPort = 8000, protocol = "tcp" }
    ]
  }])
}

resource "aws_ecs_service" "control_plane" {
  name            = "control-plane"
  cluster         = aws_ecs_cluster.fleetq_integration.id
  task_definition = aws_ecs_task_definition.control_plane.arn
  desired_count   = 1
  
  load_balancer {
    target_group_arn = aws_lb_target_group.control_plane.arn
    container_name   = "control-plane"
    container_port   = 8000
  }
}

resource "aws_ecs_task_definition" "worker" {
  family = "fleetq-worker-integration"
  
  container_definitions = jsonencode([{
    name  = "worker"
    image = "fleetq/worker:${var.version}"
    
    environment = [
      { name = "ENV", value = "integration" },
      { name = "CONTROL_PLANE_URL", value = "http://${aws_lb.control_plane.dns_name}" }
    ]
  }])
}

resource "aws_ecs_service" "workers" {
  name            = "workers"
  cluster         = aws_ecs_cluster.fleetq_integration.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = 10  # 10 workers
}

resource "aws_db_instance" "postgres" {
  identifier        = "fleetq-integration"
  engine            = "postgres"
  engine_version    = "15.3"
  instance_class    = "db.t3.medium"
  allocated_storage = 100
  
  db_name  = "fleetq"
  username = "fleetq"
  password = random_password.db_password.result
  
  skip_final_snapshot = true
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id      = "fleetq-integration"
  engine          = "redis"
  node_type       = "cache.t3.micro"
  num_cache_nodes = 1
}
```

### Configuration

```yaml
# config/integration.yaml
environment: integration

control_plane:
  host: 0.0.0.0
  port: 8000
  workers: 10

database:
  type: postgresql
  host: fleetq-integration.xxxxx.us-west-2.rds.amazonaws.com
  port: 5432
  name: fleetq
  user: fleetq
  password: ${DB_PASSWORD}  # From secrets manager

iohub:
  transport: tcp://redis:6379
  hwm: 10000

bedrock:
  region: us-west-2
  endpoint: https://bedrock-runtime.us-west-2.amazonaws.com
  models:
    - anthropic.claude-3-haiku-20240307-v1:0  # Dev quota
  
throttling:
  enabled: true
  aimd:
    initial_rate: 10
    max_rate: 50  # Lower than prod

logging:
  level: INFO
  format: json
  destination: cloudwatch

metrics:
  enabled: true
  cloudwatch: true
  prometheus: true
```

### Deployment

```bash
# Deploy to integration
./scripts/deploy-integration.sh

# Run integration tests
TEST_ENV=integration pytest -m integration

# Check health
curl https://fleetq-integration.example.com/health
```

### Scheduled Tests

```yaml
# .github/workflows/integration-tests.yml
name: Integration Tests

on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours
  workflow_dispatch:  # Manual trigger

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-west-2
      
      - name: Run integration tests
        env:
          TEST_ENV: integration
          CONTROL_PLANE_URL: https://fleetq-integration.example.com
        run: |
          poetry run pytest \
            -m integration \
            --junit-xml=integration-results.xml
      
      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: integration-test-results
          path: integration-results.xml
      
      - name: Notify on failure
        if: failure()
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK }}
          payload: |
            {
              "text": "Integration tests failed!",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "Integration tests failed in run ${{ github.run_number }}"
                  }
                }
              ]
            }
```

---

## Staging Environment

### Purpose

Pre-production validation with production-identical setup.

### Architecture

```
┌─────────────────────────────────────┐
│    AWS Account (Staging)            │
│                                     │
│  ┌──────────────┐  ┌─────────────┐ │
│  │     ECS      │  │     ECS     │ │
│  │ Control Plane│  │  Workers(20)│ │
│  │ (2 replicas) │  │ (autoscale) │ │
│  └──────────────┘  └─────────────┘ │
│         │                 │         │
│  ┌──────────────┐  ┌─────────────┐ │
│  │     RDS      │  │  ElastiCache│ │
│  │  PostgreSQL  │  │   (Redis)   │ │
│  │ (Multi-AZ)   │  │ (Cluster)   │ │
│  └──────────────┘  └─────────────┘ │
│         │                           │
│  ┌──────────────────────────────┐  │
│  │  Real Bedrock (us-east-1)    │  │
│  │  (Staging quota, all models) │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Configuration

```yaml
# config/staging.yaml
environment: staging

control_plane:
  host: 0.0.0.0
  port: 8000
  replicas: 2  # High availability
  workers: 20

database:
  type: postgresql
  host: fleetq-staging.xxxxx.us-east-1.rds.amazonaws.com
  port: 5432
  name: fleetq
  user: fleetq
  password: ${DB_PASSWORD}
  pool_size: 50
  multi_az: true
  backup_retention: 7

iohub:
  transport: tcp://redis-staging:6379
  hwm: 50000
  cluster_mode: true

bedrock:
  region: us-east-1
  endpoint: https://bedrock-runtime.us-east-1.amazonaws.com
  models:
    - anthropic.claude-3-sonnet-20240229-v1:0
    - anthropic.claude-3-haiku-20240307-v1:0
    - anthropic.claude-3-opus-20240229-v1:0

throttling:
  enabled: true
  aimd:
    initial_rate: 50
    max_rate: 200  # 80% of prod

logging:
  level: INFO
  format: json
  destination: cloudwatch
  retention_days: 30

metrics:
  enabled: true
  cloudwatch: true
  prometheus: true
  grafana: true

alarms:
  enabled: true
  sns_topic: arn:aws:sns:us-east-1:xxx:fleetq-staging-alarms
```

### Pre-Release Validation

**Smoke Tests:**

```bash
# Deploy to staging
./scripts/deploy-staging.sh v1.2.0

# Wait for deployment
aws ecs wait services-stable --cluster fleetq-staging

# Run smoke tests
TEST_ENV=staging pytest -m smoke

# Run full regression
TEST_ENV=staging pytest -m "not slow"

# Run load tests
TEST_ENV=staging pytest -m load --duration=3600  # 1 hour

# Verify metrics
./scripts/verify-staging-metrics.sh

# Manual QA validation
# ... perform manual testing ...

# Approve for production
./scripts/approve-for-production.sh v1.2.0
```

**Promotion Criteria:**

| Criterion | Requirement | Status |
|-----------|-------------|--------|
| **Smoke Tests** | 100% pass | ✅ |
| **Regression Tests** | 100% pass | ✅ |
| **Load Tests** | No degradation | ✅ |
| **Error Rate** | < 0.1% | ✅ |
| **Latency p99** | < 5s | ✅ |
| **Manual QA** | Sign-off | ✅ |
| **Security Scan** | No critical issues | ✅ |

---

## Production Environment

### Purpose

Real workload serving actual users.

### Architecture

```
┌─────────────────────────────────────┐
│     AWS Account (Production)        │
│                                     │
│  ┌──────────────┐  ┌─────────────┐ │
│  │     ECS      │  │     ECS     │ │
│  │ Control Plane│  │  Workers(50)│ │
│  │ (3 replicas) │  │ (autoscale) │ │
│  │     ALB      │  │  100-200    │ │
│  └──────────────┘  └─────────────┘ │
│         │                 │         │
│  ┌──────────────┐  ┌─────────────┐ │
│  │    Aurora    │  │ ElastiCache │ │
│  │  PostgreSQL  │  │   (Redis)   │ │
│  │ (Multi-AZ)   │  │  (Cluster)  │ │
│  │  Read Replica│  │             │ │
│  └──────────────┘  └─────────────┘ │
│         │                           │
│  ┌──────────────────────────────┐  │
│  │   Real Bedrock (multi-AZ)    │  │
│  │   (Production quota)         │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

### Configuration

```yaml
# config/production.yaml
environment: production

control_plane:
  host: 0.0.0.0
  port: 8000
  replicas: 3  # High availability
  workers: 50
  autoscale:
    min: 50
    max: 200
    target_cpu: 70

database:
  type: aurora-postgresql
  cluster_endpoint: fleetq-prod.cluster-xxxxx.us-east-1.rds.amazonaws.com
  read_endpoint: fleetq-prod.cluster-ro-xxxxx.us-east-1.rds.amazonaws.com
  port: 5432
  name: fleetq
  user: fleetq
  password: ${DB_PASSWORD}
  pool_size: 100
  multi_az: true
  backup_retention: 30
  encryption: true

iohub:
  transport: tcp://redis-prod:6379
  hwm: 100000
  cluster_mode: true
  sentinel: true

bedrock:
  region: us-east-1
  endpoint: https://bedrock-runtime.us-east-1.amazonaws.com
  models:
    - anthropic.claude-3-sonnet-20240229-v1:0
    - anthropic.claude-3-haiku-20240307-v1:0
    - anthropic.claude-3-opus-20240229-v1:0

throttling:
  enabled: true
  aimd:
    initial_rate: 100
    max_rate: 250  # Production quota

logging:
  level: WARNING  # Less verbose in prod
  format: json
  destination: cloudwatch
  retention_days: 90

metrics:
  enabled: true
  cloudwatch: true
  prometheus: true
  grafana: true
  datadog: true

alarms:
  enabled: true
  sns_topic: arn:aws:sns:us-east-1:xxx:fleetq-prod-alarms
  pagerduty: true

backup:
  enabled: true
  schedule: "0 2 * * *"  # 2 AM UTC daily
  retention: 30
```

### Monitoring (Read-Only)

**Synthetic Monitoring:**

```python
# monitoring/synthetic_check.py
import requests
import time
from datadog import statsd

def synthetic_check():
    """Synthetic check running every 5 minutes."""
    
    start = time.time()
    
    try:
        # Submit task
        response = requests.post(
            "https://fleetq.example.com/tasks",
            json={
                "payload": {
                    "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
                    "messages": [{"role": "user", "content": "Synthetic test"}]
                }
            },
            timeout=10
        )
        
        if response.status_code != 200:
            statsd.increment('synthetic.check.failure')
            return
        
        task_id = response.json()['task_id']
        
        # Poll for completion (max 60s)
        for _ in range(60):
            status_response = requests.get(
                f"https://fleetq.example.com/tasks/{task_id}",
                timeout=5
            )
            
            if status_response.json()['status'] == 'COMPLETED':
                duration = time.time() - start
                statsd.timing('synthetic.check.duration', duration)
                statsd.increment('synthetic.check.success')
                return
            
            time.sleep(1)
        
        # Timeout
        statsd.increment('synthetic.check.timeout')
    
    except Exception as e:
        statsd.increment('synthetic.check.error')
        print(f"Synthetic check failed: {e}")

# Schedule every 5 minutes
```

**Data Invariant Monitoring:**

```sql
-- Run every hour to check invariants
-- monitoring/invariant_checks.sql

-- Check for status monotonicity violations
SELECT COUNT(*) as violations
FROM (
    SELECT 
        a.task_id,
        a.state_after,
        b.state_before
    FROM task_audit a
    JOIN task_audit b ON a.task_id = b.task_id 
        AND b.timestamp > a.timestamp
    WHERE state_rank(a.state_after) > state_rank(b.state_before)
) AS backwards;

-- Alert if violations > 0
```

### No Active Testing in Production

**Prohibited:**
- ❌ Running test suites
- ❌ Chaos injection
- ❌ Load testing
- ❌ State machine testing

**Allowed:**
- ✅ Synthetic monitoring (read-only checks)
- ✅ Metric collection
- ✅ Invariant verification queries
- ✅ Canary deployments

---

## Environment Configuration

### Configuration Management

**Hierarchy:**

```
config/
├── base.yaml              # Common defaults
├── local.yaml             # Local overrides
├── ci.yaml                # CI overrides
├── integration.yaml       # Integration overrides
├── staging.yaml           # Staging overrides
└── production.yaml        # Production overrides
```

**Loading Strategy:**

```python
# config/loader.py
import yaml
from pathlib import Path
import os

def load_config(env: str = None) -> dict:
    """Load configuration for environment."""
    
    env = env or os.getenv('ENV', 'local')
    
    # Load base config
    with open('config/base.yaml') as f:
        config = yaml.safe_load(f)
    
    # Override with environment-specific config
    env_config_path = f'config/{env}.yaml'
    if Path(env_config_path).exists():
        with open(env_config_path) as f:
            env_config = yaml.safe_load(f)
            config = deep_merge(config, env_config)
    
    # Override with environment variables
    config = apply_env_vars(config)
    
    return config

def deep_merge(base: dict, override: dict) -> dict:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def apply_env_vars(config: dict) -> dict:
    """Replace ${VAR} placeholders with environment variables."""
    import re
    
    def replace_vars(obj):
        if isinstance(obj, dict):
            return {k: replace_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [replace_vars(v) for v in obj]
        elif isinstance(obj, str):
            # Replace ${VAR} with env var
            return re.sub(
                r'\$\{(\w+)\}',
                lambda m: os.getenv(m.group(1), m.group(0)),
                obj
            )
        else:
            return obj
    
    return replace_vars(config)
```

### Secrets Management

**Development:**
- `.env` file (not committed)
- Environment variables

**Production:**
- AWS Secrets Manager
- IAM roles for service accounts
- Parameter Store for non-sensitive config

**Example:**

```python
# secrets.py
import boto3
import json
from functools import lru_cache

@lru_cache(maxsize=1)
def get_secret(secret_name: str) -> dict:
    """Retrieve secret from AWS Secrets Manager."""
    
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    
    return json.loads(response['SecretString'])

# Usage
db_creds = get_secret('fleetq/production/database')
database_url = f"postgresql://{db_creds['username']}:{db_creds['password']}@{db_creds['host']}/fleetq"
```

---

## Access Control

### Access Matrix

| Environment | Developers | CI/CD | QA | DevOps | Admins |
|-------------|-----------|-------|-----|--------|--------|
| **Local** | ✅ Full | ❌ None | ❌ None | ❌ None | ❌ None |
| **CI** | ✅ Read logs | ✅ Full | ❌ None | ✅ Read | ✅ Full |
| **Integration** | ✅ Read | ✅ Deploy | ✅ Full | ✅ Full | ✅ Full |
| **Staging** | ✅ Read | ✅ Deploy | ✅ Full | ✅ Full | ✅ Full |
| **Production** | ❌ None | ✅ Deploy | ❌ None | ✅ Limited | ✅ Full |

### IAM Policies

**Developer Access (Integration):**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:DescribeServices",
        "ecs:DescribeTasks",
        "logs:GetLogEvents",
        "cloudwatch:GetMetricData"
      ],
      "Resource": "arn:aws:*:*:*:fleetq-integration-*"
    }
  ]
}
```

**CI/CD Access:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:UpdateService",
        "ecs:RegisterTaskDefinition",
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:PutImage"
      ],
      "Resource": "*"
    }
  ]
}
```

### VPN Requirements

| Environment | VPN Required | Justification |
|-------------|--------------|---------------|
| **Local** | ❌ No | Running locally |
| **CI** | ❌ No | GitHub-hosted runners |
| **Integration** | ⚠️ Optional | Public endpoint with auth |
| **Staging** | ✅ Yes | Internal network only |
| **Production** | ✅ Yes | Internal network only |

---

## Deployment & Refresh

### Deployment Strategy

**Integration:**
- Continuous deployment on main branch
- Automated rollback on test failure

**Staging:**
- Manual promotion from integration
- Canary deployment (10% → 50% → 100%)
- 30-minute soak time between phases

**Production:**
- Manual approval required
- Blue/green deployment
- Canary deployment (5% → 25% → 50% → 100%)
- 1-hour soak time between phases

### Deployment Script

```bash
#!/bin/bash
# scripts/deploy.sh

set -e

ENV=$1
VERSION=$2

if [ -z "$ENV" ] || [ -z "$VERSION" ]; then
    echo "Usage: ./deploy.sh <env> <version>"
    exit 1
fi

echo "Deploying FLEET-Q $VERSION to $ENV"

# Build and push Docker images
docker build -t fleetq/control-plane:$VERSION .
docker push fleetq/control-plane:$VERSION

docker build -t fleetq/worker:$VERSION -f Dockerfile.worker .
docker push fleetq/worker:$VERSION

# Update task definitions
aws ecs register-task-definition \
    --cli-input-json file://infra/$ENV/control-plane-task-def.json \
    --query 'taskDefinition.taskDefinitionArn' \
    --output text

# Update services
aws ecs update-service \
    --cluster fleetq-$ENV \
    --service control-plane \
    --task-definition fleetq-control-plane-$ENV:latest \
    --force-new-deployment

aws ecs update-service \
    --cluster fleetq-$ENV \
    --service workers \
    --task-definition fleetq-worker-$ENV:latest \
    --force-new-deployment

# Wait for stable
echo "Waiting for deployment to stabilize..."
aws ecs wait services-stable \
    --cluster fleetq-$ENV \
    --services control-plane workers

# Run smoke tests
echo "Running smoke tests..."
TEST_ENV=$ENV pytest -m smoke

echo "Deployment complete!"
```

### Environment Refresh

**Integration:**
- Daily database refresh from anonymized staging snapshot
- Clear old data (> 7 days)

**Staging:**
- Weekly database refresh from anonymized production snapshot
- Preserve last 30 days of data

```bash
#!/bin/bash
# scripts/refresh-integration-data.sh

# Create snapshot from staging
SNAPSHOT_ID=$(aws rds create-db-snapshot \
    --db-instance-identifier fleetq-staging \
    --db-snapshot-identifier fleetq-staging-$(date +%Y%m%d) \
    --query 'DBSnapshot.DBSnapshotIdentifier' \
    --output text)

# Wait for snapshot
aws rds wait db-snapshot-completed --db-snapshot-identifier $SNAPSHOT_ID

# Restore to integration (overwrite)
aws rds restore-db-instance-from-db-snapshot \
    --db-instance-identifier fleetq-integration-temp \
    --db-snapshot-identifier $SNAPSHOT_ID

# Run anonymization
psql -h fleetq-integration-temp -U fleetq -c "
    UPDATE tasks SET 
        payload = anonymize_payload(payload),
        metadata = anonymize_metadata(metadata);
"

# Swap DNS (blue/green)
# ... swap integration RDS endpoint ...

echo "Integration data refreshed from staging"
```

---

## Summary

This environment strategy provides:

✅ **Isolation:** Independent environments for each stage  
✅ **Parity:** Staging mirrors production architecture  
✅ **Automation:** Infrastructure as code with Terraform  
✅ **Security:** Appropriate access controls per environment  
✅ **Quality:** Progressive validation through environment stages  
✅ **Monitoring:** Comprehensive observability at each stage  

**Environment Progression:**
```
Local (fast feedback) 
  → CI (automated validation) 
  → Integration (realistic testing) 
  → Staging (pre-prod validation) 
  → Production (real users)
```

---

## References

- [AWS ECS Best Practices](https://docs.aws.amazon.com/AmazonECS/latest/bestpracticesguide/)
- [Terraform AWS](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [GitHub Actions](https://docs.github.com/en/actions)
- [12-Factor App](https://12factor.net/)

---

**Document Status:** ✅ Complete  
**Review Date:** 2026-02-15  
**Next Review:** 2026-05-08
