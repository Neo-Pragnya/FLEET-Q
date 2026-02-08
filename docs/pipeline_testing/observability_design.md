# FLEET-Q Testing Observability & Evidence Framework

**Version:** 1.0  
**Last Updated:** 2026-02-08  
**Status:** Active  
**Owner:** FLEET-Q Testing Team

---

## Table of Contents

1. [Overview](#overview)
2. [Observability Pillars](#observability-pillars)
3. [Evidence Pack Structure](#evidence-pack-structure)
4. [Metrics Framework](#metrics-framework)
5. [Logging Framework](#logging-framework)
6. [Tracing Framework](#tracing-framework)
7. [Scenario-Specific Observability](#scenario-specific-observability)
8. [Dashboard Design](#dashboard-design)
9. [Alerting & Notification](#alerting--notification)
10. [Evidence Collection Automation](#evidence-collection-automation)
11. [Storage & Retention](#storage--retention)
12. [Audit Trail](#audit-trail)

---

## Overview

### Purpose

This document defines the **observability and evidence capture framework** for FLEET-Q testing. It ensures that every test execution produces comprehensive, auditable evidence that can be:
- Used to verify correctness
- Analyzed for debugging
- Preserved for compliance
- Correlated across test runs
- Aggregated for trend analysis

### Principles

1. **Evidence-First:** Every test must produce verifiable evidence
2. **Correlation:** All signals (metrics/logs/traces) must be correlated via context IDs
3. **Tamper-Proof:** Evidence must be immutable once captured
4. **Completeness:** Capture sufficient data to reconstruct test execution
5. **Efficiency:** Minimize overhead on test execution performance

### Key Concepts

**Evidence Pack:** A structured collection of all observability data from a single test run
- Metrics snapshots
- Log aggregations
- Trace visualizations
- Database query results
- Screenshots/artifacts
- Test metadata

**Observability Context:** Unique identifiers that link all signals
```python
@dataclass
class ObservabilityContext:
    run_id: str           # e.g., "run_20260208_build123"
    scenario_id: str      # e.g., "FLEETQ-BDD-001"
    trace_id: str         # OpenTelemetry trace ID
    test_session_id: str  # Pytest session ID
    timestamp: datetime
    environment: str      # local, ci, integration, staging
    commit_sha: str
    tester: str
```

---

## Observability Pillars

### 1. Metrics (What Happened - Quantitative)

**Purpose:** Quantitative measurements of system behavior

**Collection Method:** Prometheus exporters + StatsD clients

**Key Dimensions:**
- Component (control_plane, worker, iohub, etc.)
- Operation (submit, claim, execute, etc.)
- Status (success, failure, timeout, etc.)
- Priority (p0, p1, p2)

**Example:**
```python
from prometheus_client import Counter, Histogram, Gauge

# Request counters
task_submissions_total = Counter(
    'fleetq_task_submissions_total',
    'Total task submissions',
    ['status', 'validation_result']
)

# Latency histograms
task_claim_latency_seconds = Histogram(
    'fleetq_task_claim_latency_seconds',
    'Task claim latency',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
)

# State gauges
active_leases_count = Gauge(
    'fleetq_active_leases_count',
    'Current active leases'
)
```

### 2. Logs (What Happened - Qualitative)

**Purpose:** Contextual information about events

**Format:** Structured JSON logs

**Required Fields:**
```json
{
  "timestamp": "2026-02-08T10:30:45.123Z",
  "level": "INFO",
  "logger": "fleet_q.control_plane",
  "message": "Task claimed successfully",
  "context": {
    "run_id": "run_20260208_build123",
    "scenario_id": "FLEETQ-BDD-001",
    "trace_id": "7a8f3d2e1b4c9a0f",
    "span_id": "1b4c9a0f"
  },
  "data": {
    "task_id": "task_123",
    "pod_id": "pod_1",
    "attempt": 1,
    "lease_duration": 300
  }
}
```

**Log Levels:**
- `DEBUG`: Detailed diagnostic information
- `INFO`: Significant events (claims, completions, state transitions)
- `WARNING`: Recoverable issues (retries, throttling)
- `ERROR`: Errors that don't crash the system
- `CRITICAL`: System failures

### 3. Traces (How It Happened - Causal)

**Purpose:** End-to-end request flow with timing

**Framework:** OpenTelemetry

**Span Hierarchy:**
```
test_scenario [FLEETQ-BDD-001]
├── setup
│   ├── start_control_plane
│   ├── start_workers
│   └── initialize_database
├── submit_task
│   ├── validate_request
│   ├── insert_database
│   └── publish_iohub
├── claim_task
│   ├── query_claimable
│   ├── acquire_lease
│   └── update_state
├── execute_task
│   ├── fetch_payload
│   ├── invoke_bedrock
│   └── store_result
└── teardown
    ├── stop_workers
    └── cleanup_database
```

**Span Attributes:**
```python
span.set_attribute("scenario_id", "FLEETQ-BDD-001")
span.set_attribute("task_id", "task_123")
span.set_attribute("pod_id", "pod_1")
span.set_attribute("state_before", "PENDING")
span.set_attribute("state_after", "CLAIMED")
span.set_attribute("retry_attempt", 1)
```

---

## Evidence Pack Structure

### Directory Layout

```
evidence/
└── run_20260208_build123/
    ├── metadata.json                    # Test run metadata
    ├── summary.md                       # Human-readable summary
    ├── scenarios/
    │   ├── FLEETQ-BDD-001/
    │   │   ├── scenario.json           # Scenario definition
    │   │   ├── metrics.json            # Metrics snapshot
    │   │   ├── logs.jsonl              # Structured logs
    │   │   ├── traces.json             # OpenTelemetry traces
    │   │   ├── queries/
    │   │   │   ├── state_transitions.sql
    │   │   │   ├── state_transitions_results.csv
    │   │   │   ├── outbox_durability.sql
    │   │   │   └── outbox_durability_results.csv
    │   │   ├── artifacts/
    │   │   │   ├── database_snapshot.db
    │   │   │   └── api_response.json
    │   │   └── screenshots/
    │   │       └── dashboard_20260208_103045.png
    │   ├── FLEETQ-BDD-002/
    │   └── ...
    ├── aggregated/
    │   ├── all_metrics.json            # All metrics aggregated
    │   ├── all_logs.jsonl              # All logs aggregated
    │   ├── coverage_report.html        # Coverage summary
    │   └── test_report.html            # Pytest HTML report
    └── compliance/
        ├── sign_off.json               # Sign-off record
        └── audit_log.jsonl             # Audit trail
```

### metadata.json

```json
{
  "run_id": "run_20260208_build123",
  "timestamp": "2026-02-08T10:30:00Z",
  "environment": "ci",
  "branch": "main",
  "commit_sha": "a1b2c3d4e5f6",
  "commit_message": "Add resilience testing",
  "tester": "github-actions",
  "test_session_id": "pytest_session_xyz",
  "python_version": "3.11.5",
  "pytest_version": "7.4.0",
  "fleet_q_version": "0.3.0",
  "total_scenarios": 40,
  "scenarios_passed": 26,
  "scenarios_failed": 0,
  "scenarios_skipped": 14,
  "total_duration_seconds": 1234.56,
  "p0_scenarios_passed": 23,
  "p0_scenarios_failed": 0,
  "quality_gate": "PASS"
}
```

### summary.md Template

```markdown
# Test Run Summary

**Run ID:** run_20260208_build123  
**Date:** 2026-02-08 10:30:00 UTC  
**Environment:** CI  
**Commit:** a1b2c3d4e5f6  
**Duration:** 20m 34s

---

## Overall Results

- **Total Scenarios:** 40
- **Passed:** 26 (65%)
- **Failed:** 0 (0%)
- **Skipped:** 14 (35%)
- **Quality Gate:** ✅ PASS

---

## P0 Scenarios (Release Blockers)

- **Total P0:** 34
- **Passed:** 23 (68%)
- **Failed:** 0 (0%)
- **Status:** ⚠️ 11 scenarios pending implementation

---

## By Category

| Category | Total | Passed | Failed | Skipped | % Complete |
|----------|-------|--------|--------|---------|------------|
| BDD | 12 | 8 | 0 | 4 | 67% |
| State Machine | 8 | 7 | 0 | 1 | 88% |
| Data Invariants | 5 | 3 | 0 | 2 | 60% |
| Contracts | 3 | 3 | 0 | 0 | 100% |
| Resilience | 12 | 5 | 0 | 7 | 42% |

---

## Failed Scenarios

*No failures in this run.* ✅

---

## Skipped Scenarios (Pending Implementation)

1. FLEETQ-BDD-009: Status query
2. FLEETQ-BDD-010: Batch status query
3. FLEETQ-BDD-011: Health endpoints
4. FLEETQ-BDD-012: Audit log query
5. STATE-008: FAILED → DEAD_LETTER
6. DATA-INV-04: AIMD correctness
7. DATA-INV-05: Audit completeness
8. RESILIENCE-06: ZeroMQ HWM backpressure
9. RESILIENCE-07: Lease renewal failure
10. RESILIENCE-08: Transient failures
11. RESILIENCE-09: DLQ query
12. RESILIENCE-10: Heartbeat failure
13. RESILIENCE-11: Dead pod detection
14. RESILIENCE-12: SIGTERM handling

---

## Metrics Summary

- **Total Task Submissions:** 127
- **Successful Completions:** 115 (90.5%)
- **Failed with Retry:** 10 (7.9%)
- **Dead Lettered:** 2 (1.6%)
- **Avg Claim Latency:** 45ms (p50), 89ms (p95), 234ms (p99)
- **Avg Execution Time:** 2.3s (p50), 4.7s (p95), 8.9s (p99)
- **Avg Outbox Flush:** 12ms (p50), 34ms (p95), 67ms (p99)

---

## Quality Gates

| Gate | Threshold | Actual | Status |
|------|-----------|--------|--------|
| P0 Pass Rate | 100% | 100% (23/23 impl) | ✅ PASS |
| Overall Pass Rate | ≥ 95% | 100% (26/26 run) | ✅ PASS |
| Claim Latency p95 | < 100ms | 89ms | ✅ PASS |
| State Coverage | 100% | 100% | ✅ PASS |
| Contract Validation | PASS | PASS | ✅ PASS |

---

## Evidence Location

All evidence stored in: `evidence/run_20260208_build123/`

---

## Sign-off

- **Automated Tests:** ✅ PASS
- **Manual Review Required:** No
- **Compliance:** ✅ All evidence captured
- **Retention:** 90 days (until 2026-05-09)

---

## Next Steps

1. Implement 11 pending P0 scenarios
2. Run full regression suite
3. Performance benchmarking
```

---

## Metrics Framework

### Metric Catalog

#### Task Lifecycle Metrics

```python
# Counters
task_submissions_total = Counter(
    'fleetq_task_submissions_total',
    'Total task submissions',
    ['status', 'validation_result']
)

task_claims_total = Counter(
    'fleetq_task_claims_total',
    'Total task claims',
    ['pod_id', 'status']
)

task_executions_total = Counter(
    'fleetq_task_executions_total',
    'Total task executions',
    ['status', 'attempt']
)

task_completions_total = Counter(
    'fleetq_task_completions_total',
    'Total task completions',
    ['status']  # success, failure, dead_letter
)

# Histograms
task_claim_latency_seconds = Histogram(
    'fleetq_task_claim_latency_seconds',
    'Time to claim task after submission',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)

task_execution_duration_seconds = Histogram(
    'fleetq_task_execution_duration_seconds',
    'Task execution duration',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
)

task_e2e_latency_seconds = Histogram(
    'fleetq_task_e2e_latency_seconds',
    'End-to-end task latency (submit to complete)',
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
)

# Gauges
active_tasks_by_state = Gauge(
    'fleetq_active_tasks_by_state',
    'Current task count by state',
    ['state']  # PENDING, CLAIMED, RUNNING, etc.
)

active_leases_count = Gauge(
    'fleetq_active_leases_count',
    'Current active leases'
)

worker_pool_size = Gauge(
    'fleetq_worker_pool_size',
    'Current worker pool size',
    ['pod_id']
)
```

#### State Machine Metrics

```python
state_transitions_total = Counter(
    'fleetq_state_transitions_total',
    'Total state transitions',
    ['from_state', 'to_state', 'transition_type']  # legal, illegal
)

illegal_transition_attempts_total = Counter(
    'fleetq_illegal_transition_attempts_total',
    'Illegal transition attempts',
    ['from_state', 'to_state']
)

state_dwell_time_seconds = Histogram(
    'fleetq_state_dwell_time_seconds',
    'Time spent in each state',
    ['state'],
    buckets=[0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
)
```

#### Resilience Metrics

```python
retry_attempts_total = Counter(
    'fleetq_retry_attempts_total',
    'Total retry attempts',
    ['reason', 'outcome']  # outcome: success, failure, dead_letter
)

lease_renewals_total = Counter(
    'fleetq_lease_renewals_total',
    'Total lease renewals',
    ['status']  # success, failure
)

lease_expirations_total = Counter(
    'fleetq_lease_expirations_total',
    'Total lease expirations (orphans)'
)

recovery_events_total = Counter(
    'fleetq_recovery_events_total',
    'Total recovery events',
    ['recovery_type']  # orphan, crash, partition
)

aimd_throttle_changes_total = Counter(
    'fleetq_aimd_throttle_changes_total',
    'AIMD throttle adjustments',
    ['direction']  # increase, decrease
)

current_aimd_rate = Gauge(
    'fleetq_current_aimd_rate',
    'Current AIMD rate'
)
```

#### Data Integrity Metrics

```python
outbox_writes_total = Counter(
    'fleetq_outbox_writes_total',
    'Total outbox writes',
    ['status']  # success, failure
)

outbox_flush_latency_seconds = Histogram(
    'fleetq_outbox_flush_latency_seconds',
    'Outbox flush latency',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
)

data_invariant_violations_total = Counter(
    'fleetq_data_invariant_violations_total',
    'Data invariant violations detected',
    ['invariant_type']  # monotonicity, idempotency, durability
)
```

#### API Metrics

```python
api_requests_total = Counter(
    'fleetq_api_requests_total',
    'Total API requests',
    ['method', 'endpoint', 'status_code']
)

api_request_duration_seconds = Histogram(
    'fleetq_api_request_duration_seconds',
    'API request duration',
    ['method', 'endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0]
)

api_contract_violations_total = Counter(
    'fleetq_api_contract_violations_total',
    'API contract violations',
    ['endpoint', 'violation_type']
)
```

### Metrics Collection

**Prometheus Scrape Config:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'fleetq_control_plane'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 5s
    scrape_timeout: 3s
    
  - job_name: 'fleetq_workers'
    static_configs:
      - targets: ['localhost:8001', 'localhost:8002', 'localhost:8003']
    scrape_interval: 5s
    
  - job_name: 'fleetq_iohub'
    static_configs:
      - targets: ['localhost:5555']
    scrape_interval: 5s
```

**Test Metrics Capture:**

```python
import pytest
import json
from prometheus_client import CollectorRegistry, generate_latest

@pytest.fixture
def metrics_collector():
    """Capture metrics during test execution."""
    registry = CollectorRegistry()
    
    yield registry
    
    # Snapshot metrics after test
    metrics_data = generate_latest(registry).decode('utf-8')
    
    # Parse and store
    metrics_json = parse_prometheus_text(metrics_data)
    
    # Save to evidence pack
    scenario_id = os.environ.get('SCENARIO_ID')
    run_id = os.environ.get('RUN_ID')
    metrics_path = f"evidence/{run_id}/scenarios/{scenario_id}/metrics.json"
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics_json, f, indent=2)
```

---

## Logging Framework

### Log Configuration

```python
# logging_config.py
import logging
import json
from datetime import datetime

class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'context': {
                'run_id': os.getenv('RUN_ID'),
                'scenario_id': os.getenv('SCENARIO_ID'),
                'trace_id': getattr(record, 'trace_id', None),
                'span_id': getattr(record, 'span_id', None),
            },
            'data': getattr(record, 'data', {}),
        }
        
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)

# Setup
handler = logging.FileHandler('evidence/{run_id}/scenarios/{scenario_id}/logs.jsonl')
handler.setFormatter(StructuredFormatter())
logger = logging.getLogger('fleet_q')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
```

### Contextual Logging

```python
import logging
from contextvars import ContextVar

# Context variables
trace_id_var: ContextVar[str] = ContextVar('trace_id', default=None)
span_id_var: ContextVar[str] = ContextVar('span_id', default=None)

class ContextLogger:
    """Logger with automatic context injection."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
    
    def _log(self, level, message, **data):
        extra = {
            'trace_id': trace_id_var.get(),
            'span_id': span_id_var.get(),
            'data': data
        }
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message, **data):
        self._log(logging.DEBUG, message, **data)
    
    def info(self, message, **data):
        self._log(logging.INFO, message, **data)
    
    def warning(self, message, **data):
        self._log(logging.WARNING, message, **data)
    
    def error(self, message, **data):
        self._log(logging.ERROR, message, **data)

# Usage
logger = ContextLogger('fleet_q.control_plane')

with trace_context(trace_id='abc123', span_id='def456'):
    logger.info(
        "Task claimed successfully",
        task_id='task_123',
        pod_id='pod_1',
        lease_duration=300
    )
```

### Log Aggregation

```python
@pytest.fixture(scope='session')
def log_aggregator():
    """Aggregate all logs for a test session."""
    
    class LogAggregator:
        def __init__(self):
            self.logs = []
        
        def add_log(self, log_entry):
            self.logs.append(log_entry)
        
        def save(self, run_id):
            output_path = f"evidence/{run_id}/aggregated/all_logs.jsonl"
            with open(output_path, 'w') as f:
                for log in self.logs:
                    f.write(json.dumps(log) + '\n')
        
        def filter(self, level=None, logger=None, scenario_id=None):
            filtered = self.logs
            if level:
                filtered = [l for l in filtered if l['level'] == level]
            if logger:
                filtered = [l for l in filtered if l['logger'] == logger]
            if scenario_id:
                filtered = [l for l in filtered if l['context']['scenario_id'] == scenario_id]
            return filtered
    
    aggregator = LogAggregator()
    yield aggregator
    aggregator.save(os.environ['RUN_ID'])
```

---

## Tracing Framework

### OpenTelemetry Setup

```python
# tracing.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

def setup_tracing(run_id: str, scenario_id: str):
    """Initialize OpenTelemetry tracing."""
    
    resource = Resource.create({
        "service.name": "fleetq-test",
        "run.id": run_id,
        "scenario.id": scenario_id,
    })
    
    provider = TracerProvider(resource=resource)
    
    # OTLP exporter (to Jaeger/Tempo)
    otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317")
    provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
    
    # File exporter (to evidence pack)
    file_exporter = FileSpanExporter(
        f"evidence/{run_id}/scenarios/{scenario_id}/traces.json"
    )
    provider.add_span_processor(BatchSpanProcessor(file_exporter))
    
    trace.set_tracer_provider(provider)
    
    return trace.get_tracer(__name__)
```

### Test Scenario Tracing

```python
@pytest.fixture
def tracer():
    """Provide tracer for test scenarios."""
    run_id = os.environ['RUN_ID']
    scenario_id = os.environ['SCENARIO_ID']
    return setup_tracing(run_id, scenario_id)

def test_happy_path(tracer):
    """FLEETQ-BDD-001: Happy path."""
    
    with tracer.start_as_current_span("test_scenario") as scenario_span:
        scenario_span.set_attribute("scenario_id", "FLEETQ-BDD-001")
        scenario_span.set_attribute("priority", "P0")
        
        # Setup
        with tracer.start_as_current_span("setup"):
            control_plane = start_control_plane()
            workers = start_workers(count=3)
        
        # Submit task
        with tracer.start_as_current_span("submit_task") as submit_span:
            task_id = submit_task(payload={"prompt": "Hello"})
            submit_span.set_attribute("task_id", task_id)
        
        # Wait for claim
        with tracer.start_as_current_span("wait_for_claim") as claim_span:
            wait_for_state(task_id, "CLAIMED", timeout=5.0)
            claim_span.set_attribute("task_id", task_id)
        
        # Wait for completion
        with tracer.start_as_current_span("wait_for_completion") as complete_span:
            result = wait_for_state(task_id, "COMPLETED", timeout=30.0)
            complete_span.set_attribute("task_id", task_id)
            complete_span.set_attribute("result_size", len(result))
        
        # Teardown
        with tracer.start_as_current_span("teardown"):
            stop_workers(workers)
            stop_control_plane(control_plane)
        
        scenario_span.set_status(trace.StatusCode.OK)
```

### Trace Visualization

Export traces to Jaeger UI for visualization:

```bash
# Start Jaeger
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest

# Access UI
open http://localhost:16686
```

---

## Scenario-Specific Observability

### BDD Scenarios

**Key Signals:**
- API request/response metrics
- State transition logs
- End-to-end latency traces

**Evidence:**
```python
@scenario("Happy path: Submit → Execute → Complete")
def test_happy_path(metrics_collector, log_aggregator, tracer):
    # Capture evidence
    evidence = {
        'scenario_id': 'FLEETQ-BDD-001',
        'metrics': {},
        'logs': [],
        'traces': [],
        'queries': {},
        'artifacts': {}
    }
    
    # Execute test
    with tracer.start_as_current_span("FLEETQ-BDD-001"):
        task_id = submit_task(...)
        wait_for_completion(task_id)
    
    # Capture metrics
    evidence['metrics'] = capture_metrics(metrics_collector, [
        'task_submissions_total',
        'task_executions_total',
        'task_e2e_latency_seconds'
    ])
    
    # Capture logs
    evidence['logs'] = log_aggregator.filter(scenario_id='FLEETQ-BDD-001')
    
    # Run verification queries
    evidence['queries']['state_transitions'] = execute_query("""
        SELECT * FROM task_audit 
        WHERE task_id = ? 
        ORDER BY timestamp
    """, task_id)
    
    # Save evidence pack
    save_evidence_pack('FLEETQ-BDD-001', evidence)
```

### State Machine Tests

**Key Signals:**
- State transition counters
- Illegal transition attempts
- State dwell time distributions

**Evidence:**
```python
@pytest.mark.state_machine
def test_all_legal_transitions(state_machine_verifier):
    evidence = {
        'scenario_id': 'STATE-003',
        'coverage': {},
        'transitions': [],
        'violations': []
    }
    
    # Execute all transitions
    for from_state in ALL_STATES:
        for to_state in get_legal_transitions(from_state):
            transition = execute_transition(from_state, to_state)
            evidence['transitions'].append(transition)
    
    # Verify coverage
    evidence['coverage'] = state_machine_verifier.compute_coverage()
    assert evidence['coverage']['states_visited'] == 1.0
    assert evidence['coverage']['transitions_visited'] == 1.0
    
    # Query actual transitions
    evidence['queries']['actual_transitions'] = execute_query("""
        SELECT 
            state_before,
            state_after,
            COUNT(*) as count
        FROM task_audit
        GROUP BY state_before, state_after
        ORDER BY state_before, state_after
    """)
    
    save_evidence_pack('STATE-003', evidence)
```

### Data Invariant Tests

**Key Signals:**
- Invariant violation counters
- SQL query results
- Data consistency checks

**Evidence:**
```python
@pytest.mark.data_invariant
def test_status_monotonicity(db_connection):
    evidence = {
        'scenario_id': 'DATA-INV-01',
        'invariant': 'status_monotonicity',
        'violations': [],
        'queries': {}
    }
    
    # Run invariant query
    violations = execute_query("""
        WITH state_order AS (
            SELECT 
                task_id,
                state_before,
                state_after,
                timestamp,
                ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY timestamp) as seq
            FROM task_audit
        ),
        backwards AS (
            SELECT 
                a.task_id,
                a.state_before,
                a.state_after,
                b.state_before as next_state_before
            FROM state_order a
            JOIN state_order b ON a.task_id = b.task_id AND b.seq = a.seq + 1
            WHERE state_rank(a.state_after) > state_rank(b.state_before)
        )
        SELECT * FROM backwards;
    """)
    
    evidence['queries']['violations'] = violations
    evidence['violations'] = violations
    
    assert len(violations) == 0, f"Found {len(violations)} monotonicity violations"
    
    save_evidence_pack('DATA-INV-01', evidence)
```

### Contract Tests

**Key Signals:**
- Schema validation results
- Breaking change detections
- API version compatibility

**Evidence:**
```python
@pytest.mark.contract
def test_api_schema_validation(openapi_spec):
    evidence = {
        'scenario_id': 'CONTRACT-01',
        'spec_version': '1.0.0',
        'endpoints_tested': [],
        'violations': []
    }
    
    # Test each endpoint
    for endpoint in openapi_spec['paths']:
        for method in openapi_spec['paths'][endpoint]:
            result = test_endpoint_contract(endpoint, method, openapi_spec)
            evidence['endpoints_tested'].append(result)
            
            if not result['valid']:
                evidence['violations'].append(result)
    
    # Generate contract report
    report = generate_contract_report(evidence)
    save_artifact('CONTRACT-01', 'contract_report.html', report)
    
    assert len(evidence['violations']) == 0
    
    save_evidence_pack('CONTRACT-01', evidence)
```

### Resilience Tests

**Key Signals:**
- Recovery event counters
- Lease expiration metrics
- Failure injection logs

**Evidence:**
```python
@pytest.mark.resilience
def test_pod_crash_during_lease(chaos_injector):
    evidence = {
        'scenario_id': 'RESILIENCE-01',
        'failure_type': 'pod_crash',
        'recovery_metrics': {},
        'timeline': []
    }
    
    # Start normal execution
    task_id = submit_task(...)
    wait_for_state(task_id, "RUNNING")
    evidence['timeline'].append({'event': 'task_running', 'timestamp': now()})
    
    # Inject crash
    pod_id = get_executing_pod(task_id)
    chaos_injector.crash_pod(pod_id)
    evidence['timeline'].append({'event': 'pod_crashed', 'timestamp': now(), 'pod_id': pod_id})
    
    # Wait for recovery
    recovery_start = now()
    wait_for_state(task_id, "PENDING")  # Should be recovered
    recovery_duration = (now() - recovery_start).total_seconds()
    
    evidence['timeline'].append({'event': 'task_recovered', 'timestamp': now()})
    evidence['recovery_metrics']['duration_seconds'] = recovery_duration
    
    # Query recovery audit
    evidence['queries']['recovery_audit'] = execute_query("""
        SELECT * FROM recovery_audit 
        WHERE task_id = ?
    """, task_id)
    
    # Assert recovery SLA
    assert recovery_duration < 60.0, f"Recovery took {recovery_duration}s, expected < 60s"
    
    save_evidence_pack('RESILIENCE-01', evidence)
```

---

## Dashboard Design

### Real-Time Test Dashboard

**Purpose:** Live monitoring during test execution

**Metrics:**
- Current scenario execution
- Pass/fail counts
- Latency percentiles
- Error rates
- Coverage percentages

**Grafana Dashboard JSON:**

```json
{
  "dashboard": {
    "title": "FLEET-Q Test Execution",
    "panels": [
      {
        "title": "Test Execution Progress",
        "type": "stat",
        "targets": [
          {
            "expr": "sum(fleetq_test_scenarios_passed)",
            "legendFormat": "Passed"
          },
          {
            "expr": "sum(fleetq_test_scenarios_failed)",
            "legendFormat": "Failed"
          }
        ]
      },
      {
        "title": "Task Latency (p50, p95, p99)",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.50, rate(fleetq_task_e2e_latency_seconds_bucket[5m]))",
            "legendFormat": "p50"
          },
          {
            "expr": "histogram_quantile(0.95, rate(fleetq_task_e2e_latency_seconds_bucket[5m]))",
            "legendFormat": "p95"
          },
          {
            "expr": "histogram_quantile(0.99, rate(fleetq_task_e2e_latency_seconds_bucket[5m]))",
            "legendFormat": "p99"
          }
        ]
      },
      {
        "title": "State Machine Coverage",
        "type": "gauge",
        "targets": [
          {
            "expr": "fleetq_state_coverage_percent",
            "legendFormat": "Coverage"
          }
        ],
        "thresholds": [
          {"value": 80, "color": "red"},
          {"value": 95, "color": "yellow"},
          {"value": 100, "color": "green"}
        ]
      },
      {
        "title": "Error Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(fleetq_task_executions_total{status='failure'}[5m]) / rate(fleetq_task_executions_total[5m])",
            "legendFormat": "Error Rate"
          }
        ]
      },
      {
        "title": "Active Tests by Category",
        "type": "piechart",
        "targets": [
          {
            "expr": "fleetq_active_tests_by_category"
          }
        ]
      }
    ]
  }
}
```

### Evidence Pack Dashboard

**Purpose:** Post-execution analysis

**Sections:**
1. **Summary:** Pass/fail, duration, quality gates
2. **Coverage:** By category, priority, requirement
3. **Performance:** Latency distributions, throughput
4. **Failures:** Root causes, stack traces, logs
5. **Trends:** Comparison with previous runs

**HTML Template:**

```html
<!DOCTYPE html>
<html>
<head>
    <title>Test Run: {{ run_id }}</title>
    <style>
        /* Dashboard CSS */
        .summary { background: #f0f0f0; padding: 20px; }
        .passed { color: green; }
        .failed { color: red; }
        .metric { display: inline-block; margin: 10px; }
    </style>
</head>
<body>
    <h1>Test Run: {{ run_id }}</h1>
    
    <div class="summary">
        <div class="metric">
            <h3>Total Scenarios</h3>
            <p>{{ total_scenarios }}</p>
        </div>
        <div class="metric">
            <h3 class="passed">Passed</h3>
            <p>{{ scenarios_passed }}</p>
        </div>
        <div class="metric">
            <h3 class="failed">Failed</h3>
            <p>{{ scenarios_failed }}</p>
        </div>
        <div class="metric">
            <h3>Duration</h3>
            <p>{{ duration }}s</p>
        </div>
    </div>
    
    <h2>Coverage by Category</h2>
    <table>
        <tr>
            <th>Category</th>
            <th>Total</th>
            <th>Passed</th>
            <th>Failed</th>
            <th>% Complete</th>
        </tr>
        {% for category in categories %}
        <tr>
            <td>{{ category.name }}</td>
            <td>{{ category.total }}</td>
            <td class="passed">{{ category.passed }}</td>
            <td class="failed">{{ category.failed }}</td>
            <td>{{ category.percent }}%</td>
        </tr>
        {% endfor %}
    </table>
    
    <h2>Performance Metrics</h2>
    <canvas id="latencyChart"></canvas>
    
    <h2>Failed Scenarios</h2>
    {% for failure in failures %}
    <div class="failure">
        <h3>{{ failure.scenario_id }}: {{ failure.name }}</h3>
        <pre>{{ failure.error }}</pre>
        <a href="scenarios/{{ failure.scenario_id }}/logs.jsonl">View Logs</a>
    </div>
    {% endfor %}
    
    <h2>Trend Analysis</h2>
    <canvas id="trendChart"></canvas>
    
    <script>
        // Chart.js visualizations
    </script>
</body>
</html>
```

---

## Alerting & Notification

### Alert Rules

**Prometheus Alert Rules:**

```yaml
# alerts.yml
groups:
  - name: test_execution
    interval: 10s
    rules:
      - alert: TestFailure
        expr: fleetq_test_scenarios_failed > 0
        for: 0m
        labels:
          severity: critical
          team: fleetq
        annotations:
          summary: "Test scenario failed"
          description: "{{ $value }} test scenarios failed in run {{ $labels.run_id }}"
      
      - alert: HighLatency
        expr: histogram_quantile(0.95, rate(fleetq_task_e2e_latency_seconds_bucket[5m])) > 10
        for: 1m
        labels:
          severity: warning
          team: fleetq
        annotations:
          summary: "High task latency detected"
          description: "p95 latency is {{ $value }}s, threshold is 10s"
      
      - alert: LowStateCoverage
        expr: fleetq_state_coverage_percent < 100
        for: 0m
        labels:
          severity: critical
          team: fleetq
        annotations:
          summary: "Incomplete state coverage"
          description: "State coverage is {{ $value }}%, expected 100%"
      
      - alert: DataInvariantViolation
        expr: increase(fleetq_data_invariant_violations_total[1m]) > 0
        for: 0m
        labels:
          severity: critical
          team: fleetq
        annotations:
          summary: "Data invariant violation detected"
          description: "{{ $value }} invariant violations in scenario {{ $labels.scenario_id }}"
      
      - alert: ContractBreakingChange
        expr: fleetq_api_contract_violations_total{violation_type="breaking"} > 0
        for: 0m
        labels:
          severity: critical
          team: fleetq
        annotations:
          summary: "API breaking change detected"
          description: "Breaking change in endpoint {{ $labels.endpoint }}"
```

### Notification Channels

**Slack Integration:**

```python
import requests

def send_slack_notification(run_id: str, status: str, summary: dict):
    """Send test results to Slack."""
    
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    
    color = "good" if status == "PASS" else "danger"
    
    message = {
        "attachments": [
            {
                "color": color,
                "title": f"Test Run {run_id}: {status}",
                "fields": [
                    {"title": "Total Scenarios", "value": summary['total'], "short": True},
                    {"title": "Passed", "value": summary['passed'], "short": True},
                    {"title": "Failed", "value": summary['failed'], "short": True},
                    {"title": "Duration", "value": f"{summary['duration']}s", "short": True},
                ],
                "footer": "FLEET-Q Test Framework",
                "ts": int(time.time())
            }
        ]
    }
    
    requests.post(webhook_url, json=message)
```

**Email Integration:**

```python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_report(run_id: str, recipients: list[str], evidence_url: str):
    """Send detailed email report."""
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"Test Run {run_id} - Results"
    msg['From'] = "fleetq-testing@example.com"
    msg['To'] = ", ".join(recipients)
    
    # Plain text version
    text = f"""
    Test Run: {run_id}
    Status: PASS
    
    View full report: {evidence_url}
    """
    
    # HTML version
    html = f"""
    <html>
      <body>
        <h2>Test Run: {run_id}</h2>
        <p><strong>Status:</strong> <span style="color: green;">PASS</span></p>
        <p><a href="{evidence_url}">View Full Report</a></p>
      </body>
    </html>
    """
    
    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html, 'html'))
    
    with smtplib.SMTP('smtp.example.com', 587) as server:
        server.starttls()
        server.login(os.getenv('SMTP_USER'), os.getenv('SMTP_PASSWORD'))
        server.send_message(msg)
```

---

## Evidence Collection Automation

### Pytest Plugin

```python
# conftest.py
import pytest
import os
import json
from datetime import datetime

@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    """Initialize evidence collection."""
    
    # Generate run ID
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_build{os.getenv('BUILD_NUMBER', 'local')}"
    os.environ['RUN_ID'] = run_id
    
    # Create evidence directory
    os.makedirs(f"evidence/{run_id}/scenarios", exist_ok=True)
    os.makedirs(f"evidence/{run_id}/aggregated", exist_ok=True)
    os.makedirs(f"evidence/{run_id}/compliance", exist_ok=True)
    
    # Initialize metadata
    metadata = {
        'run_id': run_id,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'environment': os.getenv('TEST_ENV', 'local'),
        'branch': os.getenv('GIT_BRANCH', 'unknown'),
        'commit_sha': os.getenv('GIT_COMMIT', 'unknown'),
        'tester': os.getenv('TESTER', os.getenv('USER', 'unknown')),
    }
    
    with open(f"evidence/{run_id}/metadata.json", 'w') as f:
        json.dump(metadata, f, indent=2)

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """Setup before each test."""
    
    # Extract scenario ID from test name
    scenario_id = extract_scenario_id(item.name)
    os.environ['SCENARIO_ID'] = scenario_id
    
    # Create scenario directory
    run_id = os.environ['RUN_ID']
    scenario_dir = f"evidence/{run_id}/scenarios/{scenario_id}"
    os.makedirs(scenario_dir, exist_ok=True)
    os.makedirs(f"{scenario_dir}/queries", exist_ok=True)
    os.makedirs(f"{scenario_dir}/artifacts", exist_ok=True)
    os.makedirs(f"{scenario_dir}/screenshots", exist_ok=True)

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Capture test results."""
    
    outcome = yield
    report = outcome.get_result()
    
    if report.when == 'call':
        scenario_id = os.environ.get('SCENARIO_ID')
        run_id = os.environ.get('RUN_ID')
        
        # Save test result
        result = {
            'scenario_id': scenario_id,
            'outcome': report.outcome,
            'duration': report.duration,
            'longrepr': str(report.longrepr) if report.longrepr else None,
        }
        
        result_path = f"evidence/{run_id}/scenarios/{scenario_id}/result.json"
        with open(result_path, 'w') as f:
            json.dump(result, f, indent=2)

@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """Finalize evidence collection."""
    
    run_id = os.environ['RUN_ID']
    
    # Generate summary
    generate_summary_report(run_id)
    
    # Generate HTML dashboard
    generate_html_dashboard(run_id)
    
    # Send notifications
    if os.getenv('SEND_NOTIFICATIONS') == 'true':
        send_slack_notification(run_id, ...)
        send_email_report(run_id, ...)
```

### Helper Functions

```python
def save_evidence_pack(scenario_id: str, evidence: dict):
    """Save evidence pack for a scenario."""
    
    run_id = os.environ['RUN_ID']
    scenario_dir = f"evidence/{run_id}/scenarios/{scenario_id}"
    
    # Save scenario definition
    with open(f"{scenario_dir}/scenario.json", 'w') as f:
        json.dump(evidence, f, indent=2)
    
    # Save metrics
    if 'metrics' in evidence:
        with open(f"{scenario_dir}/metrics.json", 'w') as f:
            json.dump(evidence['metrics'], f, indent=2)
    
    # Save logs
    if 'logs' in evidence:
        with open(f"{scenario_dir}/logs.jsonl", 'w') as f:
            for log in evidence['logs']:
                f.write(json.dumps(log) + '\n')
    
    # Save traces
    if 'traces' in evidence:
        with open(f"{scenario_dir}/traces.json", 'w') as f:
            json.dump(evidence['traces'], f, indent=2)
    
    # Save query results
    if 'queries' in evidence:
        for query_name, results in evidence['queries'].items():
            # Save SQL
            with open(f"{scenario_dir}/queries/{query_name}.sql", 'w') as f:
                f.write(results['sql'])
            
            # Save results as CSV
            import pandas as pd
            df = pd.DataFrame(results['data'])
            df.to_csv(f"{scenario_dir}/queries/{query_name}_results.csv", index=False)
    
    # Save artifacts
    if 'artifacts' in evidence:
        for artifact_name, artifact_data in evidence['artifacts'].items():
            with open(f"{scenario_dir}/artifacts/{artifact_name}", 'w') as f:
                if isinstance(artifact_data, (dict, list)):
                    json.dump(artifact_data, f, indent=2)
                else:
                    f.write(str(artifact_data))

def capture_metrics(registry, metric_names: list[str]) -> dict:
    """Capture specific metrics from registry."""
    
    from prometheus_client import generate_latest
    
    metrics_text = generate_latest(registry).decode('utf-8')
    metrics_dict = {}
    
    for line in metrics_text.split('\n'):
        if line.startswith('#'):
            continue
        
        for metric_name in metric_names:
            if line.startswith(metric_name):
                parts = line.split()
                metrics_dict[parts[0]] = float(parts[1])
    
    return metrics_dict

def execute_query(sql: str, *params) -> dict:
    """Execute SQL query and return results."""
    
    import sqlite3
    
    conn = sqlite3.connect('fleetq.db')
    cursor = conn.cursor()
    cursor.execute(sql, params)
    
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    return {
        'sql': sql,
        'params': params,
        'columns': columns,
        'data': [dict(zip(columns, row)) for row in rows]
    }

def save_artifact(scenario_id: str, filename: str, content: str):
    """Save artifact to evidence pack."""
    
    run_id = os.environ['RUN_ID']
    artifact_path = f"evidence/{run_id}/scenarios/{scenario_id}/artifacts/{filename}"
    
    with open(artifact_path, 'w') as f:
        f.write(content)
```

---

## Storage & Retention

### Storage Strategy

**Local Development:**
- Store in `evidence/` directory
- Keep last 10 runs
- Auto-cleanup older runs

**CI/CD:**
- Store in artifact storage (S3, GCS, Azure Blob)
- Compress evidence packs (tar.gz)
- Upload after test completion

**Production:**
- Store in long-term archival storage
- Immutable once uploaded
- Versioned with run ID

### Retention Policy

| Environment | Retention Period | Storage Tier | Notes |
|-------------|------------------|--------------|-------|
| Local | 7 days | Local disk | Auto-cleanup |
| CI | 30 days | Standard | Compress |
| Integration | 90 days | Standard | P0 failures kept longer |
| Staging | 180 days | Standard | Pre-release validation |
| Production | 1 year | Archive | Compliance |

### S3 Upload Script

```python
import boto3
import tarfile
import os

def upload_evidence_to_s3(run_id: str):
    """Upload evidence pack to S3."""
    
    # Compress evidence pack
    tarball_path = f"/tmp/{run_id}.tar.gz"
    with tarfile.open(tarball_path, 'w:gz') as tar:
        tar.add(f"evidence/{run_id}", arcname=run_id)
    
    # Upload to S3
    s3 = boto3.client('s3')
    bucket = os.getenv('EVIDENCE_BUCKET', 'fleetq-test-evidence')
    key = f"evidence/{run_id}.tar.gz"
    
    s3.upload_file(tarball_path, bucket, key)
    
    # Generate presigned URL (7 days)
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=7*24*3600
    )
    
    return url
```

### Cleanup Script

```python
import os
import shutil
from datetime import datetime, timedelta

def cleanup_old_evidence(days: int = 7):
    """Remove evidence packs older than N days."""
    
    cutoff = datetime.utcnow() - timedelta(days=days)
    
    for run_dir in os.listdir('evidence/'):
        if not run_dir.startswith('run_'):
            continue
        
        # Parse timestamp from run_id
        timestamp_str = run_dir.split('_')[1] + run_dir.split('_')[2]
        run_timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
        
        if run_timestamp < cutoff:
            shutil.rmtree(f"evidence/{run_dir}")
            print(f"Removed old evidence: {run_dir}")
```

---

## Audit Trail

### Audit Log Schema

```json
{
  "timestamp": "2026-02-08T10:30:45.123Z",
  "event_type": "test_execution",
  "actor": "github-actions",
  "action": "run_tests",
  "resource": "run_20260208_build123",
  "details": {
    "scenarios_executed": 26,
    "scenarios_passed": 26,
    "scenarios_failed": 0,
    "quality_gate": "PASS"
  },
  "metadata": {
    "commit_sha": "a1b2c3d4e5f6",
    "branch": "main",
    "environment": "ci"
  }
}
```

### Audit Events

- `test_execution`: Test run started/completed
- `evidence_captured`: Evidence pack created
- `evidence_uploaded`: Evidence uploaded to storage
- `sign_off`: Manual sign-off recorded
- `evidence_accessed`: Evidence pack accessed
- `evidence_deleted`: Evidence pack deleted

### Sign-off Record

```json
{
  "run_id": "run_20260208_build123",
  "sign_off_timestamp": "2026-02-08T11:00:00Z",
  "sign_off_by": "john.doe@example.com",
  "role": "QA Lead",
  "decision": "APPROVED",
  "comments": "All P0 scenarios passed. Ready for release.",
  "requirements_verified": [
    "REQ-F001", "REQ-F002", "REQ-F003",
    "REQ-NF003", "REQ-NF005", "REQ-NF006"
  ],
  "risks_mitigated": [
    "RISK-001", "RISK-002", "RISK-003",
    "RISK-004", "RISK-005", "RISK-006"
  ]
}
```

### Compliance Report

```markdown
# Test Compliance Report

**Run ID:** run_20260208_build123  
**Date:** 2026-02-08  
**Status:** COMPLIANT

---

## Evidence Checklist

- [x] All P0 scenarios executed
- [x] Metrics captured for all tests
- [x] Logs preserved in structured format
- [x] Traces available for end-to-end flows
- [x] Data invariants verified via SQL
- [x] Contract validation passed
- [x] Resilience tests completed
- [x] Evidence pack uploaded to S3
- [x] Retention policy applied (90 days)
- [x] Sign-off recorded

---

## Requirements Coverage

- **Functional:** 10/10 (100%)
- **Non-Functional:** 8/8 (100%)
- **Architecture:** 7/7 (100%)

---

## Risk Mitigation

- **P0 Risks:** 6/6 mitigated (100%)
- **P1 Risks:** 3/4 mitigated (75%)

---

## Audit Trail

All test execution events logged to `evidence/{run_id}/compliance/audit_log.jsonl`

---

## Attestation

I hereby attest that the testing for run `run_20260208_build123` was conducted in accordance with the FLEET-Q Testing Framework and that all evidence has been captured and preserved.

**Signed:** John Doe  
**Role:** QA Lead  
**Date:** 2026-02-08  
**Signature:** [Digital Signature]
```

---

## Implementation Checklist

### Phase 1: Core Observability (Week 1-2)

- [ ] Implement Prometheus metrics exporters
- [ ] Configure structured JSON logging
- [ ] Set up OpenTelemetry tracing
- [ ] Create observability context propagation
- [ ] Build metrics/logs/traces correlation

### Phase 2: Evidence Collection (Week 3-4)

- [ ] Implement pytest plugin for evidence capture
- [ ] Create evidence pack directory structure
- [ ] Build helper functions (save_evidence_pack, capture_metrics, execute_query)
- [ ] Generate summary.md template
- [ ] Implement HTML dashboard generation

### Phase 3: Integration (Week 5-6)

- [ ] Integrate observability into existing test scenarios
- [ ] Add scenario-specific evidence collection
- [ ] Configure Grafana dashboards
- [ ] Set up Jaeger for trace visualization
- [ ] Create alert rules in Prometheus

### Phase 4: Automation & Compliance (Week 7-8)

- [ ] Implement S3 upload automation
- [ ] Configure retention policies
- [ ] Build cleanup scripts
- [ ] Create audit trail logging
- [ ] Generate compliance reports
- [ ] Set up Slack/email notifications

---

## Summary

This observability framework provides:

✅ **Complete Evidence Capture:** Metrics, logs, traces, queries, artifacts  
✅ **Correlation:** All signals linked via context IDs  
✅ **Automation:** Pytest plugin handles collection automatically  
✅ **Visualization:** Grafana dashboards and HTML reports  
✅ **Compliance:** Audit trails and sign-off workflows  
✅ **Storage:** S3 integration with retention policies  

**Next Step:** Implement Phase 1 (Core Observability) by adding Prometheus metrics, structured logging, and OpenTelemetry tracing to FLEET-Q components.

---

## References

- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [OpenTelemetry Python](https://opentelemetry.io/docs/instrumentation/python/)
- [Structured Logging](https://www.structlog.org/)
- [Evidence-Based Testing](https://testing.googleblog.com/)
- [Test Observability](https://martinfowler.com/articles/test-observability.html)

---

**Document Status:** ✅ Complete  
**Review Date:** 2026-02-15  
**Next Review:** 2026-05-08
