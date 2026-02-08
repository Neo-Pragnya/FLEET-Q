# FLEET-Q Testing Framework - Scenario Index

**Version:** 1.0  
**Last Updated:** 2026-02-08  
**Total Scenarios:** 40

---

## 📊 Summary Dashboard

### By Category

| Category | Total | Complete | Partial | Pending | % Complete |
|----------|-------|----------|---------|---------|------------|
| **BDD Acceptance** | 12 | 8 | 0 | 4 | 67% |
| **State Machine** | 8 | 7 | 0 | 1 | 88% |
| **Data Invariants** | 5 | 3 | 0 | 2 | 60% |
| **API Contracts** | 3 | 3 | 0 | 0 | 100% |
| **Resilience** | 12 | 5 | 0 | 7 | 42% |
| **TOTAL** | **40** | **26** | **0** | **14** | **65%** |

### By Priority

| Priority | Total | Complete | Pending | % Complete |
|----------|-------|----------|---------|------------|
| **P0** | 34 | 23 | 11 | 68% |
| **P1** | 6 | 3 | 3 | 50% |

### By Status

```
✅ Complete: 26 scenarios (65%)
⚠️ Partial:   0 scenarios (0%)
🔴 Pending:  14 scenarios (35%)
```

---

## 🗂 Complete Scenario Listing

### 1. BDD Acceptance Tests (12 scenarios)

📄 **File:** [fleetq_pipeline_scenarios.md](scenarios/fleetq_pipeline_scenarios.md)

| ID | Scenario | Priority | Status |
|----|----------|----------|--------|
| FLEETQ-BDD-001 | Happy path: Submit → Execute → Complete | P0 | ✅ |
| FLEETQ-BDD-002 | Validation errors on submit | P0 | ✅ |
| FLEETQ-BDD-003 | Concurrent execution (no collision) | P0 | ✅ |
| FLEETQ-BDD-004 | AIMD throttle adaptation | P0 | ✅ |
| FLEETQ-BDD-005 | Execution failure and retry | P0 | ✅ |
| FLEETQ-BDD-006 | Retry exhaustion → DLQ | P0 | ✅ |
| FLEETQ-BDD-007 | Leader election and failover | P0 | ✅ |
| FLEETQ-BDD-008 | Graceful shutdown | P0 | ✅ |
| FLEETQ-BDD-009 | Status query | P0 | 🔴 |
| FLEETQ-BDD-010 | Batch status query | P1 | 🔴 |
| FLEETQ-BDD-011 | Health endpoints | P0 | 🔴 |
| FLEETQ-BDD-012 | Audit log query | P0 | 🔴 |

**Coverage:** 8/12 (67%)

---

### 2. State Machine Tests (8 scenarios)

📄 **File:** [state_machine_scenarios.md](scenarios/state_machine_scenarios.md)

| ID | Scenario | Priority | Status |
|----|----------|----------|--------|
| STATE-001 | PENDING → CLAIMED transition | P0 | ✅ |
| STATE-002 | RUNNING → COMPLETED transition | P0 | ✅ |
| STATE-003 | All legal transitions coverage | P0 | ✅ |
| STATE-004 | Illegal transition block | P0 | ✅ |
| STATE-005 | FAILED → PENDING retry cycle | P0 | ✅ |
| STATE-006 | Lease state transitions | P0 | ✅ |
| STATE-007 | Recovery reset transitions | P0 | ✅ |
| STATE-008 | FAILED → DEAD_LETTER terminal | P0 | 🔴 |

**Coverage:** 7/8 (88%)

---

### 3. Data Invariant Tests (5 scenarios)

📄 **File:** [data_invariant_scenarios.md](scenarios/data_invariant_scenarios.md)

| ID | Scenario | Priority | Status |
|----|----------|----------|--------|
| DATA-INV-01 | Status monotonicity check | P0 | ✅ |
| DATA-INV-02 | Idempotency under replay | P0 | ✅ |
| DATA-INV-03 | Outbox write durability | P0 | ✅ |
| DATA-INV-04 | AIMD correctness | P0 | 🔴 |
| DATA-INV-05 | Audit log completeness | P0 | 🔴 |

**Coverage:** 3/5 (60%)

---

### 4. API Contract Tests (3 scenarios)

📄 **File:** [api_contract_scenarios.md](scenarios/api_contract_scenarios.md)

| ID | Scenario | Priority | Status |
|----|----------|----------|--------|
| CONTRACT-01 | API schema validation | P0 | ✅ |
| CONTRACT-02 | Backward compatibility check | P0 | ✅ |
| CONTRACT-03 | Breaking change detection | P0 | ✅ |

**Coverage:** 3/3 (100%) ✨

---

### 5. Resilience Tests (12 scenarios)

📄 **File:** [resilience_scenarios.md](scenarios/resilience_scenarios.md)

| ID | Scenario | Priority | Status |
|----|----------|----------|--------|
| RESILIENCE-01 | Pod crash during lease | P0 | ✅ |
| RESILIENCE-02 | Network partition | P0 | ✅ |
| RESILIENCE-03 | Crash during outbox write | P0 | ✅ |
| RESILIENCE-04 | Orphan recovery | P0 | ✅ |
| RESILIENCE-05 | Continuous 429 throttling | P0 | ✅ |
| RESILIENCE-06 | ZeroMQ HWM backpressure | P1 | 🔴 |
| RESILIENCE-07 | Lease renewal failure | P1 | 🔴 |
| RESILIENCE-08 | Transient failures | P0 | 🔴 |
| RESILIENCE-09 | DLQ query | P0 | 🔴 |
| RESILIENCE-10 | Heartbeat failure | P0 | 🔴 |
| RESILIENCE-11 | Dead pod detection | P0 | 🔴 |
| RESILIENCE-12 | SIGTERM handling | P0 | 🔴 |

**Coverage:** 5/12 (42%)

---

## 🎯 Requirements Coverage

### Functional Requirements

| Requirement | Scenarios | Status |
|-------------|-----------|--------|
| REQ-F001: Task submission | 3 | ✅ |
| REQ-F002: Task claiming | 3 | ✅ |
| REQ-F003: Task execution | 4 | ✅ |
| REQ-F004: Result storage | 4 | ✅ |
| REQ-F005: Step lifecycle | 4 | ⚠️ |
| REQ-F006: Retry logic | 3 | ⚠️ |
| REQ-F007: Dead letter queue | 3 | ⚠️ |
| REQ-F008: Task recovery | 4 | ✅ |
| REQ-F009: Status query | 2 | 🔴 |
| REQ-F010: Health monitoring | 2 | 🔴 |

### Non-Functional Requirements

| Requirement | Scenarios | Status |
|-------------|-----------|--------|
| REQ-NF001: Max latency | 1 | 🔴 |
| REQ-NF002: Throughput | 1 | 🔴 |
| REQ-NF003: Data integrity | 7 | ✅ |
| REQ-NF004: Adaptive throttling | 3 | ⚠️ |
| REQ-NF005: Singleton control plane | 6 | ✅ |
| REQ-NF006: API compatibility | 3 | ✅ |
| REQ-NF007: Audit trail | 2 | ⚠️ |
| REQ-NF008: Graceful shutdown | 3 | ⚠️ |

---

## 🚨 Risk Coverage

### P0 Risks (All Must Be Tested)

| Risk ID | Risk Name | Scenarios | Status |
|---------|-----------|-----------|--------|
| RISK-001 | Data corruption | 4 | ✅ |
| RISK-002 | Leader election failure | 3 | ✅ |
| RISK-003 | Orphaned tasks | 2 | ✅ |
| RISK-004 | Bedrock throttling | 3 | ✅ |
| RISK-005 | Outbox data loss | 2 | ✅ |
| RISK-006 | API breaking change | 3 | ✅ |

**P0 Risk Coverage: 100%** ✅

### P1 Risks

| Risk ID | Risk Name | Scenarios | Status |
|---------|-----------|-----------|--------|
| RISK-007 | Memory leak | 1 | 🔴 |
| RISK-008 | Connection pool exhaustion | 0 | 🔴 |
| RISK-009 | Lease renewal failure | 1 | 🔴 |
| RISK-010 | Infinite retry | 1 | ✅ |

---

## 📈 Implementation Roadmap

### Phase 1: Complete P0 Scenarios (Current Sprint)

**Goal:** 100% P0 scenario coverage

#### Week 1-2: BDD Scenarios
- [ ] FLEETQ-BDD-009: Status query
- [ ] FLEETQ-BDD-011: Health endpoints
- [ ] FLEETQ-BDD-012: Audit log query

#### Week 3-4: State & Data Invariants
- [ ] STATE-008: FAILED → DEAD_LETTER
- [ ] DATA-INV-04: AIMD correctness
- [ ] DATA-INV-05: Audit completeness

#### Week 5-6: Resilience Tests
- [ ] RESILIENCE-08: Transient failures
- [ ] RESILIENCE-09: DLQ query
- [ ] RESILIENCE-10: Heartbeat failure
- [ ] RESILIENCE-11: Dead pod detection
- [ ] RESILIENCE-12: SIGTERM handling

**Target:** 100% P0 coverage by end of Sprint 1

### Phase 2: Complete P1 Scenarios (Next Sprint)

#### Week 1-2: Remaining Tests
- [ ] FLEETQ-BDD-010: Batch status query
- [ ] RESILIENCE-06: ZeroMQ HWM backpressure
- [ ] RESILIENCE-07: Lease renewal failure

**Target:** 100% overall coverage

---

## 🧪 Test Execution Guide

### Running All Tests

```bash
# Run all scenarios
pytest docs/pipeline_testing/scenarios/ -v

# Run by category
pytest -m bdd          # BDD acceptance tests
pytest -m state_machine  # State machine tests
pytest -m data_invariant # Data invariant tests
pytest -m contract      # API contract tests
pytest -m resilience    # Resilience tests

# Run by priority
pytest -m p0           # P0 only
pytest -m p1           # P1 only

# Run specific scenario
pytest -k "FLEETQ-BDD-001"
```

### Generating Reports

```bash
# Coverage report
pytest --cov=fleet_q --cov-report=html

# State machine coverage
pytest -m state_machine --state-coverage-report=state_coverage.html

# Contract validation
pytest -m contract --contract-report=contract_report.html

# Evidence pack
pytest --evidence-pack=evidence/run_$(date +%Y%m%d)_build123/
```

---

## 📚 Related Documents

### Governance
- [Test Strategy](00_test_strategy.md) - Overall testing approach
- [Risk Register](02_risk_register.md) - Risk assessment
- [Traceability Matrix](03_traceability_matrix.md) - Requirement mapping

### Scenarios
- [BDD Scenarios](scenarios/fleetq_pipeline_scenarios.md) - 12 acceptance tests
- [State Machine](scenarios/state_machine_scenarios.md) - 8 state tests
- [Data Invariants](scenarios/data_invariant_scenarios.md) - 5 invariant tests
- [API Contracts](scenarios/api_contract_scenarios.md) - 3 contract tests
- [Resilience](scenarios/resilience_scenarios.md) - 12 resilience tests

### Implementation
- [Test Data Strategy](04_test_data_strategy.md) - TBD
- [Test Environments](05_test_environments.md) - TBD
- [Observability Design](observability_design.md) - TBD

---

## 📝 Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-08 | FLEET-Q Team | Initial - 40 scenarios defined, 26 implemented |

---

## ✅ Sign-off

**Current Status:**
- Scenario Definitions: 100% complete (40/40)
- Implementations: 65% complete (26/40)
- P0 Risk Coverage: 100% (6/6 risks)
- P0 Scenario Coverage: 68% (23/34 scenarios)

**Next Steps:**
1. Implement remaining 11 P0 scenarios
2. Implement 3 P1 scenarios
3. Generate evidence packs for all implemented tests
4. Complete observability framework

---

**Ready for review and implementation prioritization.**
