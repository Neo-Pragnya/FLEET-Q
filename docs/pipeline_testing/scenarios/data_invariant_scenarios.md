# FLEET-Q Data Invariant Test Scenarios

**Version:** 1.0  
**Last Updated:** 2026-02-08  
**Framework:** pytest + SQL assertions  
**Category:** Data Correctness & Invariant Tests

---

## 📋 Scenario Index

| ID | Scenario | Priority | Status | Requirements Covered |
|----|----------|----------|--------|---------------------|
| **DATA-INV-01** | Status monotonicity check | P0 | ✅ | REQ-NF003 |
| **DATA-INV-02** | Idempotency under replay | P0 | ✅ | REQ-NF003 |
| **DATA-INV-03** | Outbox write durability | P0 | ✅ | REQ-NF003, F004 |
| **DATA-INV-04** | AIMD correctness | P0 | 🔴 | REQ-NF004 |
| **DATA-INV-05** | Audit log completeness | P0 | 🔴 | REQ-NF007 |

---

## 🧮 Data Invariants Definition

### Invariant Categories

| Category | What It Proves | Violation Impact |
|----------|----------------|------------------|
| **Status Monotonicity** | Status never regresses | Data corruption |
| **Idempotency** | Duplicate operations safe | Duplicate work |
| **Referential Integrity** | Foreign keys valid | Orphaned records |
| **Temporal Consistency** | Timestamps ordered | Timeline corruption |
| **Cardinality** | One-to-one/many correct | Missing/duplicate data |
| **State Consistency** | Required fields present | Invalid state |

---

## DATA-INV-01: Status Monotonicity Check

**Priority:** P0  
**Risk Coverage:** RISK-001 (Data Corruption)  
**Requirements:** REQ-NF003

### Invariant Definition

**Formal Statement:**
```
For any step S with status history [s1, s2, ..., sn],
the status must never regress in the ordering:
  PENDING < CLAIMED < RUNNING < COMPLETED
  PENDING < CLAIMED < RUNNING < FAILED < PENDING (retry)
  FAILED < DEAD_LETTER
  
Where COMPLETED and DEAD_LETTER are terminal (no further transitions).
```

**SQL Assertion:**
```sql
-- No status regressions detected
WITH status_order AS (
    SELECT 
        step_id,
        status,
        updated_at,
        CASE status
            WHEN 'PENDING' THEN 1
            WHEN 'CLAIMED' THEN 2
            WHEN 'RUNNING' THEN 3
            WHEN 'FAILED' THEN 2  -- Can retry to PENDING (1)
            WHEN 'COMPLETED' THEN 4
            WHEN 'DEAD_LETTER' THEN 4
        END as status_rank,
        LAG(CASE status
            WHEN 'PENDING' THEN 1
            WHEN 'CLAIMED' THEN 2
            WHEN 'RUNNING' THEN 3
            WHEN 'FAILED' THEN 2
            WHEN 'COMPLETED' THEN 4
            WHEN 'DEAD_LETTER' THEN 4
        END) OVER (PARTITION BY step_id ORDER BY updated_at) as prev_rank
    FROM step_audit_log
)
SELECT step_id, status, prev_rank, status_rank
FROM status_order
WHERE prev_rank > status_rank  -- Regression detected
  AND NOT (prev_rank = 2 AND status_rank = 1 AND status = 'PENDING');  -- Retry exception
```

### Test Implementation

```python
@pytest.mark.data_invariant
@pytest.mark.p0
class TestStatusMonotonicity:
    
    async def test_no_status_regression_single_step(self):
        """Test that a single step never regresses status"""
        step_id = "mono-001"
        
        # Execute: Complete lifecycle
        await snowflake.insert_step({"step_id": step_id, "status": "PENDING"})
        await snowflake.claim_step(step_id, "pod-001")
        await snowflake.start_step(step_id)
        await snowflake.complete_step(step_id, {"result": "done"})
        
        # Verify: Monotonic status progression
        audit_log = await snowflake.get_audit_log(step_id)
        statuses = [entry.status for entry in audit_log]
        
        assert statuses == ["PENDING", "CLAIMED", "RUNNING", "COMPLETED"]
        
        # Verify: No regressions in SQL
        violations = await snowflake.execute_query("""
            WITH status_order AS (...)  -- Query above
            SELECT COUNT(*) as regression_count
            FROM status_order
            WHERE step_id = :step_id
              AND prev_rank > status_rank
              AND NOT (prev_rank = 2 AND status_rank = 1)
        """, step_id=step_id)
        
        assert violations[0].regression_count == 0
    
    async def test_retry_cycle_monotonicity(self):
        """Test that retry cycle maintains monotonicity"""
        step_id = "mono-002"
        
        # Execute: Retry cycle
        await snowflake.insert_step({"step_id": step_id, "status": "PENDING"})
        await snowflake.claim_step(step_id, "pod-001")
        await snowflake.start_step(step_id)
        await snowflake.fail_step(step_id, error="Timeout")
        await snowflake.retry_step(step_id)  # FAILED → PENDING
        await snowflake.claim_step(step_id, "pod-002")
        await snowflake.start_step(step_id)
        await snowflake.complete_step(step_id, {"result": "done"})
        
        # Verify: Audit log
        audit_log = await snowflake.get_audit_log(step_id)
        statuses = [entry.status for entry in audit_log]
        
        expected = [
            "PENDING",    # Initial
            "CLAIMED",    # First attempt
            "RUNNING",
            "FAILED",
            "PENDING",    # Retry (allowed)
            "CLAIMED",    # Second attempt
            "RUNNING",
            "COMPLETED"
        ]
        assert statuses == expected
        
        # Verify: SQL invariant
        violations = await snowflake.check_monotonicity_violations(step_id)
        assert len(violations) == 0
    
    async def test_concurrent_steps_monotonicity(self):
        """Test monotonicity across 100 concurrent steps"""
        step_ids = [f"mono-{i:03d}" for i in range(100)]
        
        # Execute: Concurrent execution
        await run_concurrent_execution(step_ids)
        
        # Verify: No violations across all steps
        violations = await snowflake.execute_query("""
            WITH status_order AS (...)  -- Full query
            SELECT step_id, COUNT(*) as violation_count
            FROM status_order
            WHERE prev_rank > status_rank
              AND NOT (prev_rank = 2 AND status_rank = 1)
            GROUP BY step_id
        """)
        
        assert len(violations) == 0, f"Violations detected: {violations}"
    
    async def test_terminal_state_finality(self):
        """Test that terminal states are never followed by other states"""
        for terminal_state in ["COMPLETED", "DEAD_LETTER"]:
            step_id = f"terminal-{terminal_state}"
            
            # Setup: Reach terminal state
            await create_step_in_state(step_id, terminal_state)
            
            # Verify: No audit entries after terminal state
            audit_log = await snowflake.get_audit_log(step_id)
            terminal_entry = next(e for e in audit_log if e.status == terminal_state)
            
            subsequent_entries = [
                e for e in audit_log
                if e.updated_at > terminal_entry.updated_at
            ]
            assert len(subsequent_entries) == 0
```

---

## DATA-INV-02: Idempotency Under Replay

**Priority:** P0  
**Risk Coverage:** RISK-001 (Data Corruption)  
**Requirements:** REQ-NF003

### Invariant Definition

**Formal Statement:**
```
For any operation OP(step_id, params) executed multiple times:
  OP(S, P) ; OP(S, P) ≡ OP(S, P)

Operations must be idempotent:
- Claim: Second claim with same step_id fails gracefully
- Complete: Second complete with same result doesn't change data
- Fail: Second fail with same error increments retry_count only once
- Outbox write: Duplicate write stored once
```

### Test Implementation

```python
@pytest.mark.data_invariant
@pytest.mark.p0
class TestIdempotencyUnderReplay:
    
    async def test_duplicate_claim_idempotent(self):
        """Test that duplicate claims are idempotent"""
        step_id = "idem-001"
        await snowflake.insert_step({"step_id": step_id, "status": "PENDING"})
        
        # Execute: Claim twice
        result1 = await snowflake.claim_step(step_id, "pod-001")
        result2 = await snowflake.claim_step(step_id, "pod-001")  # Duplicate
        
        # Verify: First succeeds, second is idempotent
        assert result1.success is True
        assert result2.success is True or result2.already_claimed is True
        
        # Verify: Single claim record
        step = await snowflake.get_step(step_id)
        assert step.status == "CLAIMED"
        assert step.claimed_by == "pod-001"
        
        # Verify: Audit log (may have duplicate entries, but state consistent)
        audit_log = await snowflake.get_audit_log(step_id)
        claimed_entries = [e for e in audit_log if e.status == "CLAIMED"]
        assert all(e.claimed_by == "pod-001" for e in claimed_entries)
    
    async def test_duplicate_complete_idempotent(self):
        """Test that duplicate completions are idempotent"""
        step_id = "idem-002"
        await create_step_in_state(step_id, "RUNNING")
        
        result1 = {"content": "First result", "timestamp": "2026-02-08"}
        result2 = {"content": "Second result", "timestamp": "2026-02-09"}
        
        # Execute: Complete twice with different results
        await snowflake.complete_step(step_id, result1)
        await snowflake.complete_step(step_id, result2)  # Duplicate
        
        # Verify: First result wins (first write wins semantics)
        step = await snowflake.get_step(step_id)
        assert step.result_data == result1  # NOT result2
        assert step.status == "COMPLETED"
    
    async def test_duplicate_fail_idempotent(self):
        """Test that duplicate failures don't double-increment retry_count"""
        step_id = "idem-003"
        await create_step_in_state(step_id, "RUNNING")
        
        # Execute: Fail twice with same error
        await snowflake.fail_step(step_id, error="Timeout")
        step1 = await snowflake.get_step(step_id)
        initial_retry_count = step1.retry_count
        
        await snowflake.fail_step(step_id, error="Timeout")  # Duplicate
        step2 = await snowflake.get_step(step_id)
        
        # Verify: Retry count incremented only once (idempotent)
        assert step2.retry_count == initial_retry_count
        # OR if implementation increments: verify idempotency key used
    
    async def test_outbox_write_idempotency(self):
        """Test that duplicate outbox writes create single record"""
        step_id = "idem-004"
        write_intent = {
            "step_id": step_id,
            "operation": "step_update",
            "data": {"status": "COMPLETED"}
        }
        
        # Execute: Write twice with same idempotency key
        outbox = SQLiteOutbox("test.db")
        await outbox.write_step_update(
            step_id=step_id,
            status="COMPLETED",
            idempotency_key="unique-key-001"
        )
        await outbox.write_step_update(
            step_id=step_id,
            status="COMPLETED",
            idempotency_key="unique-key-001"  # Same key
        )
        
        # Verify: Single record in outbox
        pending = await outbox.get_pending_writes()
        matching = [w for w in pending if w.step_id == step_id]
        assert len(matching) == 1
    
    async def test_replay_complete_workflow(self):
        """Test replaying entire workflow is idempotent"""
        step_id = "idem-005"
        
        # Execute: Complete workflow
        await execute_workflow(step_id)
        state1 = await get_complete_state(step_id)
        
        # Execute: Replay workflow (network retry scenario)
        await execute_workflow(step_id)
        state2 = await get_complete_state(step_id)
        
        # Verify: Final state identical
        assert state1 == state2
```

---

## DATA-INV-03: Outbox Write Durability

**Priority:** P0  
**Risk Coverage:** RISK-005 (Data Loss)  
**Requirements:** REQ-NF003, REQ-F004

### Invariant Definition

**Formal Statement:**
```
For any outbox write W:
  After write_to_outbox(W) returns successfully,
  W must be retrievable from SQLite even if pod crashes.
  
  After flush_outbox(W) completes successfully,
  W must exist in Snowflake.
```

### Test Implementation

```python
@pytest.mark.data_invariant
@pytest.mark.p0
class TestOutboxWriteDurability:
    
    async def test_outbox_write_survives_pod_crash(self):
        """Test that outbox write persists after pod crash"""
        step_id = "durable-001"
        
        # Execute: Write to outbox
        pod = await start_fleetq_pod()
        await pod.outbox.write_step_update(
            step_id=step_id,
            status="COMPLETED",
            result_data={"content": "Important result"}
        )
        
        # Simulate: Pod crash (SIGKILL)
        await pod.kill(signal="SIGKILL")
        
        # Verify: Outbox data persists
        outbox = SQLiteOutbox(pod.outbox_db_path)
        pending = await outbox.get_pending_writes()
        
        matching = [w for w in pending if w.step_id == step_id]
        assert len(matching) == 1
        assert matching[0].status == "COMPLETED"
        assert matching[0].result_data["content"] == "Important result"
    
    async def test_flush_guarantees_snowflake_write(self):
        """Test that flushed writes appear in Snowflake"""
        step_id = "durable-002"
        
        # Execute: Write and flush
        pod = await start_fleetq_pod()
        await pod.outbox.write_step_update(
            step_id=step_id,
            status="COMPLETED",
            result_data={"content": "Flushed result"}
        )
        
        # Verify: Pending in outbox
        pending = await pod.outbox.get_pending_writes()
        assert any(w.step_id == step_id for w in pending)
        
        # Execute: Flush
        flushed_count = await pod.outbox.flush_pending_writes()
        assert flushed_count >= 1
        
        # Verify: In Snowflake
        step = await snowflake.get_step(step_id)
        assert step.status == "COMPLETED"
        assert step.result_data["content"] == "Flushed result"
        
        # Verify: Marked as flushed in outbox
        pending = await pod.outbox.get_pending_writes()
        assert not any(w.step_id == step_id for w in pending)
    
    async def test_wal_mode_durability(self):
        """Test that SQLite WAL mode provides crash durability"""
        outbox_path = "test_durable.db"
        
        # Setup: Create outbox with WAL mode
        outbox = SQLiteOutbox(outbox_path, wal_mode=True)
        await outbox.initialize()
        
        # Verify: WAL mode enabled
        pragma = await outbox.execute_query("PRAGMA journal_mode")
        assert pragma[0].journal_mode == "wal"
        
        # Execute: Write with fsync
        await outbox.write_step_update(
            step_id="wal-001",
            status="COMPLETED",
            fsync=True  # Force disk write
        )
        
        # Simulate: Crash (close without checkpoint)
        await outbox.close(checkpoint=False)
        
        # Verify: Data recoverable
        outbox2 = SQLiteOutbox(outbox_path, wal_mode=True)
        pending = await outbox2.get_pending_writes()
        assert any(w.step_id == "wal-001" for w in pending)
    
    async def test_no_data_loss_under_concurrent_writes(self):
        """Test that concurrent outbox writes don't lose data"""
        pod = await start_fleetq_pod()
        
        # Execute: 100 concurrent writes
        write_tasks = [
            pod.outbox.write_step_update(
                step_id=f"concurrent-{i:03d}",
                status="COMPLETED",
                result_data={"index": i}
            )
            for i in range(100)
        ]
        await asyncio.gather(*write_tasks)
        
        # Verify: All 100 writes persisted
        pending = await pod.outbox.get_pending_writes()
        step_ids = {w.step_id for w in pending}
        
        expected_ids = {f"concurrent-{i:03d}" for i in range(100)}
        assert step_ids == expected_ids
```

---

## 🔴 DATA-INV-04: AIMD Correctness (Pending)

**Priority:** P0  
**Requirements:** REQ-NF004  
**Implementation:** TBD

### Invariant Definition

**Formal Statement:**
```
AIMD Algorithm Invariants:
1. min_max_inflight <= max_inflight <= max_max_inflight
2. On 429: max_inflight_new = max_inflight_old * 0.5 (multiplicative decrease)
3. On success streak >= threshold: max_inflight_new = max_inflight_old + 1 (additive increase)
4. current_inflight <= max_inflight (always)
```

### Test Outline

```python
@pytest.mark.data_invariant
@pytest.mark.p0
class TestAIMDCorrectness:
    async def test_bounds_enforcement(self):
        """Test that max_inflight stays within bounds"""
        pass
    
    async def test_multiplicative_decrease_accuracy(self):
        """Test that 429 causes exact 0.5 multiplicative decrease"""
        pass
    
    async def test_additive_increase_accuracy(self):
        """Test that success streak causes +1 increase"""
        pass
    
    async def test_current_inflight_never_exceeds_max(self):
        """Test invariant: current_inflight <= max_inflight"""
        pass
```

---

## 🔴 DATA-INV-05: Audit Log Completeness (Pending)

**Priority:** P0  
**Requirements:** REQ-NF007  
**Implementation:** TBD

### Invariant Definition

**Formal Statement:**
```
For any step S with audit log A:
1. Every status transition must have audit entry
2. Audit entries must be temporally ordered (timestamp monotonic)
3. Audit entries must include: step_id, from_status, to_status, timestamp, actor
4. No gaps in audit trail
```

### Test Outline

```python
@pytest.mark.data_invariant
@pytest.mark.p0
class TestAuditLogCompleteness:
    async def test_all_transitions_logged(self):
        """Test that every state transition has audit entry"""
        pass
    
    async def test_temporal_ordering(self):
        """Test that audit timestamps are monotonic"""
        pass
    
    async def test_required_fields_present(self):
        """Test that all audit entries have required fields"""
        pass
```

---

## 📊 Invariant Verification Query Library

### Query Templates

```sql
-- Template 1: Status Monotonicity
SELECT step_id, status, updated_at
FROM (
    SELECT 
        step_id, status, updated_at,
        LAG(status_rank) OVER (PARTITION BY step_id ORDER BY updated_at) as prev_rank,
        status_rank
    FROM step_audit_with_ranks
)
WHERE prev_rank > status_rank
  AND NOT (prev_rank = 2 AND status_rank = 1);  -- Retry exception

-- Template 2: Referential Integrity
SELECT s.step_id
FROM steps s
LEFT JOIN jobs j ON s.job_id = j.job_id
WHERE j.job_id IS NULL;  -- Orphaned steps

-- Template 3: Temporal Consistency
SELECT step_id
FROM steps
WHERE claimed_at < created_at
   OR started_at < claimed_at
   OR completed_at < started_at;

-- Template 4: Required Fields
SELECT step_id, status
FROM steps
WHERE status = 'CLAIMED' AND claimed_by IS NULL
   OR status = 'RUNNING' AND started_at IS NULL
   OR status = 'COMPLETED' AND result_data IS NULL;

-- Template 5: Cardinality
SELECT step_id, COUNT(*) as claim_count
FROM step_claims
GROUP BY step_id
HAVING COUNT(*) > 1;  -- Multiple claims (violation)
```

---

## 📚 Related Documents

- [Test Strategy](../00_test_strategy.md)
- [State Machine Scenarios](state_machine_scenarios.md)
- [BDD Scenarios](fleetq_pipeline_scenarios.md)

---

**Status:** 3/5 scenarios complete (60%)  
**Next:** Implement DATA-INV-04 (AIMD) and DATA-INV-05 (Audit)
