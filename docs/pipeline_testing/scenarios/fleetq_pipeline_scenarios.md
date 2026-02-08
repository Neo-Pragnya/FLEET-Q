# FLEET-Q BDD Acceptance Test Scenarios

**Version:** 1.0  
**Last Updated:** 2026-02-08  
**Framework:** pytest-bdd (Gherkin)  
**Category:** End-to-End Acceptance Tests

---

## 📋 Scenario Index

| ID | Scenario | Priority | Status | Requirements Covered |
|----|----------|----------|--------|---------------------|
| **FLEETQ-BDD-001** | Happy path: Submit → Execute → Complete | P0 | ✅ | REQ-F001, F003, F004 |
| **FLEETQ-BDD-002** | Validation errors on submit | P0 | ✅ | REQ-F001 |
| **FLEETQ-BDD-003** | Concurrent execution (no collision) | P0 | ✅ | REQ-F002, NF003 |
| **FLEETQ-BDD-004** | AIMD throttle adaptation | P0 | ✅ | REQ-NF004, F003 |
| **FLEETQ-BDD-005** | Execution failure and retry | P0 | ✅ | REQ-F006 |
| **FLEETQ-BDD-006** | Retry exhaustion → DLQ | P0 | ✅ | REQ-F007 |
| **FLEETQ-BDD-007** | Leader election and failover | P0 | ✅ | REQ-NF005 |
| **FLEETQ-BDD-008** | Graceful shutdown | P0 | ✅ | REQ-NF008 |
| **FLEETQ-BDD-009** | Status query | P0 | 🔴 | REQ-F009 |
| **FLEETQ-BDD-010** | Batch status query | P1 | 🔴 | REQ-F009 |
| **FLEETQ-BDD-011** | Health endpoints | P0 | 🔴 | REQ-F010 |
| **FLEETQ-BDD-012** | Audit log query | P0 | 🔴 | REQ-NF007 |

---

## FLEETQ-BDD-001: Happy Path - Submit → Execute → Complete

**Priority:** P0  
**Risk Coverage:** N/A (positive path)  
**Requirements:** REQ-F001, REQ-F003, REQ-F004

### Business Narrative

```gherkin
Feature: Task Execution Happy Path
  As a FLEET-Q user
  I want to submit a task and have it execute successfully
  So that I can process my workload reliably

  Background:
    Given FLEET-Q is running with 1 pod
    And Snowflake coordination database is available
    And Bedrock API is available and responsive
    And the outbox flush interval is 10 seconds

  Scenario: Submit single step and complete successfully
    Given I have a valid task definition:
      | field       | value                    |
      | job_id      | job-001                  |
      | step_id     | step-001                 |
      | model       | anthropic.claude-3-sonnet |
      | prompt      | "Summarize this text"    |
      | max_tokens  | 1000                     |
    
    When I POST to "/tasks/submit" with the task definition
    Then the response status should be 202
    And the response should contain:
      | field  | value   |
      | job_id | job-001 |
    
    And the step should be in Snowflake with status "PENDING"
    
    When the claim service runs (within 5 seconds)
    Then the step status should transition to "CLAIMED"
    And the step should have "claimed_by" set to current pod_id
    
    When the worker executes the step
    Then IOHub should grant a permit
    And the step status should transition to "RUNNING"
    And the worker should invoke Bedrock with the prompt
    And Bedrock should respond with 200 OK
    
    When the worker completes execution
    Then the result should be written to SQLite outbox
    And the worker should send success feedback to IOHub
    And IOHub should increment success_streak
    
    When the outbox flush job runs (within 10 seconds)
    Then the step status in Snowflake should be "COMPLETED"
    And the step should have result_data populated
    And the step should have completion_timestamp set
    
    When I GET "/tasks/job-001/status"
    Then the response should show:
      | field       | value     |
      | status      | COMPLETED |
      | steps_total | 1         |
      | steps_done  | 1         |
```

### Test Implementation Pseudocode

```python
@pytest.mark.bdd
@pytest.mark.p0
class TestHappyPath:
    @pytest.fixture
    async def setup_system(self):
        # Start FLEET-Q pod
        pod = await start_fleetq_pod(pod_id="pod-001")
        
        # Mock Bedrock responses
        mock_bedrock = MockBedrockAPI()
        mock_bedrock.add_response(
            model="anthropic.claude-3-sonnet",
            status=200,
            response={"content": "Summary goes here"}
        )
        
        yield pod, mock_bedrock
        
        # Cleanup
        await pod.shutdown()
    
    async def test_happy_path(self, setup_system):
        pod, mock_bedrock = setup_system
        
        # Step 1: Submit task
        response = await api_client.post("/tasks/submit", json={
            "job_id": "job-001",
            "steps": [{
                "step_id": "step-001",
                "model": "anthropic.claude-3-sonnet",
                "prompt": "Summarize this text",
                "max_tokens": 1000
            }]
        })
        assert response.status_code == 202
        assert response.json()["job_id"] == "job-001"
        
        # Step 2: Verify PENDING in Snowflake
        step = await snowflake.get_step("step-001")
        assert step.status == "PENDING"
        assert step.claimed_by is None
        
        # Step 3: Wait for claim (with timeout)
        await wait_for_status("step-001", "CLAIMED", timeout=5)
        step = await snowflake.get_step("step-001")
        assert step.claimed_by == "pod-001"
        
        # Step 4: Wait for execution
        await wait_for_status("step-001", "RUNNING", timeout=10)
        
        # Step 5: Verify Bedrock was called
        assert mock_bedrock.invocation_count == 1
        call = mock_bedrock.get_call(0)
        assert call.model == "anthropic.claude-3-sonnet"
        assert "Summarize" in call.prompt
        
        # Step 6: Wait for completion
        await wait_for_status("step-001", "COMPLETED", timeout=15)
        
        # Step 7: Verify result
        step = await snowflake.get_step("step-001")
        assert step.status == "COMPLETED"
        assert step.result_data is not None
        assert "Summary" in step.result_data
        assert step.completion_timestamp is not None
        
        # Step 8: Query status via API
        response = await api_client.get("/tasks/job-001/status")
        assert response.status_code == 200
        status = response.json()
        assert status["status"] == "COMPLETED"
        assert status["steps_total"] == 1
        assert status["steps_done"] == 1
```

### Success Criteria

✅ **Data Correctness:**
- Step exists in Snowflake with correct fields
- Status transitions: PENDING → CLAIMED → RUNNING → COMPLETED
- Result data populated and retrievable

✅ **Timing:**
- Claim within 5 seconds
- Total completion within 30 seconds

✅ **Integration:**
- Bedrock invoked with correct parameters
- Outbox flushed within 10 seconds

✅ **Observability:**
- All transitions logged
- Metrics captured (latency, success count)

---

## FLEETQ-BDD-002: Validation Errors on Submit

**Priority:** P0  
**Risk Coverage:** N/A (negative testing)  
**Requirements:** REQ-F001

### Business Narrative

```gherkin
Feature: Input Validation
  As a FLEET-Q operator
  I want invalid submissions to be rejected immediately
  So that I can catch errors early and avoid wasted processing

  Scenario Outline: Reject invalid task submissions
    Given FLEET-Q API is running
    
    When I POST to "/tasks/submit" with:
      | field      | value              |
      | job_id     | <job_id>           |
      | steps      | <steps>            |
      | model      | <model>            |
    
    Then the response status should be <status>
    And the response should contain error:
      | field   | value          |
      | code    | <error_code>   |
      | message | <error_message>|
    
    And no data should be written to Snowflake
    
    Examples:
      | job_id | steps | model | status | error_code | error_message |
      | ""     | []    | valid | 400    | INVALID_JOB_ID | "job_id is required" |
      | valid  | []    | valid | 400    | NO_STEPS | "steps array cannot be empty" |
      | valid  | [{}]  | ""    | 400    | INVALID_MODEL | "model is required" |
      | valid  | [{}]  | "invalid-model" | 400 | UNSUPPORTED_MODEL | "Model not supported" |
      | valid  | [{"prompt": "x" * 100000}] | valid | 400 | PROMPT_TOO_LONG | "Prompt exceeds max length" |
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
@pytest.mark.parametrize("job_id,steps,model,expected_status,expected_error", [
    ("", [], "anthropic.claude-3-sonnet", 400, "INVALID_JOB_ID"),
    ("job-001", [], "anthropic.claude-3-sonnet", 400, "NO_STEPS"),
    ("job-001", [{}], "", 400, "INVALID_MODEL"),
    ("job-001", [{"prompt": "test"}], "invalid-model", 400, "UNSUPPORTED_MODEL"),
    ("job-001", [{"prompt": "x" * 100000}], "anthropic.claude-3-sonnet", 400, "PROMPT_TOO_LONG"),
])
async def test_validation_errors(job_id, steps, model, expected_status, expected_error):
    # Submit invalid request
    response = await api_client.post("/tasks/submit", json={
        "job_id": job_id,
        "steps": steps,
        "model": model
    })
    
    # Assert rejection
    assert response.status_code == expected_status
    error = response.json()
    assert error["error_code"] == expected_error
    assert "error_message" in error
    
    # Verify no Snowflake write
    if job_id:
        step = await snowflake.get_step(job_id)
        assert step is None
```

---

## FLEETQ-BDD-003: Concurrent Execution (No Collision)

**Priority:** P0  
**Risk Coverage:** RISK-001 (Data Corruption)  
**Requirements:** REQ-F002, REQ-NF003

### Business Narrative

```gherkin
Feature: Concurrent Task Execution
  As a FLEET-Q operator
  I want multiple pods to process tasks concurrently without conflicts
  So that I can scale horizontally without data corruption

  Background:
    Given FLEET-Q is running with 3 pods:
      | pod_id | hostname    |
      | pod-001| node-1      |
      | pod-002| node-2      |
      | pod-003| node-3      |
    And Snowflake has 100 PENDING steps

  Scenario: Multiple pods claim and execute without collision
    When all 3 pods run their claim loops simultaneously
    Then each step should be claimed by exactly one pod
    And the total claimed steps should be 100
    And no step should have multiple claimed_by values
    
    When all pods execute their claimed steps
    Then each pod should process only its claimed steps
    And no duplicate executions should occur
    
    When all executions complete
    Then all 100 steps should have status COMPLETED
    And each step should have exactly one result
    And no status corruption should be detected
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
@pytest.mark.slow
async def test_concurrent_execution_no_collision():
    # Setup: 3 pods, 100 steps
    pods = await start_multiple_pods(count=3)
    await snowflake.insert_steps([
        {"step_id": f"step-{i:03d}", "status": "PENDING"}
        for i in range(100)
    ])
    
    # Execute: All pods claim simultaneously
    claim_tasks = [pod.run_claim_loop() for pod in pods]
    await asyncio.gather(*claim_tasks, timeout=30)
    
    # Verify: No double claims
    steps = await snowflake.get_all_steps()
    claimed_by_counts = {}
    for step in steps:
        assert step.status == "CLAIMED"
        pod_id = step.claimed_by
        assert pod_id in ["pod-001", "pod-002", "pod-003"]
        claimed_by_counts[pod_id] = claimed_by_counts.get(pod_id, 0) + 1
    
    # All steps claimed
    assert sum(claimed_by_counts.values()) == 100
    
    # Balanced distribution (roughly)
    for count in claimed_by_counts.values():
        assert 20 <= count <= 50  # Allow imbalance
    
    # Execute: All pods execute
    exec_tasks = [pod.execute_claimed_steps() for pod in pods]
    await asyncio.gather(*exec_tasks, timeout=60)
    
    # Wait for outbox flush
    await asyncio.sleep(15)
    
    # Verify: All completed, no duplicates
    steps = await snowflake.get_all_steps()
    assert all(s.status == "COMPLETED" for s in steps)
    assert all(s.result_data is not None for s in steps)
    
    # Check for duplicate results (should not exist)
    result_hashes = [hash(s.result_data) for s in steps]
    assert len(result_hashes) == len(set(result_hashes))  # All unique
```

---

## FLEETQ-BDD-004: AIMD Throttle Adaptation

**Priority:** P0  
**Risk Coverage:** RISK-004 (Bedrock Throttling)  
**Requirements:** REQ-NF004, REQ-F003

### Business Narrative

```gherkin
Feature: Adaptive Throttling
  As a FLEET-Q operator
  I want the system to adapt to Bedrock rate limits automatically
  So that I don't waste money on failed API calls

  Background:
    Given FLEET-Q is running with IOHub configured:
      | parameter          | value |
      | initial_max_inflight | 20   |
      | min_max_inflight     | 1    |
      | max_max_inflight     | 100  |
      | increase_threshold   | 10   |
    And Snowflake has 200 PENDING steps

  Scenario: AIMD decreases on 429 and recovers
    Given IOHub max_inflight starts at 20
    And success_streak is 0
    
    When workers execute steps
    And Bedrock starts returning 429 errors (50% of calls)
    Then IOHub should receive 429 feedback
    And max_inflight should decrease multiplicatively:
      | feedback | max_inflight |
      | 429      | 10           |
      | 429      | 5            |
      | 429      | 2            |
      | 429      | 1 (floor)    |
    
    When Bedrock throttling stops (0% 429 errors)
    Then workers should succeed consistently
    And success_streak should increment on each success
    
    When success_streak reaches 10
    Then max_inflight should increase to 2
    
    When success_streak reaches 20
    Then max_inflight should increase to 3
    
    And max_inflight should continue increasing additively
    Until it returns to 20
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
async def test_aimd_throttle_adaptation():
    # Setup
    pod = await start_fleetq_pod(
        aimd_config={
            "initial_max_inflight": 20,
            "min_max_inflight": 1,
            "max_max_inflight": 100,
            "increase_threshold": 10
        }
    )
    await snowflake.insert_steps([
        {"step_id": f"step-{i:03d}", "status": "PENDING"}
        for i in range(200)
    ])
    
    # Mock Bedrock: 50% 429 rate
    mock_bedrock = MockBedrockAPI()
    mock_bedrock.set_throttle_rate(0.5)  # 50% of calls fail with 429
    
    # Phase 1: Decrease on throttling
    initial_state = await pod.iohub.get_status()
    assert initial_state["max_inflight"] == 20
    
    # Let system run for 30 seconds
    await asyncio.sleep(30)
    
    # Check decrease
    state = await pod.iohub.get_status()
    assert state["max_inflight"] <= 5  # Should have decreased significantly
    assert state["current_inflight"] <= state["max_inflight"]
    
    # Phase 2: Recovery
    mock_bedrock.set_throttle_rate(0.0)  # Stop throttling
    
    # Track max_inflight over time
    max_inflight_history = []
    for _ in range(60):  # Monitor for 60 seconds
        await asyncio.sleep(1)
        state = await pod.iohub.get_status()
        max_inflight_history.append(state["max_inflight"])
    
    # Verify additive increase
    assert max_inflight_history[-1] > max_inflight_history[0]
    assert max_inflight_history[-1] >= 10  # Should recover significantly
    
    # Verify monotonic increase (after initial stabilization)
    stable_history = max_inflight_history[10:]  # Skip first 10s
    for i in range(len(stable_history) - 1):
        # Allow plateaus (same value) but no decreases
        assert stable_history[i+1] >= stable_history[i]
```

---

## FLEETQ-BDD-005: Execution Failure and Retry

**Priority:** P0  
**Risk Coverage:** N/A (positive retry testing)  
**Requirements:** REQ-F006

### Business Narrative

```gherkin
Feature: Retry on Transient Failures
  As a FLEET-Q operator
  I want failed tasks to retry automatically
  So that transient errors don't require manual intervention

  Background:
    Given FLEET-Q is running
    And retry policy is configured:
      | parameter      | value |
      | max_retries    | 5     |
      | initial_delay  | 1s    |
      | backoff_factor | 2     |
    And Snowflake has 1 PENDING step

  Scenario: Transient failure triggers retry with backoff
    Given Bedrock will fail the first 2 attempts with 500 error
    And Bedrock will succeed on the 3rd attempt
    
    When the worker executes the step
    Then the first attempt should fail with 500
    And the step status should transition to FAILED
    And retry_count should be 1
    
    When the retry delay elapses (1 second)
    Then the step should transition back to PENDING
    
    When the claim service picks up the step again
    Then retry_count should still be 1
    And the step should transition to CLAIMED → RUNNING
    
    When the worker executes the step again
    Then the second attempt should fail with 500
    And retry_count should increment to 2
    And the step should transition to FAILED again
    
    When the retry delay elapses (2 seconds)
    Then the step should transition to PENDING
    
    When the worker executes the step the third time
    Then the third attempt should succeed
    And the step should transition to COMPLETED
    And retry_count should remain 2 (final count)
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
async def test_execution_failure_and_retry():
    pod = await start_fleetq_pod()
    await snowflake.insert_step({"step_id": "step-001", "status": "PENDING"})
    
    # Mock Bedrock: fail twice, then succeed
    mock_bedrock = MockBedrockAPI()
    mock_bedrock.add_response(status=500, error="Internal Server Error")  # Attempt 1
    mock_bedrock.add_response(status=500, error="Internal Server Error")  # Attempt 2
    mock_bedrock.add_response(status=200, response={"content": "Success"})  # Attempt 3
    
    # Attempt 1
    await pod.execute_step("step-001")
    step = await snowflake.get_step("step-001")
    assert step.status == "FAILED"
    assert step.retry_count == 1
    assert step.last_error == "Internal Server Error"
    
    # Wait for retry delay
    await asyncio.sleep(2)  # 1s initial delay + buffer
    
    # Retry 1 (Attempt 2)
    await pod.claim_and_execute()
    step = await snowflake.get_step("step-001")
    assert step.status == "FAILED"
    assert step.retry_count == 2
    
    # Wait for retry delay (backoff: 2s)
    await asyncio.sleep(3)
    
    # Retry 2 (Attempt 3)
    await pod.claim_and_execute()
    await asyncio.sleep(15)  # Wait for outbox flush
    
    step = await snowflake.get_step("step-001")
    assert step.status == "COMPLETED"
    assert step.retry_count == 2
    assert step.result_data is not None
    
    # Verify Bedrock call count
    assert mock_bedrock.invocation_count == 3
```

---

## FLEETQ-BDD-006: Retry Exhaustion → DLQ

**Priority:** P0  
**Risk Coverage:** RISK-010 (Infinite Retry)  
**Requirements:** REQ-F007

### Business Narrative

```gherkin
Feature: Dead Letter Queue
  As a FLEET-Q operator
  I want persistently failing tasks to move to DLQ
  So that they don't retry forever and waste resources

  Background:
    Given FLEET-Q is running
    And max_retries is configured to 5
    And Snowflake has 1 PENDING step

  Scenario: Max retries exceeded moves task to DLQ
    Given Bedrock will always return 400 Bad Request (non-retryable)
    
    When the worker executes the step
    Then the attempt should fail
    And retry_count should be 1
    And the step should retry
    
    When the step is retried 5 times total
    Then all 5 attempts should fail
    And retry_count should be 5
    
    When the 6th attempt would occur
    Then the step status should transition to DEAD_LETTER
    And the step should have dlq_reason set to "Max retries exceeded"
    And the step should have last_error captured
    And the step should NOT retry again
    
    When I query DLQ steps via API
    Then the step should appear in DLQ results
    And the DLQ entry should include:
      | field        | value                   |
      | step_id      | step-001                |
      | status       | DEAD_LETTER             |
      | retry_count  | 5                       |
      | dlq_reason   | Max retries exceeded    |
      | last_error   | Bad Request: Invalid... |
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
async def test_retry_exhaustion_dlq():
    pod = await start_fleetq_pod(max_retries=5)
    await snowflake.insert_step({"step_id": "step-001", "status": "PENDING"})
    
    # Mock Bedrock: always 400
    mock_bedrock = MockBedrockAPI()
    mock_bedrock.set_permanent_error(status=400, error="Bad Request: Invalid prompt")
    
    # Execute with retries
    for attempt in range(6):
        await pod.claim_and_execute()
        await asyncio.sleep(2)  # Retry delay
        
        step = await snowflake.get_step("step-001")
        
        if attempt < 5:
            # Still retrying
            assert step.status == "FAILED"
            assert step.retry_count == attempt + 1
        else:
            # Exhausted → DLQ
            assert step.status == "DEAD_LETTER"
            assert step.retry_count == 5
            assert step.dlq_reason == "Max retries exceeded"
            assert "Bad Request" in step.last_error
    
    # Verify no more retries
    await asyncio.sleep(10)
    step = await snowflake.get_step("step-001")
    assert step.status == "DEAD_LETTER"  # Still DLQ
    
    # Query DLQ via API
    response = await api_client.get("/tasks/dlq")
    dlq_steps = response.json()
    assert len(dlq_steps) == 1
    assert dlq_steps[0]["step_id"] == "step-001"
    assert dlq_steps[0]["retry_count"] == 5
```

---

## FLEETQ-BDD-007: Leader Election and Failover

**Priority:** P0  
**Risk Coverage:** RISK-002 (Leader Election Failure)  
**Requirements:** REQ-NF005

### Business Narrative

```gherkin
Feature: Leader Election
  As a FLEET-Q operator
  I want only one control plane per pod
  So that scheduled jobs don't run multiple times

  Background:
    Given FLEET-Q is running with 3 pods
    And SQLite lease table is empty

  Scenario: Single leader elected, others become followers
    When all 3 pods start simultaneously
    Then exactly 1 pod should acquire the control plane lease
    And the lease should have:
      | field          | value             |
      | holder_pid     | <winning_pod_pid> |
      | holder_hostname| <pod_hostname>    |
      | expires_at     | <now + 15s>       |
    
    And the winning pod should start:
      | component      | status  |
      | IOHub          | running |
      | APScheduler    | running |
      | Outbox Manager | running |
    
    And the 2 follower pods should:
      | component      | status  |
      | IOHub          | skipped |
      | APScheduler    | skipped |
      | Outbox Manager | skipped |
  
  Scenario: Leader failover on pod crash
    Given pod-001 holds the control plane lease
    And APScheduler is running on pod-001
    
    When pod-001 crashes (SIGKILL)
    Then the lease should expire after 15 seconds
    
    When pod-002 attempts lease acquisition
    Then pod-002 should successfully acquire the lease
    And pod-002 should start all control plane components
    
    And scheduled jobs should resume on pod-002:
      | job            | status  |
      | lease_renewal  | running |
      | outbox_flush   | running |
      | recovery_loop  | running |
    
    And no duplicate job executions should occur
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
async def test_leader_election_and_failover():
    # Phase 1: Election
    pods = await start_multiple_pods(count=3, simultaneous=True)
    await asyncio.sleep(2)  # Let election complete
    
    # Check lease
    lease = await snowflake.get_lease()
    assert lease is not None
    assert lease.holder_pid in [p.pid for p in pods]
    
    leader_pod = next(p for p in pods if p.pid == lease.holder_pid)
    follower_pods = [p for p in pods if p.pid != lease.holder_pid]
    
    # Verify leader started components
    leader_status = await leader_pod.get_status()
    assert leader_status["iohub"]["running"] is True
    assert leader_status["scheduler"]["running"] is True
    assert leader_status["outbox"]["initialized"] is True
    
    # Verify followers skipped components
    for follower in follower_pods:
        follower_status = await follower.get_status()
        assert follower_status["iohub"]["running"] is False
        assert follower_status["scheduler"]["running"] is False
    
    # Phase 2: Failover
    await leader_pod.kill(signal="SIGKILL")
    await asyncio.sleep(20)  # Wait for lease expiry (15s) + buffer
    
    # Check new lease
    new_lease = await snowflake.get_lease()
    assert new_lease is not None
    assert new_lease.holder_pid != lease.holder_pid  # Different leader
    assert new_lease.holder_pid in [p.pid for p in follower_pods]
    
    new_leader = next(p for p in follower_pods if p.pid == new_lease.holder_pid)
    new_leader_status = await new_leader.get_status()
    assert new_leader_status["iohub"]["running"] is True
    assert new_leader_status["scheduler"]["running"] is True
```

---

## FLEETQ-BDD-008: Graceful Shutdown

**Priority:** P0  
**Risk Coverage:** RISK-005 (Data Loss on Crash)  
**Requirements:** REQ-NF008

### Business Narrative

```gherkin
Feature: Graceful Shutdown
  As a FLEET-Q operator
  I want pods to shut down cleanly
  So that no data is lost when pods restart

  Background:
    Given FLEET-Q pod-001 is running
    And pod-001 holds the control plane lease
    And pod-001 has 5 steps in RUNNING state
    And the SQLite outbox has 10 pending writes

  Scenario: SIGTERM triggers graceful shutdown
    When pod-001 receives SIGTERM signal
    Then the pod should:
      | action                    | result        |
      | Stop accepting new tasks  | immediate     |
      | Wait for in-flight tasks  | max 30s       |
      | Flush outbox to Snowflake | complete      |
      | Release control plane lease| complete     |
      | Stop all services          | ordered       |
    
    And all 5 running steps should complete
    And all 10 outbox writes should be flushed
    And the lease should be released
    And the pod should exit with code 0
    
    When another pod starts
    Then it should successfully acquire the lease
    And no data should be lost
```

### Test Implementation

```python
@pytest.mark.bdd
@pytest.mark.p0
async def test_graceful_shutdown():
    pod = await start_fleetq_pod()
    
    # Setup: In-flight tasks
    await snowflake.insert_steps([
        {"step_id": f"step-{i:03d}", "status": "PENDING"}
        for i in range(5)
    ])
    
    # Start execution
    exec_task = asyncio.create_task(pod.run_forever())
    await asyncio.sleep(5)  # Let some tasks start
    
    # Verify in-flight tasks
    steps = await snowflake.get_steps_by_status("RUNNING")
    assert len(steps) > 0
    in_flight_ids = {s.step_id for s in steps}
    
    # Verify outbox has pending writes
    outbox_count = await pod.outbox.get_pending_count()
    assert outbox_count > 0
    
    # Send SIGTERM
    await pod.send_signal("SIGTERM")
    
    # Wait for shutdown (with timeout)
    try:
        await asyncio.wait_for(exec_task, timeout=60)
    except asyncio.TimeoutError:
        pytest.fail("Shutdown timed out")
    
    # Verify all in-flight tasks completed
    for step_id in in_flight_ids:
        step = await snowflake.get_step(step_id)
        assert step.status == "COMPLETED"
    
    # Verify outbox flushed
    outbox_count = await pod.outbox.get_pending_count()
    assert outbox_count == 0
    
    # Verify lease released
    lease = await snowflake.get_lease()
    assert lease is None or lease.holder_pid != pod.pid
    
    # Verify clean exit
    assert pod.exit_code == 0
```

---

## 🔴 Pending Scenarios (To Be Implemented)

### FLEETQ-BDD-009: Status Query

**Requirements:** REQ-F009  
**Implementation:** TBD

```gherkin
Feature: Task Status Query
  Scenario: Query individual task status
    Given a completed task exists
    When I GET "/tasks/{job_id}/status"
    Then I should receive complete status details
```

### FLEETQ-BDD-010: Batch Status Query

**Requirements:** REQ-F009  
**Implementation:** TBD

```gherkin
Feature: Batch Status Query
  Scenario: Query multiple task statuses
    Given 100 tasks exist
    When I GET "/tasks/status?job_ids=..."
    Then I should receive all statuses in one response
```

### FLEETQ-BDD-011: Health Endpoints

**Requirements:** REQ-F010  
**Implementation:** TBD

```gherkin
Feature: Health Checks
  Scenario: Liveness and readiness probes
    When I GET "/health/liveness"
    Then response should be 200 OK
    When I GET "/health/readiness"
    Then response should indicate if pod can accept traffic
```

### FLEETQ-BDD-012: Audit Log Query

**Requirements:** REQ-NF007  
**Implementation:** TBD

```gherkin
Feature: Audit Trail
  Scenario: Query complete audit log
    Given multiple state transitions occurred
    When I query audit log
    Then all transitions should be present with timestamps
```

---

## 📚 Related Documents

- [Test Strategy](../00_test_strategy.md)
- [Risk Register](../02_risk_register.md)
- [Traceability Matrix](../03_traceability_matrix.md)

---

**Status:** 8/12 scenarios complete (67%)  
**Next:** Implement remaining 4 scenarios (BDD-009 through BDD-012)
