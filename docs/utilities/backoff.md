# Exponential Backoff Utility

The FLEET-Q backoff utility provides a robust, reusable exponential backoff mechanism for handling retries across all system operations. It's designed to handle transient failures gracefully while preventing thundering herd problems.

## Overview

The backoff utility implements exponential backoff with jitter, a proven pattern for handling retries in distributed systems. It's used throughout FLEET-Q for:

- Database connection retries
- Step claiming conflicts
- Snowflake transaction retries
- Leader election failures
- Recovery operation retries

## Basic Usage

### Decorator Pattern

The most common way to use the backoff utility is as a decorator:

```python
from fleet_q.backoff import with_backoff

@with_backoff(max_attempts=5, base_delay_ms=100, max_delay_ms=30000)
async def claim_steps(pod_id: str, limit: int):
    # This function will be retried with exponential backoff
    # if it raises a retryable exception
    async with snowflake_transaction() as tx:
        # Atomic step claiming logic
        return await tx.claim_steps(pod_id, limit)

# Usage
steps = await claim_steps("pod-123", 5)
```

### Context Manager Pattern

For more control over retry logic:

```python
from fleet_q.backoff import BackoffManager

async def complex_operation():
    backoff = BackoffManager(
        max_attempts=3,
        base_delay_ms=200,
        max_delay_ms=10000,
        jitter=True
    )
    
    async for attempt in backoff:
        try:
            result = await risky_operation()
            return result
        except RetryableError as e:
            if attempt.is_last:
                raise
            await attempt.wait()
```

## Configuration Options

### Basic Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_attempts` | int | 5 | Maximum number of retry attempts |
| `base_delay_ms` | int | 100 | Base delay in milliseconds |
| `max_delay_ms` | int | 30000 | Maximum delay cap in milliseconds |
| `jitter` | bool | True | Whether to add jitter to delays |

### Advanced Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `retry_on` | List[Exception] | `[Exception]` | Exception types to retry on |
| `stop_on` | List[Exception] | `[]` | Exception types to never retry |
| `on_retry` | Callable | None | Callback function called on each retry |
| `timeout_total_s` | float | None | Total timeout for all attempts |
| `backoff_factor` | float | 2.0 | Exponential backoff multiplier |

## Backoff Algorithm

### Exponential Backoff Formula

The delay for attempt `n` is calculated as:

```
delay = min(base_delay_ms * (backoff_factor ^ (n-1)), max_delay_ms)
```

With jitter enabled (recommended), the actual delay becomes:

```
actual_delay = delay * random.uniform(0.5, 1.0)
```

### Example Delay Sequence

With `base_delay_ms=100`, `backoff_factor=2.0`, and `max_delay_ms=30000`:

| Attempt | Base Delay (ms) | With Jitter (ms) |
|---------|----------------|------------------|
| 1 | 100 | 50-100 |
| 2 | 200 | 100-200 |
| 3 | 400 | 200-400 |
| 4 | 800 | 400-800 |
| 5 | 1600 | 800-1600 |
| 6 | 3200 | 1600-3200 |
| 7 | 6400 | 3200-6400 |
| 8 | 12800 | 6400-12800 |
| 9 | 25600 | 12800-25600 |
| 10 | 30000 | 15000-30000 (capped) |

## Exception Handling

### Retryable vs Non-Retryable Exceptions

```python
from fleet_q.backoff import with_backoff
from snowflake.connector.errors import DatabaseError, ProgrammingError

@with_backoff(
    max_attempts=3,
    retry_on=[DatabaseError],  # Retry on database errors
    stop_on=[ProgrammingError]  # Don't retry on programming errors
)
async def database_operation():
    # This will retry on DatabaseError but not ProgrammingError
    pass
```

### Custom Exception Predicates

```python
def should_retry(exception: Exception) -> bool:
    if isinstance(exception, DatabaseError):
        # Retry on connection errors but not syntax errors
        return "connection" in str(exception).lower()
    return False

@with_backoff(
    max_attempts=5,
    retry_predicate=should_retry
)
async def selective_retry_operation():
    pass
```

## Logging and Observability

### Retry Callbacks

```python
import structlog

logger = structlog.get_logger()

def log_retry(attempt: int, delay_ms: int, exception: Exception):
    logger.warning(
        "Operation failed, retrying",
        attempt=attempt,
        delay_ms=delay_ms,
        exception_type=type(exception).__name__,
        exception_message=str(exception)
    )

@with_backoff(
    max_attempts=5,
    on_retry=log_retry
)
async def monitored_operation():
    pass
```

### Structured Logging Output

```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "level": "warning",
  "message": "Operation failed, retrying",
  "attempt": 2,
  "delay_ms": 200,
  "exception_type": "DatabaseError",
  "exception_message": "Connection timeout",
  "operation": "claim_steps",
  "pod_id": "pod-123"
}
```

## Advanced Usage Patterns

### Timeout-Based Backoff

```python
@with_backoff(
    max_attempts=float('inf'),  # No attempt limit
    timeout_total_s=30.0,       # But timeout after 30 seconds
    base_delay_ms=100
)
async def time_limited_operation():
    # Will retry for up to 30 seconds regardless of attempt count
    pass
```

### Custom Backoff Strategies

```python
from fleet_q.backoff import BackoffStrategy

class LinearBackoff(BackoffStrategy):
    def calculate_delay(self, attempt: int) -> float:
        return self.base_delay_ms * attempt

@with_backoff(
    strategy=LinearBackoff(base_delay_ms=500),
    max_attempts=5
)
async def linear_backoff_operation():
    pass
```

### Circuit Breaker Integration

```python
from fleet_q.backoff import with_backoff, CircuitBreakerError

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.is_open = False
    
    def record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.is_open = True
    
    def record_success(self):
        self.failure_count = 0
        self.is_open = False

circuit_breaker = CircuitBreaker()

@with_backoff(
    max_attempts=3,
    stop_on=[CircuitBreakerError]
)
async def protected_operation():
    if circuit_breaker.is_open:
        raise CircuitBreakerError("Circuit breaker is open")
    
    try:
        result = await risky_operation()
        circuit_breaker.record_success()
        return result
    except Exception as e:
        circuit_breaker.record_failure()
        raise
```

## Performance Considerations

### Memory Usage

The backoff utility is designed to be lightweight:
- No persistent state between calls
- Minimal memory allocation
- Efficient random number generation

### CPU Impact

- Jitter calculation: ~1μs per retry
- Delay calculation: ~0.5μs per retry
- Logging overhead: ~10-100μs per retry (if enabled)

### Network Impact

Exponential backoff with jitter helps reduce network congestion:
- Spreads retry attempts over time
- Prevents thundering herd effects
- Reduces load on downstream services

## Testing Strategies

### Unit Testing with Backoff

```python
import pytest
from unittest.mock import AsyncMock, patch
from fleet_q.backoff import with_backoff

@pytest.mark.asyncio
async def test_backoff_retries():
    mock_operation = AsyncMock()
    mock_operation.side_effect = [
        Exception("First failure"),
        Exception("Second failure"),
        "Success"
    ]
    
    @with_backoff(max_attempts=3, base_delay_ms=1)
    async def test_operation():
        return await mock_operation()
    
    result = await test_operation()
    assert result == "Success"
    assert mock_operation.call_count == 3
```

### Integration Testing

```python
@pytest.mark.asyncio
async def test_database_retry_integration():
    # Test actual database retry behavior
    with patch('fleet_q.snowflake_storage.snowflake') as mock_sf:
        mock_sf.connect.side_effect = [
            ConnectionError("Network error"),
            ConnectionError("Timeout"),
            MagicMock()  # Successful connection
        ]
        
        storage = SnowflakeStorage(config)
        await storage.connect()  # Should succeed after retries
```

### Load Testing

```python
import asyncio
import time

async def load_test_backoff():
    start_time = time.time()
    
    @with_backoff(max_attempts=5, base_delay_ms=10)
    async def failing_operation():
        raise Exception("Always fails")
    
    tasks = []
    for i in range(100):
        tasks.append(failing_operation())
    
    # All should fail after retries
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    duration = time.time() - start_time
    print(f"100 operations with retries took {duration:.2f}s")
```

## Common Patterns in FLEET-Q

### Database Transaction Retries

```python
@with_backoff(
    max_attempts=5,
    base_delay_ms=100,
    max_delay_ms=5000,
    retry_on=[snowflake.connector.errors.DatabaseError]
)
async def atomic_step_claim(pod_id: str, limit: int):
    async with snowflake_transaction() as tx:
        steps = await tx.execute("""
            SELECT step_id FROM STEP_TRACKER 
            WHERE status = 'pending' 
            ORDER BY priority DESC, created_ts ASC 
            LIMIT %s FOR UPDATE
        """, [limit])
        
        if steps:
            await tx.execute("""
                UPDATE STEP_TRACKER 
                SET status = 'claimed', claimed_by = %s 
                WHERE step_id IN (%s)
            """, [pod_id, ','.join(step.step_id for step in steps)])
        
        return steps
```

### Leader Election Retries

```python
@with_backoff(
    max_attempts=3,
    base_delay_ms=1000,
    on_retry=lambda attempt, delay, exc: logger.warning(
        "Leader election failed, retrying",
        attempt=attempt,
        delay_ms=delay,
        error=str(exc)
    )
)
async def determine_leader():
    current_time = datetime.utcnow()
    threshold = current_time - timedelta(seconds=dead_pod_threshold)
    
    result = await snowflake.execute("""
        SELECT pod_id, birth_ts 
        FROM POD_HEALTH 
        WHERE status = 'up' AND last_heartbeat_ts > %s 
        ORDER BY birth_ts ASC 
        LIMIT 1
    """, [threshold])
    
    return result[0].pod_id if result else None
```

### Recovery Operation Retries

```python
@with_backoff(
    max_attempts=3,
    base_delay_ms=2000,
    max_delay_ms=10000
)
async def recovery_cycle():
    cycle_id = f"recovery_{int(time.time())}"
    
    try:
        # Create local DLQ
        await sqlite_storage.create_dlq_table(cycle_id)
        
        # Find dead pods and orphaned steps
        dead_pods = await snowflake_storage.get_dead_pods()
        orphaned_steps = await snowflake_storage.get_orphaned_steps(dead_pods)
        
        # Process orphans
        for step in orphaned_steps:
            await sqlite_storage.insert_orphan_record(cycle_id, step)
        
        # Requeue eligible steps
        eligible_steps = [s for s in orphaned_steps if s.retry_count < s.max_retries]
        if eligible_steps:
            await snowflake_storage.requeue_steps(eligible_steps)
        
    finally:
        # Always cleanup
        await sqlite_storage.drop_dlq_table(cycle_id)
```

## Best Practices

### Configuration Guidelines

1. **Start Conservative**: Begin with longer delays and fewer attempts
2. **Add Jitter**: Always enable jitter to prevent thundering herds
3. **Set Reasonable Limits**: Cap maximum delays to prevent excessive waits
4. **Monitor Retry Rates**: Track retry frequency to identify systemic issues

### Error Handling

1. **Be Specific**: Only retry on truly transient errors
2. **Fail Fast**: Don't retry on permanent failures (authentication, syntax errors)
3. **Log Appropriately**: Log retries at WARNING level, final failures at ERROR
4. **Preserve Context**: Include relevant context in retry logs

### Performance Optimization

1. **Use Appropriate Delays**: Match delays to expected recovery times
2. **Limit Total Time**: Set timeout_total_s for time-sensitive operations
3. **Batch Operations**: Prefer batching over individual retries when possible
4. **Circuit Breakers**: Combine with circuit breakers for cascading failure protection

## Troubleshooting

### Common Issues

**Excessive Retry Delays**
```python
# Problem: Delays too long for interactive operations
@with_backoff(max_delay_ms=1000)  # Cap at 1 second
async def interactive_operation():
    pass
```

**Retry Storms**
```python
# Problem: All pods retrying simultaneously
@with_backoff(jitter=True)  # Enable jitter to spread retries
async def distributed_operation():
    pass
```

**Permanent Failures Being Retried**
```python
# Problem: Retrying non-transient errors
@with_backoff(
    retry_on=[ConnectionError, TimeoutError],  # Only retry these
    stop_on=[AuthenticationError, ValueError]  # Never retry these
)
async def selective_retry():
    pass
```

### Debugging Retry Behavior

```python
import logging

# Enable debug logging for backoff
logging.getLogger('fleet_q.backoff').setLevel(logging.DEBUG)

@with_backoff(
    max_attempts=3,
    on_retry=lambda attempt, delay, exc: print(
        f"Retry {attempt}: waiting {delay}ms after {type(exc).__name__}"
    )
)
async def debug_operation():
    pass
```

## Next Steps

- **[Storage Interfaces](storage.md)** - Learn about storage abstraction patterns
- **[Examples](../examples/basic-usage.md)** - See backoff utility in action
- **[API Reference](../api/endpoints.md)** - Understand how backoff is used in APIs