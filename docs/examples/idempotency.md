# Idempotency Patterns

Idempotency is crucial in distributed systems like FLEET-Q where tasks may be retried due to pod failures or network issues. This guide covers patterns and techniques for implementing idempotent operations.

## Understanding Idempotency

An idempotent operation produces the same result when executed multiple times with the same input. In FLEET-Q, this means:

- **Safe Retries**: Steps can be safely retried without causing duplicate side effects
- **Failure Recovery**: When pods fail, orphaned steps can be requeued without data corruption
- **Consistency**: System state remains consistent even under failure scenarios

## Idempotency Key Generation

### Basic Key Generation

```python
import hashlib
import json
from fleet_q.models import StepPayload

def generate_idempotency_key(payload: StepPayload) -> str:
    """Generate deterministic idempotency key from step payload"""
    
    # Create deterministic content hash
    content = {
        "step_id": payload.step_id,
        "type": payload.type,
        "args": payload.args,
        # Note: Don't include metadata as it may contain non-deterministic data
    }
    
    # Sort keys for consistency
    content_str = json.dumps(content, sort_keys=True)
    
    # Generate hash
    hash_obj = hashlib.sha256(content_str.encode('utf-8'))
    return hash_obj.hexdigest()[:16]  # Use first 16 characters

# Example usage
payload = StepPayload(
    step_id="step_123",
    type="data_processing",
    args={"file": "data.csv", "format": "json"},
    metadata={"user": "alice"}
)

key = generate_idempotency_key(payload)
print(f"Idempotency key: {key}")  # Always the same for same payload
```

### Advanced Key Generation

```python
from typing import List, Any
import hashlib
import json

class IdempotencyKeyGenerator:
    """Advanced idempotency key generation with customizable fields"""
    
    def __init__(self, include_fields: List[str] = None, exclude_fields: List[str] = None):
        self.include_fields = include_fields or ["step_id", "type", "args"]
        self.exclude_fields = exclude_fields or []
    
    def generate_key(self, payload: StepPayload, custom_data: dict = None) -> str:
        """Generate idempotency key with custom logic"""
        
        # Start with base payload data
        content = {}
        
        if "step_id" in self.include_fields:
            content["step_id"] = payload.step_id
        
        if "type" in self.include_fields:
            content["type"] = payload.type
        
        if "args" in self.include_fields:
            # Filter args if needed
            filtered_args = {
                k: v for k, v in payload.args.items()
                if k not in self.exclude_fields
            }
            content["args"] = filtered_args
        
        # Add custom data
        if custom_data:
            content["custom"] = custom_data
        
        # Generate deterministic hash
        content_str = json.dumps(content, sort_keys=True, default=str)
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]
    
    def generate_business_key(self, business_id: str, operation: str) -> str:
        """Generate key based on business logic rather than technical payload"""
        content = {
            "business_id": business_id,
            "operation": operation
        }
        content_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]

# Usage examples
key_gen = IdempotencyKeyGenerator(
    include_fields=["type", "args"],
    exclude_fields=["timestamp", "request_id"]  # Exclude non-deterministic fields
)

# Technical idempotency
tech_key = key_gen.generate_key(payload)

# Business idempotency
business_key = key_gen.generate_business_key("customer_123", "send_welcome_email")
```

## File-Based Idempotency

### Simple File Tracking

```python
import json
import aiofiles
from pathlib import Path
from fleet_q.models import StepPayload

class FileBasedIdempotency:
    """File-based idempotency tracking"""
    
    def __init__(self, cache_dir: str = "/tmp/fleet_q_idempotency"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def _get_cache_file(self, idempotency_key: str) -> Path:
        """Get cache file path for idempotency key"""
        return self.cache_dir / f"{idempotency_key}.json"
    
    async def is_already_processed(self, idempotency_key: str) -> bool:
        """Check if operation was already processed"""
        cache_file = self._get_cache_file(idempotency_key)
        return cache_file.exists()
    
    async def get_cached_result(self, idempotency_key: str) -> dict:
        """Get cached result for processed operation"""
        cache_file = self._get_cache_file(idempotency_key)
        
        if not cache_file.exists():
            return None
        
        async with aiofiles.open(cache_file, 'r') as f:
            content = await f.read()
            return json.loads(content)
    
    async def cache_result(self, idempotency_key: str, result: dict):
        """Cache operation result"""
        cache_file = self._get_cache_file(idempotency_key)
        
        cache_data = {
            "result": result,
            "cached_at": "2024-01-01T12:00:00Z",
            "idempotency_key": idempotency_key
        }
        
        async with aiofiles.open(cache_file, 'w') as f:
            await f.write(json.dumps(cache_data, indent=2))
    
    async def clear_cache(self, idempotency_key: str):
        """Clear cached result"""
        cache_file = self._get_cache_file(idempotency_key)
        if cache_file.exists():
            cache_file.unlink()

# Usage in step processor
class IdempotentFileProcessor:
    def __init__(self):
        self.idempotency = FileBasedIdempotency()
    
    async def execute(self, payload: StepPayload) -> dict:
        # Generate idempotency key
        key = generate_idempotency_key(payload)
        
        # Check if already processed
        if await self.idempotency.is_already_processed(key):
            cached_result = await self.idempotency.get_cached_result(key)
            logger.info(
                "Returning cached result",
                step_id=payload.step_id,
                idempotency_key=key
            )
            return cached_result["result"]
        
        # Process step
        result = await self._do_work(payload)
        
        # Cache result for future idempotency
        await self.idempotency.cache_result(key, result)
        
        return result
    
    async def _do_work(self, payload: StepPayload) -> dict:
        # Actual work implementation
        await asyncio.sleep(2)  # Simulate work
        return {"status": "completed", "processed_at": "2024-01-01T12:00:00Z"}
```

### Database-Based Idempotency

```python
import asyncpg
from datetime import datetime, timedelta
from fleet_q.models import StepPayload

class DatabaseIdempotency:
    """Database-based idempotency tracking with TTL"""
    
    def __init__(self, database_url: str, ttl_hours: int = 24):
        self.database_url = database_url
        self.ttl_hours = ttl_hours
        self.pool = None
    
    async def initialize(self):
        """Initialize database connection and schema"""
        self.pool = await asyncpg.create_pool(self.database_url)
        
        # Create idempotency table
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_cache (
                    idempotency_key VARCHAR(64) PRIMARY KEY,
                    result JSONB NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMP NOT NULL
                )
            """)
            
            # Create index for cleanup
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_idempotency_expires 
                ON idempotency_cache(expires_at)
            """)
    
    async def is_already_processed(self, idempotency_key: str) -> bool:
        """Check if operation was already processed"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT 1 FROM idempotency_cache 
                WHERE idempotency_key = $1 AND expires_at > NOW()
            """, idempotency_key)
            
            return result is not None
    
    async def get_cached_result(self, idempotency_key: str) -> dict:
        """Get cached result for processed operation"""
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT result FROM idempotency_cache 
                WHERE idempotency_key = $1 AND expires_at > NOW()
            """, idempotency_key)
            
            return result
    
    async def cache_result(self, idempotency_key: str, result: dict):
        """Cache operation result with TTL"""
        expires_at = datetime.utcnow() + timedelta(hours=self.ttl_hours)
        
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO idempotency_cache 
                (idempotency_key, result, expires_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (idempotency_key) 
                DO UPDATE SET result = $2, expires_at = $3
            """, idempotency_key, json.dumps(result), expires_at)
    
    async def cleanup_expired(self):
        """Clean up expired idempotency records"""
        async with self.pool.acquire() as conn:
            deleted_count = await conn.fetchval("""
                DELETE FROM idempotency_cache 
                WHERE expires_at <= NOW()
                RETURNING COUNT(*)
            """)
            
            logger.info(f"Cleaned up {deleted_count} expired idempotency records")
    
    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()

# Usage with automatic cleanup
class DatabaseIdempotentProcessor:
    def __init__(self, database_url: str):
        self.idempotency = DatabaseIdempotency(database_url)
    
    async def initialize(self):
        await self.idempotency.initialize()
    
    async def execute(self, payload: StepPayload) -> dict:
        key = generate_idempotency_key(payload)
        
        # Check cache
        if await self.idempotency.is_already_processed(key):
            result = await self.idempotency.get_cached_result(key)
            logger.info(
                "Returning cached result from database",
                step_id=payload.step_id,
                idempotency_key=key
            )
            return result
        
        # Process and cache
        result = await self._process_step(payload)
        await self.idempotency.cache_result(key, result)
        
        return result
    
    async def _process_step(self, payload: StepPayload) -> dict:
        # Implementation here
        pass
```

## External System Idempotency

### API Idempotency Headers

```python
import httpx
from fleet_q.models import StepPayload

class IdempotentAPIClient:
    """HTTP client with idempotency header support"""
    
    def __init__(self, base_url: str, api_key: str):
        self.client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"}
        )
    
    async def idempotent_post(
        self, 
        endpoint: str, 
        data: dict, 
        idempotency_key: str
    ) -> dict:
        """Make idempotent POST request"""
        
        headers = {
            "Idempotency-Key": idempotency_key,
            "Content-Type": "application/json"
        }
        
        response = await self.client.post(
            endpoint, 
            json=data, 
            headers=headers
        )
        
        response.raise_for_status()
        return response.json()
    
    async def idempotent_put(
        self, 
        endpoint: str, 
        data: dict, 
        idempotency_key: str
    ) -> dict:
        """Make idempotent PUT request"""
        
        headers = {
            "Idempotency-Key": idempotency_key,
            "Content-Type": "application/json"
        }
        
        response = await self.client.put(
            endpoint, 
            json=data, 
            headers=headers
        )
        
        response.raise_for_status()
        return response.json()

# Usage in step processor
class APIIntegrationStep:
    def __init__(self, api_base_url: str, api_key: str):
        self.api_client = IdempotentAPIClient(api_base_url, api_key)
    
    async def execute(self, payload: StepPayload) -> dict:
        # Generate idempotency key
        key = generate_idempotency_key(payload)
        
        # Make idempotent API call
        api_data = payload.args["api_data"]
        endpoint = payload.args["endpoint"]
        
        try:
            api_response = await self.api_client.idempotent_post(
                endpoint, api_data, key
            )
            
            return {
                "status": "completed",
                "api_response": api_response,
                "idempotency_key": key
            }
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                # Conflict - operation already processed
                logger.info(
                    "API operation already processed",
                    step_id=payload.step_id,
                    idempotency_key=key
                )
                return {
                    "status": "completed",
                    "message": "Operation already processed",
                    "idempotency_key": key
                }
            else:
                raise
```

### Database Upsert Patterns

```python
import asyncpg
from fleet_q.models import StepPayload

class DatabaseUpsertStep:
    """Database operations with built-in idempotency via upserts"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None
    
    async def initialize(self):
        self.pool = await asyncpg.create_pool(self.database_url)
    
    async def execute(self, payload: StepPayload) -> dict:
        operation = payload.args["operation"]
        
        if operation == "create_user":
            return await self._create_user_idempotent(payload)
        elif operation == "update_balance":
            return await self._update_balance_idempotent(payload)
        else:
            raise ValueError(f"Unknown operation: {operation}")
    
    async def _create_user_idempotent(self, payload: StepPayload) -> dict:
        """Create user with idempotent behavior"""
        
        user_data = payload.args["user_data"]
        email = user_data["email"]
        
        async with self.pool.acquire() as conn:
            # Use ON CONFLICT for idempotency
            result = await conn.fetchrow("""
                INSERT INTO users (email, name, created_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (email) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    updated_at = NOW()
                RETURNING id, email, created_at, updated_at
            """, email, user_data["name"])
            
            return {
                "status": "completed",
                "user_id": result["id"],
                "email": result["email"],
                "was_created": result["created_at"] == result.get("updated_at"),
                "operation": "create_user"
            }
    
    async def _update_balance_idempotent(self, payload: StepPayload) -> dict:
        """Update balance with idempotent behavior using transaction log"""
        
        user_id = payload.args["user_id"]
        amount = payload.args["amount"]
        transaction_id = payload.args["transaction_id"]  # Business idempotency key
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Check if transaction already processed
                existing = await conn.fetchrow("""
                    SELECT id, amount FROM balance_transactions 
                    WHERE transaction_id = $1
                """, transaction_id)
                
                if existing:
                    # Transaction already processed
                    return {
                        "status": "completed",
                        "message": "Transaction already processed",
                        "transaction_id": transaction_id,
                        "amount": existing["amount"],
                        "operation": "update_balance"
                    }
                
                # Record transaction
                await conn.execute("""
                    INSERT INTO balance_transactions 
                    (transaction_id, user_id, amount, created_at)
                    VALUES ($1, $2, $3, NOW())
                """, transaction_id, user_id, amount)
                
                # Update balance
                new_balance = await conn.fetchval("""
                    UPDATE user_balances 
                    SET balance = balance + $1, updated_at = NOW()
                    WHERE user_id = $2
                    RETURNING balance
                """, amount, user_id)
                
                return {
                    "status": "completed",
                    "transaction_id": transaction_id,
                    "amount": amount,
                    "new_balance": new_balance,
                    "operation": "update_balance"
                }
```

## Business Logic Idempotency

### Email Notification Idempotency

```python
import hashlib
from fleet_q.models import StepPayload

class IdempotentEmailService:
    """Email service with built-in idempotency"""
    
    def __init__(self, email_provider, cache_backend):
        self.email_provider = email_provider
        self.cache = cache_backend
    
    def _generate_email_key(self, recipient: str, subject: str, content_hash: str) -> str:
        """Generate idempotency key for email"""
        content = f"{recipient}:{subject}:{content_hash}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    async def send_email_idempotent(
        self, 
        recipient: str, 
        subject: str, 
        body: str,
        template_data: dict = None
    ) -> dict:
        """Send email with idempotency protection"""
        
        # Generate content hash
        content_to_hash = f"{body}:{json.dumps(template_data or {}, sort_keys=True)}"
        content_hash = hashlib.sha256(content_to_hash.encode()).hexdigest()[:8]
        
        # Generate idempotency key
        email_key = self._generate_email_key(recipient, subject, content_hash)
        
        # Check if already sent
        cached_result = await self.cache.get_cached_result(email_key)
        if cached_result:
            logger.info(
                "Email already sent, returning cached result",
                recipient=recipient,
                subject=subject,
                email_key=email_key
            )
            return cached_result
        
        # Send email
        try:
            message_id = await self.email_provider.send(
                to=recipient,
                subject=subject,
                body=body,
                template_data=template_data
            )
            
            result = {
                "status": "sent",
                "recipient": recipient,
                "subject": subject,
                "message_id": message_id,
                "sent_at": "2024-01-01T12:00:00Z"
            }
            
            # Cache result
            await self.cache.cache_result(email_key, result)
            
            logger.info(
                "Email sent successfully",
                recipient=recipient,
                subject=subject,
                message_id=message_id,
                email_key=email_key
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "Failed to send email",
                recipient=recipient,
                subject=subject,
                error=str(e),
                email_key=email_key
            )
            raise

# Usage in step processor
class EmailNotificationStep:
    def __init__(self):
        self.email_service = IdempotentEmailService(
            email_provider=EmailProvider(),
            cache_backend=FileBasedIdempotency()
        )
    
    async def execute(self, payload: StepPayload) -> dict:
        email_data = payload.args
        
        result = await self.email_service.send_email_idempotent(
            recipient=email_data["recipient"],
            subject=email_data["subject"],
            body=email_data["body"],
            template_data=email_data.get("template_data")
        )
        
        return {
            "status": "completed",
            "email_result": result,
            "step_id": payload.step_id
        }
```

### Payment Processing Idempotency

```python
from decimal import Decimal
from fleet_q.models import StepPayload

class IdempotentPaymentProcessor:
    """Payment processor with comprehensive idempotency"""
    
    def __init__(self, payment_gateway, database_pool):
        self.gateway = payment_gateway
        self.db = database_pool
    
    async def process_payment_idempotent(
        self, 
        payment_id: str,
        amount: Decimal,
        currency: str,
        customer_id: str,
        payment_method: str
    ) -> dict:
        """Process payment with full idempotency protection"""
        
        async with self.db.acquire() as conn:
            async with conn.transaction():
                # Check if payment already processed
                existing_payment = await conn.fetchrow("""
                    SELECT status, gateway_transaction_id, processed_at
                    FROM payments 
                    WHERE payment_id = $1
                """, payment_id)
                
                if existing_payment:
                    if existing_payment["status"] == "completed":
                        return {
                            "status": "completed",
                            "payment_id": payment_id,
                            "gateway_transaction_id": existing_payment["gateway_transaction_id"],
                            "message": "Payment already processed",
                            "processed_at": existing_payment["processed_at"].isoformat()
                        }
                    elif existing_payment["status"] == "failed":
                        # Allow retry of failed payments
                        pass
                    elif existing_payment["status"] == "processing":
                        # Payment in progress - check gateway status
                        gateway_status = await self.gateway.check_status(
                            existing_payment["gateway_transaction_id"]
                        )
                        
                        if gateway_status["status"] == "completed":
                            # Update local status
                            await conn.execute("""
                                UPDATE payments 
                                SET status = 'completed', processed_at = NOW()
                                WHERE payment_id = $1
                            """, payment_id)
                            
                            return {
                                "status": "completed",
                                "payment_id": payment_id,
                                "gateway_transaction_id": existing_payment["gateway_transaction_id"],
                                "message": "Payment completed during retry check"
                            }
                
                # Create or update payment record
                await conn.execute("""
                    INSERT INTO payments 
                    (payment_id, amount, currency, customer_id, status, created_at)
                    VALUES ($1, $2, $3, $4, 'processing', NOW())
                    ON CONFLICT (payment_id) 
                    DO UPDATE SET status = 'processing', updated_at = NOW()
                """, payment_id, amount, currency, customer_id)
                
                try:
                    # Process payment with gateway
                    gateway_result = await self.gateway.charge(
                        amount=amount,
                        currency=currency,
                        customer_id=customer_id,
                        payment_method=payment_method,
                        idempotency_key=payment_id  # Use payment_id as idempotency key
                    )
                    
                    # Update payment status
                    await conn.execute("""
                        UPDATE payments 
                        SET status = 'completed', 
                            gateway_transaction_id = $1,
                            processed_at = NOW()
                        WHERE payment_id = $2
                    """, gateway_result["transaction_id"], payment_id)
                    
                    return {
                        "status": "completed",
                        "payment_id": payment_id,
                        "gateway_transaction_id": gateway_result["transaction_id"],
                        "amount": amount,
                        "currency": currency
                    }
                    
                except Exception as e:
                    # Mark payment as failed
                    await conn.execute("""
                        UPDATE payments 
                        SET status = 'failed', 
                            error_message = $1,
                            failed_at = NOW()
                        WHERE payment_id = $2
                    """, str(e), payment_id)
                    
                    raise

# Usage in step processor
class PaymentProcessingStep:
    def __init__(self, payment_processor):
        self.processor = payment_processor
    
    async def execute(self, payload: StepPayload) -> dict:
        payment_data = payload.args
        
        result = await self.processor.process_payment_idempotent(
            payment_id=payment_data["payment_id"],
            amount=Decimal(payment_data["amount"]),
            currency=payment_data["currency"],
            customer_id=payment_data["customer_id"],
            payment_method=payment_data["payment_method"]
        )
        
        return {
            "status": "completed",
            "payment_result": result,
            "step_id": payload.step_id
        }
```

## Testing Idempotency

### Unit Testing

```python
import pytest
from unittest.mock import AsyncMock, patch
from fleet_q.models import StepPayload

@pytest.mark.asyncio
async def test_idempotent_execution():
    """Test that step execution is idempotent"""
    
    payload = StepPayload(
        step_id="test_123",
        type="test_step",
        args={"data": "test"},
        metadata={}
    )
    
    processor = IdempotentFileProcessor()
    
    # First execution
    result1 = await processor.execute(payload)
    
    # Second execution should return same result
    result2 = await processor.execute(payload)
    
    assert result1 == result2
    
    # Verify work was only done once
    with patch.object(processor, '_do_work', new_callable=AsyncMock) as mock_work:
        mock_work.return_value = {"status": "completed"}
        
        # Third execution should use cache
        result3 = await processor.execute(payload)
        
        # Work should not be called again
        mock_work.assert_not_called()

@pytest.mark.asyncio
async def test_idempotency_key_consistency():
    """Test that idempotency keys are consistent"""
    
    payload1 = StepPayload(
        step_id="test_123",
        type="test_step",
        args={"data": "test"},
        metadata={"user": "alice"}
    )
    
    payload2 = StepPayload(
        step_id="test_123",
        type="test_step",
        args={"data": "test"},
        metadata={"user": "bob"}  # Different metadata
    )
    
    key1 = generate_idempotency_key(payload1)
    key2 = generate_idempotency_key(payload2)
    
    # Keys should be the same (metadata not included)
    assert key1 == key2

@pytest.mark.asyncio
async def test_failure_does_not_cache():
    """Test that failures are not cached"""
    
    payload = StepPayload(
        step_id="failure_test",
        type="test_step",
        args={"should_fail": True},
        metadata={}
    )
    
    processor = IdempotentFileProcessor()
    
    # Mock work to fail first time, succeed second time
    with patch.object(processor, '_do_work', new_callable=AsyncMock) as mock_work:
        mock_work.side_effect = [
            Exception("First attempt fails"),
            {"status": "completed"}
        ]
        
        # First execution should fail
        with pytest.raises(Exception):
            await processor.execute(payload)
        
        # Second execution should succeed (not cached)
        result = await processor.execute(payload)
        assert result["status"] == "completed"
        
        # Work should be called twice
        assert mock_work.call_count == 2
```

### Integration Testing

```python
@pytest.mark.asyncio
async def test_end_to_end_idempotency():
    """Test idempotency in full FLEET-Q integration"""
    
    import httpx
    
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit the same step twice
    step_data = {
        "type": "idempotent_test",
        "args": {"unique_id": "test_12345", "data": "test_data"},
        "metadata": {"test": "idempotency"}
    }
    
    # First submission
    response1 = await client.post("/submit", json=step_data)
    step_id1 = response1.json()["step_id"]
    
    # Second submission (should be idempotent)
    response2 = await client.post("/submit", json=step_data)
    step_id2 = response2.json()["step_id"]
    
    # Wait for both to complete
    for step_id in [step_id1, step_id2]:
        while True:
            status_response = await client.get(f"/status/{step_id}")
            status = status_response.json()
            
            if status["status"] in ["completed", "failed"]:
                break
            
            await asyncio.sleep(0.5)
    
    # Both should complete successfully
    status1 = await client.get(f"/status/{step_id1}")
    status2 = await client.get(f"/status/{step_id2}")
    
    assert status1.json()["status"] == "completed"
    assert status2.json()["status"] == "completed"
    
    await client.aclose()
```

## Best Practices

### Design Guidelines

1. **Use Business Keys When Possible**
   - Prefer business identifiers over technical ones
   - Include operation type in the key
   - Consider time boundaries for time-sensitive operations

2. **Handle Partial Failures**
   - Don't cache partial results
   - Use checkpointing for long operations
   - Implement proper rollback mechanisms

3. **Cache Management**
   - Set appropriate TTL for cached results
   - Clean up expired cache entries
   - Monitor cache hit rates

### Implementation Tips

1. **Key Generation**
   - Use deterministic algorithms
   - Exclude non-deterministic fields
   - Include version information for schema changes

2. **Storage Considerations**
   - Choose appropriate storage backend
   - Consider performance implications
   - Plan for cache invalidation

3. **Error Handling**
   - Don't cache error results
   - Distinguish between retryable and permanent failures
   - Provide clear error messages

### Monitoring and Observability

1. **Metrics to Track**
   - Cache hit/miss rates
   - Idempotency key collisions
   - Operation retry rates

2. **Logging**
   - Log idempotency key usage
   - Track cache operations
   - Monitor for unexpected patterns

3. **Alerting**
   - High cache miss rates
   - Frequent key collisions
   - Unusual retry patterns

Implementing proper idempotency patterns ensures that FLEET-Q can safely retry operations and recover from failures without causing data corruption or duplicate side effects.

## Next Steps

- **[Environment Configurations](environments.md)** - Configure idempotency for different environments
- **[Basic Usage](basic-usage.md)** - See idempotency in action
- **[Step Implementation](step-implementation.md)** - Implement idempotent step processors