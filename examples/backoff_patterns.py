#!/usr/bin/env python3
"""
Backoff Utility Usage Patterns

This example demonstrates various patterns for using FLEET-Q's exponential
backoff utility in different scenarios including database operations,
API calls, and custom retry logic.
"""

import asyncio
import random
import time
from typing import Dict, Any, List
from fleet_q.backoff import with_backoff
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


# Custom exceptions for demonstration
class DatabaseConnectionError(Exception):
    """Simulated database connection error"""
    pass


class TransactionConflictError(Exception):
    """Simulated transaction conflict error"""
    pass


class APIRateLimitError(Exception):
    """Simulated API rate limit error"""
    pass


class APIServerError(Exception):
    """Simulated API server error"""
    pass


class APIClientError(Exception):
    """Simulated API client error (should not retry)"""
    pass


# Pattern 1: Basic Database Operations with Backoff
class DatabaseOperations:
    """Database operations with exponential backoff"""
    
    def __init__(self):
        self.connection_failure_rate = 0.3  # 30% chance of connection failure
        self.conflict_rate = 0.2  # 20% chance of transaction conflict
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=100,
        max_delay_ms=10000,
        retry_on=[DatabaseConnectionError, TransactionConflictError]
    )
    async def connect_to_database(self) -> str:
        """Connect to database with retry logic"""
        
        # Simulate connection failure
        if random.random() < self.connection_failure_rate:
            logger.warning("Database connection failed, will retry")
            raise DatabaseConnectionError("Failed to connect to database")
        
        logger.info("Successfully connected to database")
        return "connection_established"
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=200,
        max_delay_ms=5000,
        retry_on=[TransactionConflictError],
        on_retry=lambda attempt, delay, exc: logger.warning(
            "Transaction conflict, retrying",
            attempt=attempt,
            delay_ms=delay,
            error=str(exc)
        )
    )
    async def execute_transaction(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute database transaction with conflict handling"""
        
        # Simulate transaction conflict
        if random.random() < self.conflict_rate:
            raise TransactionConflictError("Transaction conflict detected")
        
        # Simulate processing time
        await asyncio.sleep(0.1)
        
        logger.info("Transaction executed successfully", data=data)
        return {"status": "committed", "transaction_id": f"tx_{int(time.time())}"}


# Pattern 2: API Client with Different Retry Strategies
class APIClient:
    """API client with sophisticated retry logic"""
    
    def __init__(self):
        self.rate_limit_rate = 0.4  # 40% chance of rate limiting
        self.server_error_rate = 0.2  # 20% chance of server error
        self.client_error_rate = 0.1  # 10% chance of client error
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=1000,  # Start with 1 second for rate limits
        max_delay_ms=60000,  # Max 1 minute for rate limits
        retry_on=[APIRateLimitError, APIServerError],
        stop_on=[APIClientError],  # Never retry client errors
        on_retry=lambda attempt, delay, exc: logger.warning(
            "API call failed, retrying",
            attempt=attempt,
            delay_ms=delay,
            error_type=type(exc).__name__,
            error_message=str(exc)
        )
    )
    async def make_api_call(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make API call with comprehensive retry logic"""
        
        # Simulate different types of failures
        rand = random.random()
        
        if rand < self.client_error_rate:
            # Client errors should not be retried
            raise APIClientError("Invalid request format")
        elif rand < self.client_error_rate + self.rate_limit_rate:
            # Rate limits should be retried with longer delays
            raise APIRateLimitError("Rate limit exceeded")
        elif rand < self.client_error_rate + self.rate_limit_rate + self.server_error_rate:
            # Server errors should be retried
            raise APIServerError("Internal server error")
        
        # Simulate successful API call
        await asyncio.sleep(0.2)
        
        logger.info("API call successful", endpoint=endpoint)
        return {
            "status": "success",
            "endpoint": endpoint,
            "response_time": 0.2,
            "data": data
        }
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=500,
        max_delay_ms=5000,
        timeout_total_s=30.0,  # Total timeout of 30 seconds
        retry_on=[APIServerError]
    )
    async def quick_api_call(self, endpoint: str) -> Dict[str, Any]:
        """Quick API call with total timeout"""
        
        if random.random() < 0.3:
            raise APIServerError("Server temporarily unavailable")
        
        await asyncio.sleep(0.1)
        return {"status": "success", "endpoint": endpoint}


# Pattern 3: Custom Backoff Strategy
class CustomBackoffOperations:
    """Operations with custom backoff strategies"""
    
    def __init__(self):
        self.failure_rate = 0.5
    
    @with_backoff(
        max_attempts=10,
        base_delay_ms=50,
        max_delay_ms=2000,
        backoff_factor=1.5,  # Slower exponential growth
        jitter=True,
        retry_on=[Exception]
    )
    async def gradual_backoff_operation(self) -> str:
        """Operation with gradual backoff increase"""
        
        if random.random() < self.failure_rate:
            raise Exception("Operation failed")
        
        return "success"
    
    @with_backoff(
        max_attempts=float('inf'),  # No attempt limit
        timeout_total_s=10.0,       # But timeout after 10 seconds
        base_delay_ms=100,
        retry_on=[Exception]
    )
    async def time_limited_operation(self) -> str:
        """Operation limited by total time rather than attempts"""
        
        if random.random() < 0.8:  # High failure rate
            raise Exception("Operation failed")
        
        return "success"


# Pattern 4: Conditional Retry Logic
class ConditionalRetryOperations:
    """Operations with conditional retry logic"""
    
    @staticmethod
    def should_retry_predicate(exception: Exception) -> bool:
        """Custom predicate to determine if exception should be retried"""
        if isinstance(exception, ValueError):
            # Only retry ValueError if message contains "temporary"
            return "temporary" in str(exception).lower()
        elif isinstance(exception, ConnectionError):
            # Always retry connection errors
            return True
        else:
            # Don't retry other exceptions
            return False
    
    @with_backoff(
        max_attempts=5,
        base_delay_ms=200,
        retry_predicate=should_retry_predicate,
        on_retry=lambda attempt, delay, exc: logger.info(
            "Conditional retry triggered",
            attempt=attempt,
            delay_ms=delay,
            exception_type=type(exc).__name__,
            should_retry=ConditionalRetryOperations.should_retry_predicate(exc)
        )
    )
    async def conditional_retry_operation(self, error_type: str) -> str:
        """Operation with conditional retry logic"""
        
        if error_type == "temporary_value_error":
            raise ValueError("Temporary processing error")
        elif error_type == "permanent_value_error":
            raise ValueError("Permanent configuration error")
        elif error_type == "connection_error":
            raise ConnectionError("Network connection failed")
        elif error_type == "other_error":
            raise RuntimeError("Unknown error occurred")
        
        return "success"


# Pattern 5: Circuit Breaker Integration
class CircuitBreakerOperations:
    """Operations with circuit breaker pattern"""
    
    def __init__(self):
        self.failure_count = 0
        self.failure_threshold = 3
        self.circuit_open = False
        self.last_failure_time = 0
        self.recovery_timeout = 5  # seconds
    
    def is_circuit_open(self) -> bool:
        """Check if circuit breaker is open"""
        if self.circuit_open:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.circuit_open = False
                self.failure_count = 0
                logger.info("Circuit breaker reset")
        
        return self.circuit_open
    
    def record_failure(self):
        """Record operation failure"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.circuit_open = True
            logger.warning("Circuit breaker opened", failure_count=self.failure_count)
    
    def record_success(self):
        """Record operation success"""
        self.failure_count = 0
        self.circuit_open = False
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=1000,
        retry_on=[Exception],
        stop_on=[RuntimeError]  # Circuit breaker open
    )
    async def protected_operation(self) -> str:
        """Operation protected by circuit breaker"""
        
        if self.is_circuit_open():
            raise RuntimeError("Circuit breaker is open")
        
        try:
            # Simulate operation that might fail
            if random.random() < 0.6:  # 60% failure rate
                raise Exception("Operation failed")
            
            # Success
            self.record_success()
            logger.info("Protected operation succeeded")
            return "success"
            
        except Exception as e:
            self.record_failure()
            raise


# Pattern 6: Batch Operations with Backoff
class BatchOperations:
    """Batch operations with backoff for individual items"""
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        retry_on=[Exception]
    )
    async def process_single_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Process single item with retry"""
        
        # Simulate processing failure for some items
        if item.get("should_fail", False):
            raise Exception(f"Failed to process item {item['id']}")
        
        await asyncio.sleep(0.1)  # Simulate processing time
        
        return {
            "id": item["id"],
            "status": "processed",
            "processed_at": time.time()
        }
    
    async def process_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process batch of items with individual retry logic"""
        
        logger.info("Starting batch processing", item_count=len(items))
        
        # Process items concurrently, each with its own retry logic
        tasks = [self.process_single_item(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Separate successful results from failures
        successful = []
        failed = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed.append({
                    "item": items[i],
                    "error": str(result)
                })
            else:
                successful.append(result)
        
        logger.info(
            "Batch processing completed",
            successful_count=len(successful),
            failed_count=len(failed)
        )
        
        return successful


# Demonstration functions
async def demonstrate_database_operations():
    """Demonstrate database operations with backoff"""
    print("\n🗄️  Database Operations with Backoff")
    print("-" * 50)
    
    db_ops = DatabaseOperations()
    
    try:
        # Test connection with retries
        connection = await db_ops.connect_to_database()
        print(f"✅ Database connection: {connection}")
        
        # Test transaction with conflict handling
        transaction_result = await db_ops.execute_transaction({
            "operation": "insert",
            "table": "users",
            "data": {"name": "Alice", "email": "alice@example.com"}
        })
        print(f"✅ Transaction result: {transaction_result}")
        
    except Exception as e:
        print(f"❌ Database operation failed: {e}")


async def demonstrate_api_operations():
    """Demonstrate API operations with different retry strategies"""
    print("\n🌐 API Operations with Backoff")
    print("-" * 50)
    
    api_client = APIClient()
    
    try:
        # Test API call with comprehensive retry
        api_result = await api_client.make_api_call(
            "/api/users",
            {"name": "Bob", "email": "bob@example.com"}
        )
        print(f"✅ API call result: {api_result}")
        
        # Test quick API call with timeout
        quick_result = await api_client.quick_api_call("/api/health")
        print(f"✅ Quick API result: {quick_result}")
        
    except Exception as e:
        print(f"❌ API operation failed: {e}")


async def demonstrate_custom_backoff():
    """Demonstrate custom backoff strategies"""
    print("\n⚙️  Custom Backoff Strategies")
    print("-" * 50)
    
    custom_ops = CustomBackoffOperations()
    
    try:
        # Test gradual backoff
        gradual_result = await custom_ops.gradual_backoff_operation()
        print(f"✅ Gradual backoff result: {gradual_result}")
        
        # Test time-limited operation
        time_limited_result = await custom_ops.time_limited_operation()
        print(f"✅ Time-limited result: {time_limited_result}")
        
    except Exception as e:
        print(f"❌ Custom backoff operation failed: {e}")


async def demonstrate_conditional_retry():
    """Demonstrate conditional retry logic"""
    print("\n🔀 Conditional Retry Logic")
    print("-" * 50)
    
    conditional_ops = ConditionalRetryOperations()
    
    test_cases = [
        ("temporary_value_error", "Should retry"),
        ("permanent_value_error", "Should not retry"),
        ("connection_error", "Should retry"),
        ("other_error", "Should not retry"),
        ("success", "Should succeed")
    ]
    
    for error_type, description in test_cases:
        try:
            if error_type == "success":
                result = await conditional_ops.conditional_retry_operation("")
                print(f"✅ {description}: {result}")
            else:
                result = await conditional_ops.conditional_retry_operation(error_type)
                print(f"✅ {description}: {result}")
        except Exception as e:
            print(f"❌ {description}: {type(e).__name__}: {e}")


async def demonstrate_circuit_breaker():
    """Demonstrate circuit breaker integration"""
    print("\n🔌 Circuit Breaker Integration")
    print("-" * 50)
    
    circuit_ops = CircuitBreakerOperations()
    
    # Try multiple operations to trigger circuit breaker
    for i in range(8):
        try:
            result = await circuit_ops.protected_operation()
            print(f"✅ Operation {i+1}: {result}")
        except Exception as e:
            print(f"❌ Operation {i+1}: {type(e).__name__}: {e}")
        
        # Small delay between operations
        await asyncio.sleep(0.5)


async def demonstrate_batch_operations():
    """Demonstrate batch operations with individual retry"""
    print("\n📦 Batch Operations with Backoff")
    print("-" * 50)
    
    batch_ops = BatchOperations()
    
    # Create test items (some will fail)
    items = [
        {"id": 1, "data": "item1"},
        {"id": 2, "data": "item2", "should_fail": True},
        {"id": 3, "data": "item3"},
        {"id": 4, "data": "item4", "should_fail": True},
        {"id": 5, "data": "item5"},
    ]
    
    try:
        results = await batch_ops.process_batch(items)
        print(f"✅ Batch processing completed: {len(results)} items processed")
        for result in results:
            print(f"   - Item {result['id']}: {result['status']}")
    except Exception as e:
        print(f"❌ Batch processing failed: {e}")


async def main():
    """Main demonstration function"""
    print("🔄 FLEET-Q Backoff Utility Usage Patterns")
    print("=" * 60)
    
    # Run all demonstrations
    await demonstrate_database_operations()
    await demonstrate_api_operations()
    await demonstrate_custom_backoff()
    await demonstrate_conditional_retry()
    await demonstrate_circuit_breaker()
    await demonstrate_batch_operations()
    
    print("\n✅ All backoff pattern demonstrations completed!")
    print("\nKey Takeaways:")
    print("  • Use appropriate retry_on exceptions for each scenario")
    print("  • Set reasonable max_attempts and delays")
    print("  • Enable jitter to prevent thundering herd")
    print("  • Use timeout_total_s for time-sensitive operations")
    print("  • Implement circuit breakers for cascading failure protection")
    print("  • Log retry attempts for observability")


if __name__ == "__main__":
    asyncio.run(main())