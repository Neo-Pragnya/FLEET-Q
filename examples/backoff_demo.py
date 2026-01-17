#!/usr/bin/env python3
"""
Demonstration of the FLEET-Q exponential backoff utility.

This script shows various usage patterns of the backoff decorator
including sync/async functions, configuration integration, and logging.
"""

import asyncio
import time
from fleet_q.backoff import (
    with_backoff, 
    with_database_backoff, 
    with_network_backoff,
    create_retry_callback
)
from fleet_q.config import FleetQConfig
from fleet_q.logging_config import configure_logging


def demo_basic_backoff():
    """Demonstrate basic backoff functionality."""
    print("=== Basic Backoff Demo ===")
    
    attempt_count = 0
    
    @with_backoff(max_attempts=3, base_delay_ms=100, jitter=False)
    def unreliable_operation():
        nonlocal attempt_count
        attempt_count += 1
        print(f"Attempt {attempt_count}")
        
        if attempt_count < 3:
            raise ConnectionError(f"Simulated failure on attempt {attempt_count}")
        
        return "Success!"
    
    try:
        result = unreliable_operation()
        print(f"Result: {result}")
        print(f"Total attempts: {attempt_count}\n")
    except Exception as e:
        print(f"Failed after {attempt_count} attempts: {e}\n")


async def demo_async_backoff():
    """Demonstrate async backoff functionality."""
    print("=== Async Backoff Demo ===")
    
    attempt_count = 0
    
    @with_backoff(max_attempts=3, base_delay_ms=50, jitter=False)
    async def unreliable_async_operation():
        nonlocal attempt_count
        attempt_count += 1
        print(f"Async attempt {attempt_count}")
        
        if attempt_count < 3:
            raise TimeoutError(f"Simulated async failure on attempt {attempt_count}")
        
        return "Async Success!"
    
    try:
        result = await unreliable_async_operation()
        print(f"Result: {result}")
        print(f"Total attempts: {attempt_count}\n")
    except Exception as e:
        print(f"Failed after {attempt_count} attempts: {e}\n")


def demo_config_integration():
    """Demonstrate backoff with configuration integration."""
    print("=== Configuration Integration Demo ===")
    
    # Create a config with custom backoff settings
    config = FleetQConfig(
        snowflake_account="demo",
        snowflake_user="demo",
        snowflake_password="demo",
        snowflake_database="demo",
        default_max_attempts=4,
        default_base_delay_ms=25,
        default_max_delay_ms=1000
    )
    
    attempt_count = 0
    
    @with_backoff(config=config, jitter=False)  # Uses config defaults
    def operation_with_config():
        nonlocal attempt_count
        attempt_count += 1
        print(f"Config-based attempt {attempt_count}")
        
        if attempt_count < 4:
            raise ValueError(f"Config failure on attempt {attempt_count}")
        
        return "Config Success!"
    
    try:
        result = operation_with_config()
        print(f"Result: {result}")
        print(f"Total attempts: {attempt_count}\n")
    except Exception as e:
        print(f"Failed after {attempt_count} attempts: {e}\n")


def demo_convenience_decorators():
    """Demonstrate convenience decorators."""
    print("=== Convenience Decorators Demo ===")
    
    # Database backoff demo
    db_attempts = 0
    
    @with_database_backoff()
    def database_operation():
        nonlocal db_attempts
        db_attempts += 1
        print(f"Database attempt {db_attempts}")
        
        if db_attempts < 2:
            raise ConnectionError("Database connection failed")
        
        return "Database connected!"
    
    try:
        result = database_operation()
        print(f"DB Result: {result}")
    except Exception as e:
        print(f"DB Failed: {e}")
    
    # Network backoff demo
    net_attempts = 0
    
    @with_network_backoff()
    def network_operation():
        nonlocal net_attempts
        net_attempts += 1
        print(f"Network attempt {net_attempts}")
        
        if net_attempts < 2:
            raise OSError("Network timeout")
        
        return "Network request successful!"
    
    try:
        result = network_operation()
        print(f"Network Result: {result}")
        print()
    except Exception as e:
        print(f"Network Failed: {e}\n")


def demo_logging_integration():
    """Demonstrate backoff with logging integration."""
    print("=== Logging Integration Demo ===")
    
    # Set up configuration and logging
    config = FleetQConfig(
        snowflake_account="demo",
        snowflake_user="demo",
        snowflake_password="demo",
        snowflake_database="demo",
        log_level="INFO",
        log_format="console"  # Use console format for demo
    )
    
    logger = configure_logging(config)
    retry_callback = create_retry_callback(logger, "demo_operation")
    
    attempt_count = 0
    
    @with_backoff(
        max_attempts=3, 
        base_delay_ms=100, 
        jitter=False,
        on_retry=retry_callback
    )
    def logged_operation():
        nonlocal attempt_count
        attempt_count += 1
        print(f"Logged attempt {attempt_count}")
        
        if attempt_count < 3:
            raise RuntimeError(f"Logged failure on attempt {attempt_count}")
        
        return "Logged Success!"
    
    try:
        result = logged_operation()
        print(f"Result: {result}")
        print(f"Total attempts: {attempt_count}\n")
    except Exception as e:
        print(f"Failed after {attempt_count} attempts: {e}\n")


def demo_error_handling():
    """Demonstrate different error handling scenarios."""
    print("=== Error Handling Demo ===")
    
    # Selective retry demo
    attempt_count = 0
    
    @with_backoff(
        max_attempts=3, 
        base_delay_ms=50,
        retry_on=[ConnectionError, TimeoutError],  # Only retry these errors
        jitter=False
    )
    def selective_retry_operation():
        nonlocal attempt_count
        attempt_count += 1
        print(f"Selective retry attempt {attempt_count}")
        
        if attempt_count == 1:
            raise ConnectionError("Retryable connection error")
        elif attempt_count == 2:
            raise ValueError("Non-retryable value error")  # Won't be retried
        
        return "Should not reach here"
    
    try:
        result = selective_retry_operation()
        print(f"Result: {result}")
    except ValueError as e:
        print(f"Caught non-retryable error after {attempt_count} attempts: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    print()


async def main():
    """Run all demonstrations."""
    print("FLEET-Q Exponential Backoff Utility Demonstration")
    print("=" * 50)
    print()
    
    # Run sync demos
    demo_basic_backoff()
    demo_config_integration()
    demo_convenience_decorators()
    demo_logging_integration()
    demo_error_handling()
    
    # Run async demo
    await demo_async_backoff()
    
    print("Demo completed!")


if __name__ == "__main__":
    asyncio.run(main())