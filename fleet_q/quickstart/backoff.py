"""
FLEET-Q Exponential Backoff Utility

Reusable decorator for handling retries with exponential backoff and jitter.
Works with both sync and async functions.
"""

import asyncio
import functools
import logging
import random
import time
from typing import Any, Callable, Optional, Tuple, Type, Union

logger = logging.getLogger(__name__)


class BackoffConfig:
    """Configuration for exponential backoff behavior"""
    
    def __init__(
        self,
        max_attempts: int = 5,
        base_delay_ms: int = 50,
        max_delay_ms: int = 5000,
        jitter: bool = True,
        retry_on: Optional[Union[Type[Exception], Tuple[Type[Exception], ...]]] = None,
        on_retry: Optional[Callable[[int, float, Exception], None]] = None,
        timeout_total_s: Optional[float] = None,
    ):
        """
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay_ms: Base delay in milliseconds
            max_delay_ms: Maximum delay in milliseconds
            jitter: Whether to apply full jitter to delays
            retry_on: Exception types to retry on (None = retry on all)
            on_retry: Callback function(attempt, delay, exception) called on each retry
            timeout_total_s: Total timeout for all attempts in seconds
        """
        self.max_attempts = max_attempts
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.jitter = jitter
        self.retry_on = retry_on or Exception
        self.on_retry = on_retry
        self.timeout_total_s = timeout_total_s


def calculate_delay(attempt: int, config: BackoffConfig) -> float:
    """
    Calculate delay for given attempt number using exponential backoff.
    
    Args:
        attempt: Current attempt number (0-indexed)
        config: Backoff configuration
        
    Returns:
        Delay in seconds
    """
    # Exponential backoff: base * 2^attempt
    delay_ms = min(config.base_delay_ms * (2 ** attempt), config.max_delay_ms)
    
    # Apply full jitter if enabled
    if config.jitter:
        delay_ms = random.uniform(0, delay_ms)
    
    return delay_ms / 1000.0  # Convert to seconds


def with_backoff(
    max_attempts: int = 5,
    base_delay_ms: int = 50,
    max_delay_ms: int = 5000,
    jitter: bool = True,
    retry_on: Optional[Union[Type[Exception], Tuple[Type[Exception], ...]]] = None,
    on_retry: Optional[Callable[[int, float, Exception], None]] = None,
    timeout_total_s: Optional[float] = None,
):
    """
    Decorator that adds exponential backoff retry logic to a function.
    
    Works with both sync and async functions.
    
    Example:
        @with_backoff(max_attempts=3, base_delay_ms=100)
        def my_function():
            # ... code that might fail ...
            pass
        
        @with_backoff(max_attempts=5, retry_on=(ConnectionError, TimeoutError))
        async def my_async_function():
            # ... async code that might fail ...
            pass
    
    Args:
        max_attempts: Maximum number of retry attempts
        base_delay_ms: Base delay in milliseconds
        max_delay_ms: Maximum delay in milliseconds
        jitter: Whether to apply full jitter to delays
        retry_on: Exception types to retry on (None = retry on all)
        on_retry: Callback function(attempt, delay, exception) called on each retry
        timeout_total_s: Total timeout for all attempts in seconds
    """
    config = BackoffConfig(
        max_attempts=max_attempts,
        base_delay_ms=base_delay_ms,
        max_delay_ms=max_delay_ms,
        jitter=jitter,
        retry_on=retry_on,
        on_retry=on_retry,
        timeout_total_s=timeout_total_s,
    )
    
    def decorator(func: Callable) -> Callable:
        is_async = asyncio.iscoroutinefunction(func)
        
        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> Any:
                start_time = time.time()
                last_exception = None
                
                for attempt in range(config.max_attempts):
                    # Check total timeout
                    if config.timeout_total_s is not None:
                        elapsed = time.time() - start_time
                        if elapsed >= config.timeout_total_s:
                            logger.warning(
                                f"{func.__name__}: Total timeout {config.timeout_total_s}s exceeded"
                            )
                            raise TimeoutError(
                                f"Total timeout {config.timeout_total_s}s exceeded after {attempt} attempts"
                            )
                    
                    try:
                        return await func(*args, **kwargs)
                    except config.retry_on as e:
                        last_exception = e
                        
                        # Don't retry on last attempt
                        if attempt >= config.max_attempts - 1:
                            logger.error(
                                f"{func.__name__}: Max attempts ({config.max_attempts}) reached. "
                                f"Last error: {e}"
                            )
                            raise
                        
                        # Calculate delay and wait
                        delay = calculate_delay(attempt, config)
                        
                        # Call retry callback if provided
                        if config.on_retry:
                            config.on_retry(attempt + 1, delay, e)
                        
                        logger.debug(
                            f"{func.__name__}: Attempt {attempt + 1}/{config.max_attempts} failed: {e}. "
                            f"Retrying in {delay:.2f}s"
                        )
                        
                        await asyncio.sleep(delay)
                
                # Should not reach here, but just in case
                raise last_exception or Exception("Unexpected error in backoff logic")
            
            return async_wrapper
        
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs) -> Any:
                start_time = time.time()
                last_exception = None
                
                for attempt in range(config.max_attempts):
                    # Check total timeout
                    if config.timeout_total_s is not None:
                        elapsed = time.time() - start_time
                        if elapsed >= config.timeout_total_s:
                            logger.warning(
                                f"{func.__name__}: Total timeout {config.timeout_total_s}s exceeded"
                            )
                            raise TimeoutError(
                                f"Total timeout {config.timeout_total_s}s exceeded after {attempt} attempts"
                            )
                    
                    try:
                        return func(*args, **kwargs)
                    except config.retry_on as e:
                        last_exception = e
                        
                        # Don't retry on last attempt
                        if attempt >= config.max_attempts - 1:
                            logger.error(
                                f"{func.__name__}: Max attempts ({config.max_attempts}) reached. "
                                f"Last error: {e}"
                            )
                            raise
                        
                        # Calculate delay and wait
                        delay = calculate_delay(attempt, config)
                        
                        # Call retry callback if provided
                        if config.on_retry:
                            config.on_retry(attempt + 1, delay, e)
                        
                        logger.debug(
                            f"{func.__name__}: Attempt {attempt + 1}/{config.max_attempts} failed: {e}. "
                            f"Retrying in {delay:.2f}s"
                        )
                        
                        time.sleep(delay)
                
                # Should not reach here, but just in case
                raise last_exception or Exception("Unexpected error in backoff logic")
            
            return sync_wrapper
    
    return decorator


# Example usage and testing
if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.DEBUG)
    
    # Test sync function
    attempt_count = 0
    
    @with_backoff(max_attempts=3, base_delay_ms=100)
    def flaky_function():
        global attempt_count
        attempt_count += 1
        print(f"Attempt {attempt_count}")
        if attempt_count < 3:
            raise ValueError(f"Simulated error on attempt {attempt_count}")
        return "Success!"
    
    try:
        result = flaky_function()
        print(f"Result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
    
    # Test async function
    async def test_async():
        attempt_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=100)
        async def async_flaky_function():
            nonlocal attempt_count
            attempt_count += 1
            print(f"Async attempt {attempt_count}")
            if attempt_count < 2:
                raise ConnectionError(f"Simulated error on attempt {attempt_count}")
            return "Async Success!"
        
        try:
            result = await async_flaky_function()
            print(f"Async result: {result}")
        except Exception as e:
            print(f"Async failed: {e}")
    
    asyncio.run(test_async())
