"""
Exponential backoff utility for FLEET-Q.

Provides a reusable exponential backoff decorator that can be applied to any function
to handle retries with configurable parameters, jitter support, and comprehensive
logging. Supports both synchronous and asynchronous functions.

"""

import asyncio
import functools
import random
import time
from typing import Any, Callable, List, Optional, Type, TypeVar, Union
from dataclasses import dataclass

from .logging_config import log_backoff_retry, log_operation_error
from .config import FleetQConfig

# Type variables for generic function decoration
F = TypeVar('F', bound=Callable[..., Any])
AsyncF = TypeVar('AsyncF', bound=Callable[..., Any])


@dataclass
class BackoffConfig:
    """Configuration for exponential backoff behavior."""
    max_attempts: int = 5
    base_delay_ms: int = 100
    max_delay_ms: int = 30000
    jitter: bool = True
    timeout_total_s: Optional[int] = None


class BackoffExhaustedError(Exception):
    """Raised when all retry attempts have been exhausted."""
    
    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Backoff exhausted after {attempts} attempts. Last error: {last_error}"
        )


def calculate_delay(
    attempt: int, 
    base_delay_ms: int, 
    max_delay_ms: int, 
    jitter: bool = True
) -> float:
    """
    Calculate the delay for a given retry attempt using exponential backoff.
    
    Args:
        attempt: Current attempt number (1-based)
        base_delay_ms: Base delay in milliseconds
        max_delay_ms: Maximum delay in milliseconds
        jitter: Whether to apply jitter to prevent thundering herd
        
    Returns:
        float: Delay in milliseconds
    """
    # Calculate exponential delay: base_delay * 2^(attempt-1)
    exponential_delay = base_delay_ms * (2 ** (attempt - 1))
    
    # Cap at maximum delay
    delay = min(exponential_delay, max_delay_ms)
    
    # Apply jitter if enabled (±25% random variation)
    if jitter:
        jitter_range = delay * 0.25
        jitter_offset = random.uniform(-jitter_range, jitter_range)
        delay = max(0, delay + jitter_offset)
    
    return delay


def with_backoff(
    max_attempts: Optional[int] = None,
    base_delay_ms: Optional[int] = None,
    max_delay_ms: Optional[int] = None,
    jitter: Optional[bool] = None,
    retry_on: Optional[List[Type[Exception]]] = None,
    timeout_total_s: Optional[int] = None,
    on_retry: Optional[Callable[[int, float, Exception], None]] = None,
    config: Optional[FleetQConfig] = None
) -> Callable[[F], F]:
    """
    Decorator that adds exponential backoff retry logic to a function.
    
    Can be used with both synchronous and asynchronous functions. Provides
    configurable retry behavior with exponential backoff and jitter support.
    
    Args:
        max_attempts: Maximum number of retry attempts (defaults to config or 5)
        base_delay_ms: Base delay in milliseconds (defaults to config or 100)
        max_delay_ms: Maximum delay in milliseconds (defaults to config or 30000)
        jitter: Whether to apply jitter (defaults to config or True)
        retry_on: List of exception types to retry on (defaults to all exceptions)
        timeout_total_s: Total timeout for all attempts in seconds
        on_retry: Callback function called on each retry attempt
        config: FleetQConfig instance for default values
        
    Returns:
        Decorated function with backoff retry logic
        
    Example:
        @with_backoff(max_attempts=3, base_delay_ms=200)
        async def unreliable_operation():
            # This function will be retried up to 3 times with exponential backoff
            pass
    """
    
    def decorator(func: F) -> F:
        # Determine if the function is async
        is_async = asyncio.iscoroutinefunction(func)
        
        # Get configuration values with fallbacks
        _max_attempts = max_attempts if max_attempts is not None else (config.default_max_attempts if config else 5)
        _base_delay_ms = base_delay_ms if base_delay_ms is not None else (config.default_base_delay_ms if config else 100)
        _max_delay_ms = max_delay_ms if max_delay_ms is not None else (config.default_max_delay_ms if config else 30000)
        _jitter = jitter if jitter is not None else True
        _retry_on = retry_on or [Exception]  # Retry on all exceptions by default
        
        # Handle edge case of zero max attempts
        if _max_attempts <= 0:
            def immediate_failure(*args, **kwargs):
                raise BackoffExhaustedError(0, ValueError("Zero max attempts configured"))
            return immediate_failure
        
        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                return await _execute_with_backoff_async(
                    func, args, kwargs,
                    _max_attempts, _base_delay_ms, _max_delay_ms, _jitter,
                    _retry_on, timeout_total_s, on_retry
                )
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                return _execute_with_backoff_sync(
                    func, args, kwargs,
                    _max_attempts, _base_delay_ms, _max_delay_ms, _jitter,
                    _retry_on, timeout_total_s, on_retry
                )
            return sync_wrapper
    
    return decorator


async def _execute_with_backoff_async(
    func: Callable,
    args: tuple,
    kwargs: dict,
    max_attempts: int,
    base_delay_ms: int,
    max_delay_ms: int,
    jitter: bool,
    retry_on: List[Type[Exception]],
    timeout_total_s: Optional[int],
    on_retry: Optional[Callable[[int, float, Exception], None]]
) -> Any:
    """Execute an async function with backoff retry logic."""
    start_time = time.time()
    last_error = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            # Check total timeout
            if timeout_total_s and (time.time() - start_time) > timeout_total_s:
                raise TimeoutError(f"Total timeout of {timeout_total_s}s exceeded")
            
            return await func(*args, **kwargs)
            
        except Exception as e:
            last_error = e
            
            # If this is a TimeoutError from our timeout check, raise it directly
            if isinstance(e, TimeoutError) and "Total timeout" in str(e):
                raise e
            
            # Check if this exception type should trigger a retry
            if not any(isinstance(e, exc_type) for exc_type in retry_on):
                raise e
            
            # If this was the last attempt, raise the error
            if attempt >= max_attempts:
                break
            
            # Calculate delay for next attempt
            delay_ms = calculate_delay(attempt, base_delay_ms, max_delay_ms, jitter)
            
            # Call retry callback if provided
            if on_retry:
                on_retry(attempt, delay_ms, e)
            
            # Wait before retrying
            await asyncio.sleep(delay_ms / 1000.0)
    
    # All attempts exhausted, raise the final error
    raise BackoffExhaustedError(max_attempts, last_error)


def _execute_with_backoff_sync(
    func: Callable,
    args: tuple,
    kwargs: dict,
    max_attempts: int,
    base_delay_ms: int,
    max_delay_ms: int,
    jitter: bool,
    retry_on: List[Type[Exception]],
    timeout_total_s: Optional[int],
    on_retry: Optional[Callable[[int, float, Exception], None]]
) -> Any:
    """Execute a sync function with backoff retry logic."""
    start_time = time.time()
    last_error = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            # Check total timeout
            if timeout_total_s and (time.time() - start_time) > timeout_total_s:
                raise TimeoutError(f"Total timeout of {timeout_total_s}s exceeded")
            
            return func(*args, **kwargs)
            
        except Exception as e:
            last_error = e
            
            # If this is a TimeoutError from our timeout check, raise it directly
            if isinstance(e, TimeoutError) and "Total timeout" in str(e):
                raise e
            
            # Check if this exception type should trigger a retry
            if not any(isinstance(e, exc_type) for exc_type in retry_on):
                raise e
            
            # If this was the last attempt, raise the error
            if attempt >= max_attempts:
                break
            
            # Calculate delay for next attempt
            delay_ms = calculate_delay(attempt, base_delay_ms, max_delay_ms, jitter)
            
            # Call retry callback if provided
            if on_retry:
                on_retry(attempt, delay_ms, e)
            
            # Wait before retrying
            time.sleep(delay_ms / 1000.0)
    
    # All attempts exhausted, raise the final error
    raise BackoffExhaustedError(max_attempts, last_error)


def create_retry_callback(logger, operation_name: str):
    """
    Create a retry callback function that logs retry attempts with enhanced context.
    
    Args:
        logger: Logger instance for logging retry attempts
        operation_name: Name of the operation being retried
        
    Returns:
        Callback function for use with with_backoff decorator
    """
    def retry_callback(attempt: int, delay_ms: float, error: Exception):
        log_backoff_retry(
            logger=logger,
            operation=operation_name,
            attempt=attempt,
            delay_ms=delay_ms,
            error=error,
            # Add additional context for better debugging
            total_attempts_so_far=attempt,
            next_delay_ms=delay_ms,
            error_occurred_at=attempt
        )
    
    return retry_callback


# Convenience decorators with common configurations
def with_database_backoff(config: Optional[FleetQConfig] = None):
    """
    Convenience decorator for database operations with appropriate retry settings.
    
    Configured for database-specific exceptions and longer delays suitable
    for database connection and transaction retry scenarios.
    """
    # Common database exceptions that should trigger retries
    database_exceptions = [
        ConnectionError,
        TimeoutError,
        # Add specific database exceptions as needed
    ]
    
    return with_backoff(
        max_attempts=5,
        base_delay_ms=200,
        max_delay_ms=10000,
        jitter=True,
        retry_on=database_exceptions,
        config=config
    )


def with_network_backoff(config: Optional[FleetQConfig] = None):
    """
    Convenience decorator for network operations with appropriate retry settings.
    
    Configured for network-specific exceptions and shorter delays suitable
    for HTTP requests and network communication.
    """
    # Common network exceptions that should trigger retries
    network_exceptions = [
        ConnectionError,
        TimeoutError,
        OSError,  # Covers various network-related OS errors
    ]
    
    return with_backoff(
        max_attempts=3,
        base_delay_ms=100,
        max_delay_ms=5000,
        jitter=True,
        retry_on=network_exceptions,
        config=config
    )


def with_conflict_backoff(config: Optional[FleetQConfig] = None):
    """
    Convenience decorator for operations that may encounter conflicts.
    
    Configured for conflict scenarios like database transaction conflicts
    or resource contention with shorter delays and more attempts.
    """
    return with_backoff(
        max_attempts=8,
        base_delay_ms=50,
        max_delay_ms=2000,
        jitter=True,
        config=config
    )