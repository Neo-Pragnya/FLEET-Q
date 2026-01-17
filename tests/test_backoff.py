"""
Unit tests for the exponential backoff utility.

Tests both synchronous and asynchronous function decoration, error handling,
configuration validation, and edge cases.
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, patch

from fleet_q.backoff import (
    with_backoff,
    calculate_delay,
    BackoffExhaustedError,
    BackoffConfig,
    create_retry_callback,
    with_database_backoff,
    with_network_backoff,
    with_conflict_backoff
)
from fleet_q.config import FleetQConfig


class TestCalculateDelay:
    """Test the delay calculation function."""
    
    def test_exponential_growth_without_jitter(self):
        """Test that delay grows exponentially without jitter."""
        base_delay = 100
        max_delay = 10000
        
        # Test exponential growth: 100, 200, 400, 800, 1600
        assert calculate_delay(1, base_delay, max_delay, jitter=False) == 100
        assert calculate_delay(2, base_delay, max_delay, jitter=False) == 200
        assert calculate_delay(3, base_delay, max_delay, jitter=False) == 400
        assert calculate_delay(4, base_delay, max_delay, jitter=False) == 800
        assert calculate_delay(5, base_delay, max_delay, jitter=False) == 1600
    
    def test_max_delay_cap(self):
        """Test that delay is capped at maximum value."""
        base_delay = 100
        max_delay = 500
        
        # After a few attempts, delay should be capped at max_delay
        delay = calculate_delay(10, base_delay, max_delay, jitter=False)
        assert delay == max_delay
    
    def test_jitter_variation(self):
        """Test that jitter introduces variation in delay."""
        base_delay = 1000
        max_delay = 10000
        attempt = 3
        
        # Generate multiple delays with jitter and ensure they vary
        delays = [calculate_delay(attempt, base_delay, max_delay, jitter=True) for _ in range(10)]
        
        # All delays should be different (with high probability)
        assert len(set(delays)) > 1
        
        # All delays should be within reasonable bounds (±25% of base)
        expected_base = base_delay * (2 ** (attempt - 1))
        for delay in delays:
            assert delay >= expected_base * 0.75
            assert delay <= expected_base * 1.25
    
    def test_jitter_disabled(self):
        """Test that disabling jitter produces consistent delays."""
        base_delay = 100
        max_delay = 10000
        attempt = 3
        
        # Generate multiple delays without jitter
        delays = [calculate_delay(attempt, base_delay, max_delay, jitter=False) for _ in range(5)]
        
        # All delays should be identical
        assert len(set(delays)) == 1
        assert delays[0] == 400  # 100 * 2^(3-1)


class TestSyncBackoff:
    """Test backoff decorator with synchronous functions."""
    
    def test_successful_function_no_retry(self):
        """Test that successful functions are not retried."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10)
        def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_func()
        assert result == "success"
        assert call_count == 1
    
    def test_function_succeeds_after_retries(self):
        """Test that function succeeds after some failures."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10, jitter=False)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"
        
        result = flaky_func()
        assert result == "success"
        assert call_count == 3
    
    def test_function_exhausts_retries(self):
        """Test that function raises BackoffExhaustedError after max attempts."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")
        
        with pytest.raises(BackoffExhaustedError) as exc_info:
            always_fails()
        
        assert call_count == 3
        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_error, ValueError)
    
    def test_retry_on_specific_exceptions(self):
        """Test that only specific exceptions trigger retries."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10, retry_on=[ValueError])
        def selective_retry():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("Should retry")
            elif call_count == 2:
                raise TypeError("Should not retry")
            return "success"
        
        with pytest.raises(TypeError):
            selective_retry()
        
        assert call_count == 2
    
    def test_on_retry_callback(self):
        """Test that retry callback is called with correct parameters."""
        retry_calls = []
        
        def retry_callback(attempt, delay_ms, error):
            retry_calls.append((attempt, delay_ms, type(error).__name__))
        
        @with_backoff(max_attempts=3, base_delay_ms=100, jitter=False, on_retry=retry_callback)
        def failing_func():
            raise ValueError("Test error")
        
        with pytest.raises(BackoffExhaustedError):
            failing_func()
        
        assert len(retry_calls) == 2  # 2 retries after initial failure
        assert retry_calls[0] == (1, 100.0, "ValueError")
        assert retry_calls[1] == (2, 200.0, "ValueError")
    
    def test_total_timeout(self):
        """Test that total timeout is respected."""
        @with_backoff(max_attempts=10, base_delay_ms=100, timeout_total_s=0.2)
        def slow_failing_func():
            time.sleep(0.1)  # Each attempt takes 100ms
            raise ValueError("Timeout test")
        
        start_time = time.time()
        with pytest.raises(TimeoutError):
            slow_failing_func()
        
        elapsed = time.time() - start_time
        assert elapsed < 0.6  # Should timeout before completing many attempts


class TestAsyncBackoff:
    """Test backoff decorator with asynchronous functions."""
    
    @pytest.mark.asyncio
    async def test_async_successful_function_no_retry(self):
        """Test that successful async functions are not retried."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10)
        async def successful_async_func():
            nonlocal call_count
            call_count += 1
            return "async_success"
        
        result = await successful_async_func()
        assert result == "async_success"
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_async_function_succeeds_after_retries(self):
        """Test that async function succeeds after some failures."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10, jitter=False)
        async def flaky_async_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary async failure")
            return "async_success"
        
        result = await flaky_async_func()
        assert result == "async_success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_async_function_exhausts_retries(self):
        """Test that async function raises BackoffExhaustedError after max attempts."""
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10)
        async def always_fails_async():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails async")
        
        with pytest.raises(BackoffExhaustedError) as exc_info:
            await always_fails_async()
        
        assert call_count == 3
        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_error, ValueError)
    
    @pytest.mark.asyncio
    async def test_async_total_timeout(self):
        """Test that total timeout is respected for async functions."""
        @with_backoff(max_attempts=10, base_delay_ms=100, timeout_total_s=0.2)
        async def slow_failing_async_func():
            await asyncio.sleep(0.1)  # Each attempt takes 100ms
            raise ValueError("Async timeout test")
        
        start_time = time.time()
        with pytest.raises(TimeoutError):
            await slow_failing_async_func()
        
        elapsed = time.time() - start_time
        assert elapsed < 0.6  # Should timeout before completing many attempts


class TestBackoffWithConfig:
    """Test backoff decorator with FleetQConfig integration."""
    
    def test_config_default_values(self):
        """Test that config provides default values for backoff parameters."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            default_max_attempts=7,
            default_base_delay_ms=250,
            default_max_delay_ms=15000
        )
        
        call_count = 0
        
        @with_backoff(config=config)
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Test")
        
        with pytest.raises(BackoffExhaustedError) as exc_info:
            test_func()
        
        # Should use config's max_attempts value
        assert call_count == 7
        assert exc_info.value.attempts == 7
    
    def test_explicit_params_override_config(self):
        """Test that explicit parameters override config defaults."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            default_max_attempts=7
        )
        
        call_count = 0
        
        @with_backoff(max_attempts=3, config=config)  # Explicit override
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Test")
        
        with pytest.raises(BackoffExhaustedError):
            test_func()
        
        # Should use explicit max_attempts, not config value
        assert call_count == 3


class TestRetryCallback:
    """Test the retry callback creation utility."""
    
    def test_create_retry_callback(self):
        """Test that retry callback logs correctly."""
        mock_logger = Mock()
        callback = create_retry_callback(mock_logger, "test_operation")
        
        test_error = ValueError("Test error")
        callback(2, 500.0, test_error)
        
        # Verify the logger was called with correct parameters
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        assert "Retrying operation after backoff" in call_args[0]


class TestConvenienceDecorators:
    """Test the convenience decorator functions."""
    
    def test_database_backoff_decorator(self):
        """Test database backoff decorator configuration."""
        call_count = 0
        
        @with_database_backoff()
        def db_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Database connection failed")
            return "db_success"
        
        result = db_operation()
        assert result == "db_success"
        assert call_count == 3
    
    def test_network_backoff_decorator(self):
        """Test network backoff decorator configuration."""
        call_count = 0
        
        @with_network_backoff()
        def network_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("Network timeout")
            return "network_success"
        
        result = network_operation()
        assert result == "network_success"
        assert call_count == 2
    
    def test_conflict_backoff_decorator(self):
        """Test conflict backoff decorator configuration."""
        call_count = 0
        
        @with_conflict_backoff()
        def conflict_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise Exception("Resource conflict")
            return "conflict_resolved"
        
        result = conflict_operation()
        assert result == "conflict_resolved"
        assert call_count == 4


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_zero_max_attempts(self):
        """Test behavior with zero max attempts."""
        @with_backoff(max_attempts=0)
        def zero_attempts():
            raise ValueError("Should not retry")
        
        # With 0 max attempts, should raise BackoffExhaustedError immediately
        with pytest.raises(BackoffExhaustedError) as exc_info:
            zero_attempts()
        
        assert exc_info.value.attempts == 0
    
    def test_negative_delays(self):
        """Test that negative delays are handled gracefully."""
        # This shouldn't happen in normal usage, but test robustness
        delay = calculate_delay(1, -100, 1000, jitter=True)
        assert delay >= 0
    
    def test_very_large_attempt_numbers(self):
        """Test behavior with very large attempt numbers."""
        delay = calculate_delay(100, 100, 30000, jitter=False)
        assert delay == 30000  # Should be capped at max_delay
    
    def test_function_with_args_and_kwargs(self):
        """Test that function arguments are preserved through retries."""
        call_args = []
        
        @with_backoff(max_attempts=2, base_delay_ms=10)
        def func_with_args(arg1, arg2, kwarg1=None):
            call_args.append((arg1, arg2, kwarg1))
            if len(call_args) == 1:
                raise ValueError("First call fails")
            return f"{arg1}-{arg2}-{kwarg1}"
        
        result = func_with_args("a", "b", kwarg1="c")
        assert result == "a-b-c"
        assert len(call_args) == 2
        assert call_args[0] == ("a", "b", "c")
        assert call_args[1] == ("a", "b", "c")