"""
Integration tests for backoff utility with logging and configuration.

Tests the integration between the backoff decorator, logging system,
and configuration management.
"""

import pytest
from unittest.mock import Mock, patch
import structlog

from fleet_q.backoff import with_backoff, create_retry_callback
from fleet_q.config import FleetQConfig
from fleet_q.logging_config import configure_logging


class TestBackoffIntegration:
    """Test backoff integration with other system components."""
    
    def test_backoff_with_config_integration(self):
        """Test backoff decorator using FleetQConfig defaults."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test", 
            snowflake_password="test",
            snowflake_database="test",
            default_max_attempts=3,
            default_base_delay_ms=50,
            default_max_delay_ms=1000
        )
        
        call_count = 0
        
        @with_backoff(config=config)
        def test_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Test connection error")
            return "success"
        
        result = test_operation()
        assert result == "success"
        assert call_count == 3
    
    def test_backoff_with_logging_integration(self):
        """Test backoff decorator with structured logging."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test", 
            snowflake_database="test",
            log_level="INFO",
            log_format="json"
        )
        
        logger = configure_logging(config)
        retry_callback = create_retry_callback(logger, "test_operation")
        
        call_count = 0
        
        @with_backoff(max_attempts=3, base_delay_ms=10, on_retry=retry_callback)
        def test_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Test error for logging")
            return "logged_success"
        
        # Capture log output
        with patch('structlog.get_logger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            result = test_operation()
            assert result == "logged_success"
            assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_async_backoff_with_config(self):
        """Test async backoff decorator with configuration."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            default_max_attempts=2,
            default_base_delay_ms=20
        )
        
        call_count = 0
        
        @with_backoff(config=config)
        async def async_test_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("Async test timeout")
            return "async_success"
        
        result = await async_test_operation()
        assert result == "async_success"
        assert call_count == 2
    
    def test_convenience_decorators_integration(self):
        """Test that convenience decorators work with configuration."""
        from fleet_q.backoff import with_database_backoff, with_network_backoff, with_conflict_backoff
        
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test"
        )
        
        # Test database backoff
        db_call_count = 0
        
        @with_database_backoff(config=config)
        def db_operation():
            nonlocal db_call_count
            db_call_count += 1
            if db_call_count < 2:
                raise ConnectionError("DB connection failed")
            return "db_success"
        
        result = db_operation()
        assert result == "db_success"
        assert db_call_count == 2
        
        # Test network backoff
        net_call_count = 0
        
        @with_network_backoff(config=config)
        def network_operation():
            nonlocal net_call_count
            net_call_count += 1
            if net_call_count < 2:
                raise OSError("Network error")
            return "network_success"
        
        result = network_operation()
        assert result == "network_success"
        assert net_call_count == 2
        
        # Test conflict backoff
        conflict_call_count = 0
        
        @with_conflict_backoff(config=config)
        def conflict_operation():
            nonlocal conflict_call_count
            conflict_call_count += 1
            if conflict_call_count < 3:
                raise Exception("Resource conflict")
            return "conflict_resolved"
        
        result = conflict_operation()
        assert result == "conflict_resolved"
        assert conflict_call_count == 3