"""
Tests for FLEET-Q logging configuration and structured logging.

Tests logging setup, context managers, and comprehensive event logging.
"""

import pytest
from unittest.mock import Mock, patch
import structlog
from structlog.types import FilteringBoundLogger

from fleet_q.config import FleetQConfig
from fleet_q.logging_config import (
    configure_logging,
    LogContext,
    ErrorContext,
    log_operation_error,
    log_configuration_error,
    log_storage_operation,
    log_claim_attempt,
    log_backoff_retry,
    log_step_transition,
    log_leader_election,
    log_recovery_cycle
)


class TestConfigureLogging:
    """Test cases for logging configuration."""
    
    def test_configure_logging_json_format(self):
        """Test logging configuration with JSON format."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            log_level="INFO",
            log_format="json"
        )
        
        logger = configure_logging(config)
        # The logger should be a structlog logger, not necessarily FilteringBoundLogger
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')
    
    def test_configure_logging_console_format(self):
        """Test logging configuration with console format."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            log_level="DEBUG",
            log_format="console"
        )
        
        logger = configure_logging(config)
        # The logger should be a structlog logger, not necessarily FilteringBoundLogger
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')


class TestLogContext:
    """Test cases for LogContext context manager."""
    
    def test_log_context_binding(self):
        """Test that LogContext properly binds context to logger."""
        mock_logger = Mock()
        mock_bound_logger = Mock()
        mock_logger.bind.return_value = mock_bound_logger
        
        context = {"operation": "test", "pod_id": "test-pod"}
        
        with LogContext(mock_logger, **context) as bound_logger:
            assert bound_logger == mock_bound_logger
            mock_logger.bind.assert_called_once_with(**context)


class TestErrorContext:
    """Test cases for ErrorContext context manager."""
    
    def test_error_context_success_case(self):
        """Test ErrorContext logs success when no exception occurs."""
        mock_logger = Mock()
        mock_bound_logger = Mock()
        mock_logger.bind.return_value = mock_bound_logger
        
        with patch('time.time', side_effect=[1000.0, 1001.0]):  # 1 second duration
            with ErrorContext(mock_logger, "test_operation", pod_id="test-pod"):
                pass  # No exception
        
        # Should log operation start and success
        mock_bound_logger.debug.assert_called_once()
        mock_bound_logger.info.assert_called_once()
        
        # Check success log call
        success_call = mock_bound_logger.info.call_args
        assert "Operation completed successfully" in success_call[0]
        assert success_call[1]["duration_ms"] == 1000.0
    
    def test_error_context_failure_case(self):
        """Test ErrorContext logs error when exception occurs."""
        mock_logger = Mock()
        mock_bound_logger = Mock()
        mock_logger.bind.return_value = mock_bound_logger
        
        test_error = ValueError("Test error")
        
        with patch('time.time', side_effect=[1000.0, 1001.5]):  # 1.5 second duration
            with pytest.raises(ValueError):
                with ErrorContext(mock_logger, "test_operation", pod_id="test-pod"):
                    raise test_error
        
        # Should log operation start and error
        mock_bound_logger.debug.assert_called_once()
        mock_bound_logger.error.assert_called_once()


class TestLoggingFunctions:
    """Test cases for structured logging functions."""
    
    def test_log_operation_error(self):
        """Test comprehensive operation error logging."""
        mock_logger = Mock()
        test_error = ValueError("Test error")
        
        log_operation_error(
            mock_logger,
            "test_operation",
            test_error,
            1500.0,
            retry_count=2,
            pod_id="test-pod"
        )
        
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        
        assert "Operation failed" in call_args[0]
        assert call_args[1]["operation"] == "test_operation"
        assert call_args[1]["error_type"] == "ValueError"
        assert call_args[1]["error_message"] == "Test error"
        assert call_args[1]["duration_ms"] == 1500.0
        assert call_args[1]["retry_count"] == 2
        assert call_args[1]["pod_id"] == "test-pod"
    
    def test_log_configuration_error(self):
        """Test configuration error logging."""
        mock_logger = Mock()
        test_error = ValueError("Invalid value")
        
        log_configuration_error(
            mock_logger,
            "snowflake_account",
            test_error,
            provided_value="invalid_account"
        )
        
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        
        assert "Configuration validation failed" in call_args[0]
        assert call_args[1]["config_field"] == "snowflake_account"
        assert call_args[1]["error_type"] == "ValueError"
        assert call_args[1]["provided_value"] == "invalid_account"
    
    def test_log_storage_operation(self):
        """Test storage operation logging."""
        mock_logger = Mock()
        
        log_storage_operation(
            mock_logger,
            "create",
            "step",
            "step-123",
            "success",
            250.0,
            pod_id="test-pod"
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert "Storage operation" in call_args[0]
        assert call_args[1]["operation"] == "create"
        assert call_args[1]["entity_type"] == "step"
        assert call_args[1]["entity_id"] == "step-123"
        assert call_args[1]["status"] == "success"
        assert call_args[1]["duration_ms"] == 250.0
    
    def test_log_claim_attempt(self):
        """Test claim attempt logging."""
        mock_logger = Mock()
        
        log_claim_attempt(
            mock_logger,
            "test-pod",
            5,
            3,
            8,
            150.0,
            conflict_occurred=False
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert "Step claim attempt" in call_args[0]
        assert call_args[1]["pod_id"] == "test-pod"
        assert call_args[1]["requested_count"] == 5
        assert call_args[1]["claimed_count"] == 3
        assert call_args[1]["available_capacity"] == 8
        assert call_args[1]["claim_efficiency"] == 0.6  # 3/5
    
    def test_log_claim_attempt_with_conflict(self):
        """Test claim attempt logging with conflict."""
        mock_logger = Mock()
        
        log_claim_attempt(
            mock_logger,
            "test-pod",
            5,
            0,
            8,
            150.0,
            conflict_occurred=True
        )
        
        # Should log as warning when conflict occurs
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        
        assert call_args[1]["conflict_occurred"] is True
    
    def test_log_backoff_retry(self):
        """Test backoff retry logging."""
        mock_logger = Mock()
        test_error = ConnectionError("Connection failed")
        
        log_backoff_retry(
            mock_logger,
            "database_operation",
            3,
            800.0,
            test_error,
            pod_id="test-pod"
        )
        
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args
        
        assert "Retrying operation after backoff" in call_args[0]
        assert call_args[1]["operation"] == "database_operation"
        assert call_args[1]["attempt"] == 3
        assert call_args[1]["delay_ms"] == 800.0
        assert call_args[1]["error_type"] == "ConnectionError"
    
    def test_log_step_transition(self):
        """Test step transition logging."""
        mock_logger = Mock()
        
        log_step_transition(
            mock_logger,
            "step-123",
            "pending",
            "claimed",
            "test-pod"
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert "Step status transition" in call_args[0]
        assert call_args[1]["step_id"] == "step-123"
        assert call_args[1]["from_status"] == "pending"
        assert call_args[1]["to_status"] == "claimed"
        assert call_args[1]["pod_id"] == "test-pod"
    
    def test_log_leader_election(self):
        """Test leader election logging."""
        mock_logger = Mock()
        
        log_leader_election(
            mock_logger,
            "leader-pod",
            "test-pod",
            False,
            3
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert "Leader election status" in call_args[0]
        assert call_args[1]["current_leader"] == "leader-pod"
        assert call_args[1]["pod_id"] == "test-pod"
        assert call_args[1]["is_leader"] is False
        assert call_args[1]["active_pods"] == 3
    
    def test_log_recovery_cycle(self):
        """Test recovery cycle logging."""
        mock_logger = Mock()
        
        log_recovery_cycle(
            mock_logger,
            "recovery-123",
            ["dead-pod-1", "dead-pod-2"],
            10,
            8,
            2,
            5000.0
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert "Recovery cycle completed" in call_args[0]
        assert call_args[1]["cycle_id"] == "recovery-123"
        assert call_args[1]["dead_pods"] == ["dead-pod-1", "dead-pod-2"]
        assert call_args[1]["dead_pod_count"] == 2
        assert call_args[1]["orphaned_steps"] == 10
        assert call_args[1]["requeued_steps"] == 8
        assert call_args[1]["terminal_steps"] == 2
        assert call_args[1]["duration_ms"] == 5000.0


class TestLoggingIntegration:
    """Integration tests for logging functionality."""
    
    def test_logging_with_real_structlog(self):
        """Test logging integration with real structlog configuration."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            log_level="INFO",
            log_format="json"
        )
        
        logger = configure_logging(config)
        
        # Test that we can actually log messages without errors
        logger.info("Test message", test_field="test_value")
        
        # Test context manager
        with LogContext(logger, operation="test") as bound_logger:
            bound_logger.info("Context test")
        
        # Test error context
        with pytest.raises(ValueError):
            with ErrorContext(logger, "test_operation"):
                raise ValueError("Test error")