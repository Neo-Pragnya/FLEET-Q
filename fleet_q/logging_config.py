"""
Logging configuration for FLEET-Q.

Provides structured logging setup with JSON and console formatters,
supporting comprehensive event logging.
"""

import logging
import sys
from typing import Any, Dict
import structlog
from structlog.types import FilteringBoundLogger

from .config import FleetQConfig


def configure_logging(config: FleetQConfig) -> FilteringBoundLogger:
    """
    Configure structured logging for FLEET-Q.
    
    Sets up structlog with appropriate processors, formatters, and log levels
    based on the provided configuration.
    
    Args:
        config: FleetQConfig instance with logging settings
        
    Returns:
        FilteringBoundLogger: Configured logger instance
    """
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, config.log_level),
    )
    
    # Common processors for all log formats
    common_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    
    # Format-specific processors
    if config.log_format == "json":
        processors = common_processors + [
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ]
    else:  # console format
        processors = common_processors + [
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.dev.ConsoleRenderer(colors=True)
        ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.log_level)
        ),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Create and return the main logger
    logger = structlog.get_logger("fleet_q")
    
    # Log configuration startup
    logger.info(
        "Logging configured",
        log_level=config.log_level,
        log_format=config.log_format,
        pod_id=config.pod_id,
    )
    
    return logger


class LogContext:
    """
    Context manager for adding structured context to log entries.
    
    Provides a convenient way to add consistent context information
    to all log entries within a scope.
    """
    
    def __init__(self, logger: FilteringBoundLogger, **context: Any):
        self.logger = logger
        self.context = context
        self.bound_logger = None
    
    def __enter__(self) -> FilteringBoundLogger:
        self.bound_logger = self.logger.bind(**self.context)
        return self.bound_logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.bound_logger = None


class ErrorContext:
    """
    Context manager for comprehensive error tracking and logging.
    
    Automatically logs operation start, success, or failure with timing
    and error context information.
    """
    
    def __init__(
        self,
        logger: FilteringBoundLogger,
        operation: str,
        **context: Any
    ):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time = None
        self.bound_logger = None
    
    def __enter__(self) -> FilteringBoundLogger:
        import time
        self.start_time = time.time()
        self.bound_logger = self.logger.bind(operation=self.operation, **self.context)
        
        self.bound_logger.debug(
            "Operation started",
            operation=self.operation,
            **self.context
        )
        
        return self.bound_logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration_ms = (time.time() - self.start_time) * 1000 if self.start_time else 0
        
        if exc_type is None:
            # Success case
            self.bound_logger.info(
                "Operation completed successfully",
                operation=self.operation,
                duration_ms=duration_ms,
                **self.context
            )
        else:
            # Error case
            log_operation_error(
                self.bound_logger,
                self.operation,
                exc_val,
                duration_ms,
                **self.context
            )
        
        self.bound_logger = None


def log_operation_start(
    logger: FilteringBoundLogger,
    operation: str,
    **context: Any
) -> FilteringBoundLogger:
    """
    Log the start of an operation with context.
    
    Args:
        logger: The logger instance
        operation: Name of the operation being started
        **context: Additional context to include in the log
        
    Returns:
        FilteringBoundLogger: Bound logger with operation context
    """
    bound_logger = logger.bind(operation=operation, **context)
    bound_logger.info("Operation started", operation=operation)
    return bound_logger


def log_operation_success(
    logger: FilteringBoundLogger,
    operation: str,
    duration_ms: float,
    **context: Any
) -> None:
    """
    Log successful completion of an operation.
    
    Args:
        logger: The logger instance
        operation: Name of the operation that completed
        duration_ms: Duration of the operation in milliseconds
        **context: Additional context to include in the log
    """
    logger.info(
        "Operation completed successfully",
        operation=operation,
        duration_ms=duration_ms,
        **context
    )


def log_operation_error(
    logger: FilteringBoundLogger,
    operation: str,
    error: Exception,
    duration_ms: float,
    retry_count: int = 0,
    **context: Any
) -> None:
    """
    Log an operation error with comprehensive context.
    
    Args:
        logger: The logger instance
        operation: Name of the operation that failed
        error: The exception that occurred
        duration_ms: Duration of the operation in milliseconds
        retry_count: Current retry attempt number
        **context: Additional context to include in the log
    """
    # Extract error context
    error_context = {
        "operation": operation,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "duration_ms": duration_ms,
        "retry_count": retry_count,
        "exc_info": error,
        **context
    }
    
    # Add original error if this is a wrapped exception
    if hasattr(error, '__cause__') and error.__cause__:
        error_context["original_error_type"] = type(error.__cause__).__name__
        error_context["original_error_message"] = str(error.__cause__)
    
    # Add stack trace context for debugging
    if hasattr(error, '__traceback__') and error.__traceback__:
        import traceback
        error_context["stack_trace"] = ''.join(traceback.format_tb(error.__traceback__))
    
    logger.error(
        "Operation failed",
        **error_context
    )


def log_configuration_error(
    logger: FilteringBoundLogger,
    config_field: str,
    error: Exception,
    provided_value: Any = None,
    **context: Any
) -> None:
    """
    Log configuration validation errors with detailed context.
    
    Args:
        logger: The logger instance
        config_field: Name of the configuration field that failed
        error: The validation error that occurred
        provided_value: The value that failed validation (if safe to log)
        **context: Additional context to include in the log
    """
    logger.error(
        "Configuration validation failed",
        config_field=config_field,
        error_type=type(error).__name__,
        error_message=str(error),
        provided_value=provided_value if provided_value is not None else "not_provided",
        **context
    )


def log_storage_operation(
    logger: FilteringBoundLogger,
    operation: str,
    entity_type: str,
    entity_id: str,
    status: str,
    duration_ms: float,
    **context: Any
) -> None:
    """
    Log storage operations with consistent structure.
    
    Args:
        logger: The logger instance
        operation: Storage operation (create, update, delete, query)
        entity_type: Type of entity (step, pod_health, dlq_record)
        entity_id: Unique identifier for the entity
        status: Operation status (success, failed, conflict)
        duration_ms: Duration of the operation in milliseconds
        **context: Additional context to include in the log
    """
    logger.info(
        "Storage operation",
        operation=operation,
        entity_type=entity_type,
        entity_id=entity_id,
        status=status,
        duration_ms=duration_ms,
        **context
    )


def log_claim_attempt(
    logger: FilteringBoundLogger,
    pod_id: str,
    requested_count: int,
    claimed_count: int,
    available_capacity: int,
    duration_ms: float,
    conflict_occurred: bool = False,
    **context: Any
) -> None:
    """
    Log step claiming attempts with detailed metrics.
    
    Args:
        logger: The logger instance
        pod_id: ID of the pod attempting to claim
        requested_count: Number of steps requested
        claimed_count: Number of steps actually claimed
        available_capacity: Available capacity at time of claim
        duration_ms: Duration of the claim operation
        conflict_occurred: Whether a transaction conflict occurred
        **context: Additional context to include in the log
    """
    log_level = "warning" if conflict_occurred else "info"
    
    getattr(logger, log_level)(
        "Step claim attempt",
        pod_id=pod_id,
        requested_count=requested_count,
        claimed_count=claimed_count,
        available_capacity=available_capacity,
        duration_ms=duration_ms,
        conflict_occurred=conflict_occurred,
        claim_efficiency=claimed_count / requested_count if requested_count > 0 else 0,
        **context
    )


def log_backoff_retry(
    logger: FilteringBoundLogger,
    operation: str,
    attempt: int,
    delay_ms: float,
    error: Exception,
    **context: Any
) -> None:
    """
    Log a backoff retry attempt.
    
    Args:
        logger: The logger instance
        operation: Name of the operation being retried
        attempt: Current attempt number
        delay_ms: Delay before this retry in milliseconds
        error: The error that triggered the retry
        **context: Additional context to include in the log
    """
    logger.warning(
        "Retrying operation after backoff",
        operation=operation,
        attempt=attempt,
        delay_ms=delay_ms,
        error_type=type(error).__name__,
        error_message=str(error),
        **context
    )


def log_step_transition(
    logger: FilteringBoundLogger,
    step_id: str,
    from_status: str,
    to_status: str,
    pod_id: str,
    **context: Any
) -> None:
    """
    Log a step status transition.
    
    Args:
        logger: The logger instance
        step_id: ID of the step
        from_status: Previous status
        to_status: New status
        pod_id: ID of the pod making the transition
        **context: Additional context to include in the log
    """
    logger.info(
        "Step status transition",
        step_id=step_id,
        from_status=from_status,
        to_status=to_status,
        pod_id=pod_id,
        **context
    )


def log_leader_election(
    logger: FilteringBoundLogger,
    current_leader: str,
    pod_id: str,
    is_leader: bool,
    active_pods: int,
    **context: Any
) -> None:
    """
    Log leader election information.
    
    Args:
        logger: The logger instance
        current_leader: ID of the current leader pod
        pod_id: ID of this pod
        is_leader: Whether this pod is the leader
        active_pods: Number of active pods
        **context: Additional context to include in the log
    """
    logger.info(
        "Leader election status",
        current_leader=current_leader,
        pod_id=pod_id,
        is_leader=is_leader,
        active_pods=active_pods,
        **context
    )


def log_recovery_cycle(
    logger: FilteringBoundLogger,
    cycle_id: str,
    dead_pods: list[str],
    orphaned_steps: int,
    requeued_steps: int,
    terminal_steps: int,
    duration_ms: float,
    **context: Any
) -> None:
    """
    Log recovery cycle summary.
    
    Args:
        logger: The logger instance
        cycle_id: Unique identifier for the recovery cycle
        dead_pods: List of dead pod IDs
        orphaned_steps: Number of orphaned steps found
        requeued_steps: Number of steps requeued
        terminal_steps: Number of steps marked as terminal
        duration_ms: Duration of the recovery cycle in milliseconds
        **context: Additional context to include in the log
    """
    logger.info(
        "Recovery cycle completed",
        cycle_id=cycle_id,
        dead_pods=dead_pods,
        dead_pod_count=len(dead_pods),
        orphaned_steps=orphaned_steps,
        requeued_steps=requeued_steps,
        terminal_steps=terminal_steps,
        duration_ms=duration_ms,
        **context
    )
