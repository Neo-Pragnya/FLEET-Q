"""
Configuration management for FLEET-Q.

Handles environment variable loading, validation, and provides sensible defaults
for all configuration parameters. 
"""

import os
import uuid
from typing import Optional
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings


def generate_pod_id() -> str:
    """Generate a unique pod ID for this instance."""
    hostname = os.getenv("HOSTNAME", "unknown")
    unique_suffix = str(uuid.uuid4())[:8]
    return f"pod-{hostname}-{unique_suffix}"


class FleetQConfig(BaseSettings):
    """
    FLEET-Q configuration loaded from environment variables.
    
    All configuration parameters can be set via environment variables with
    the FLEET_Q_ prefix. For example, FLEET_Q_SNOWFLAKE_ACCOUNT sets the
    snowflake_account field.
    """
    
    # Snowflake Connection Configuration
    snowflake_account: str = Field(
        ..., 
        description="Snowflake account identifier"
    )
    snowflake_user: str = Field(
        ..., 
        description="Snowflake username"
    )
    snowflake_password: str = Field(
        ..., 
        description="Snowflake password"
    )
    snowflake_database: str = Field(
        ..., 
        description="Snowflake database name"
    )
    snowflake_schema: str = Field(
        default="PUBLIC", 
        description="Snowflake schema name"
    )
    snowflake_warehouse: Optional[str] = Field(
        default=None, 
        description="Snowflake warehouse name (optional)"
    )
    snowflake_role: Optional[str] = Field(
        default=None, 
        description="Snowflake role name (optional)"
    )
    
    # Pod Configuration
    pod_id: str = Field(
        default_factory=generate_pod_id,
        description="Unique identifier for this pod instance"
    )
    max_parallelism: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of concurrent steps this pod can execute"
    )
    capacity_threshold: float = Field(
        default=0.8,
        ge=0.1,
        le=1.0,
        description="Percentage of max_parallelism to use for capacity calculation"
    )
    
    # Timing Configuration
    heartbeat_interval_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Interval between heartbeat updates in seconds"
    )
    claim_interval_seconds: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Interval between step claiming attempts in seconds"
    )
    leader_check_interval_seconds: int = Field(
        default=60,
        ge=10,
        le=600,
        description="Interval between leader election checks in seconds"
    )
    recovery_interval_seconds: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="Interval between recovery cycles in seconds"
    )
    dead_pod_threshold_seconds: int = Field(
        default=180,
        ge=60,
        le=1800,
        description="Threshold for considering a pod dead based on stale heartbeat"
    )
    
    # Backoff Configuration
    default_max_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Default maximum retry attempts for backoff operations"
    )
    default_base_delay_ms: int = Field(
        default=100,
        ge=10,
        le=5000,
        description="Default base delay in milliseconds for backoff"
    )
    default_max_delay_ms: int = Field(
        default=30000,
        ge=1000,
        le=300000,
        description="Default maximum delay in milliseconds for backoff"
    )
    
    # API Configuration
    api_host: str = Field(
        default="0.0.0.0",
        description="Host address for the FastAPI server"
    )
    api_port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="Port for the FastAPI server"
    )
    
    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    log_format: str = Field(
        default="json",
        description="Log format (json or console)"
    )
    
    # SQLite Configuration (for local DLQ)
    sqlite_db_path: str = Field(
        default=":memory:",
        description="Path to SQLite database file (use :memory: for in-memory)"
    )
    
    # Storage Configuration
    storage_mode: str = Field(
        default="snowflake",
        description="Storage backend mode (snowflake or sqlite)"
    )
    
    model_config = {"env_prefix": "FLEET_Q_", "case_sensitive": False}
        
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level is one of the standard Python logging levels."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()
    
    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v):
        """Validate log format is supported."""
        valid_formats = {"json", "console"}
        if v.lower() not in valid_formats:
            raise ValueError(f"log_format must be one of {valid_formats}")
        return v.lower()
    
    @field_validator("storage_mode")
    @classmethod
    def validate_storage_mode(cls, v):
        """Validate storage mode is supported."""
        valid_modes = {"snowflake", "sqlite"}
        if v.lower() not in valid_modes:
            raise ValueError(f"storage_mode must be one of {valid_modes}")
        return v.lower()
    
    @field_validator("capacity_threshold")
    @classmethod
    def validate_capacity_threshold(cls, v):
        """Ensure capacity threshold is reasonable."""
        if v <= 0 or v > 1:
            raise ValueError("capacity_threshold must be between 0 and 1")
        return v
    
    @field_validator("dead_pod_threshold_seconds")
    @classmethod
    def validate_dead_pod_threshold(cls, v, info):
        """Ensure dead pod threshold is reasonable compared to heartbeat interval."""
        if info.data:
            heartbeat_interval = info.data.get("heartbeat_interval_seconds", 30)
            if v < heartbeat_interval * 2:
                raise ValueError(
                    f"dead_pod_threshold_seconds ({v}) must be at least 2x "
                    f"heartbeat_interval_seconds ({heartbeat_interval})"
                )
        return v
    
    @model_validator(mode='after')
    def validate_timing_relationships(self):
        """Validate relationships between timing configuration fields."""
        # Validate recovery interval vs dead pod threshold
        if self.recovery_interval_seconds < self.dead_pod_threshold_seconds:
            raise ValueError(
                f"recovery_interval_seconds ({self.recovery_interval_seconds}) should be at least equal to "
                f"dead_pod_threshold_seconds ({self.dead_pod_threshold_seconds}) to avoid "
                f"premature recovery attempts"
            )
        
        # Validate max delay vs base delay
        if self.default_max_delay_ms <= self.default_base_delay_ms:
            raise ValueError(
                f"default_max_delay_ms ({self.default_max_delay_ms}) must be greater than "
                f"default_base_delay_ms ({self.default_base_delay_ms})"
            )
        
        return self
    
    @field_validator("snowflake_account")
    @classmethod
    def validate_snowflake_account(cls, v, info):
        """Validate Snowflake account format."""
        # Skip validation if using SQLite storage mode
        if info.data and info.data.get("storage_mode", "snowflake").lower() == "sqlite":
            return v or "not-required-for-sqlite"
            
        if not v or not isinstance(v, str):
            raise ValueError("snowflake_account must be a non-empty string")
        # Basic format validation - Snowflake accounts typically have specific patterns
        if len(v.strip()) == 0:
            raise ValueError("snowflake_account cannot be empty or whitespace only")
        return v.strip()
    
    @field_validator("snowflake_user", "snowflake_password", "snowflake_database")
    @classmethod
    def validate_required_snowflake_fields(cls, v, info):
        """Validate required Snowflake connection fields."""
        # Skip validation if using SQLite storage mode
        if info.data and info.data.get("storage_mode", "snowflake").lower() == "sqlite":
            return v or "not-required-for-sqlite"
            
        if not v or not isinstance(v, str):
            raise ValueError("Snowflake connection fields must be non-empty strings")
        if len(v.strip()) == 0:
            raise ValueError("Snowflake connection fields cannot be empty or whitespace only")
        return v.strip()
    
    @property
    def max_concurrent_steps(self) -> int:
        """Calculate the maximum number of concurrent steps based on capacity."""
        return int(self.max_parallelism * self.capacity_threshold)
    
    @property
    def snowflake_connection_params(self) -> dict:
        """Get Snowflake connection parameters as a dictionary."""
        params = {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "password": self.snowflake_password,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
        }
        
        if self.snowflake_warehouse:
            params["warehouse"] = self.snowflake_warehouse
        if self.snowflake_role:
            params["role"] = self.snowflake_role
            
        return params


def load_config() -> FleetQConfig:
    """
    Load and validate configuration from environment variables.
    
    Returns:
        FleetQConfig: Validated configuration instance
        
    Raises:
        ValueError: If configuration validation fails with detailed error context
        pydantic.ValidationError: If required fields are missing or invalid
    """
    try:
        return FleetQConfig()
    except Exception as e:
        # Extract specific validation errors for better user experience
        error_details = []
        
        if hasattr(e, 'errors'):
            # Pydantic validation errors
            for error in e.errors():
                field = '.'.join(str(loc) for loc in error['loc'])
                message = error['msg']
                error_details.append(f"  - {field}: {message}")
        
        if error_details:
            detailed_message = (
                f"Configuration validation failed with the following errors:\n"
                f"{''.join(error_details)}\n\n"
                f"Please check your environment variables with FLEET_Q_ prefix.\n"
                f"Required variables: FLEET_Q_SNOWFLAKE_ACCOUNT, FLEET_Q_SNOWFLAKE_USER, "
                f"FLEET_Q_SNOWFLAKE_PASSWORD, FLEET_Q_SNOWFLAKE_DATABASE\n"
                f"Example: export FLEET_Q_SNOWFLAKE_ACCOUNT=myaccount.region"
            )
        else:
            detailed_message = (
                f"Configuration validation failed: {str(e)}. "
                f"Please check your environment variables with FLEET_Q_ prefix."
            )
        
        raise ValueError(detailed_message) from e


def validate_config_at_startup(config: FleetQConfig) -> None:
    """
    Perform additional runtime validation of configuration.
    
    This function performs validation that requires runtime context
    or external resource checks that can't be done in Pydantic validators.
    
    Args:
        config: The loaded configuration to validate
        
    Raises:
        ValueError: If runtime validation fails
    """
    # Validate that capacity calculations make sense
    max_concurrent = config.max_concurrent_steps
    if max_concurrent < 1:
        raise ValueError(
            f"Calculated max_concurrent_steps ({max_concurrent}) must be at least 1. "
            f"Check max_parallelism ({config.max_parallelism}) and "
            f"capacity_threshold ({config.capacity_threshold})"
        )
    
    # Validate timing relationships
    if config.claim_interval_seconds >= config.heartbeat_interval_seconds:
        raise ValueError(
            f"claim_interval_seconds ({config.claim_interval_seconds}) should be "
            f"less than heartbeat_interval_seconds ({config.heartbeat_interval_seconds}) "
            f"for optimal performance"
        )
    
    # Validate backoff configuration makes sense
    if config.default_max_attempts < 1:
        raise ValueError(f"default_max_attempts must be at least 1")
    
    if config.default_base_delay_ms >= config.default_max_delay_ms:
        raise ValueError(
            f"default_base_delay_ms ({config.default_base_delay_ms}) must be "
            f"less than default_max_delay_ms ({config.default_max_delay_ms})"
        )