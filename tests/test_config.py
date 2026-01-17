"""
Tests for FLEET-Q configuration management.

Tests configuration loading, validation, and environment variable handling.
"""

import os
import pytest
from unittest.mock import patch
from pydantic import ValidationError

from fleet_q.config import FleetQConfig, load_config, generate_pod_id, validate_config_at_startup


class TestFleetQConfig:
    """Test cases for FleetQConfig class."""
    
    def test_generate_pod_id(self):
        """Test pod ID generation creates unique identifiers."""
        pod_id1 = generate_pod_id()
        pod_id2 = generate_pod_id()
        
        assert pod_id1 != pod_id2
        assert pod_id1.startswith("pod-")
        assert pod_id2.startswith("pod-")
    
    def test_config_with_minimal_required_fields(self):
        """Test configuration with only required Snowflake fields."""
        config = FleetQConfig(
            snowflake_account="test_account",
            snowflake_user="test_user", 
            snowflake_password="test_password",
            snowflake_database="test_db"
        )
        
        assert config.snowflake_account == "test_account"
        assert config.snowflake_user == "test_user"
        assert config.snowflake_password == "test_password"
        assert config.snowflake_database == "test_db"
        assert config.snowflake_schema == "PUBLIC"  # default
        assert config.max_parallelism == 10  # default
        assert config.capacity_threshold == 0.8  # default
    
    def test_config_validation_errors(self):
        """Test configuration validation catches invalid values."""
        # Missing required fields
        with pytest.raises(ValidationError):
            FleetQConfig()
        
        # Invalid capacity threshold
        with pytest.raises(ValidationError):
            FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                capacity_threshold=1.5  # > 1.0
            )
        
        # Invalid log level
        with pytest.raises(ValidationError):
            FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                log_level="INVALID"
            )
    
    def test_enhanced_validation_errors(self):
        """Test enhanced configuration validation catches additional invalid values."""
        # Empty snowflake account
        with pytest.raises(ValidationError, match="cannot be empty"):
            FleetQConfig(
                snowflake_account="   ",  # whitespace only
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test"
            )
        
        # Invalid timing relationships
        with pytest.raises(ValidationError, match="must be at least 2x"):
            FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                heartbeat_interval_seconds=60,
                dead_pod_threshold_seconds=90  # Less than 2x heartbeat
            )
        
        # Invalid backoff configuration - using model validator now
        with pytest.raises(ValidationError, match="must be greater than"):
            FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                default_base_delay_ms=2000,
                default_max_delay_ms=1500  # Less than base delay
            )
    
    def test_recovery_interval_validation(self):
        """Test recovery interval validation against dead pod threshold."""
        # This test validates our custom validator logic
        # Note: recovery_interval_seconds has a minimum of 60 from field constraints
        with pytest.raises(ValidationError, match="should be at least equal"):
            FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                dead_pod_threshold_seconds=300,
                recovery_interval_seconds=200  # Less than dead pod threshold but > 60
            )
    
    def test_max_concurrent_steps_calculation(self):
        """Test capacity calculation property."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            max_parallelism=10,
            capacity_threshold=0.8
        )
        
        assert config.max_concurrent_steps == 8  # 10 * 0.8
    
    def test_snowflake_connection_params(self):
        """Test Snowflake connection parameter generation."""
        config = FleetQConfig(
            snowflake_account="test_account",
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_database="test_db",
            snowflake_schema="test_schema",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role"
        )
        
        params = config.snowflake_connection_params
        expected = {
            "account": "test_account",
            "user": "test_user", 
            "password": "test_password",
            "database": "test_db",
            "schema": "test_schema",
            "warehouse": "test_warehouse",
            "role": "test_role"
        }
        
        assert params == expected
    
    @patch.dict(os.environ, {
        "FLEET_Q_SNOWFLAKE_ACCOUNT": "env_account",
        "FLEET_Q_SNOWFLAKE_USER": "env_user",
        "FLEET_Q_SNOWFLAKE_PASSWORD": "env_password", 
        "FLEET_Q_SNOWFLAKE_DATABASE": "env_db",
        "FLEET_Q_MAX_PARALLELISM": "20"
    })
    def test_environment_variable_loading(self):
        """Test configuration loading from environment variables."""
        config = FleetQConfig()
        
        assert config.snowflake_account == "env_account"
        assert config.snowflake_user == "env_user"
        assert config.snowflake_password == "env_password"
        assert config.snowflake_database == "env_db"
        assert config.max_parallelism == 20


class TestLoadConfig:
    """Test cases for load_config function."""
    
    @patch.dict(os.environ, {
        "FLEET_Q_SNOWFLAKE_ACCOUNT": "test_account",
        "FLEET_Q_SNOWFLAKE_USER": "test_user",
        "FLEET_Q_SNOWFLAKE_PASSWORD": "test_password",
        "FLEET_Q_SNOWFLAKE_DATABASE": "test_db"
    })
    def test_load_config_success(self):
        """Test successful configuration loading."""
        config = load_config()
        assert isinstance(config, FleetQConfig)
        assert config.snowflake_account == "test_account"
    
    def test_load_config_failure(self):
        """Test configuration loading failure with clear error message."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                load_config()
            
            assert "Configuration validation failed" in str(exc_info.value)
            assert "FLEET_Q_" in str(exc_info.value)
            assert "Required variables" in str(exc_info.value)
    
    def test_load_config_detailed_error_messages(self):
        """Test that detailed error messages are provided for specific validation failures."""
        with patch.dict(os.environ, {
            "FLEET_Q_SNOWFLAKE_ACCOUNT": "test",
            "FLEET_Q_SNOWFLAKE_USER": "test",
            "FLEET_Q_SNOWFLAKE_PASSWORD": "test",
            "FLEET_Q_SNOWFLAKE_DATABASE": "test",
            "FLEET_Q_CAPACITY_THRESHOLD": "1.5"  # Invalid value
        }):
            with pytest.raises(ValueError) as exc_info:
                load_config()
            
            error_message = str(exc_info.value)
            assert "Configuration validation failed with the following errors" in error_message
            assert "capacity_threshold" in error_message


class TestValidateConfigAtStartup:
    """Test cases for validate_config_at_startup function."""
    
    def test_valid_config_passes_validation(self):
        """Test that valid configuration passes runtime validation."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            max_parallelism=10,
            capacity_threshold=0.8,
            claim_interval_seconds=5,
            heartbeat_interval_seconds=30
        )
        
        # Should not raise any exception
        validate_config_at_startup(config)
    
    def test_zero_max_concurrent_steps_fails(self):
        """Test that configuration resulting in zero max concurrent steps fails."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            max_parallelism=1,
            capacity_threshold=0.1  # Results in 0 max concurrent steps
        )
        
        with pytest.raises(ValueError, match="max_concurrent_steps.*must be at least 1"):
            validate_config_at_startup(config)
    
    def test_claim_interval_too_large_fails(self):
        """Test that claim interval >= heartbeat interval fails validation."""
        config = FleetQConfig(
            snowflake_account="test",
            snowflake_user="test",
            snowflake_password="test",
            snowflake_database="test",
            claim_interval_seconds=60,
            heartbeat_interval_seconds=30  # Less than claim interval
        )
        
        with pytest.raises(ValueError, match="claim_interval_seconds.*should be less than"):
            validate_config_at_startup(config)
    
    def test_zero_max_attempts_fails(self):
        """Test that zero max attempts fails validation due to field constraints."""
        # This is caught by Pydantic field constraints (ge=1)
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            FleetQConfig(
                snowflake_account="test",
                snowflake_user="test",
                snowflake_password="test",
                snowflake_database="test",
                default_max_attempts=0
            )