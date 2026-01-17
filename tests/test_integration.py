"""
Integration tests for FLEET-Q project setup.

Tests that all components work together correctly.
"""

import os
import pytest
from unittest.mock import patch, AsyncMock

from fleet_q.config import FleetQConfig, load_config
from fleet_q.logging_config import configure_logging
from fleet_q.main import FleetQApplication


class TestProjectSetup:
    """Test cases for project setup and foundation."""
    
    @patch.dict(os.environ, {
        "FLEET_Q_SNOWFLAKE_ACCOUNT": "test_account",
        "FLEET_Q_SNOWFLAKE_USER": "test_user",
        "FLEET_Q_SNOWFLAKE_PASSWORD": "test_password",
        "FLEET_Q_SNOWFLAKE_DATABASE": "test_db",
        "FLEET_Q_LOG_LEVEL": "DEBUG",
        "FLEET_Q_LOG_FORMAT": "console"
    })
    def test_full_application_initialization(self):
        """Test complete application initialization flow."""
        # Load configuration
        config = load_config()
        assert config.snowflake_account == "test_account"
        assert config.log_level == "DEBUG"
        assert config.log_format == "console"
        
        # Configure logging
        logger = configure_logging(config)
        assert logger is not None
        
        # Test application can be created
        app = FleetQApplication()
        assert app.config is None  # Not initialized yet
        assert app.logger is None
    
    @pytest.mark.asyncio
    async def test_application_initialization_async(self):
        """Test async application initialization."""
        # Set environment variables directly for this test
        test_env = {
            "FLEET_Q_SNOWFLAKE_ACCOUNT": "test_account",
            "FLEET_Q_SNOWFLAKE_USER": "test_user",
            "FLEET_Q_SNOWFLAKE_PASSWORD": "test_password",
            "FLEET_Q_SNOWFLAKE_DATABASE": "test_db"
        }
        
        with patch.dict(os.environ, test_env):
            app = FleetQApplication()
            
            # Mock the SnowflakeStorage to avoid real connection
            with patch('fleet_q.main.SnowflakeStorage') as mock_storage_class:
                mock_storage = AsyncMock()
                mock_storage.initialize = AsyncMock()
                mock_storage.close = AsyncMock()
                mock_storage_class.return_value = mock_storage
                
                # Initialize the application
                await app.initialize()
                
                # Verify initialization
                assert app.config is not None
                assert app.logger is not None
                assert app.config.snowflake_account == "test_account"
                assert app.storage is not None
                assert app.health_service is not None
                
                # Verify storage was initialized
                mock_storage.initialize.assert_called_once()
                
                # Test graceful stop
                await app.stop()
                
                # Verify storage was closed
                mock_storage.close.assert_called_once()
    
    def test_package_imports(self):
        """Test that all main package components can be imported."""
        # Test main package import
        import fleet_q
        assert fleet_q.__version__ == "0.1.0"
        
        # Test individual component imports
        from fleet_q.config import FleetQConfig
        from fleet_q.models import Step, StepPayload, PodHealth, StepStatus, PodStatus
        from fleet_q.logging_config import configure_logging
        from fleet_q.main import FleetQApplication
        
        # Verify classes can be instantiated (basic smoke test)
        assert FleetQConfig is not None
        assert Step is not None
        assert StepPayload is not None
        assert PodHealth is not None
        assert StepStatus.PENDING == "pending"
        assert PodStatus.UP == "up"
    
    @patch.dict(os.environ, {
        "FLEET_Q_SNOWFLAKE_ACCOUNT": "test_account",
        "FLEET_Q_SNOWFLAKE_USER": "test_user",
        "FLEET_Q_SNOWFLAKE_PASSWORD": "test_password",
        "FLEET_Q_SNOWFLAKE_DATABASE": "test_db",
        "FLEET_Q_MAX_PARALLELISM": "20",
        "FLEET_Q_CAPACITY_THRESHOLD": "0.9"
    })
    def test_environment_configuration_integration(self):
        """Test that environment variables are properly loaded and validated."""
        config = load_config()
        
        # Verify environment variables are loaded
        assert config.snowflake_account == "test_account"
        assert config.max_parallelism == 20
        assert config.capacity_threshold == 0.9
        
        # Verify calculated properties work
        assert config.max_concurrent_steps == 18  # 20 * 0.9
        
        # Verify Snowflake connection params
        params = config.snowflake_connection_params
        assert params["account"] == "test_account"
        assert params["user"] == "test_user"
        assert params["password"] == "test_password"
        assert params["database"] == "test_db"