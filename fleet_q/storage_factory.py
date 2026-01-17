"""
Storage factory for FLEET-Q.

Provides a factory function to create the appropriate storage backend
based on configuration. Supports both Snowflake (production) and 
SQLite simulation (development/testing) modes.
"""

import logging
from typing import Tuple

from .config import FleetQConfig
from .storage import StorageInterface, SQLiteStorageInterface
from .snowflake_storage import SnowflakeStorage
from .sqlite_storage import SQLiteStorage
from .sqlite_simulation_storage import SQLiteSimulationStorage

logger = logging.getLogger(__name__)


def create_storage_backends(config: FleetQConfig) -> Tuple[StorageInterface, SQLiteStorageInterface]:
    """
    Create storage backends based on configuration.
    
    Args:
        config: FleetQ configuration
        
    Returns:
        Tuple[StorageInterface, SQLiteStorageInterface]: Main storage and SQLite storage
        
    Raises:
        ValueError: If storage mode is not supported
    """
    storage_mode = config.storage_mode.lower()
    
    if storage_mode == "snowflake":
        # Production mode: Snowflake for main storage, SQLite for DLQ
        logger.info("Using Snowflake storage backend for production")
        main_storage = SnowflakeStorage(config)
        sqlite_storage = SQLiteStorage(config)
        return main_storage, sqlite_storage
        
    elif storage_mode == "sqlite":
        # Development/simulation mode: SQLite for everything
        logger.info("Using SQLite simulation storage backend for development")
        simulation_storage = SQLiteSimulationStorage(config)
        # Use the same instance for both interfaces since it implements both
        return simulation_storage, simulation_storage
        
    else:
        raise ValueError(f"Unsupported storage mode: {storage_mode}. Must be 'snowflake' or 'sqlite'")


async def initialize_storage_backends(
    main_storage: StorageInterface, 
    sqlite_storage: SQLiteStorageInterface
) -> None:
    """
    Initialize both storage backends.
    
    Args:
        main_storage: Main storage backend
        sqlite_storage: SQLite storage backend
        
    Raises:
        Exception: If initialization fails
    """
    try:
        # Initialize main storage
        await main_storage.initialize()
        logger.info("Main storage backend initialized successfully")
        
        # Initialize SQLite storage (only if it's different from main storage)
        if sqlite_storage is not main_storage:
            await sqlite_storage.initialize()
            logger.info("SQLite storage backend initialized successfully")
        else:
            logger.info("SQLite storage backend shares main storage (simulation mode)")
            
    except Exception as e:
        logger.error(f"Failed to initialize storage backends: {e}")
        raise


async def close_storage_backends(
    main_storage: StorageInterface, 
    sqlite_storage: SQLiteStorageInterface
) -> None:
    """
    Close both storage backends gracefully.
    
    Args:
        main_storage: Main storage backend
        sqlite_storage: SQLite storage backend
    """
    try:
        # Close SQLite storage first (only if it's different from main storage)
        if sqlite_storage is not main_storage:
            await sqlite_storage.close()
            logger.info("SQLite storage backend closed")
        
        # Close main storage
        await main_storage.close()
        logger.info("Main storage backend closed")
        
    except Exception as e:
        logger.error(f"Error closing storage backends: {e}")