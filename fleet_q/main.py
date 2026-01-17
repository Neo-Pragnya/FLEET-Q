"""
Main entry point for FLEET-Q application.

Provides the main function for starting the FLEET-Q service and handles
application initialization, configuration loading, and graceful shutdown.
Integrates FastAPI server with background task coordination.
"""

import asyncio
import signal
import sys
import uvicorn
from typing import Optional

from .config import load_config, FleetQConfig
from .logging_config import configure_logging
from .models import PodStatus
from .snowflake_storage import SnowflakeStorage
from .sqlite_storage import SQLiteStorage
from .health_service import create_health_service
from .recovery_service import create_recovery_service
from .claim_service import create_claim_service
from .api import create_app
from .step_service import create_step_service


class FleetQApplication:
    """
    Main FLEET-Q application class.
    
    Coordinates all components of the system including configuration,
    logging, storage, health service, recovery service, and FastAPI server.
    Implements background task coordination using asyncio.
    """
    
    def __init__(self):
        self.config: Optional[FleetQConfig] = None
        self.logger = None
        self.storage: Optional[SnowflakeStorage] = None
        self.sqlite_storage: Optional[SQLiteStorage] = None
        self.health_service = None
        self.recovery_service = None
        self.claim_service = None
        self.step_service = None
        self.fastapi_app = None
        self.uvicorn_server = None
        self.shutdown_event = asyncio.Event()
        self.background_tasks = []
        
    async def initialize(self) -> None:
        """
        Initialize the FLEET-Q application.
        
        Loads configuration, sets up logging, initializes storage backends,
        and prepares all components. Implements fail-fast validation.
        """
        try:
            # Load and validate configuration with enhanced error handling
            self.config = load_config()
            
            # Configure structured logging
            self.logger = configure_logging(self.config)
            
            # Perform additional runtime configuration validation
            from .config import validate_config_at_startup
            validate_config_at_startup(self.config)
            
            self.logger.info(
                "Configuration loaded and validated successfully",
                pod_id=self.config.pod_id,
                max_parallelism=self.config.max_parallelism,
                max_concurrent_steps=self.config.max_concurrent_steps,
                snowflake_account=self.config.snowflake_account,
                snowflake_database=self.config.snowflake_database,
                log_level=self.config.log_level,
                log_format=self.config.log_format
            )
            
            # Initialize storage backends using factory
            from .storage_factory import create_storage_backends, initialize_storage_backends
            self.storage, self.sqlite_storage = create_storage_backends(self.config)
            await initialize_storage_backends(self.storage, self.sqlite_storage)
            
            # Initialize step service
            self.step_service = create_step_service(self.storage, self.config)
            
            # Initialize health service
            self.health_service = create_health_service(self.storage, self.config)
            
            # Initialize claim service
            self.claim_service = create_claim_service(self.storage, self.config)
            
            # Initialize recovery service
            self.recovery_service = create_recovery_service(
                self.storage, 
                self.sqlite_storage, 
                self.config
            )
            
            # Create FastAPI application
            self.fastapi_app = create_app(self.storage, self.config, self.sqlite_storage)
            
            # Set up FastAPI startup and shutdown handlers
            self._setup_fastapi_handlers()
            
            self.logger.info(
                "FLEET-Q application initialized successfully",
                pod_id=self.config.pod_id,
                max_parallelism=self.config.max_parallelism,
                capacity_threshold=self.config.capacity_threshold,
                heartbeat_interval=self.config.heartbeat_interval_seconds,
                claim_interval=self.config.claim_interval_seconds,
                recovery_interval=self.config.recovery_interval_seconds,
                dead_pod_threshold=self.config.dead_pod_threshold_seconds,
                api_host=self.config.api_host,
                api_port=self.config.api_port,
            )
            
        except Exception as e:
            # Fail fast with clear error message
            print(f"Failed to initialize FLEET-Q: {e}", file=sys.stderr)
            sys.exit(1)
    
    def _setup_fastapi_handlers(self) -> None:
        """Set up FastAPI startup and shutdown event handlers."""
        
        @self.fastapi_app.on_event("startup")
        async def startup_handler():
            """FastAPI startup handler - starts background loops."""
            self.logger.info("FastAPI startup handler called")
            await self._start_background_loops()
        
        @self.fastapi_app.on_event("shutdown")
        async def shutdown_handler():
            """FastAPI shutdown handler - stops background loops."""
            self.logger.info("FastAPI shutdown handler called")
            await self._stop_background_loops()
    
    async def _start_background_loops(self) -> None:
        """Start all background loops as asyncio tasks."""
        try:
            self.logger.info("Starting background loops")
            
            # Start health service heartbeat loop
            if self.health_service:
                health_task = asyncio.create_task(
                    self.health_service.start(),
                    name="health_service"
                )
                self.background_tasks.append(health_task)
            
            # Start claim service loop
            if self.claim_service:
                claim_task = asyncio.create_task(
                    self.claim_service.start(),
                    name="claim_service"
                )
                self.background_tasks.append(claim_task)
            
            # Start recovery service loop
            if self.recovery_service:
                recovery_task = asyncio.create_task(
                    self.recovery_service.start(),
                    name="recovery_service"
                )
                self.background_tasks.append(recovery_task)
            
            self.logger.info(f"Started {len(self.background_tasks)} background tasks")
            
        except Exception as e:
            self.logger.error(f"Failed to start background loops: {e}")
            raise
    
    async def _stop_background_loops(self) -> None:
        """Stop all background loops gracefully."""
        try:
            self.logger.info("Stopping background loops")
            
            # Stop claim service
            if self.claim_service:
                try:
                    await self.claim_service.stop()
                except Exception as e:
                    self.logger.error(f"Error stopping claim service: {e}")
            
            # Stop recovery service first
            if self.recovery_service:
                try:
                    await self.recovery_service.stop()
                except Exception as e:
                    self.logger.error(f"Error stopping recovery service: {e}")
            
            # Stop health service
            if self.health_service:
                try:
                    await self.health_service.stop()
                except Exception as e:
                    self.logger.error(f"Error stopping health service: {e}")
            
            # Cancel any remaining background tasks
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        self.logger.error(f"Error cancelling background task {task.get_name()}: {e}")
            
            self.background_tasks.clear()
            self.logger.info("Background loops stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping background loops: {e}")
    
    async def start(self) -> None:
        """
        Start the FLEET-Q application.
        
        Starts the FastAPI server which will trigger background services
        through the startup handler.
        """
        await self.initialize()
        
        self.logger.info("FLEET-Q application starting")
        
        # Set up signal handlers for graceful shutdown
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)
        
        try:
            # Create uvicorn server configuration
            uvicorn_config = uvicorn.Config(
                app=self.fastapi_app,
                host=self.config.api_host,
                port=self.config.api_port,
                log_level=self.config.log_level.lower(),
                access_log=True,
                loop="asyncio"
            )
            
            # Create and start uvicorn server
            self.uvicorn_server = uvicorn.Server(uvicorn_config)
            
            self.logger.info(
                f"Starting FastAPI server on {self.config.api_host}:{self.config.api_port}",
                api_host=self.config.api_host,
                api_port=self.config.api_port
            )
            
            # Start the server (this will block until shutdown)
            await self.uvicorn_server.serve()
            
        except Exception as e:
            self.logger.error(f"Error during application startup: {e}")
            raise
        finally:
            self.logger.info("FLEET-Q application shutting down")
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals gracefully."""
        if self.logger:
            self.logger.info(f"Received signal {signum}, initiating shutdown")
        
        # Trigger uvicorn server shutdown
        if self.uvicorn_server:
            self.uvicorn_server.should_exit = True
        
        self.shutdown_event.set()
    
    async def stop(self) -> None:
        """
        Stop the FLEET-Q application gracefully.
        
        Implements graceful shutdown of all services in reverse order.
        """
        if self.logger:
            self.logger.info("FLEET-Q application stopping")
        
        # Stop uvicorn server
        if self.uvicorn_server:
            try:
                self.uvicorn_server.should_exit = True
                # The FastAPI shutdown handler will handle background loops
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error stopping uvicorn server: {e}")
        
        # Close storage backends using factory
        from .storage_factory import close_storage_backends
        await close_storage_backends(self.storage, self.sqlite_storage)
        
        if self.logger:
            self.logger.info("FLEET-Q application stopped")


async def async_main() -> None:
    """Async main function for running FLEET-Q."""
    app = FleetQApplication()
    try:
        await app.start()
    except KeyboardInterrupt:
        pass
    finally:
        await app.stop()


def run_server() -> None:
    """
    Run the FLEET-Q FastAPI server.
    
    This function is used for running the server directly with uvicorn
    for development purposes.
    """
    try:
        # Load configuration
        config = load_config()
        
        # Configure logging
        logger = configure_logging(config)
        
        # Create storage (this will be done properly in the startup handler)
        # For now, we'll let the FastAPI startup handler handle initialization
        
        # Import here to avoid circular imports
        from .api import FleetQAPI
        from .snowflake_storage import SnowflakeStorage
        
        # Create a minimal app for development
        storage = SnowflakeStorage(config)
        api = FleetQAPI(storage, config)
        app = api.app
        
        # Run with uvicorn
        uvicorn.run(
            app,
            host=config.api_host,
            port=config.api_port,
            log_level=config.log_level.lower(),
            access_log=True
        )
        
    except Exception as e:
        print(f"Failed to start server: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    """
    Main entry point for FLEET-Q application.
    
    This function is used as the console script entry point defined in pyproject.toml.
    """
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()