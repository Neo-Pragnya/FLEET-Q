"""
Pod health tracking service for FLEET-Q.

Implements the pod health tracking system that allows pods to register themselves,
send regular heartbeats, and handle graceful shutdown. This is essential for the
leader election and recovery systems.

Provides:
- Heartbeat loop with configurable intervals
- Pod registration with birth timestamp and metadata
- Graceful shutdown with status updates
- Pod metadata collection and reporting
"""

import asyncio
import logging
import platform
import psutil
import threading
from datetime import datetime
from typing import Dict, Any, Optional, List

from .config import FleetQConfig
from .models import PodHealth, PodStatus
from .storage import StorageInterface, create_pod_health_record

logger = logging.getLogger(__name__)


class HealthService:
    """
    Service for managing pod health tracking and heartbeat operations.
    
    Handles pod registration, regular heartbeat updates, and graceful shutdown
    status updates. Collects and reports pod metadata for operational visibility.
    """
    
    def __init__(self, storage: StorageInterface, config: FleetQConfig):
        """
        Initialize the health service.
        
        Args:
            storage: Storage backend for pod health operations
            config: Application configuration
        """
        self.storage = storage
        self.config = config
        self.pod_id = config.pod_id
        self.birth_ts = datetime.utcnow()
        
        # Control flags for the heartbeat loop
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._is_running = False
        
        # Pod metadata cache
        self._metadata_cache: Optional[Dict[str, Any]] = None
        self._metadata_lock = threading.Lock()
        
        logger.info(f"Health service initialized for pod {self.pod_id}")
    
    def collect_pod_metadata(self) -> Dict[str, Any]:
        """
        Collect current pod metadata for heartbeat reporting.
        
        Gathers system information including CPU count, memory, Python version,
        and other operational metadata useful for monitoring and debugging.
        
        Returns:
            Dict[str, Any]: Pod metadata dictionary
        """
        with self._metadata_lock:
            # Cache metadata for a short time to avoid expensive system calls
            if self._metadata_cache is None:
                try:
                    # System information
                    cpu_count = psutil.cpu_count()
                    memory_info = psutil.virtual_memory()
                    
                    # Process information
                    process = psutil.Process()
                    process_info = process.as_dict(attrs=[
                        'pid', 'ppid', 'name', 'create_time', 'num_threads'
                    ])
                    
                    # Platform information
                    platform_info = {
                        'system': platform.system(),
                        'release': platform.release(),
                        'machine': platform.machine(),
                        'python_version': platform.python_version()
                    }
                    
                    # FLEET-Q specific metadata
                    fleet_q_info = {
                        'max_parallelism': self.config.max_parallelism,
                        'capacity_threshold': self.config.capacity_threshold,
                        'max_concurrent_steps': self.config.max_concurrent_steps,
                        'heartbeat_interval': self.config.heartbeat_interval_seconds,
                        'version': '1.0.0'  # TODO: Get from package metadata
                    }
                    
                    self._metadata_cache = {
                        'system': {
                            'cpu_count': cpu_count,
                            'memory_total_gb': round(memory_info.total / (1024**3), 2),
                            'memory_available_gb': round(memory_info.available / (1024**3), 2),
                            **platform_info
                        },
                        'process': process_info,
                        'fleet_q': fleet_q_info,
                        'collected_at': datetime.utcnow().isoformat()
                    }
                    
                except Exception as e:
                    logger.warning(f"Failed to collect some pod metadata: {e}")
                    # Return minimal metadata on error
                    self._metadata_cache = {
                        'fleet_q': {
                            'max_parallelism': self.config.max_parallelism,
                            'capacity_threshold': self.config.capacity_threshold,
                            'max_concurrent_steps': self.config.max_concurrent_steps,
                            'version': '1.0.0'
                        },
                        'error': str(e),
                        'collected_at': datetime.utcnow().isoformat()
                    }
            
            return self._metadata_cache.copy()
    
    def invalidate_metadata_cache(self) -> None:
        """
        Invalidate the metadata cache to force fresh collection on next heartbeat.
        
        Should be called periodically or when system state might have changed
        significantly.
        """
        with self._metadata_lock:
            self._metadata_cache = None
    
    async def register_pod(self) -> None:
        """
        Register this pod in the POD_HEALTH table.
        
        Creates initial pod health record with birth timestamp, UP status,
        and current metadata. This should be called once during pod startup.
        
        Raises:
            Exception: If pod registration fails
        """
        try:
            logger.info(f"Registering pod {self.pod_id}")
            
            # Collect initial metadata
            metadata = self.collect_pod_metadata()
            
            # Create pod health record
            pod_health = create_pod_health_record(
                pod_id=self.pod_id,
                metadata=metadata
            )
            
            # Override birth timestamp with our cached value
            pod_health.birth_ts = self.birth_ts
            pod_health.last_heartbeat_ts = self.birth_ts
            
            # Store in database
            await self.storage.upsert_pod_health(pod_health)
            
            logger.info(
                f"Successfully registered pod {self.pod_id}",
                extra={
                    'pod_id': self.pod_id,
                    'birth_ts': self.birth_ts.isoformat(),
                    'max_parallelism': self.config.max_parallelism,
                    'capacity_threshold': self.config.capacity_threshold
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to register pod {self.pod_id}: {e}")
            raise
    
    async def send_heartbeat(self) -> None:
        """
        Send a heartbeat update to the POD_HEALTH table.
        
        Updates the last_heartbeat_ts field and refreshes pod metadata.
        This method is called regularly by the heartbeat loop.
        
        Raises:
            Exception: If heartbeat update fails
        """
        try:
            current_time = datetime.utcnow()
            
            # Collect current metadata
            metadata = self.collect_pod_metadata()
            
            # Create pod health record for update
            pod_health = PodHealth(
                pod_id=self.pod_id,
                birth_ts=self.birth_ts,  # Birth timestamp never changes
                last_heartbeat_ts=current_time,
                status=PodStatus.UP,
                meta=metadata
            )
            
            # Update in database
            await self.storage.upsert_pod_health(pod_health)
            
            logger.debug(
                f"Heartbeat sent for pod {self.pod_id}",
                extra={
                    'pod_id': self.pod_id,
                    'heartbeat_ts': current_time.isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to send heartbeat for pod {self.pod_id}: {e}")
            # Don't re-raise - heartbeat failures shouldn't crash the service
            # The pod will be marked as dead by other pods if heartbeats consistently fail
    
    async def mark_pod_down(self) -> None:
        """
        Mark this pod as DOWN in the POD_HEALTH table.
        
        Updates the pod status to DOWN for graceful shutdown. This should be
        called during application shutdown to indicate the pod is no longer
        available for work.
        
        Raises:
            Exception: If status update fails
        """
        try:
            logger.info(f"Marking pod {self.pod_id} as DOWN")
            
            current_time = datetime.utcnow()
            
            # Collect final metadata
            metadata = self.collect_pod_metadata()
            metadata['shutdown_ts'] = current_time.isoformat()
            
            # Create pod health record for shutdown
            pod_health = PodHealth(
                pod_id=self.pod_id,
                birth_ts=self.birth_ts,
                last_heartbeat_ts=current_time,
                status=PodStatus.DOWN,
                meta=metadata
            )
            
            # Update in database
            await self.storage.upsert_pod_health(pod_health)
            
            logger.info(
                f"Successfully marked pod {self.pod_id} as DOWN",
                extra={
                    'pod_id': self.pod_id,
                    'shutdown_ts': current_time.isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to mark pod {self.pod_id} as DOWN: {e}")
            raise
    
    async def _heartbeat_loop(self) -> None:
        """
        Main heartbeat loop that runs continuously in the background.
        
        Sends regular heartbeat updates at the configured interval until
        shutdown is requested. Handles errors gracefully and includes
        periodic metadata cache invalidation.
        """
        logger.info(
            f"Starting heartbeat loop for pod {self.pod_id} "
            f"(interval: {self.config.heartbeat_interval_seconds}s)"
        )
        
        heartbeat_count = 0
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # Send heartbeat
                    await self.send_heartbeat()
                    heartbeat_count += 1
                    
                    # Invalidate metadata cache periodically (every 10 heartbeats)
                    # This ensures we pick up system changes over time
                    if heartbeat_count % 10 == 0:
                        self.invalidate_metadata_cache()
                        logger.debug(f"Invalidated metadata cache after {heartbeat_count} heartbeats")
                    
                    # Wait for next heartbeat interval or shutdown signal
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self.config.heartbeat_interval_seconds
                        )
                        # If we get here, shutdown was requested
                        break
                    except asyncio.TimeoutError:
                        # Timeout is expected - continue with next heartbeat
                        continue
                        
                except Exception as e:
                    logger.error(f"Error in heartbeat loop: {e}")
                    # Continue the loop even if individual heartbeats fail
                    # Wait a bit before retrying to avoid tight error loops
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=min(5.0, self.config.heartbeat_interval_seconds)
                        )
                        break
                    except asyncio.TimeoutError:
                        continue
        
        except asyncio.CancelledError:
            logger.info(f"Heartbeat loop cancelled for pod {self.pod_id}")
            raise
        except Exception as e:
            logger.error(f"Fatal error in heartbeat loop: {e}")
            raise
        finally:
            logger.info(
                f"Heartbeat loop stopped for pod {self.pod_id} "
                f"(sent {heartbeat_count} heartbeats)"
            )
    
    async def start(self) -> None:
        """
        Start the health service.
        
        Registers the pod and starts the background heartbeat loop.
        This method should be called during application startup.
        
        Raises:
            RuntimeError: If service is already running
            Exception: If startup fails
        """
        if self._is_running:
            raise RuntimeError(f"Health service for pod {self.pod_id} is already running")
        
        try:
            logger.info(f"Starting health service for pod {self.pod_id}")
            
            # Register pod in database
            await self.register_pod()
            
            # Start heartbeat loop
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._is_running = True
            
            logger.info(f"Health service started for pod {self.pod_id}")
            
        except Exception as e:
            logger.error(f"Failed to start health service for pod {self.pod_id}: {e}")
            await self.stop()  # Clean up any partial state
            raise
    
    async def stop(self) -> None:
        """
        Stop the health service gracefully.
        
        Signals the heartbeat loop to stop, marks the pod as DOWN,
        and cleans up resources. This method should be called during
        application shutdown.
        """
        if not self._is_running:
            logger.debug(f"Health service for pod {self.pod_id} is not running")
            return
        
        try:
            logger.info(f"Stopping health service for pod {self.pod_id}")
            
            # Signal shutdown to heartbeat loop
            self._shutdown_event.set()
            
            # Wait for heartbeat loop to finish
            if self._heartbeat_task and not self._heartbeat_task.done():
                try:
                    await asyncio.wait_for(self._heartbeat_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Heartbeat loop did not stop within timeout, cancelling")
                    self._heartbeat_task.cancel()
                    try:
                        await self._heartbeat_task
                    except asyncio.CancelledError:
                        pass
            
            # Mark pod as DOWN in database
            try:
                await self.mark_pod_down()
            except Exception as e:
                logger.error(f"Failed to mark pod as DOWN during shutdown: {e}")
                # Don't re-raise - we want shutdown to continue
            
            # Clean up state
            self._heartbeat_task = None
            self._is_running = False
            self._shutdown_event.clear()
            
            logger.info(f"Health service stopped for pod {self.pod_id}")
            
        except Exception as e:
            logger.error(f"Error during health service shutdown: {e}")
            # Ensure we clean up state even if shutdown fails
            self._heartbeat_task = None
            self._is_running = False
            self._shutdown_event.clear()
            raise
    
    @property
    def is_running(self) -> bool:
        """
        Check if the health service is currently running.
        
        Returns:
            bool: True if service is running, False otherwise
        """
        return self._is_running
    
    @property
    def uptime_seconds(self) -> float:
        """
        Get the current uptime of this pod in seconds.
        
        Returns:
            float: Uptime in seconds since pod birth
        """
        return (datetime.utcnow() - self.birth_ts).total_seconds()
    
    async def get_pod_health(self) -> Optional[PodHealth]:
        """
        Get the current pod health record from the database.
        
        Returns:
            Optional[PodHealth]: Current pod health record, None if not found
        """
        try:
            return await self.storage.get_pod_health(self.pod_id)
        except Exception as e:
            logger.error(f"Failed to get pod health for {self.pod_id}: {e}")
            return None
    
    async def get_all_active_pods(self) -> list[PodHealth]:
        """
        Get all currently active pods based on heartbeat threshold.
        
        Returns:
            List[PodHealth]: List of active pod health records
        """
        try:
            return await self.storage.get_active_pods(
                self.config.dead_pod_threshold_seconds
            )
        except Exception as e:
            logger.error(f"Failed to get active pods: {e}")
            return []
    
    async def am_i_leader(self) -> bool:
        """
        Check if this pod is the current leader.
        
        Uses the leader election algorithm to determine if this pod should
        be responsible for recovery operations.
        
        Returns:
            bool: True if this pod is the leader, False otherwise
        """
        try:
            return await self.storage.am_i_leader(
                self.pod_id,
                self.config.dead_pod_threshold_seconds
            )
        except Exception as e:
            logger.error(f"Failed to check leader status for pod {self.pod_id}: {e}")
            return False
    
    async def get_leader_info(self) -> Dict[str, Any]:
        """
        Get comprehensive leader election information.
        
        Returns:
            Dict[str, Any]: Leader information including current leader, active pods count, etc.
        """
        try:
            return await self.storage.get_leader_info(
                self.config.dead_pod_threshold_seconds
            )
        except Exception as e:
            logger.error(f"Failed to get leader info: {e}")
            return {
                'current_leader': None,
                'leader_birth_ts': None,
                'active_pods_count': 0,
                'active_pod_ids': [],
                'election_timestamp': datetime.utcnow(),
                'error': str(e)
            }
    
    async def detect_and_mark_dead_pods(self) -> List[PodHealth]:
        """
        Detect pods with stale heartbeats and mark them as dead.
        
        This method identifies pods whose heartbeats are older than the configured
        threshold and updates their status to 'down'. This is typically called
        by the leader pod as part of the recovery process.
        
        Returns:
            List[PodHealth]: List of pods that were marked as dead
            
        Raises:
            Exception: If dead pod detection or marking fails
        """
        try:
            logger.debug(
                f"Starting dead pod detection with threshold {self.config.dead_pod_threshold_seconds}s"
            )
            
            # Get pods with stale heartbeats
            dead_pods = await self.storage.get_dead_pods(
                self.config.dead_pod_threshold_seconds
            )
            
            if not dead_pods:
                logger.debug("No dead pods detected")
                return []
            
            # Extract pod IDs for marking
            dead_pod_ids = [pod.pod_id for pod in dead_pods]
            
            logger.info(
                f"Detected {len(dead_pods)} dead pods: {dead_pod_ids}",
                extra={
                    'dead_pod_count': len(dead_pods),
                    'dead_pod_ids': dead_pod_ids,
                    'threshold_seconds': self.config.dead_pod_threshold_seconds
                }
            )
            
            # Mark pods as dead in the database
            await self.storage.mark_pods_dead(dead_pod_ids)
            
            logger.info(
                f"Successfully marked {len(dead_pods)} pods as dead",
                extra={
                    'marked_dead_count': len(dead_pods),
                    'marked_dead_pod_ids': dead_pod_ids
                }
            )
            
            return dead_pods
            
        except Exception as e:
            logger.error(f"Failed to detect and mark dead pods: {e}")
            raise
    
    async def get_dead_pods(self) -> List[PodHealth]:
        """
        Get pods that are currently considered dead based on heartbeat threshold.
        
        Returns:
            List[PodHealth]: List of dead pod health records
        """
        try:
            return await self.storage.get_dead_pods(
                self.config.dead_pod_threshold_seconds
            )
        except Exception as e:
            logger.error(f"Failed to get dead pods: {e}")
            return []


def create_health_service(storage: StorageInterface, config: FleetQConfig) -> HealthService:
    """
    Factory function to create a HealthService instance.
    
    Args:
        storage: Storage backend for pod health operations
        config: Application configuration
        
    Returns:
        HealthService: Configured health service instance
    """
    return HealthService(storage, config)