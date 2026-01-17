"""
Leader election service for FLEET-Q.

Implements the leader election system that determines which pod should handle
recovery operations. The leader is selected deterministically based on the
eldest pod (earliest birth_ts) that has a fresh heartbeat and UP status.

Provides:
- Leader determination logic with fresh heartbeat validation
- Leader status checking for current pod
- Leader election information for monitoring and admin endpoints
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from .config import FleetQConfig
from .models import LeaderInfoResponse
from .storage import StorageInterface
from .logging_config import log_leader_election

logger = logging.getLogger(__name__)


class LeaderElectionService:
    """
    Service for managing leader election and status checking.
    
    Handles deterministic leader selection based on eldest pod with fresh heartbeat,
    provides leader status checking, and maintains leader election information
    for operational visibility.
    """
    
    def __init__(self, storage: StorageInterface, config: FleetQConfig):
        """
        Initialize the leader election service.
        
        Args:
            storage: Storage backend for pod health operations
            config: Application configuration
        """
        self.storage = storage
        self.config = config
        self.pod_id = config.pod_id
        
        # Cache for leader election results to avoid excessive database queries
        self._last_leader_check: Optional[datetime] = None
        self._cached_leader: Optional[str] = None
        self._cached_is_leader: Optional[bool] = None
        self._cache_duration_seconds = 10  # Cache results for 10 seconds
        
        logger.info(f"Leader election service initialized for pod {self.pod_id}")
    
    async def determine_leader(self) -> Optional[str]:
        """
        Determine the current leader pod.
        
        Uses the deterministic leader election rule: eldest pod (earliest birth_ts)
        among all pods with status='up' and fresh heartbeat.
        
        Returns:
            Optional[str]: Pod ID of the current leader, None if no eligible pods
            
        Raises:
            Exception: If leader determination fails
        """
        try:
            leader_id = await self.storage.determine_leader(
                self.config.dead_pod_threshold_seconds
            )
            
            # Update cache
            self._last_leader_check = datetime.utcnow()
            self._cached_leader = leader_id
            self._cached_is_leader = (leader_id == self.pod_id) if leader_id else False
            
            return leader_id
            
        except Exception as e:
            logger.error(f"Failed to determine leader: {e}")
            raise
    
    async def am_i_leader(self) -> bool:
        """
        Check if this pod is the current leader.
        
        Uses caching to avoid excessive database queries while still maintaining
        reasonable freshness of leader election results.
        
        Returns:
            bool: True if this pod is the leader, False otherwise
            
        Raises:
            Exception: If leader check fails
        """
        try:
            # Check if we have a recent cached result
            if (self._last_leader_check and 
                self._cached_is_leader is not None and
                (datetime.utcnow() - self._last_leader_check).total_seconds() < self._cache_duration_seconds):
                
                logger.debug(f"Using cached leader status for pod {self.pod_id}: {self._cached_is_leader}")
                return self._cached_is_leader
            
            # Perform fresh leader check
            is_leader = await self.storage.am_i_leader(
                self.pod_id,
                self.config.dead_pod_threshold_seconds
            )
            
            # Update cache
            self._last_leader_check = datetime.utcnow()
            self._cached_is_leader = is_leader
            
            return is_leader
            
        except Exception as e:
            logger.error(f"Failed to check leader status for pod {self.pod_id}: {e}")
            raise
    
    async def get_leader_info(self) -> Dict[str, Any]:
        """
        Get comprehensive leader election information.
        
        Returns detailed information about the current leader, active pods,
        and election status for monitoring and administrative purposes.
        
        Returns:
            Dict[str, Any]: Leader information including current leader, active pods count, etc.
            
        Raises:
            Exception: If leader info retrieval fails
        """
        try:
            leader_info = await self.storage.get_leader_info(
                self.config.dead_pod_threshold_seconds
            )
            
            # Add this pod's perspective
            leader_info['this_pod_id'] = self.pod_id
            leader_info['this_pod_is_leader'] = (
                leader_info['current_leader'] == self.pod_id
                if leader_info['current_leader'] else False
            )
            
            # Log leader election status
            log_leader_election(
                logger,
                current_leader=leader_info['current_leader'] or 'none',
                pod_id=self.pod_id,
                is_leader=leader_info['this_pod_is_leader'],
                active_pods=leader_info['active_pods_count']
            )
            
            return leader_info
            
        except Exception as e:
            logger.error(f"Failed to get leader info: {e}")
            raise
    
    async def get_leader_info_response(self) -> LeaderInfoResponse:
        """
        Get leader election information formatted for API response.
        
        Returns:
            LeaderInfoResponse: Formatted leader information for API endpoints
            
        Raises:
            Exception: If leader info retrieval fails
        """
        try:
            leader_info = await self.get_leader_info()
            
            return LeaderInfoResponse(
                current_leader=leader_info['current_leader'],
                leader_birth_ts=leader_info['leader_birth_ts'],
                active_pods=leader_info['active_pods_count'],
                last_election_check=leader_info['election_timestamp']
            )
            
        except Exception as e:
            logger.error(f"Failed to get leader info response: {e}")
            raise
    
    def invalidate_cache(self) -> None:
        """
        Invalidate the leader election cache.
        
        Forces the next leader check to query the database rather than
        using cached results. Useful when we know the leader status
        might have changed.
        """
        self._last_leader_check = None
        self._cached_leader = None
        self._cached_is_leader = None
        logger.debug(f"Invalidated leader election cache for pod {self.pod_id}")
    
    async def wait_for_leadership(self, timeout_seconds: float = 60.0) -> bool:
        """
        Wait for this pod to become the leader.
        
        Polls the leader election status until this pod becomes the leader
        or the timeout is reached. Useful during startup or testing.
        
        Args:
            timeout_seconds: Maximum time to wait for leadership
            
        Returns:
            bool: True if this pod became the leader, False if timeout
            
        Raises:
            Exception: If leader checking fails
        """
        start_time = datetime.utcnow()
        check_interval = min(5.0, self.config.heartbeat_interval_seconds)
        
        logger.info(f"Waiting for pod {self.pod_id} to become leader (timeout: {timeout_seconds}s)")
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout_seconds:
            try:
                if await self.am_i_leader():
                    elapsed = (datetime.utcnow() - start_time).total_seconds()
                    logger.info(f"Pod {self.pod_id} became leader after {elapsed:.1f}s")
                    return True
                
                # Wait before next check
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                logger.warning(f"Error checking leadership during wait: {e}")
                await asyncio.sleep(check_interval)
        
        logger.warning(f"Pod {self.pod_id} did not become leader within {timeout_seconds}s timeout")
        return False
    
    async def ensure_leader_or_wait(self, max_wait_seconds: float = 30.0) -> str:
        """
        Ensure there is a leader in the system, waiting if necessary.
        
        Checks if there is currently a leader, and if not, waits for one to be elected.
        This is useful during system startup when pods are still registering.
        
        Args:
            max_wait_seconds: Maximum time to wait for a leader to be elected
            
        Returns:
            str: Pod ID of the current leader
            
        Raises:
            RuntimeError: If no leader is elected within the timeout
            Exception: If leader checking fails
        """
        start_time = datetime.utcnow()
        check_interval = min(2.0, self.config.heartbeat_interval_seconds / 2)
        
        logger.info(f"Ensuring leader exists (max wait: {max_wait_seconds}s)")
        
        while (datetime.utcnow() - start_time).total_seconds() < max_wait_seconds:
            try:
                leader_id = await self.determine_leader()
                if leader_id:
                    elapsed = (datetime.utcnow() - start_time).total_seconds()
                    logger.info(f"Leader found: {leader_id} (after {elapsed:.1f}s)")
                    return leader_id
                
                logger.debug("No leader found, waiting for election...")
                await asyncio.sleep(check_interval)
                
            except Exception as e:
                logger.warning(f"Error checking for leader: {e}")
                await asyncio.sleep(check_interval)
        
        raise RuntimeError(f"No leader elected within {max_wait_seconds}s timeout")
    
    @property
    def cached_leader_status(self) -> Optional[bool]:
        """
        Get the cached leader status without triggering a database query.
        
        Returns:
            Optional[bool]: Cached leader status, None if no cached result
        """
        if (self._last_leader_check and 
            (datetime.utcnow() - self._last_leader_check).total_seconds() < self._cache_duration_seconds):
            return self._cached_is_leader
        return None
    
    @property
    def cache_age_seconds(self) -> Optional[float]:
        """
        Get the age of the cached leader election result in seconds.
        
        Returns:
            Optional[float]: Age of cached result in seconds, None if no cache
        """
        if self._last_leader_check:
            return (datetime.utcnow() - self._last_leader_check).total_seconds()
        return None


def create_leader_service(storage: StorageInterface, config: FleetQConfig) -> LeaderElectionService:
    """
    Factory function to create a LeaderElectionService instance.
    
    Args:
        storage: Storage backend for pod health operations
        config: Application configuration
        
    Returns:
        LeaderElectionService: Configured leader election service instance
    """
    return LeaderElectionService(storage, config)