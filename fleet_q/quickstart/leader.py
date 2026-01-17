"""
FLEET-Q Leader Election and Recovery

The leader is responsible for:
1. Detecting dead pods
2. Finding orphaned steps (claimed by dead pods)
3. Building ephemeral local DLQ
4. Resubmitting eligible steps (respecting retry limits)
5. Cleaning up local DLQ after processing
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

from config import FleetQConfig
from queue import QueueOperations
from storage import LocalStorage, SnowflakeStorage
from worker import HealthMonitor

logger = logging.getLogger(__name__)


class LeaderRecovery:
    """Handles leader-specific recovery operations"""
    
    def __init__(
        self,
        config: FleetQConfig,
        storage: SnowflakeStorage,
        local_storage: LocalStorage,
        queue_ops: QueueOperations,
        health_monitor: HealthMonitor
    ):
        self.config = config
        self.storage = storage
        self.local_storage = local_storage
        self.queue_ops = queue_ops
        self.health_monitor = health_monitor
        self.running = False
        self._recovery_task = None
    
    def _generate_dlq_table_name(self) -> str:
        """Generate unique DLQ table name with timestamp"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"dlq_orphans_{timestamp}"
    
    def find_orphaned_steps(self, dead_pod_ids: List[str]) -> List[Dict]:
        """
        Find steps that are claimed by dead pods
        
        Args:
            dead_pod_ids: List of dead pod IDs
            
        Returns:
            List of orphaned step records
        """
        if not dead_pod_ids:
            return []
        
        pod_ids_str = "','".join(dead_pod_ids)
        
        query = f"""
            SELECT step_id, payload, claimed_by, last_update_ts, retry_count, priority
            FROM {self.config.step_tracker_table}
            WHERE status = 'claimed'
            AND claimed_by IN ('{pod_ids_str}')
        """
        
        orphans = self.storage.execute(query, {})
        logger.info(f"Found {len(orphans)} orphaned steps from dead pods")
        return orphans
    
    def apply_retry_policy(self, orphan: Dict) -> tuple[bool, str]:
        """
        Determine if an orphaned step should be retried
        
        Args:
            orphan: Orphaned step record
            
        Returns:
            Tuple of (should_retry, reason)
        """
        retry_count = orphan.get('RETRY_COUNT', 0)
        step_id = orphan['STEP_ID']
        
        if retry_count >= self.config.max_retries:
            reason = f"Max retries ({self.config.max_retries}) exceeded"
            logger.warning(f"Step {step_id} marked as poison pill: {reason}")
            return False, reason
        
        return True, "eligible for retry"
    
    def build_dlq_and_resubmit(self, orphaned_steps: List[Dict]) -> Dict[str, any]:
        """
        Build local DLQ, apply retry policy, and resubmit eligible steps
        
        Args:
            orphaned_steps: List of orphaned step records
            
        Returns:
            Recovery statistics
        """
        if not orphaned_steps:
            logger.info("No orphaned steps to process")
            return {
                'total_orphans': 0,
                'resubmitted': 0,
                'terminal_failures': 0
            }
        
        # Generate DLQ table name
        dlq_table = self._generate_dlq_table_name()
        
        try:
            # Create DLQ table
            self.local_storage.create_dlq_table(dlq_table)
            
            # Prepare DLQ records
            dlq_records = []
            steps_to_requeue = []
            terminal_failures = []
            
            for orphan in orphaned_steps:
                step_id = orphan['STEP_ID']
                should_retry, reason = self.apply_retry_policy(orphan)
                
                # Add to DLQ
                dlq_record = {
                    'step_id': step_id,
                    'payload': orphan['PAYLOAD'],
                    'claimed_by': orphan['CLAIMED_BY'],
                    'last_update_ts': str(orphan['LAST_UPDATE_TS']),
                    'retry_count': orphan['RETRY_COUNT'],
                    'reason': reason,
                    'discovered_at': datetime.utcnow().isoformat()
                }
                dlq_records.append(dlq_record)
                
                # Categorize for action
                if should_retry:
                    steps_to_requeue.append(step_id)
                else:
                    terminal_failures.append(step_id)
            
            # Insert all records into DLQ
            self.local_storage.insert_dlq_records(dlq_table, dlq_records)
            
            # Resubmit eligible steps
            if steps_to_requeue:
                self.queue_ops.requeue_steps_bulk(steps_to_requeue, increment_retry=True)
                logger.info(f"Resubmitted {len(steps_to_requeue)} steps")
            
            # Mark terminal failures as failed
            if terminal_failures:
                for step_id in terminal_failures:
                    # Update to failed status in Snowflake
                    query = f"""
                        UPDATE {self.config.step_tracker_table}
                        SET status = 'failed',
                            last_update_ts = CURRENT_TIMESTAMP()
                        WHERE step_id = %(step_id)s
                    """
                    self.storage.execute(query, {'step_id': step_id})
                
                logger.warning(f"Marked {len(terminal_failures)} steps as terminal failures")
            
            # Mark DLQ as processed
            self.local_storage.mark_dlq_processed(dlq_table)
            
            # Clean up DLQ table
            self.local_storage.drop_dlq_table(dlq_table)
            
            stats = {
                'total_orphans': len(orphaned_steps),
                'resubmitted': len(steps_to_requeue),
                'terminal_failures': len(terminal_failures),
                'dlq_table': dlq_table
            }
            
            logger.info(f"Recovery cycle complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error in DLQ build and resubmit: {e}", exc_info=True)
            # Try to clean up DLQ table
            try:
                self.local_storage.drop_dlq_table(dlq_table)
            except:
                pass
            raise
    
    def run_recovery_cycle(self) -> Optional[Dict]:
        """
        Execute a complete recovery cycle
        
        Returns:
            Recovery statistics or None if no recovery needed
        """
        logger.info("Starting recovery cycle...")
        
        try:
            # Step 1: Find dead pods
            dead_pods = self.health_monitor.get_dead_pods()
            
            if not dead_pods:
                logger.debug("No dead pods detected")
                return None
            
            dead_pod_ids = [p['POD_ID'] for p in dead_pods]
            logger.warning(f"Detected {len(dead_pod_ids)} dead pods: {dead_pod_ids}")
            
            # Step 2: Mark pods as dead in POD_HEALTH
            self.health_monitor.mark_pods_dead(dead_pod_ids)
            
            # Step 3: Find orphaned steps
            orphaned_steps = self.find_orphaned_steps(dead_pod_ids)
            
            if not orphaned_steps:
                logger.info("No orphaned steps found")
                return {
                    'dead_pods': len(dead_pod_ids),
                    'orphaned_steps': 0,
                    'resubmitted': 0,
                    'terminal_failures': 0
                }
            
            # Step 4: Build DLQ and resubmit
            recovery_stats = self.build_dlq_and_resubmit(orphaned_steps)
            recovery_stats['dead_pods'] = len(dead_pod_ids)
            
            logger.info(f"Recovery cycle completed: {recovery_stats}")
            return recovery_stats
            
        except Exception as e:
            logger.error(f"Recovery cycle failed: {e}", exc_info=True)
            raise
    
    async def recovery_loop(self):
        """
        Background loop that runs recovery cycles periodically
        
        Only runs if this pod is the leader
        """
        logger.info(f"Starting recovery loop (interval: {self.config.recovery_interval}s)")
        
        while self.running:
            try:
                # Check if we are the leader
                if self.health_monitor.is_leader():
                    logger.info("This pod is the leader - running recovery cycle")
                    
                    # Run recovery in executor to avoid blocking
                    loop = asyncio.get_event_loop()
                    stats = await loop.run_in_executor(None, self.run_recovery_cycle)
                    
                    if stats:
                        logger.info(f"Recovery stats: {stats}")
                else:
                    logger.debug("Not the leader - skipping recovery cycle")
                
            except Exception as e:
                logger.error(f"Recovery loop error: {e}", exc_info=True)
            
            await asyncio.sleep(self.config.recovery_interval)
    
    async def start(self):
        """Start the recovery loop"""
        if self.running:
            logger.warning("Recovery loop already running")
            return
        
        self.running = True
        self._recovery_task = asyncio.create_task(self.recovery_loop())
        logger.info("Recovery loop started")
    
    async def stop(self):
        """Stop the recovery loop gracefully"""
        if not self.running:
            return
        
        logger.info("Stopping recovery loop...")
        self.running = False
        
        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Recovery loop stopped")


class LeaderElection:
    """Simplified leader election helper"""
    
    def __init__(self, config: FleetQConfig, health_monitor: HealthMonitor):
        self.config = config
        self.health_monitor = health_monitor
    
    def get_current_leader(self) -> Optional[str]:
        """
        Get the current leader pod ID
        
        Returns:
            Leader pod ID or None
        """
        return self.health_monitor.get_leader_pod()
    
    def am_i_leader(self) -> bool:
        """
        Check if this pod is the current leader
        
        Returns:
            True if this pod is the leader
        """
        return self.health_monitor.is_leader()
    
    def get_leader_info(self) -> Dict[str, any]:
        """
        Get information about current leadership
        
        Returns:
            Dictionary with leader info
        """
        leader_pod = self.get_current_leader()
        is_leader = leader_pod == self.config.pod_id
        
        alive_pods = self.health_monitor.get_alive_pods()
        
        return {
            'leader_pod_id': leader_pod,
            'this_pod_id': self.config.pod_id,
            'is_leader': is_leader,
            'total_alive_pods': len(alive_pods),
            'alive_pods': [p['POD_ID'] for p in alive_pods]
        }


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("LeaderRecovery module loaded successfully")
    print("Example usage:")
    print("  leader = LeaderRecovery(config, storage, local_storage, queue_ops, health_monitor)")
    print("  await leader.start()")
    print("  # ... recovery runs automatically if this pod is leader ...")
    print("  await leader.stop()")
