"""
FLEET-Q Worker Loops

Implements the core worker behavior:
- Heartbeat loop: keeps pod alive in POD_HEALTH
- Claim loop: atomically claims pending steps based on capacity
- Execute loop: processes claimed steps
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional

from config import FleetQConfig
from queue import QueueOperations
from storage import SnowflakeStorage

logger = logging.getLogger(__name__)


class WorkerLoops:
    """Manages all worker background loops"""
    
    def __init__(
        self,
        config: FleetQConfig,
        storage: SnowflakeStorage,
        queue_ops: QueueOperations,
        task_executor: Optional[Callable[[Dict[str, Any]], Any]] = None
    ):
        self.config = config
        self.storage = storage
        self.queue_ops = queue_ops
        self.task_executor = task_executor or self._default_task_executor
        self.running = False
        self._loops = []
    
    def _default_task_executor(self, step: Dict[str, Any]) -> Any:
        """
        Default task executor - just logs and sleeps
        
        Replace this with your actual task execution logic
        """
        step_id = step['STEP_ID']
        payload = step['PAYLOAD']
        
        logger.info(f"Executing step {step_id}: {payload}")
        
        # Simulate work
        time.sleep(2)
        
        logger.info(f"Completed step {step_id}")
        return {"status": "success"}
    
    async def heartbeat_loop(self):
        """
        Heartbeat loop - updates POD_HEALTH with current timestamp
        
        Runs every heartbeat_interval seconds
        """
        logger.info(f"Starting heartbeat loop (interval: {self.config.heartbeat_interval}s)")
        
        while self.running:
            try:
                # Upsert into POD_HEALTH
                query = f"""
                    MERGE INTO {self.config.pod_health_table} AS target
                    USING (SELECT %(pod_id)s AS pod_id) AS source
                    ON target.pod_id = source.pod_id
                    WHEN MATCHED THEN
                        UPDATE SET 
                            last_heartbeat_ts = CURRENT_TIMESTAMP(),
                            status = 'up'
                    WHEN NOT MATCHED THEN
                        INSERT (pod_id, birth_ts, last_heartbeat_ts, status, meta)
                        VALUES (%(pod_id)s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
                                'up', PARSE_JSON(%(meta)s))
                """
                
                meta = json.dumps({
                    'max_parallelism': self.config.max_parallelism,
                    'capacity_threshold': self.config.capacity_threshold,
                })
                
                self.storage.execute(query, {
                    'pod_id': self.config.pod_id,
                    'meta': meta
                })
                
                logger.debug(f"Heartbeat sent for pod {self.config.pod_id}")
                
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}", exc_info=True)
            
            await asyncio.sleep(self.config.heartbeat_interval)
    
    async def claim_loop(self):
        """
        Claim loop - periodically claims available pending steps
        
        Runs every claim_interval seconds
        """
        logger.info(f"Starting claim loop (interval: {self.config.claim_interval}s)")
        
        while self.running:
            try:
                # Calculate available capacity
                available = self.queue_ops.calculate_available_capacity()
                
                if available > 0:
                    # Try to claim steps
                    claimed = self.queue_ops.claim_steps(available)
                    
                    if claimed:
                        logger.info(f"Claimed {len(claimed)} steps, processing...")
                        
                        # Process claimed steps asynchronously
                        for step in claimed:
                            asyncio.create_task(self._execute_step(step))
                else:
                    logger.debug("No available capacity, skipping claim")
                
            except Exception as e:
                logger.error(f"Claim loop error: {e}", exc_info=True)
            
            await asyncio.sleep(self.config.claim_interval)
    
    async def _execute_step(self, step: Dict[str, Any]):
        """
        Execute a single step asynchronously
        
        Args:
            step: Step details from claim
        """
        step_id = step['STEP_ID']
        
        try:
            logger.info(f"Starting execution of step {step_id}")
            
            # Execute the task
            # If task_executor is blocking, run it in executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.task_executor, step)
            
            # Mark as completed
            self.queue_ops.complete_step(step_id)
            logger.info(f"Successfully completed step {step_id}")
            
        except Exception as e:
            logger.error(f"Step {step_id} execution failed: {e}", exc_info=True)
            
            # Mark as failed
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.queue_ops.fail_step(step_id, error_msg)
    
    async def start(self):
        """Start all worker loops"""
        if self.running:
            logger.warning("Worker loops already running")
            return
        
        self.running = True
        logger.info("Starting worker loops...")
        
        # Start heartbeat loop
        self._loops.append(asyncio.create_task(self.heartbeat_loop()))
        
        # Start claim loop
        self._loops.append(asyncio.create_task(self.claim_loop()))
        
        logger.info("Worker loops started")
    
    async def stop(self):
        """Stop all worker loops gracefully"""
        if not self.running:
            return
        
        logger.info("Stopping worker loops...")
        self.running = False
        
        # Cancel all loops
        for loop_task in self._loops:
            loop_task.cancel()
        
        # Wait for all to finish
        await asyncio.gather(*self._loops, return_exceptions=True)
        
        self._loops.clear()
        logger.info("Worker loops stopped")


class HealthMonitor:
    """Helper class for pod health operations"""
    
    def __init__(self, config: FleetQConfig, storage: SnowflakeStorage):
        self.config = config
        self.storage = storage
    
    def get_alive_pods(self) -> list:
        """
        Get list of alive pods (fresh heartbeat within threshold)
        
        Returns:
            List of pod details
        """
        query = f"""
            SELECT pod_id, birth_ts, last_heartbeat_ts, status, meta
            FROM {self.config.pod_health_table}
            WHERE status = 'up'
            AND last_heartbeat_ts > DATEADD(second, -%(threshold)s, CURRENT_TIMESTAMP())
            ORDER BY birth_ts ASC
        """
        
        return self.storage.execute(query, {
            'threshold': self.config.dead_pod_threshold_seconds
        })
    
    def get_dead_pods(self) -> list:
        """
        Get list of dead pods (stale heartbeat beyond threshold)
        
        Returns:
            List of dead pod IDs
        """
        query = f"""
            SELECT pod_id, last_heartbeat_ts
            FROM {self.config.pod_health_table}
            WHERE status = 'up'
            AND last_heartbeat_ts <= DATEADD(second, -%(threshold)s, CURRENT_TIMESTAMP())
        """
        
        return self.storage.execute(query, {
            'threshold': self.config.dead_pod_threshold_seconds
        })
    
    def mark_pods_dead(self, pod_ids: list) -> None:
        """
        Mark pods as dead in POD_HEALTH
        
        Args:
            pod_ids: List of pod IDs to mark as dead
        """
        if not pod_ids:
            return
        
        pod_ids_str = "','".join(pod_ids)
        
        query = f"""
            UPDATE {self.config.pod_health_table}
            SET status = 'down',
                last_heartbeat_ts = CURRENT_TIMESTAMP()
            WHERE pod_id IN ('{pod_ids_str}')
        """
        
        self.storage.execute(query, {})
        logger.info(f"Marked {len(pod_ids)} pods as dead")
    
    def get_leader_pod(self) -> Optional[str]:
        """
        Determine the leader pod (oldest alive pod by birth_ts)
        
        Returns:
            Leader pod ID or None if no alive pods
        """
        alive_pods = self.get_alive_pods()
        
        if not alive_pods:
            return None
        
        # Leader is the oldest pod (earliest birth_ts)
        leader = alive_pods[0]
        return leader['POD_ID']
    
    def is_leader(self) -> bool:
        """
        Check if this pod is the current leader
        
        Returns:
            True if this pod is the leader
        """
        leader_pod = self.get_leader_pod()
        is_leader = leader_pod == self.config.pod_id
        
        logger.debug(f"Leader check: current={leader_pod}, this={self.config.pod_id}, is_leader={is_leader}")
        return is_leader


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Note: This requires actual config and storage to run
    print("WorkerLoops module loaded successfully")
    print("Example usage:")
    print("  worker = WorkerLoops(config, storage, queue_ops, task_executor)")
    print("  await worker.start()")
    print("  # ... let it run ...")
    print("  await worker.stop()")
