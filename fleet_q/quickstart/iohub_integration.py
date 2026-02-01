"""
IOHub Integration Example with FLEET-Q

This module demonstrates how to integrate IOHub pattern with FLEET-Q task execution.
Shows both simple and production-ready patterns.
"""

import asyncio
import multiprocessing as mp
import os
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from iohub import IOHubZMQBased, IOHubClientZMQ
from config import FleetQConfig
from storage import SnowflakeStorage
from queue import QueueOps
from worker import WorkerLoops

# ============================================================================
# PATTERN 1: Simple IOHub Integration
# ============================================================================

class SimpleIOHubExecutor:
    """
    Simple executor that uses IOHub for permit management.
    
    Use case: Quick integration for testing IOHub benefits.
    """
    
    def __init__(self, iohub_address: str = "tcp://127.0.0.1:5555"):
        self.iohub_address = iohub_address
        self.worker_id = f"worker-{os.getpid()}"
        self.client: Optional[IOHubClientZMQ] = None
    
    def setup(self):
        """Create IOHub client"""
        self.client = IOHubClientZMQ(self.iohub_address, self.worker_id)
    
    def teardown(self):
        """Close IOHub client"""
        if self.client:
            self.client.close()
    
    async def execute_task(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single task with IOHub coordination.
        
        Args:
            step: Task from STEP_TRACKER
        
        Returns:
            Result dictionary
        """
        if not self.client:
            self.setup()
        
        step_id = step['STEP_ID']
        payload = step['PAYLOAD']
        
        # Request permit from IOHub
        if not self.client.request_permit(timeout=10.0):
            return {
                "status": "error",
                "error": "Failed to acquire permit from IOHub"
            }
        
        try:
            # Execute with Bedrock (or other API)
            start_time = time.time()
            result = await self._call_bedrock(payload)
            latency = time.time() - start_time
            
            # Report success to IOHub
            self.client.report_outcome('success', latency=latency)
            
            # Enqueue write to IOHub (batched to Snowflake)
            self.client.enqueue_write(
                step_id=step_id,
                table_name='RESULTS',
                record_data={
                    'step_id': step_id,
                    'result': result,
                    'latency': latency,
                    'completed_at': time.time()
                }
            )
            
            return {
                "status": "success",
                "result": result,
                "latency": latency
            }
        
        except Exception as e:
            # Report error to IOHub
            if "ThrottlingException" in str(e):
                self.client.report_outcome('throttle')
            else:
                self.client.report_outcome('error')
            
            return {
                "status": "error",
                "error": str(e)
            }
        
        finally:
            # Always release permit
            self.client.release_permit()
    
    async def _call_bedrock(self, payload: Dict[str, Any]) -> str:
        """
        Call Bedrock API (mock implementation).
        
        In production, replace with real boto3 bedrock-runtime call.
        """
        # Simulate API call
        await asyncio.sleep(0.1 + 0.2 * (hash(str(payload)) % 10) / 10)
        
        # Mock response
        return f"Processed: {payload.get('prompt', 'N/A')}"


# ============================================================================
# PATTERN 2: Production IOHub Integration with Lifespan
# ============================================================================

class ProductionIOHubExecutor:
    """
    Production-ready executor with IOHub, connection pooling, and monitoring.
    
    Use case: High-scale production deployments with many workers.
    """
    
    def __init__(
        self,
        iohub_address: str = "tcp://127.0.0.1:5555",
        enable_metrics: bool = True
    ):
        self.iohub_address = iohub_address
        self.worker_id = f"worker-{os.getpid()}"
        self.client: Optional[IOHubClientZMQ] = None
        self.enable_metrics = enable_metrics
        
        # Metrics
        self.total_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.throttled_tasks = 0
        self.total_latency = 0.0
    
    def setup(self):
        """Create IOHub client with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.client = IOHubClientZMQ(self.iohub_address, self.worker_id)
                print(f"[{self.worker_id}] Connected to IOHub")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise Exception(f"Failed to connect to IOHub after {max_retries} attempts: {e}")
    
    def teardown(self):
        """Close IOHub client and log metrics"""
        if self.enable_metrics:
            self._log_metrics()
        
        if self.client:
            self.client.close()
    
    async def execute_task(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single task with full error handling and metrics.
        
        Args:
            step: Task from STEP_TRACKER
        
        Returns:
            Result dictionary with status, result, and metrics
        """
        if not self.client:
            self.setup()
        
        self.total_tasks += 1
        step_id = step['STEP_ID']
        payload = step['PAYLOAD']
        
        # Request permit with retry
        permit_acquired = False
        for attempt in range(3):
            if self.client.request_permit(timeout=10.0):
                permit_acquired = True
                break
            await asyncio.sleep(0.5)
        
        if not permit_acquired:
            self.failed_tasks += 1
            return {
                "status": "error",
                "error": "Permit timeout - IOHub at capacity",
                "retry_recommended": True
            }
        
        try:
            # Execute task
            start_time = time.time()
            result = await self._call_bedrock_with_retry(payload)
            latency = time.time() - start_time
            
            # Update metrics
            self.successful_tasks += 1
            self.total_latency += latency
            
            # Report success
            self.client.report_outcome('success', latency=latency)
            
            # Enqueue write
            self.client.enqueue_write(
                step_id=step_id,
                table_name='RESULTS',
                record_data={
                    'step_id': step_id,
                    'worker_id': self.worker_id,
                    'result': result,
                    'latency': latency,
                    'timestamp': time.time()
                }
            )
            
            return {
                "status": "success",
                "result": result,
                "latency": latency,
                "worker_id": self.worker_id
            }
        
        except Exception as e:
            # Handle different error types
            error_type = 'error'
            
            if "ThrottlingException" in str(e) or "TooManyRequestsException" in str(e):
                error_type = 'throttle'
                self.throttled_tasks += 1
            else:
                self.failed_tasks += 1
            
            self.client.report_outcome(error_type)
            
            return {
                "status": "error",
                "error": str(e),
                "error_type": error_type,
                "retry_recommended": error_type == 'throttle'
            }
        
        finally:
            # Always release permit
            self.client.release_permit()
    
    async def _call_bedrock_with_retry(
        self,
        payload: Dict[str, Any],
        max_retries: int = 2
    ) -> str:
        """
        Call Bedrock with exponential backoff retry.
        
        Args:
            payload: Request payload
            max_retries: Maximum retry attempts
        
        Returns:
            API response string
        """
        for attempt in range(max_retries + 1):
            try:
                # In production: use boto3 bedrock-runtime
                # client.invoke_model(...)
                
                # Mock for demo
                await asyncio.sleep(0.1 + 0.2 * (hash(str(payload)) % 10) / 10)
                
                # Simulate occasional throttles (5%)
                if hash(str(payload)) % 20 == 0:
                    raise Exception("ThrottlingException: Rate exceeded")
                
                return f"Bedrock response for: {payload.get('prompt', 'N/A')}"
            
            except Exception as e:
                if "ThrottlingException" in str(e) and attempt < max_retries:
                    # Exponential backoff
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise
    
    def _log_metrics(self):
        """Log executor metrics"""
        avg_latency = self.total_latency / self.successful_tasks if self.successful_tasks > 0 else 0
        success_rate = self.successful_tasks / self.total_tasks if self.total_tasks > 0 else 0
        
        print(f"\n[{self.worker_id}] Metrics:")
        print(f"  Total tasks: {self.total_tasks}")
        print(f"  Successful: {self.successful_tasks} ({success_rate:.1%})")
        print(f"  Failed: {self.failed_tasks}")
        print(f"  Throttled: {self.throttled_tasks}")
        print(f"  Avg latency: {avg_latency:.3f}s")


# ============================================================================
# FLEET-Q Integration with IOHub Lifespan
# ============================================================================

# Global IOHub process
iohub_process: Optional[mp.Process] = None
executor: Optional[ProductionIOHubExecutor] = None


@asynccontextmanager
async def lifespan_with_iohub(app):
    """
    FastAPI lifespan context manager with IOHub.
    
    Starts IOHub on startup, shuts down on exit.
    """
    global iohub_process, executor
    
    print("\n" + "=" * 70)
    print("Starting FLEET-Q with IOHub Pattern")
    print("=" * 70)
    
    # Start IOHub process
    iohub_address = "tcp://127.0.0.1:5555"
    hub = IOHubZMQBased(bind_address=iohub_address)
    iohub_process = mp.Process(target=hub.run, name="IOHub")
    iohub_process.start()
    
    # Wait for IOHub to initialize
    await asyncio.sleep(1)
    print(f"[Startup] IOHub started (PID: {iohub_process.pid})")
    
    # Create executor
    executor = ProductionIOHubExecutor(iohub_address=iohub_address)
    executor.setup()
    print(f"[Startup] Executor initialized")
    
    # Start FLEET-Q worker loops (existing code)
    config = FleetQConfig()
    storage = SnowflakeStorage(config)
    queue_ops = QueueOps(config, storage)
    worker = WorkerLoops(config, storage, queue_ops)
    
    worker_task = asyncio.create_task(worker.start_all())
    print(f"[Startup] Worker loops started")
    
    print("=" * 70)
    print("FLEET-Q with IOHub ready!")
    print("=" * 70 + "\n")
    
    yield
    
    # Shutdown
    print("\n" + "=" * 70)
    print("Shutting down FLEET-Q with IOHub")
    print("=" * 70)
    
    # Stop worker loops
    worker.stop_all()
    await worker_task
    print("[Shutdown] Worker loops stopped")
    
    # Teardown executor
    if executor:
        executor.teardown()
        print("[Shutdown] Executor cleaned up")
    
    # Stop IOHub
    if iohub_process:
        iohub_process.terminate()
        iohub_process.join(timeout=5)
        if iohub_process.is_alive():
            iohub_process.kill()
        print("[Shutdown] IOHub stopped")
    
    print("=" * 70)
    print("Shutdown complete")
    print("=" * 70 + "\n")


async def execute_task_with_iohub(step: Dict[str, Any]) -> Dict[str, Any]:
    """
    Task executor that uses IOHub pattern.
    
    This replaces the default execute_task() in main.py.
    
    Args:
        step: Task from STEP_TRACKER
    
    Returns:
        Result dictionary
    """
    global executor
    
    if not executor:
        return {
            "status": "error",
            "error": "Executor not initialized"
        }
    
    return await executor.execute_task(step)


# ============================================================================
# Standalone Demo
# ============================================================================

async def demo_iohub_integration():
    """
    Standalone demo showing IOHub integration.
    
    Simulates multiple concurrent tasks using IOHub.
    """
    print("\n" + "=" * 70)
    print("IOHub Integration Demo")
    print("=" * 70 + "\n")
    
    # Start IOHub
    iohub_address = "tcp://127.0.0.1:5555"
    hub = IOHubZMQBased(bind_address=iohub_address)
    hub_process = mp.Process(target=hub.run)
    hub_process.start()
    
    await asyncio.sleep(1)  # Wait for startup
    
    # Create executor
    executor = ProductionIOHubExecutor(iohub_address=iohub_address)
    executor.setup()
    
    # Simulate tasks
    tasks = []
    for i in range(20):
        step = {
            'STEP_ID': f'task-{i:03d}',
            'PAYLOAD': {
                'prompt': f'Test prompt {i}',
                'task_type': 'bedrock'
            }
        }
        tasks.append(executor.execute_task(step))
    
    # Execute concurrently
    start = time.time()
    results = await asyncio.gather(*tasks)
    elapsed = time.time() - start
    
    # Summary
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'error')
    
    print("\n" + "=" * 70)
    print("Demo Results")
    print("=" * 70)
    print(f"Tasks completed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {len(results) / elapsed:.2f} tasks/sec")
    print("=" * 70 + "\n")
    
    # Cleanup
    executor.teardown()
    hub_process.terminate()
    hub_process.join()


if __name__ == "__main__":
    """
    Run standalone demo.
    
    Usage:
        python iohub_integration.py
    """
    asyncio.run(demo_iohub_integration())
