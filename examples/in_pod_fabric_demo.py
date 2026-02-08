"""
Complete Integration Example: FLEET-Q In-Pod Execution Fabric

This demonstrates the full in-pod execution fabric with:
- Control Plane Runner (singleton)
- IOHub with AIMD permit control
- Worker processes using aiomultiprocess
- APScheduler for time-based triggers
- SQLite outbox for durable side effects
- ZeroMQ for in-pod messaging

Scenario: Bedrock-based document processing with SharePoint integration
"""

import asyncio
import logging
import time
from typing import Dict, Any
import multiprocessing as mp
import os

# FLEET-Q imports
from fleet_q.control_plane_runner import ControlPlaneRunner, ControlPlaneConfig
from fleet_q.iohub import IOHubClient, AIMDConfig
from fleet_q.zeromq_utils import create_ipc_address
from fleet_q.sqlite_outbox import SharePointIntent, SharePointOpType

try:
    import aiomultiprocess
    AIOMULTIPROCESS_AVAILABLE = True
except ImportError:
    AIOMULTIPROCESS_AVAILABLE = False
    print("Install aiomultiprocess: pip install aiomultiprocess")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Mock Bedrock Client (Replace with real implementation)
# ============================================================================

async def mock_bedrock_call(prompt: str, document: str) -> Dict[str, Any]:
    """
    Mock Bedrock API call.
    
    In production, replace with:
        import boto3
        bedrock = boto3.client('bedrock-runtime')
        response = bedrock.invoke_model(...)
    """
    await asyncio.sleep(0.3)  # Simulate I/O
    
    # Simulate occasional throttle
    import random
    if random.random() < 0.1:
        raise Exception("ThrottlingException")
    
    return {
        'summary': f'Summary of document (length: {len(document)})',
        'sentiment': 'positive',
        'key_points': ['point1', 'point2', 'point3']
    }


# ============================================================================
# Worker Function (aiomultiprocess)
# ============================================================================

async def bedrock_worker(
    task: Dict[str, Any],
    iohub_address: str,
    worker_id: str
) -> Dict[str, Any]:
    """
    Worker function for aiomultiprocess.
    
    This function:
    1. Connects to IOHub
    2. Requests permit before Bedrock call
    3. Executes Bedrock call (async I/O)
    4. Reports outcome to IOHub
    5. Enqueues result write to outbox
    """
    logger.info(f"Worker {worker_id} processing task {task['step_id']}")
    
    # Create IOHub client
    client = IOHubClient(iohub_address, worker_id)
    
    try:
        # Request permit (with retries)
        permit_acquired = False
        for attempt in range(5):
            if await client.request_permit(timeout_ms=2000):
                permit_acquired = True
                break
            logger.debug(f"Worker {worker_id} waiting for permit (attempt {attempt + 1})")
            await asyncio.sleep(0.5)
        
        if not permit_acquired:
            logger.error(f"Worker {worker_id} failed to acquire permit")
            return {'status': 'error', 'error': 'permit_timeout'}
        
        # Execute Bedrock call
        start_time = time.time()
        
        try:
            result = await mock_bedrock_call(
                prompt=task['prompt'],
                document=task['document']
            )
            
            latency = time.time() - start_time
            
            # Report success
            await client.report_success(latency=latency)
            
            # Enqueue result write
            await client.enqueue_result(
                step_id=task['step_id'],
                table_name='BEDROCK_RESULTS',
                record_data={
                    'step_id': task['step_id'],
                    'summary': result['summary'],
                    'sentiment': result['sentiment'],
                    'key_points': result['key_points'],
                    'latency': latency
                }
            )
            
            logger.info(f"Worker {worker_id} completed task {task['step_id']} in {latency:.2f}s")
            
            return {'status': 'success', 'latency': latency}
        
        except Exception as e:
            error_str = str(e)
            
            if 'ThrottlingException' in error_str or '429' in error_str:
                # Report throttle
                await client.report_throttle()
                logger.warning(f"Worker {worker_id} throttled on task {task['step_id']}")
                return {'status': 'throttled'}
            else:
                # Report error
                await client.report_error(error_str)
                logger.error(f"Worker {worker_id} error on task {task['step_id']}: {e}")
                return {'status': 'error', 'error': error_str}
    
    finally:
        client.close()


# ============================================================================
# Task Generator (Simulates claim loop)
# ============================================================================

async def generate_tasks(count: int = 20) -> list:
    """
    Generate mock tasks.
    
    In production, this would be replaced by claim_service.claim_pending_steps()
    """
    tasks = []
    for i in range(count):
        tasks.append({
            'step_id': f'step-{i:03d}',
            'prompt': 'Summarize this document and extract key points',
            'document': f'Document content for step {i}...' * 50
        })
    return tasks


# ============================================================================
# aiomultiprocess Executor
# ============================================================================

async def execute_tasks_with_aiomultiprocess(
    tasks: list,
    iohub_address: str,
    num_workers: int = 4
):
    """
    Execute tasks using aiomultiprocess pool.
    
    Args:
        tasks: List of task dictionaries
        iohub_address: IOHub address for worker connections
        num_workers: Number of worker processes
    """
    if not AIOMULTIPROCESS_AVAILABLE:
        logger.error("aiomultiprocess not available")
        return
    
    logger.info(f"Starting aiomultiprocess pool with {num_workers} workers")
    
    async def worker_wrapper(task_with_index):
        idx, task = task_with_index
        worker_id = f"worker-{mp.current_process().pid}-{idx}"
        return await bedrock_worker(task, iohub_address, worker_id)
    
    # Create task list with indices
    tasks_with_indices = list(enumerate(tasks))
    
    # Execute with aiomultiprocess
    async with aiomultiprocess.Pool(processes=num_workers) as pool:
        results = await pool.map(worker_wrapper, tasks_with_indices)
    
    # Summarize results
    successes = sum(1 for r in results if r.get('status') == 'success')
    throttles = sum(1 for r in results if r.get('status') == 'throttled')
    errors = sum(1 for r in results if r.get('status') == 'error')
    
    logger.info(
        f"Task execution complete: "
        f"{successes} successes, {throttles} throttles, {errors} errors"
    )


# ============================================================================
# Custom Scheduled Job Example
# ============================================================================

async def custom_maintenance_job():
    """
    Custom scheduled job example.
    
    This could be:
    - Data quality checks
    - Report generation
    - Model retraining trigger
    - etc.
    """
    logger.info("Running custom maintenance job...")
    await asyncio.sleep(1)  # Simulate work
    logger.info("Custom maintenance job completed")


# ============================================================================
# Main Orchestration
# ============================================================================

async def main():
    """
    Main orchestration demonstrating full integration.
    
    Flow:
    1. Start Control Plane Runner (singleton)
    2. Add custom scheduled jobs
    3. Generate and execute tasks with aiomultiprocess
    4. Monitor status
    5. Graceful shutdown
    """
    logger.info("=" * 60)
    logger.info("FLEET-Q In-Pod Execution Fabric - Complete Integration Demo")
    logger.info("=" * 60)
    
    # ========================================================================
    # 1. Configure Control Plane
    # ========================================================================
    
    pod_id = os.environ.get('POD_ID', 'demo-pod-1')
    
    config = ControlPlaneConfig(
        pod_id=pod_id,
        outbox_db_path=f"/tmp/fleetq_demo_{pod_id}.db",
        lease_ttl_seconds=30,
        lease_renewal_interval_seconds=10,
        outbox_flush_interval_seconds=15,
        enable_metrics_logging=True,
        enable_claim_loop=False,  # We'll simulate tasks manually
        enable_heartbeat_loop=False,  # Optional
        aimd_config=AIMDConfig(
            initial_max_inflight=5,
            min_max_inflight=1,
            max_max_inflight=20,
            increase_rate=1.0,
            decrease_factor=0.5,
            success_streak_threshold=5
        )
    )
    
    # ========================================================================
    # 2. Create and Start Control Plane Runner
    # ========================================================================
    
    runner = ControlPlaneRunner(config)
    
    # Add custom scheduled job
    runner.add_custom_job(
        custom_maintenance_job,
        job_id="custom-maintenance",
        job_type="interval",
        minutes=5
    )
    
    # Start control plane
    started = await runner.start()
    
    if not started:
        logger.error("Failed to start Control Plane Runner (lease not acquired)")
        return
    
    # Give components time to initialize
    await asyncio.sleep(2)
    
    # ========================================================================
    # 3. Generate and Execute Tasks
    # ========================================================================
    
    logger.info("\n" + "=" * 60)
    logger.info("Executing tasks with aiomultiprocess")
    logger.info("=" * 60)
    
    tasks = await generate_tasks(count=30)
    
    iohub_address = config.iohub_address
    
    start_time = time.time()
    
    await execute_tasks_with_aiomultiprocess(
        tasks=tasks,
        iohub_address=iohub_address,
        num_workers=4
    )
    
    elapsed = time.time() - start_time
    throughput = len(tasks) / elapsed
    
    logger.info(f"\nExecution complete:")
    logger.info(f"  Total tasks: {len(tasks)}")
    logger.info(f"  Elapsed time: {elapsed:.2f}s")
    logger.info(f"  Throughput: {throughput:.2f} tasks/sec")
    
    # ========================================================================
    # 4. Monitor Status
    # ========================================================================
    
    logger.info("\n" + "=" * 60)
    logger.info("Control Plane Status")
    logger.info("=" * 60)
    
    await asyncio.sleep(2)  # Let outbox flush
    
    runner.print_status()
    
    # ========================================================================
    # 5. Run for a bit then shutdown
    # ========================================================================
    
    logger.info("\nRunning for 30 seconds before shutdown...")
    await asyncio.sleep(30)
    
    # ========================================================================
    # 6. Graceful Shutdown
    # ========================================================================
    
    logger.info("\n" + "=" * 60)
    logger.info("Initiating graceful shutdown")
    logger.info("=" * 60)
    
    await runner.stop()
    
    logger.info("\n✅ Demo completed successfully")


# ============================================================================
# Alternative: Run Forever Mode (Production)
# ============================================================================

async def production_mode():
    """
    Production mode: Run control plane forever.
    
    This is what you'd use in actual deployment with FastAPI workers.
    """
    logger.info("Starting FLEET-Q Control Plane Runner (production mode)")
    
    config = ControlPlaneConfig(
        pod_id=os.environ.get('POD_ID', 'pod-unknown'),
        enable_claim_loop=True,
        enable_heartbeat_loop=True
    )
    
    runner = ControlPlaneRunner(config)
    
    # Run forever (handles SIGINT/SIGTERM)
    await runner.run_forever()


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"
    
    if mode == "production":
        # Production mode: Run forever
        asyncio.run(production_mode())
    else:
        # Demo mode: Run example and exit
        asyncio.run(main())
