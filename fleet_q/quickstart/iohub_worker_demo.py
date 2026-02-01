"""
IOHub-Enhanced Worker Example

Demonstrates how workers use IOHub for:
- Shared AIMD throttle control
- Local SQLite outbox writes
- Bedrock API calls with permits

This shows the complete pattern:
Worker → IOHub (request permit) → Bedrock API → IOHub (report outcome)
"""

import asyncio
import time
import logging
from typing import Dict, Any, Optional
import multiprocessing as mp

from iohub import (
    IOHubPipeBased,
    IOHubZMQBased,
    IOHubClientPipe,
    IOHubClientZMQ,
    IOHubMessageType
)
from throttle import ThrottleConfig


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(processName)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# Worker Functions
# ============================================================

async def bedrock_worker_with_iohub(
    worker_id: str,
    iohub_client,
    tasks: list,
    use_mock: bool = True
):
    """
    Worker that calls Bedrock with IOHub coordination.
    
    Flow:
    1. Request permit from IOHub
    2. Call Bedrock API
    3. Report outcome to IOHub
    4. Release permit
    5. Enqueue result write to IOHub
    """
    logger.info(f"Worker {worker_id} starting with {len(tasks)} tasks")
    
    results = []
    
    for task in tasks:
        task_id = task.get('id', 'unknown')
        prompt = task.get('prompt', '')
        
        logger.info(f"[{worker_id}] Processing task {task_id}")
        
        # Step 1: Request permit from IOHub
        logger.debug(f"[{worker_id}] Requesting permit...")
        start_wait = time.time()
        
        if not iohub_client.request_permit():
            logger.error(f"[{worker_id}] Failed to get permit for {task_id}")
            continue
        
        wait_time = time.time() - start_wait
        logger.debug(f"[{worker_id}] Permit granted (waited {wait_time:.3f}s)")
        
        try:
            # Step 2: Call Bedrock API
            start_call = time.time()
            
            if use_mock:
                response = await mock_bedrock_call(prompt)
            else:
                response = await real_bedrock_call(prompt)
            
            call_latency = time.time() - start_call
            
            logger.info(
                f"[{worker_id}] Task {task_id} succeeded "
                f"(latency: {call_latency:.3f}s)"
            )
            
            # Step 3: Report success to IOHub
            iohub_client.report_outcome('success', latency=call_latency)
            
            # Store result
            result = {
                'task_id': task_id,
                'response': response,
                'latency': call_latency,
                'success': True
            }
            results.append(result)
            
            # Step 5: Enqueue write to IOHub outbox
            iohub_client.enqueue_write(
                step_id=task_id,
                table_name='BEDROCK_RESULTS',
                record_data=result
            )
        
        except ThrottleException as e:
            logger.warning(f"[{worker_id}] Task {task_id} throttled: {e}")
            
            # Report throttle to IOHub
            iohub_client.report_outcome('throttle')
        
        except Exception as e:
            logger.error(f"[{worker_id}] Task {task_id} failed: {e}")
            
            # Report timeout/error
            iohub_client.report_outcome('timeout')
        
        finally:
            # Step 4: Always release permit
            iohub_client.release_permit()
    
    logger.info(f"Worker {worker_id} completed {len(results)}/{len(tasks)} tasks")
    return results


async def mock_bedrock_call(prompt: str) -> str:
    """Mock Bedrock API call"""
    import random
    
    # Simulate variable latency
    await asyncio.sleep(random.uniform(0.1, 0.5))
    
    # Simulate occasional throttling (5%)
    if random.random() < 0.05:
        raise ThrottleException("Mock throttle error")
    
    return f"Mock response for: {prompt[:30]}..."


async def real_bedrock_call(prompt: str) -> str:
    """Real Bedrock API call"""
    import boto3
    import json
    
    client = boto3.client('bedrock-runtime')
    
    body = json.dumps({
        "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
        "max_tokens_to_sample": 512,
        "temperature": 0.7,
    })
    
    response = client.invoke_model(
        modelId="anthropic.claude-v2",
        body=body
    )
    
    response_body = json.loads(response['body'].read())
    return response_body.get('completion', '')


class ThrottleException(Exception):
    """Bedrock throttle error"""
    pass


# ============================================================
# Demo Scenarios
# ============================================================

def demo_pipe_based():
    """
    Demo: Pipe-based IOHub with multiple workers
    """
    print("\n" + "="*70)
    print("Pipe-Based IOHub Demo")
    print("="*70 + "\n")
    
    # Create tasks
    tasks = [
        {'id': f'task-{i:03d}', 'prompt': f'Tell me about topic {i}'}
        for i in range(1, 21)
    ]
    
    # Create pipe and IOHub
    parent_conn, child_conn = mp.Pipe()
    
    throttle_config = ThrottleConfig(
        initial_limit=5,
        min_limit=2,
        max_limit=15,
        enable_latency_tracking=True
    )
    
    hub = IOHubPipeBased(throttle_config=throttle_config)
    hub_process = mp.Process(target=hub.run, args=(child_conn,))
    hub_process.start()
    
    time.sleep(0.5)  # Let hub start
    
    # Single worker with IOHub client
    client = IOHubClientPipe(parent_conn, "worker-001")
    
    # Run worker
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    start_time = time.time()
    results = loop.run_until_complete(
        bedrock_worker_with_iohub("worker-001", client, tasks)
    )
    elapsed = time.time() - start_time
    
    # Get final throttle status
    status = hub.throttle_controller.get_status()
    
    print("\n" + "="*70)
    print("Results:")
    print("="*70)
    print(f"Tasks processed: {len(results)}/{len(tasks)}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {len(results)/elapsed:.2f} tasks/sec")
    print(f"\nFinal throttle limit: {status['max_inflight']}")
    print(f"Throttle rate: {status['throttle_rate']:.2%}")
    print("="*70 + "\n")
    
    # Get outbox stats
    outbox_stats = hub.outbox.get_stats()
    print("Outbox stats:")
    print(f"  Snowflake pending: {outbox_stats['snowflake'].get('pending', 0)}")
    print(f"  Snowflake flushed: {outbox_stats['snowflake'].get('flushed', 0)}")
    
    # Cleanup
    hub.running = False
    hub_process.join(timeout=2)
    hub_process.terminate()


def demo_zmq_based():
    """
    Demo: ZMQ ROUTER/DEALER IOHub with multiple workers
    """
    print("\n" + "="*70)
    print("ZMQ ROUTER/DEALER IOHub Demo")
    print("="*70 + "\n")
    
    try:
        import zmq
    except ImportError:
        print("ERROR: pyzmq not installed")
        print("Run: pip install pyzmq")
        return
    
    # Create tasks
    tasks = [
        {'id': f'task-{i:03d}', 'prompt': f'Tell me about topic {i}'}
        for i in range(1, 21)
    ]
    
    # Start IOHub
    throttle_config = ThrottleConfig(
        initial_limit=5,
        min_limit=2,
        max_limit=15,
        enable_latency_tracking=True
    )
    
    hub = IOHubZMQBased(
        bind_address="tcp://127.0.0.1:5555",
        throttle_config=throttle_config
    )
    hub_process = mp.Process(target=hub.run)
    hub_process.start()
    
    time.sleep(1)  # Let hub start
    
    # Create worker with ZMQ client
    client = IOHubClientZMQ("tcp://127.0.0.1:5555", "worker-001")
    
    # Run worker
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    start_time = time.time()
    results = loop.run_until_complete(
        bedrock_worker_with_iohub("worker-001", client, tasks)
    )
    elapsed = time.time() - start_time
    
    # Get final throttle status
    status = hub.throttle_controller.get_status()
    
    print("\n" + "="*70)
    print("Results:")
    print("="*70)
    print(f"Tasks processed: {len(results)}/{len(tasks)}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {len(results)/elapsed:.2f} tasks/sec")
    print(f"\nFinal throttle limit: {status['max_inflight']}")
    print(f"Throttle rate: {status['throttle_rate']:.2%}")
    print("="*70 + "\n")
    
    # Get outbox stats
    outbox_stats = hub.outbox.get_stats()
    print("Outbox stats:")
    print(f"  Snowflake pending: {outbox_stats['snowflake'].get('pending', 0)}")
    print(f"  Snowflake flushed: {outbox_stats['snowflake'].get('flushed', 0)}")
    
    # Cleanup
    client.close()
    hub.running = False
    hub_process.join(timeout=2)
    hub_process.terminate()


def demo_multi_worker_zmq():
    """
    Demo: Multiple workers sharing IOHub via ZMQ
    """
    print("\n" + "="*70)
    print("Multi-Worker ZMQ IOHub Demo")
    print("="*70 + "\n")
    
    try:
        import zmq
    except ImportError:
        print("ERROR: pyzmq not installed")
        return
    
    # Create tasks (split across workers)
    all_tasks = [
        {'id': f'task-{i:03d}', 'prompt': f'Tell me about topic {i}'}
        for i in range(1, 41)
    ]
    
    # Start IOHub
    throttle_config = ThrottleConfig(
        initial_limit=5,
        min_limit=2,
        max_limit=20,
        enable_latency_tracking=True
    )
    
    hub = IOHubZMQBased(
        bind_address="tcp://127.0.0.1:5555",
        throttle_config=throttle_config
    )
    hub_process = mp.Process(target=hub.run)
    hub_process.start()
    
    time.sleep(1)
    
    # Split tasks across 3 workers
    chunk_size = len(all_tasks) // 3
    worker_tasks = [
        all_tasks[:chunk_size],
        all_tasks[chunk_size:chunk_size*2],
        all_tasks[chunk_size*2:]
    ]
    
    # Run workers concurrently
    async def run_worker(worker_id: str, tasks: list):
        client = IOHubClientZMQ("tcp://127.0.0.1:5555", worker_id)
        results = await bedrock_worker_with_iohub(worker_id, client, tasks)
        client.close()
        return results
    
    async def run_all_workers():
        tasks = [
            run_worker(f"worker-{i:03d}", worker_tasks[i])
            for i in range(3)
        ]
        return await asyncio.gather(*tasks)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    start_time = time.time()
    all_results = loop.run_until_complete(run_all_workers())
    elapsed = time.time() - start_time
    
    total_tasks = sum(len(r) for r in all_results)
    
    # Get final throttle status
    status = hub.throttle_controller.get_status()
    
    print("\n" + "="*70)
    print("Results:")
    print("="*70)
    print(f"Workers: 3")
    print(f"Tasks processed: {total_tasks}/{len(all_tasks)}")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Throughput: {total_tasks/elapsed:.2f} tasks/sec")
    print(f"\nFinal throttle limit: {status['max_inflight']}")
    print(f"Throttle rate: {status['throttle_rate']:.2%}")
    print("="*70 + "\n")
    
    # Cleanup
    hub.running = False
    hub_process.join(timeout=2)
    hub_process.terminate()


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    """
    Run IOHub demos
    """
    print("\n" + "="*70)
    print("IOHub Worker Demo")
    print("="*70)
    print("\nChoose demo:")
    print("1. Pipe-based IOHub (single worker)")
    print("2. ZMQ IOHub (single worker)")
    print("3. ZMQ IOHub (multiple workers)")
    print()
    
    choice = input("Enter choice (1-3): ").strip()
    
    if choice == "1":
        demo_pipe_based()
    elif choice == "2":
        demo_zmq_based()
    elif choice == "3":
        demo_multi_worker_zmq()
    else:
        print("Invalid choice")
