"""
Integration Example: Using Pipeline within FLEET-Q Task Execution

This shows how to integrate the in-pod pipeline into FLEET-Q's task executor.

Two approaches:
1. Pipeline per task (create/destroy for each task)
2. Persistent pipeline (reuse across multiple tasks)
"""

import asyncio
import json
import logging
from typing import Dict, Any, Optional

from pipeline import Pipeline
from sharepoint_reader import SharePointReaderStage, SharePointDownloadRequest
from bedrock_processor import BedrockProcessorStage, BedrockRequest
from snowflake_writer import SnowflakeWriterStage, WriteRecord
from throttle import ThrottleConfig


logger = logging.getLogger(__name__)


# ============================================================
# Approach 1: Pipeline Per Task
# ============================================================

async def execute_batch_task_with_pipeline(step: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a batch processing task using a dedicated pipeline.
    
    Use this when:
    - Each task has many documents to process
    - Task volume is low
    - Setup overhead is acceptable
    """
    payload = step['PAYLOAD']
    documents = payload.get('documents', [])
    
    if not documents:
        return {"status": "error", "message": "No documents provided"}
    
    logger.info(f"Processing {len(documents)} documents with pipeline")
    
    # Create pipeline
    pipeline = Pipeline(f"task-{step['STEP_ID']}")
    
    pipeline.add_stage(SharePointReaderStage(
        stage_name="Download",
        max_concurrent=10,
        use_mock=False
    ))
    
    pipeline.add_stage(BedrockProcessorStage(
        stage_name="Process",
        throttle_config=ThrottleConfig(
            initial_limit=10,
            max_limit=50,
            enable_latency_tracking=True
        ),
        use_mock=False
    ))
    
    pipeline.add_stage(SnowflakeWriterStage(
        stage_name="Write",
        table_name="PROCESSED_DOCUMENTS",
        batch_size=50
    ))
    
    pipeline.build()
    
    try:
        # Start pipeline
        pipeline.start()
        
        # Feed documents
        input_queue = pipeline.get_first_queue()
        for doc in documents:
            request = SharePointDownloadRequest(
                sharepoint_url=doc['url'],
                local_path=doc['filename'],
                request_id=doc['id']
            )
            
            from pipeline import PipelineMessage, MessageType
            message = PipelineMessage(MessageType.DATA, request)
            input_queue.put(message)
        
        # Send poison pill
        input_queue.put(PipelineMessage(MessageType.POISON, None))
        
        # Wait for completion
        pipeline.wait()
        
        return {
            "status": "success",
            "documents_processed": len(documents)
        }
    
    finally:
        pipeline.stop()


# ============================================================
# Approach 2: Persistent Pipeline (More Efficient)
# ============================================================

class PipelineManager:
    """
    Manages a persistent pipeline that's reused across multiple tasks.
    
    Use this when:
    - High task volume
    - Want to amortize setup cost
    - Tasks have similar processing needs
    """
    
    def __init__(self):
        self.pipeline: Optional[Pipeline] = None
        self.is_running = False
    
    def start(self):
        """Start the persistent pipeline"""
        if self.is_running:
            logger.warning("Pipeline already running")
            return
        
        logger.info("Starting persistent pipeline")
        
        self.pipeline = Pipeline("persistent")
        
        self.pipeline.add_stage(SharePointReaderStage(
            stage_name="Download",
            max_concurrent=20,
            use_mock=False
        ))
        
        self.pipeline.add_stage(BedrockProcessorStage(
            stage_name="Process",
            throttle_config=ThrottleConfig(
                initial_limit=10,
                max_limit=100,
                enable_latency_tracking=True
            ),
            use_mock=False
        ))
        
        self.pipeline.add_stage(SnowflakeWriterStage(
            stage_name="Write",
            table_name="PROCESSED_DOCUMENTS",
            batch_size=100,
            flush_timeout_seconds=10.0
        ))
        
        self.pipeline.build()
        self.pipeline.start()
        
        self.is_running = True
        logger.info("Persistent pipeline started")
    
    def stop(self):
        """Stop the persistent pipeline"""
        if not self.is_running:
            return
        
        logger.info("Stopping persistent pipeline")
        
        if self.pipeline:
            self.pipeline.stop()
        
        self.is_running = False
        logger.info("Persistent pipeline stopped")
    
    def process_task(self, step: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task through the persistent pipeline"""
        if not self.is_running:
            raise RuntimeError("Pipeline not running")
        
        payload = step['PAYLOAD']
        documents = payload.get('documents', [])
        
        if not documents:
            return {"status": "error", "message": "No documents"}
        
        logger.info(f"Feeding {len(documents)} documents into persistent pipeline")
        
        # Feed into pipeline
        input_queue = self.pipeline.get_first_queue()
        
        from pipeline import PipelineMessage, MessageType
        
        for doc in documents:
            request = SharePointDownloadRequest(
                sharepoint_url=doc['url'],
                local_path=doc['filename'],
                request_id=doc['id'],
                metadata={'task_id': step['STEP_ID']}
            )
            
            message = PipelineMessage(MessageType.DATA, request)
            input_queue.put(message)
        
        return {
            "status": "accepted",
            "documents_queued": len(documents),
            "note": "Processing asynchronously in pipeline"
        }


# Global pipeline manager (initialized at startup)
_pipeline_manager: Optional[PipelineManager] = None


def get_pipeline_manager() -> PipelineManager:
    """Get or create the global pipeline manager"""
    global _pipeline_manager
    
    if _pipeline_manager is None:
        _pipeline_manager = PipelineManager()
    
    return _pipeline_manager


# ============================================================
# Integration with main.py
# ============================================================

async def execute_task_with_pipeline_support(step: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enhanced task executor that supports both regular and pipeline tasks.
    
    This can be used in main.py's execute_task() function.
    """
    task_type = step['PAYLOAD'].get('task_type', 'unknown')
    
    if task_type == 'batch_document_processing':
        # Use persistent pipeline
        manager = get_pipeline_manager()
        
        if not manager.is_running:
            manager.start()
        
        return manager.process_task(step)
    
    elif task_type == 'large_document_batch':
        # Use dedicated pipeline for very large batches
        return await execute_batch_task_with_pipeline(step)
    
    else:
        # Regular task execution (no pipeline)
        return await execute_regular_task(step)


async def execute_regular_task(step: Dict[str, Any]) -> Dict[str, Any]:
    """Regular task execution (without pipeline)"""
    task_type = step['PAYLOAD'].get('task_type')
    
    # Your existing task execution logic
    logger.info(f"Executing regular task: {task_type}")
    
    # Example
    await asyncio.sleep(1)
    
    return {"status": "success", "task_type": task_type}


# ============================================================
# Startup/Shutdown Integration
# ============================================================

def startup_pipeline():
    """
    Call this during application startup.
    
    In main.py's lifespan context manager:
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Existing startup
        ...
        
        # Start pipeline
        startup_pipeline()
        
        yield
        
        # Shutdown pipeline
        shutdown_pipeline()
    """
    logger.info("Starting persistent pipeline at application startup")
    manager = get_pipeline_manager()
    manager.start()


def shutdown_pipeline():
    """Call this during application shutdown"""
    logger.info("Shutting down persistent pipeline")
    manager = get_pipeline_manager()
    manager.stop()


# ============================================================
# Example: Modified main.py lifespan
# ============================================================

def example_lifespan_integration():
    """
    Example showing how to integrate with main.py's lifespan.
    
    Replace your lifespan function with:
    """
    from contextlib import asynccontextmanager
    from fastapi import FastAPI
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Existing startup
        logger.info("Starting FLEET-Q worker")
        
        # Start worker loops
        worker = WorkerLoops(config, storage, queue_ops)
        worker_task = asyncio.create_task(worker.start_all())
        
        # Start persistent pipeline
        startup_pipeline()
        
        yield
        
        # Shutdown
        logger.info("Shutting down")
        worker.stop_all()
        
        # Shutdown pipeline
        shutdown_pipeline()
        
        await worker_task


# ============================================================
# Performance Comparison
# ============================================================

def performance_comparison():
    """
    Expected performance characteristics:
    
    Without Pipeline (naive multiprocessing):
    - 50 documents
    - 4 processes
    - Fixed concurrency
    - ~60s total time
    - Frequent throttle errors
    
    With Pipeline (async + AIMD):
    - 50 documents
    - 1 process per stage (3 total)
    - Adaptive concurrency (5-20)
    - ~25s total time
    - Zero throttle errors
    
    Improvement: 2.4x faster, more stable
    """
    pass


if __name__ == "__main__":
    """
    Demo the integration patterns.
    """
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(processName)s] [%(levelname)s] %(message)s'
    )
    
    print("\n" + "="*70)
    print("FLEET-Q Pipeline Integration Examples")
    print("="*70)
    print("\nApproach 1: Pipeline Per Task")
    print("  - Creates fresh pipeline for each task")
    print("  - Good for: Low volume, large batches")
    print("  - Overhead: ~2-3s startup per task")
    
    print("\nApproach 2: Persistent Pipeline")
    print("  - Reuses pipeline across tasks")
    print("  - Good for: High volume, streaming")
    print("  - Overhead: ~2-3s once at startup")
    
    print("\nRecommendation:")
    print("  Use persistent pipeline for production workloads.")
    print("  It amortizes startup cost and maintains warm connections.")
    
    print("\n" + "="*70)
    print("\nIntegration Steps:")
    print("  1. Add pipeline startup to main.py lifespan")
    print("  2. Modify execute_task() to check task_type")
    print("  3. Route pipeline-eligible tasks accordingly")
    print("  4. Monitor throttle metrics via /admin/throttle")
    
    print("\n" + "="*70 + "\n")
