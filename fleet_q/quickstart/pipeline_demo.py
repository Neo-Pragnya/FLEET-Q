"""
Complete Pipeline Demo: SharePoint → Bedrock → Snowflake

This demonstrates the full in-pod parallelization pattern described in
Parallelization.md:

Architecture:
    SharePoint Reader → Bedrock Processor → Snowflake Writer
    (async downloads)   (async + throttle)   (batched writes)

This pipeline:
1. Downloads files from SharePoint (async I/O)
2. Processes them with Bedrock API (async + AIMD throttling)
3. Writes results to Snowflake (batched)

Key Features:
- Message-driven coordination (no external broker)
- Automatic backpressure (slow stages throttle upstream)
- Adaptive throttling (AIMD protects Bedrock)
- Graceful shutdown with flush
"""

import time
import logging
import signal
import sys
from typing import List, Dict, Any

from pipeline import Pipeline, PipelineMessage, MessageType
from sharepoint_reader import SharePointReaderStage, SharePointDownloadRequest, SharePointDownloadResponse
from bedrock_processor import BedrockProcessorStage, BedrockRequest, BedrockResponse
from snowflake_writer import SnowflakeWriterStage, WriteRecord
from throttle import ThrottleConfig


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(processName)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class SharePointToBedrockAdapter:
    """
    Adapter stage: SharePointDownloadResponse → BedrockRequest
    
    Converts downloaded files into Bedrock API requests.
    """
    
    @staticmethod
    def adapt(message: PipelineMessage) -> PipelineMessage:
        """Convert download response to Bedrock request"""
        if message.msg_type != MessageType.DATA:
            return message
        
        response = message.payload
        if not isinstance(response, SharePointDownloadResponse):
            return message
        
        if not response.success:
            # Skip failed downloads
            logger.warning(f"Skipping failed download: {response.request_id}")
            return None
        
        # Read file content to create prompt
        try:
            with open(response.local_path, 'r') as f:
                content = f.read()[:500]  # First 500 chars
        except Exception as e:
            logger.error(f"Failed to read {response.local_path}: {e}")
            return None
        
        # Create Bedrock request
        bedrock_request = BedrockRequest(
            model_id="anthropic.claude-v2",
            prompt=f"Summarize this document:\n\n{content}",
            request_id=response.request_id,
            metadata={
                'sharepoint_url': response.sharepoint_url,
                'local_path': response.local_path,
                'file_size': response.file_size_bytes
            }
        )
        
        return PipelineMessage(
            msg_type=MessageType.DATA,
            payload=bedrock_request,
            metadata=message.metadata
        )


class BedrockToSnowflakeAdapter:
    """
    Adapter stage: BedrockResponse → WriteRecord
    
    Converts Bedrock responses into Snowflake write records.
    """
    
    @staticmethod
    def adapt(message: PipelineMessage) -> PipelineMessage:
        """Convert Bedrock response to write record"""
        if message.msg_type != MessageType.DATA:
            return message
        
        response = message.payload
        if not isinstance(response, BedrockResponse):
            return message
        
        # Create write record
        write_record = WriteRecord(
            record_id=response.request_id,
            data={
                'request_id': response.request_id,
                'model_id': response.model_id,
                'response_text': response.response_text[:1000],  # Truncate
                'success': response.success,
                'latency': response.latency,
                'tokens_used': response.tokens_used,
                'error': response.error,
                'processed_at': time.time()
            }
        )
        
        return PipelineMessage(
            msg_type=MessageType.DATA,
            payload=write_record,
            metadata=message.metadata
        )


def create_demo_pipeline(
    num_files: int = 20,
    use_mock: bool = True
) -> Pipeline:
    """
    Create a complete demo pipeline.
    
    Args:
        num_files: Number of test files to process
        use_mock: Use mock clients (True) or real APIs (False)
    
    Returns:
        Configured Pipeline ready to start
    """
    logger.info(f"Creating demo pipeline (files={num_files}, mock={use_mock})")
    
    # Create pipeline
    pipeline = Pipeline(name="sharepoint-bedrock-snowflake")
    
    # ============================================================
    # Stage 1: SharePoint Reader (Async Downloads)
    # ============================================================
    
    reader = SharePointReaderStage(
        stage_name="SharePointReader",
        max_concurrent=10,
        download_dir="/tmp/fleet_q_demo/downloads",
        use_mock=use_mock
    )
    
    # ============================================================
    # Stage 2: Adapter (Download → Bedrock Request)
    # ============================================================
    
    from pipeline import TransformStage
    
    adapter1 = TransformStage(
        stage_name="DownloadToBedrock",
        transform_fn=lambda msg: SharePointToBedrockAdapter.adapt(msg) if isinstance(msg, PipelineMessage) else msg
    )
    
    # ============================================================
    # Stage 3: Bedrock Processor (Async + AIMD Throttling)
    # ============================================================
    
    processor = BedrockProcessorStage(
        stage_name="BedrockProcessor",
        throttle_config=ThrottleConfig(
            initial_limit=5,
            min_limit=2,
            max_limit=20,
            additive_increase=1,
            multiplicative_decrease=0.5,
            enable_latency_tracking=True,
            latency_window_size=50,
            latency_increase_threshold=1.5
        ),
        use_mock=use_mock
    )
    
    # ============================================================
    # Stage 4: Adapter (Bedrock Response → Write Record)
    # ============================================================
    
    adapter2 = TransformStage(
        stage_name="BedrockToSnowflake",
        transform_fn=lambda msg: BedrockToSnowflakeAdapter.adapt(msg) if isinstance(msg, PipelineMessage) else msg
    )
    
    # ============================================================
    # Stage 5: Snowflake Writer (Batched Writes)
    # ============================================================
    
    writer = SnowflakeWriterStage(
        stage_name="SnowflakeWriter",
        table_name="BEDROCK_RESULTS",
        batch_size=10,
        flush_timeout_seconds=3.0,
        use_mock=use_mock
    )
    
    # Add all stages
    pipeline.add_stage(reader)
    pipeline.add_stage(adapter1)
    pipeline.add_stage(processor)
    pipeline.add_stage(adapter2)
    pipeline.add_stage(writer)
    
    # Build pipeline (creates queues)
    pipeline.build()
    
    return pipeline


def feed_pipeline(pipeline: Pipeline, num_files: int):
    """
    Feed download requests into the pipeline.
    
    This simulates tasks being claimed from FLEET-Q's STEP_TRACKER.
    """
    logger.info(f"Feeding {num_files} download requests into pipeline")
    
    # Get input queue
    input_queue = pipeline.get_first_queue()
    if not input_queue:
        logger.error("Pipeline has no input queue")
        return
    
    # Feed requests
    for i in range(1, num_files + 1):
        request = SharePointDownloadRequest(
            sharepoint_url=f"/sites/demo/documents/doc{i}.txt",
            local_path=f"doc{i}.txt",
            request_id=f"file-{i:03d}",
            metadata={'batch': 'demo', 'index': i}
        )
        
        message = PipelineMessage(
            msg_type=MessageType.DATA,
            payload=request,
            metadata={'source': 'demo'}
        )
        
        input_queue.put(message)
    
    # Send poison pill to signal completion
    poison = PipelineMessage(
        msg_type=MessageType.POISON,
        payload=None
    )
    input_queue.put(poison)
    
    logger.info("All requests fed into pipeline")


def run_demo(num_files: int = 20, use_mock: bool = True):
    """
    Run complete pipeline demo.
    
    Args:
        num_files: Number of files to process
        use_mock: Use mock clients for demo
    """
    print("\n" + "="*70)
    print("FLEET-Q In-Pod Pipeline Demo")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Files to process: {num_files}")
    print(f"  Mode: {'MOCK (demo)' if use_mock else 'REAL (requires credentials)'}")
    print(f"\nPipeline stages:")
    print(f"  1. SharePoint Reader  (async downloads)")
    print(f"  2. Bedrock Processor  (async + AIMD throttling)")
    print(f"  3. Snowflake Writer   (batched writes)")
    print("\n" + "="*70 + "\n")
    
    # Create pipeline
    pipeline = create_demo_pipeline(num_files=num_files, use_mock=use_mock)
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print("\n\nShutdown signal received...")
        pipeline.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start pipeline
    start_time = time.time()
    
    try:
        logger.info("Starting pipeline...")
        pipeline.start()
        
        # Feed requests
        time.sleep(1)  # Let stages initialize
        feed_pipeline(pipeline, num_files)
        
        # Wait for completion
        logger.info("Waiting for pipeline to complete...")
        pipeline.wait()
        
        elapsed = time.time() - start_time
        
        print("\n" + "="*70)
        print("Pipeline Complete!")
        print("="*70)
        print(f"\nTotal time: {elapsed:.2f}s")
        print(f"Throughput: {num_files/elapsed:.2f} files/sec")
        print("\n" + "="*70 + "\n")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        pipeline.stop()
    
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        pipeline.stop()
        raise


def run_stress_test(duration_seconds: int = 60):
    """
    Run stress test to observe AIMD adaptation.
    
    This continuously feeds requests and monitors throttle behavior.
    """
    print("\n" + "="*70)
    print("FLEET-Q Pipeline Stress Test")
    print("="*70)
    print(f"\nDuration: {duration_seconds}s")
    print("Watch the Bedrock throttle adapt to load...\n")
    print("="*70 + "\n")
    
    pipeline = create_demo_pipeline(num_files=1000, use_mock=True)
    
    def signal_handler(sig, frame):
        print("\nStopping stress test...")
        pipeline.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        pipeline.start()
        time.sleep(1)
        
        # Continuously feed requests
        start_time = time.time()
        request_count = 0
        
        input_queue = pipeline.get_first_queue()
        
        while time.time() - start_time < duration_seconds:
            request = SharePointDownloadRequest(
                sharepoint_url=f"/sites/stress/doc{request_count}.txt",
                local_path=f"stress{request_count}.txt",
                request_id=f"stress-{request_count:06d}"
            )
            
            message = PipelineMessage(
                msg_type=MessageType.DATA,
                payload=request
            )
            
            input_queue.put(message, timeout=1.0)
            request_count += 1
            
            # Log every 50 requests
            if request_count % 50 == 0:
                logger.info(f"Fed {request_count} requests")
        
        # Send poison pill
        input_queue.put(PipelineMessage(MessageType.POISON, None))
        
        logger.info(f"Stress test complete. Fed {request_count} requests.")
        pipeline.wait()
    
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    """
    Demo entry point with multiple modes.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="FLEET-Q Pipeline Demo")
    parser.add_argument(
        '--mode',
        choices=['demo', 'stress'],
        default='demo',
        help='Demo mode: normal demo or stress test'
    )
    parser.add_argument(
        '--files',
        type=int,
        default=20,
        help='Number of files to process (demo mode)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Duration in seconds (stress mode)'
    )
    parser.add_argument(
        '--real',
        action='store_true',
        help='Use real APIs (requires credentials)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'demo':
            run_demo(num_files=args.files, use_mock=not args.real)
        elif args.mode == 'stress':
            run_stress_test(duration_seconds=args.duration)
    
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        sys.exit(1)
