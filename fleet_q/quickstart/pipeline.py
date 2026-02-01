"""
Core Pipeline Infrastructure for In-Pod Multi-Stage Execution

This module implements a message-driven pipeline for efficient processing
of HTTP-heavy workloads (like Bedrock API calls) using:
- Multiprocessing for isolation
- Async I/O for efficiency
- In-pod message queues for coordination
- Automatic backpressure

Architecture:
    Stage 1 (Reader) → Queue → Stage 2 (Processor) → Queue → Stage 3 (Writer)

Each stage runs in its own process and communicates via multiprocessing queues.
"""

import multiprocessing as mp
from multiprocessing import Queue, Process, Event
import time
import logging
from typing import Any, Callable, Optional, Dict, List
from dataclasses import dataclass
from enum import Enum
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(processName)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Message types for pipeline coordination"""
    DATA = "data"
    POISON = "poison"  # Signal to shutdown gracefully
    HEARTBEAT = "heartbeat"


@dataclass
class PipelineMessage:
    """Standard message format for pipeline stages"""
    msg_type: MessageType
    payload: Any
    metadata: Optional[Dict[str, Any]] = None
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


class PipelineStage:
    """
    Base class for pipeline stages.
    
    Each stage:
    - Reads from input queue
    - Processes messages
    - Writes to output queue
    - Handles graceful shutdown
    """
    
    def __init__(
        self,
        stage_name: str,
        input_queue: Optional[Queue] = None,
        output_queue: Optional[Queue] = None,
        shutdown_event: Optional[Event] = None
    ):
        self.stage_name = stage_name
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.shutdown_event = shutdown_event or Event()
        self.logger = logging.getLogger(f"Stage.{stage_name}")
        self.processed_count = 0
        self.error_count = 0
    
    def setup(self):
        """Initialize stage-specific resources (override in subclass)"""
        pass
    
    def teardown(self):
        """Cleanup stage-specific resources (override in subclass)"""
        pass
    
    def process_message(self, message: PipelineMessage) -> Optional[PipelineMessage]:
        """
        Process a single message (override in subclass).
        
        Returns:
            - New message to send downstream
            - None to filter out message
        """
        raise NotImplementedError("Subclass must implement process_message()")
    
    def send_downstream(self, message: PipelineMessage):
        """Send message to next stage"""
        if self.output_queue:
            self.output_queue.put(message)
    
    def run(self):
        """Main stage execution loop"""
        self.logger.info(f"Stage '{self.stage_name}' starting")
        
        try:
            self.setup()
            
            while not self.shutdown_event.is_set():
                try:
                    # Non-blocking read with timeout for shutdown checks
                    if not self.input_queue:
                        self.logger.warning("No input queue - stage idling")
                        time.sleep(1)
                        continue
                    
                    try:
                        message = self.input_queue.get(timeout=1.0)
                    except Exception:
                        # Timeout - check shutdown and continue
                        continue
                    
                    # Handle poison pill (shutdown signal)
                    if message.msg_type == MessageType.POISON:
                        self.logger.info("Received POISON - shutting down")
                        # Forward poison pill downstream
                        if self.output_queue:
                            self.send_downstream(message)
                        break
                    
                    # Process message
                    try:
                        result = self.process_message(message)
                        
                        if result:
                            self.send_downstream(result)
                        
                        self.processed_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}", exc_info=True)
                        self.error_count += 1
                
                except KeyboardInterrupt:
                    self.logger.info("Keyboard interrupt - shutting down")
                    break
        
        finally:
            self.teardown()
            self.logger.info(
                f"Stage '{self.stage_name}' stopped. "
                f"Processed: {self.processed_count}, Errors: {self.error_count}"
            )


class Pipeline:
    """
    Multi-stage pipeline orchestrator.
    
    Manages:
    - Stage processes
    - Inter-stage queues
    - Graceful shutdown
    - Monitoring
    """
    
    def __init__(self, name: str = "pipeline"):
        self.name = name
        self.stages: List[PipelineStage] = []
        self.queues: List[Queue] = []
        self.processes: List[Process] = []
        self.shutdown_event = Event()
        self.logger = logging.getLogger(f"Pipeline.{name}")
    
    def add_stage(self, stage: PipelineStage):
        """Add a stage to the pipeline"""
        self.stages.append(stage)
    
    def build(self):
        """
        Build the pipeline by creating queues and connecting stages.
        Call this after adding all stages.
        """
        if not self.stages:
            raise ValueError("No stages added to pipeline")
        
        # Create queues between stages
        for i in range(len(self.stages) - 1):
            queue = Queue(maxsize=100)  # Bounded queue for backpressure
            self.queues.append(queue)
        
        # Wire up stages
        for i, stage in enumerate(self.stages):
            # Input queue (None for first stage if it's a source)
            if i > 0:
                stage.input_queue = self.queues[i - 1]
            
            # Output queue (None for last stage if it's a sink)
            if i < len(self.stages) - 1:
                stage.output_queue = self.queues[i]
            
            stage.shutdown_event = self.shutdown_event
        
        self.logger.info(f"Pipeline built with {len(self.stages)} stages")
    
    def start(self):
        """Start all stage processes"""
        self.logger.info(f"Starting pipeline '{self.name}'")
        
        for stage in self.stages:
            process = Process(
                target=stage.run,
                name=f"{self.name}.{stage.stage_name}"
            )
            process.start()
            self.processes.append(process)
            self.logger.info(f"Started process for stage '{stage.stage_name}' (PID: {process.pid})")
    
    def stop(self, timeout: float = 30.0):
        """
        Stop all stages gracefully.
        
        Args:
            timeout: Seconds to wait for graceful shutdown
        """
        self.logger.info("Initiating pipeline shutdown")
        
        # Signal shutdown event
        self.shutdown_event.set()
        
        # Send poison pills through pipeline
        if self.stages and self.stages[0].input_queue:
            poison = PipelineMessage(
                msg_type=MessageType.POISON,
                payload=None
            )
            try:
                self.stages[0].input_queue.put(poison, timeout=5.0)
            except Exception as e:
                self.logger.warning(f"Could not send poison pill: {e}")
        
        # Wait for processes to finish
        start_time = time.time()
        for process in self.processes:
            remaining = timeout - (time.time() - start_time)
            if remaining > 0:
                process.join(timeout=remaining)
                if process.is_alive():
                    self.logger.warning(f"Process {process.name} did not stop gracefully, terminating")
                    process.terminate()
                    process.join(timeout=5.0)
            else:
                self.logger.warning(f"Timeout exceeded, terminating {process.name}")
                process.terminate()
        
        self.logger.info("Pipeline stopped")
    
    def get_first_queue(self) -> Optional[Queue]:
        """Get the input queue of the first stage"""
        if self.stages and self.stages[0].input_queue:
            return self.stages[0].input_queue
        return None
    
    def wait(self):
        """Wait for all processes to complete"""
        for process in self.processes:
            process.join()


# Example stages for demonstration

class SourceStage(PipelineStage):
    """
    Example source stage that generates messages.
    Useful for testing or feeding initial data.
    """
    
    def __init__(self, stage_name: str, items: List[Any], **kwargs):
        super().__init__(stage_name, **kwargs)
        self.items = items
    
    def run(self):
        """Override run to generate messages"""
        self.logger.info(f"Source stage '{self.stage_name}' starting")
        
        try:
            for i, item in enumerate(self.items):
                if self.shutdown_event.is_set():
                    break
                
                message = PipelineMessage(
                    msg_type=MessageType.DATA,
                    payload=item,
                    metadata={"index": i}
                )
                
                self.send_downstream(message)
                self.processed_count += 1
            
            # Send poison pill when done
            if self.output_queue:
                self.send_downstream(PipelineMessage(
                    msg_type=MessageType.POISON,
                    payload=None
                ))
        
        finally:
            self.logger.info(
                f"Source stage '{self.stage_name}' completed. "
                f"Generated: {self.processed_count}"
            )


class TransformStage(PipelineStage):
    """
    Example transform stage that applies a function to messages.
    """
    
    def __init__(
        self,
        stage_name: str,
        transform_fn: Callable[[Any], Any],
        **kwargs
    ):
        super().__init__(stage_name, **kwargs)
        self.transform_fn = transform_fn
    
    def process_message(self, message: PipelineMessage) -> Optional[PipelineMessage]:
        """Apply transformation"""
        if message.msg_type != MessageType.DATA:
            return message
        
        try:
            transformed = self.transform_fn(message.payload)
            
            return PipelineMessage(
                msg_type=MessageType.DATA,
                payload=transformed,
                metadata=message.metadata
            )
        except Exception as e:
            self.logger.error(f"Transform error: {e}")
            return None


class SinkStage(PipelineStage):
    """
    Example sink stage that collects results.
    """
    
    def __init__(self, stage_name: str, **kwargs):
        super().__init__(stage_name, **kwargs)
        self.results = []
    
    def process_message(self, message: PipelineMessage) -> Optional[PipelineMessage]:
        """Collect result"""
        if message.msg_type == MessageType.DATA:
            self.results.append(message.payload)
            self.logger.info(f"Collected result: {message.payload}")
        
        return None  # Sink - no downstream


# Demo usage
if __name__ == "__main__":
    """
    Demonstrate a simple 3-stage pipeline:
    Source → Transform → Sink
    """
    
    print("=== Pipeline Demo ===\n")
    
    # Create pipeline
    pipeline = Pipeline(name="demo")
    
    # Stage 1: Source (generate numbers)
    source = SourceStage(
        stage_name="NumberGenerator",
        items=list(range(1, 11))
    )
    
    # Stage 2: Transform (square numbers)
    def square(x):
        time.sleep(0.1)  # Simulate work
        return x * x
    
    transform = TransformStage(
        stage_name="Squarer",
        transform_fn=square
    )
    
    # Stage 3: Sink (collect results)
    sink = SinkStage(stage_name="Collector")
    
    # Add stages and build
    pipeline.add_stage(source)
    pipeline.add_stage(transform)
    pipeline.add_stage(sink)
    pipeline.build()
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print("\nShutdown signal received")
        pipeline.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start pipeline
    try:
        pipeline.start()
        pipeline.wait()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        pipeline.stop()
    
    print("\n=== Pipeline Complete ===")
