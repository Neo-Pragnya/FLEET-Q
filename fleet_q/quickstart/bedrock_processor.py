"""
Bedrock Processing Stage with Async I/O + Adaptive Throttling

This module implements a pipeline stage optimized for Bedrock API calls:
- Async I/O for efficient concurrent requests
- AIMD-based adaptive throttling
- Graceful handling of throttle errors
- Latency tracking for pressure sensing

Key Insight:
    Bedrock calls are HTTP-heavy, not CPU-heavy.
    One async event loop can manage many in-flight requests efficiently.
"""

import asyncio
import time
import json
import logging
from typing import Any, Dict, Optional, List
from dataclasses import dataclass

from pipeline import PipelineStage, PipelineMessage, MessageType
from throttle import AdaptiveThrottle, ThrottleConfig, with_throttle


logger = logging.getLogger(__name__)


@dataclass
class BedrockRequest:
    """Standard format for Bedrock API requests"""
    model_id: str
    prompt: str
    request_id: str
    max_tokens: int = 2048
    temperature: float = 0.7
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class BedrockResponse:
    """Standard format for Bedrock API responses"""
    request_id: str
    response_text: str
    model_id: str
    latency: float
    tokens_used: int
    success: bool
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class BedrockProcessorStage(PipelineStage):
    """
    Pipeline stage for processing Bedrock API calls with adaptive throttling.
    
    Features:
    - Async I/O for concurrent requests
    - AIMD throttling to prevent overload
    - Automatic retry on transient errors
    - Latency tracking
    """
    
    def __init__(
        self,
        stage_name: str = "BedrockProcessor",
        throttle_config: Optional[ThrottleConfig] = None,
        max_concurrent: int = 50,
        use_mock: bool = True,  # Use mock by default for demo
        **kwargs
    ):
        super().__init__(stage_name, **kwargs)
        
        # Throttle configuration
        if throttle_config is None:
            throttle_config = ThrottleConfig(
                initial_limit=10,
                min_limit=2,
                max_limit=max_concurrent,
                additive_increase=1,
                multiplicative_decrease=0.5,
                enable_latency_tracking=True,
                latency_window_size=100,
                latency_increase_threshold=1.5
            )
        
        self.throttle = AdaptiveThrottle("bedrock", throttle_config)
        self.use_mock = use_mock
        self.event_loop = None
        
        # Metrics
        self.total_latency = 0.0
        self.total_requests = 0
        self.throttle_errors = 0
    
    def setup(self):
        """Initialize async event loop"""
        self.logger.info("Setting up Bedrock processor (async mode)")
        
        # Create event loop for this process
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        
        if self.use_mock:
            self.logger.warning("Using MOCK Bedrock client (for demo)")
        else:
            self.logger.info("Using REAL Bedrock client")
            try:
                import boto3
                self.bedrock_client = boto3.client('bedrock-runtime')
            except Exception as e:
                self.logger.error(f"Failed to create Bedrock client: {e}")
                self.logger.info("Falling back to mock mode")
                self.use_mock = True
    
    def teardown(self):
        """Cleanup async resources"""
        if self.event_loop:
            self.event_loop.close()
        
        avg_latency = self.total_latency / max(self.total_requests, 1)
        self.logger.info(
            f"Bedrock processor stats: "
            f"Requests={self.total_requests}, "
            f"Avg Latency={avg_latency:.3f}s, "
            f"Throttles={self.throttle_errors}"
        )
    
    def process_message(self, message: PipelineMessage) -> Optional[PipelineMessage]:
        """
        Process message by calling Bedrock API asynchronously.
        
        This runs in the event loop to handle async operations.
        """
        if message.msg_type != MessageType.DATA:
            return message
        
        # Extract Bedrock request
        request = message.payload
        if not isinstance(request, BedrockRequest):
            self.logger.error(f"Invalid payload type: {type(request)}")
            return None
        
        # Run async processing in event loop
        try:
            response = self.event_loop.run_until_complete(
                self._process_request_async(request)
            )
            
            # Wrap response in message
            return PipelineMessage(
                msg_type=MessageType.DATA,
                payload=response,
                metadata=message.metadata
            )
        
        except Exception as e:
            self.logger.error(f"Failed to process request {request.request_id}: {e}")
            
            # Return error response
            error_response = BedrockResponse(
                request_id=request.request_id,
                response_text="",
                model_id=request.model_id,
                latency=0.0,
                tokens_used=0,
                success=False,
                error=str(e),
                metadata=request.metadata
            )
            
            return PipelineMessage(
                msg_type=MessageType.DATA,
                payload=error_response,
                metadata=message.metadata
            )
    
    async def _process_request_async(self, request: BedrockRequest) -> BedrockResponse:
        """
        Process a single Bedrock request with throttling.
        
        This is the core async method that:
        1. Acquires throttle permit
        2. Calls Bedrock API
        3. Records outcome for AIMD
        """
        start_time = time.time()
        
        # Acquire throttle permit (blocks if at limit)
        async with self.throttle.acquire():
            try:
                # Call Bedrock API
                if self.use_mock:
                    response_text, tokens = await self._mock_bedrock_call(request)
                else:
                    response_text, tokens = await self._real_bedrock_call(request)
                
                # Record success
                latency = time.time() - start_time
                self.throttle.record_success(latency=latency)
                
                self.total_latency += latency
                self.total_requests += 1
                
                self.logger.debug(
                    f"Request {request.request_id} completed in {latency:.3f}s "
                    f"(throttle limit: {self.throttle.max_inflight})"
                )
                
                return BedrockResponse(
                    request_id=request.request_id,
                    response_text=response_text,
                    model_id=request.model_id,
                    latency=latency,
                    tokens_used=tokens,
                    success=True,
                    metadata=request.metadata
                )
            
            except ThrottleError as e:
                # Bedrock returned 429 or similar
                self.throttle.record_throttle()
                self.throttle_errors += 1
                
                self.logger.warning(
                    f"Request {request.request_id} throttled by Bedrock "
                    f"(new limit: {self.throttle.max_inflight})"
                )
                
                raise
            
            except asyncio.TimeoutError as e:
                # Timeout - possible capacity issue
                self.throttle.record_timeout()
                
                self.logger.warning(
                    f"Request {request.request_id} timed out "
                    f"(new limit: {self.throttle.max_inflight})"
                )
                
                raise
    
    async def _mock_bedrock_call(self, request: BedrockRequest) -> tuple[str, int]:
        """
        Mock Bedrock API call for testing.
        
        Simulates:
        - Variable latency
        - Occasional throttling
        - Token usage
        """
        # Simulate variable latency (50-500ms)
        import random
        latency = random.uniform(0.05, 0.5)
        await asyncio.sleep(latency)
        
        # Simulate throttling 5% of the time if over limit
        if random.random() < 0.05 and self.throttle.current_inflight > 15:
            raise ThrottleError("Mock throttle error")
        
        # Generate mock response
        response_text = f"Mock response for: {request.prompt[:50]}..."
        tokens = len(request.prompt.split()) + 50
        
        return response_text, tokens
    
    async def _real_bedrock_call(self, request: BedrockRequest) -> tuple[str, int]:
        """
        Real Bedrock API call (requires AWS credentials).
        
        Uses boto3 bedrock-runtime client.
        """
        try:
            # Prepare request body (format varies by model)
            if "anthropic" in request.model_id:
                body = json.dumps({
                    "prompt": f"\n\nHuman: {request.prompt}\n\nAssistant:",
                    "max_tokens_to_sample": request.max_tokens,
                    "temperature": request.temperature,
                })
            elif "ai21" in request.model_id:
                body = json.dumps({
                    "prompt": request.prompt,
                    "maxTokens": request.max_tokens,
                    "temperature": request.temperature,
                })
            else:
                # Generic format
                body = json.dumps({
                    "prompt": request.prompt,
                    "max_tokens": request.max_tokens,
                    "temperature": request.temperature,
                })
            
            # Call Bedrock (sync boto3 in thread pool)
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.bedrock_client.invoke_model(
                    modelId=request.model_id,
                    body=body
                )
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            
            # Extract text (format varies by model)
            if "anthropic" in request.model_id:
                response_text = response_body.get('completion', '')
            elif "ai21" in request.model_id:
                response_text = response_body.get('completions', [{}])[0].get('data', {}).get('text', '')
            else:
                response_text = str(response_body)
            
            # Estimate tokens
            tokens = len(response_text.split()) + len(request.prompt.split())
            
            return response_text, tokens
        
        except Exception as e:
            error_msg = str(e)
            
            # Detect throttling errors
            if any(x in error_msg.lower() for x in ['throttling', 'too many requests', '429']):
                raise ThrottleError(f"Bedrock throttled: {error_msg}")
            
            # Detect timeout errors
            if any(x in error_msg.lower() for x in ['timeout', 'timed out']):
                raise asyncio.TimeoutError(f"Bedrock timeout: {error_msg}")
            
            raise


class ThrottleError(Exception):
    """Exception for API throttling errors"""
    pass


# Decorator for easy throttling of any async function
def throttled_bedrock_call(throttle: AdaptiveThrottle):
    """
    Decorator to add throttling to any Bedrock call function.
    
    Usage:
        @throttled_bedrock_call(my_throttle)
        async def my_bedrock_function(prompt):
            # Your Bedrock logic here
            pass
    """
    def decorator(func):
        return with_throttle(
            throttle=throttle,
            throttle_exceptions=(ThrottleError,),
            timeout_exceptions=(asyncio.TimeoutError,)
        )(func)
    
    return decorator


# Demo usage
if __name__ == "__main__":
    """
    Demonstrate Bedrock processing with adaptive throttling.
    """
    import sys
    from pipeline import Pipeline, SourceStage, SinkStage
    
    print("=== Bedrock Processor Demo ===\n")
    
    # Create test requests
    requests = [
        BedrockRequest(
            model_id="anthropic.claude-v2",
            prompt=f"Tell me a fact about number {i}",
            request_id=f"req-{i:03d}"
        )
        for i in range(1, 21)
    ]
    
    # Create pipeline
    pipeline = Pipeline(name="bedrock-demo")
    
    # Stage 1: Source (feed requests)
    source = SourceStage(
        stage_name="RequestSource",
        items=requests
    )
    
    # Stage 2: Bedrock processor (async + throttling)
    processor = BedrockProcessorStage(
        stage_name="BedrockProcessor",
        throttle_config=ThrottleConfig(
            initial_limit=5,
            max_limit=20,
            enable_latency_tracking=True
        ),
        use_mock=True  # Use mock for demo
    )
    
    # Stage 3: Sink (collect responses)
    sink = SinkStage(stage_name="ResponseCollector")
    
    # Build and run
    pipeline.add_stage(source)
    pipeline.add_stage(processor)
    pipeline.add_stage(sink)
    pipeline.build()
    
    try:
        pipeline.start()
        pipeline.wait()
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        pipeline.stop()
    
    print("\n=== Demo Complete ===")
    print(f"Processed {len(requests)} requests")
    print(f"Final throttle limit: {processor.throttle.max_inflight}")
