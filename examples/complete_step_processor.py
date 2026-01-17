#!/usr/bin/env python3
"""
Complete Step Processor Example

This example demonstrates how to implement a comprehensive step processor
with idempotency, error handling, and integration with external systems.
"""

import asyncio
import json
import hashlib
import aiofiles
import httpx
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from fleet_q.models import StepPayload
from fleet_q.backoff import with_backoff
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class IdempotencyManager:
    """Manages idempotency for step execution"""
    
    def __init__(self, cache_dir: str = "/tmp/fleet_q_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def generate_key(self, payload: StepPayload) -> str:
        """Generate idempotency key from step payload"""
        content = {
            "step_id": payload.step_id,
            "type": payload.type,
            "args": payload.args
        }
        content_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]
    
    def get_cache_file(self, key: str) -> Path:
        """Get cache file path for idempotency key"""
        return self.cache_dir / f"{key}.json"
    
    async def is_cached(self, key: str) -> bool:
        """Check if result is already cached"""
        return self.get_cache_file(key).exists()
    
    async def get_cached_result(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached result"""
        cache_file = self.get_cache_file(key)
        if not cache_file.exists():
            return None
        
        async with aiofiles.open(cache_file, 'r') as f:
            content = await f.read()
            return json.loads(content)
    
    async def cache_result(self, key: str, result: Dict[str, Any]):
        """Cache execution result"""
        cache_file = self.get_cache_file(key)
        cache_data = {
            "result": result,
            "cached_at": datetime.utcnow().isoformat(),
            "idempotency_key": key
        }
        
        async with aiofiles.open(cache_file, 'w') as f:
            await f.write(json.dumps(cache_data, indent=2))


class ExternalAPIClient:
    """HTTP client for external API integration"""
    
    def __init__(self, base_url: str, api_key: str):
        self.client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30.0
        )
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=1000,
        max_delay_ms=10000,
        retry_on=[httpx.RequestError, httpx.HTTPStatusError]
    )
    async def post_data(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make POST request with retry logic"""
        response = await self.client.post(endpoint, json=data)
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()


class DataProcessingStep:
    """
    Complete step processor example with:
    - Idempotency protection
    - External API integration
    - File processing
    - Error handling
    - Comprehensive logging
    """
    
    def __init__(self, work_dir: str = "/tmp/fleet_q_work"):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
        self.idempotency = IdempotencyManager()
        self.api_client = None
    
    async def initialize(self):
        """Initialize external dependencies"""
        # Initialize API client if needed
        api_base_url = "https://api.example.com"
        api_key = "your-api-key"
        self.api_client = ExternalAPIClient(api_base_url, api_key)
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute step with full idempotency and error handling"""
        
        # Generate idempotency key
        idempotency_key = self.idempotency.generate_key(payload)
        
        logger.info(
            "Starting step execution",
            step_id=payload.step_id,
            step_type=payload.type,
            idempotency_key=idempotency_key
        )
        
        # Check if already processed
        if await self.idempotency.is_cached(idempotency_key):
            cached_result = await self.idempotency.get_cached_result(idempotency_key)
            logger.info(
                "Returning cached result",
                step_id=payload.step_id,
                idempotency_key=idempotency_key
            )
            return cached_result["result"]
        
        try:
            # Validate payload
            await self._validate_payload(payload)
            
            # Execute step based on type
            if payload.type == "data_processing":
                result = await self._process_data(payload)
            elif payload.type == "api_integration":
                result = await self._integrate_api(payload)
            elif payload.type == "file_processing":
                result = await self._process_file(payload)
            else:
                raise ValueError(f"Unknown step type: {payload.type}")
            
            # Cache successful result
            await self.idempotency.cache_result(idempotency_key, result)
            
            logger.info(
                "Step execution completed successfully",
                step_id=payload.step_id,
                step_type=payload.type,
                idempotency_key=idempotency_key
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "Step execution failed",
                step_id=payload.step_id,
                step_type=payload.type,
                idempotency_key=idempotency_key,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def _validate_payload(self, payload: StepPayload):
        """Validate step payload"""
        required_fields = {
            "data_processing": ["input_data", "output_format"],
            "api_integration": ["endpoint", "data"],
            "file_processing": ["input_file", "operation"]
        }
        
        if payload.type not in required_fields:
            raise ValueError(f"Unknown step type: {payload.type}")
        
        for field in required_fields[payload.type]:
            if field not in payload.args:
                raise ValueError(f"Missing required field: {field}")
    
    async def _process_data(self, payload: StepPayload) -> Dict[str, Any]:
        """Process data step"""
        input_data = payload.args["input_data"]
        output_format = payload.args.get("output_format", "json")
        
        logger.info(
            "Processing data",
            step_id=payload.step_id,
            input_size=len(str(input_data)),
            output_format=output_format
        )
        
        # Simulate data processing
        await asyncio.sleep(2)
        
        # Transform data based on format
        if output_format == "json":
            processed_data = {"processed": input_data, "format": "json"}
        elif output_format == "csv":
            processed_data = {"processed": input_data, "format": "csv"}
        else:
            processed_data = {"processed": input_data, "format": "raw"}
        
        # Save processed data
        output_file = self.work_dir / f"{payload.step_id}_output.{output_format}"
        async with aiofiles.open(output_file, 'w') as f:
            if output_format == "json":
                await f.write(json.dumps(processed_data, indent=2))
            else:
                await f.write(str(processed_data))
        
        return {
            "status": "completed",
            "operation": "data_processing",
            "input_size": len(str(input_data)),
            "output_file": str(output_file),
            "output_format": output_format,
            "processed_at": datetime.utcnow().isoformat()
        }
    
    async def _integrate_api(self, payload: StepPayload) -> Dict[str, Any]:
        """Integrate with external API"""
        endpoint = payload.args["endpoint"]
        data = payload.args["data"]
        
        logger.info(
            "Integrating with external API",
            step_id=payload.step_id,
            endpoint=endpoint
        )
        
        if not self.api_client:
            await self.initialize()
        
        try:
            # Make API call with retry logic
            api_response = await self.api_client.post_data(endpoint, data)
            
            return {
                "status": "completed",
                "operation": "api_integration",
                "endpoint": endpoint,
                "api_response": api_response,
                "processed_at": datetime.utcnow().isoformat()
            }
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code in [400, 401, 403, 404]:
                # Don't retry client errors
                raise ValueError(f"API client error: {e.response.status_code}")
            else:
                # Retry server errors
                raise
    
    async def _process_file(self, payload: StepPayload) -> Dict[str, Any]:
        """Process file step"""
        input_file = payload.args["input_file"]
        operation = payload.args["operation"]
        
        logger.info(
            "Processing file",
            step_id=payload.step_id,
            input_file=input_file,
            operation=operation
        )
        
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Read input file
        async with aiofiles.open(input_path, 'r') as f:
            content = await f.read()
        
        # Apply operation
        if operation == "uppercase":
            processed_content = content.upper()
        elif operation == "lowercase":
            processed_content = content.lower()
        elif operation == "word_count":
            word_count = len(content.split())
            processed_content = f"Word count: {word_count}"
        else:
            processed_content = content
        
        # Write output file
        output_file = self.work_dir / f"{payload.step_id}_{operation}_output.txt"
        async with aiofiles.open(output_file, 'w') as f:
            await f.write(processed_content)
        
        return {
            "status": "completed",
            "operation": "file_processing",
            "input_file": input_file,
            "output_file": str(output_file),
            "file_operation": operation,
            "input_size": len(content),
            "output_size": len(processed_content),
            "processed_at": datetime.utcnow().isoformat()
        }
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.api_client:
            await self.api_client.close()


# Example usage and testing
async def main():
    """Example usage of the complete step processor"""
    
    # Create step processor
    processor = DataProcessingStep()
    
    try:
        # Example 1: Data processing step
        data_payload = StepPayload(
            step_id="data_step_001",
            type="data_processing",
            args={
                "input_data": {"numbers": [1, 2, 3, 4, 5], "operation": "sum"},
                "output_format": "json"
            },
            metadata={"user": "example", "priority": 5}
        )
        
        print("🔄 Processing data step...")
        result1 = await processor.execute(data_payload)
        print(f"✅ Data step result: {result1}")
        
        # Example 2: File processing step (create test file first)
        test_file = Path("/tmp/test_input.txt")
        async with aiofiles.open(test_file, 'w') as f:
            await f.write("Hello World! This is a test file for processing.")
        
        file_payload = StepPayload(
            step_id="file_step_001",
            type="file_processing",
            args={
                "input_file": str(test_file),
                "operation": "word_count"
            },
            metadata={"user": "example", "priority": 3}
        )
        
        print("\n🔄 Processing file step...")
        result2 = await processor.execute(file_payload)
        print(f"✅ File step result: {result2}")
        
        # Example 3: Test idempotency by running the same step again
        print("\n🔄 Testing idempotency (running same step again)...")
        result3 = await processor.execute(data_payload)
        print(f"✅ Idempotent result: {result3}")
        
        # Verify results are identical
        assert result1 == result3, "Idempotency failed - results differ"
        print("✅ Idempotency test passed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    print("🚀 FLEET-Q Complete Step Processor Example")
    print("=" * 50)
    asyncio.run(main())