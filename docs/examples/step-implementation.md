# Step Implementation Examples

This guide shows how to implement custom step processors that integrate with FLEET-Q's idempotency and retry mechanisms.

## Step Implementation Patterns

### Basic Step Processor

```python
from fleet_q.models import StepPayload
from typing import Dict, Any
import asyncio
import structlog

logger = structlog.get_logger()

class DataProcessingStep:
    """Example step processor for data processing tasks"""
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute the data processing step"""
        
        # Extract arguments
        input_file = payload.args["input_file"]
        output_format = payload.args.get("output_format", "json")
        
        logger.info(
            "Starting data processing",
            step_id=payload.step_id,
            input_file=input_file,
            output_format=output_format
        )
        
        try:
            # Simulate data processing
            await self._process_file(input_file, output_format)
            
            result = {
                "status": "completed",
                "output_file": f"{input_file}.{output_format}",
                "records_processed": 1000,
                "processing_time_seconds": 5.2
            }
            
            logger.info(
                "Data processing completed",
                step_id=payload.step_id,
                **result
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "Data processing failed",
                step_id=payload.step_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def _process_file(self, input_file: str, output_format: str):
        """Simulate file processing"""
        # Simulate processing time
        await asyncio.sleep(2)
        
        # Simulate potential failure
        if "error" in input_file:
            raise ValueError(f"Invalid file: {input_file}")
```

### Idempotent Step Implementation

```python
import hashlib
import json
from pathlib import Path
from fleet_q.models import StepPayload

class IdempotentFileProcessor:
    """File processor with built-in idempotency"""
    
    def __init__(self, work_dir: str = "/tmp/fleet_q_work"):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
    
    def _get_idempotency_key(self, payload: StepPayload) -> str:
        """Generate idempotency key from step payload"""
        # Create deterministic hash from step content
        content = {
            "type": payload.type,
            "args": payload.args,
            "step_id": payload.step_id
        }
        content_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]
    
    def _get_result_file(self, idempotency_key: str) -> Path:
        """Get path to result file for idempotency check"""
        return self.work_dir / f"result_{idempotency_key}.json"
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute with idempotency protection"""
        
        idempotency_key = self._get_idempotency_key(payload)
        result_file = self._get_result_file(idempotency_key)
        
        # Check if already processed
        if result_file.exists():
            logger.info(
                "Step already processed, returning cached result",
                step_id=payload.step_id,
                idempotency_key=idempotency_key
            )
            
            with open(result_file) as f:
                return json.load(f)
        
        logger.info(
            "Processing step with idempotency protection",
            step_id=payload.step_id,
            idempotency_key=idempotency_key
        )
        
        try:
            # Perform actual work
            result = await self._do_work(payload)
            
            # Cache result for idempotency
            with open(result_file, 'w') as f:
                json.dump(result, f)
            
            logger.info(
                "Step completed and result cached",
                step_id=payload.step_id,
                idempotency_key=idempotency_key
            )
            
            return result
            
        except Exception as e:
            # Don't cache failures - allow retries
            logger.error(
                "Step failed, not caching result",
                step_id=payload.step_id,
                idempotency_key=idempotency_key,
                error=str(e)
            )
            raise
    
    async def _do_work(self, payload: StepPayload) -> Dict[str, Any]:
        """Actual work implementation"""
        input_file = payload.args["input_file"]
        
        # Simulate work that should only happen once
        await asyncio.sleep(3)
        
        return {
            "status": "completed",
            "input_file": input_file,
            "processed_at": "2024-01-01T12:00:00Z",
            "checksum": "abc123def456"
        }
```

### External API Integration

```python
import httpx
from fleet_q.models import StepPayload
from fleet_q.backoff import with_backoff

class ExternalAPIStep:
    """Step that calls external APIs with retry logic"""
    
    def __init__(self, api_base_url: str, api_key: str):
        self.api_base_url = api_base_url
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            base_url=api_base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30.0
        )
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=1000,
        max_delay_ms=10000,
        retry_on=[httpx.RequestError, httpx.HTTPStatusError]
    )
    async def _call_api(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make API call with retry logic"""
        response = await self.client.post(endpoint, json=data)
        response.raise_for_status()
        return response.json()
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute API integration step"""
        
        endpoint = payload.args["endpoint"]
        api_data = payload.args["data"]
        
        logger.info(
            "Calling external API",
            step_id=payload.step_id,
            endpoint=endpoint
        )
        
        try:
            # Call external API with retries
            api_response = await self._call_api(endpoint, api_data)
            
            result = {
                "status": "completed",
                "api_response": api_response,
                "endpoint": endpoint,
                "called_at": "2024-01-01T12:00:00Z"
            }
            
            logger.info(
                "API call successful",
                step_id=payload.step_id,
                endpoint=endpoint,
                response_status=api_response.get("status")
            )
            
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code in [400, 401, 403, 404]:
                # Don't retry client errors
                logger.error(
                    "API call failed with client error",
                    step_id=payload.step_id,
                    status_code=e.response.status_code,
                    response=e.response.text
                )
                raise ValueError(f"API client error: {e.response.status_code}")
            else:
                # Retry server errors
                logger.warning(
                    "API call failed with server error, will retry",
                    step_id=payload.step_id,
                    status_code=e.response.status_code
                )
                raise
        
        except Exception as e:
            logger.error(
                "API call failed with unexpected error",
                step_id=payload.step_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.client.aclose()
```

### Database Operations Step

```python
import asyncpg
from fleet_q.models import StepPayload
from fleet_q.backoff import with_backoff

class DatabaseOperationStep:
    """Step for database operations with transaction safety"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None
    
    async def initialize(self):
        """Initialize database connection pool"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=1,
            max_size=5,
            command_timeout=60
        )
    
    @with_backoff(
        max_attempts=3,
        base_delay_ms=500,
        retry_on=[asyncpg.PostgresConnectionError, asyncpg.PostgresError]
    )
    async def _execute_query(self, query: str, params: list) -> list:
        """Execute database query with retry logic"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                return await conn.fetch(query, *params)
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute database operation step"""
        
        operation = payload.args["operation"]
        table = payload.args["table"]
        data = payload.args.get("data", {})
        
        logger.info(
            "Executing database operation",
            step_id=payload.step_id,
            operation=operation,
            table=table
        )
        
        try:
            if operation == "insert":
                result = await self._insert_record(table, data)
            elif operation == "update":
                result = await self._update_record(table, data)
            elif operation == "delete":
                result = await self._delete_record(table, data)
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            logger.info(
                "Database operation completed",
                step_id=payload.step_id,
                operation=operation,
                affected_rows=result.get("affected_rows", 0)
            )
            
            return {
                "status": "completed",
                "operation": operation,
                "table": table,
                **result
            }
            
        except Exception as e:
            logger.error(
                "Database operation failed",
                step_id=payload.step_id,
                operation=operation,
                table=table,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def _insert_record(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert record with idempotency check"""
        
        # Check if record already exists (idempotency)
        if "id" in data:
            existing = await self._execute_query(
                f"SELECT id FROM {table} WHERE id = $1",
                [data["id"]]
            )
            if existing:
                return {"affected_rows": 0, "message": "Record already exists"}
        
        # Insert new record
        columns = list(data.keys())
        values = list(data.values())
        placeholders = [f"${i+1}" for i in range(len(values))]
        
        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING id
        """
        
        result = await self._execute_query(query, values)
        return {
            "affected_rows": len(result),
            "inserted_id": result[0]["id"] if result else None
        }
    
    async def _update_record(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update record"""
        record_id = data.pop("id")
        
        set_clauses = [f"{col} = ${i+2}" for i, col in enumerate(data.keys())]
        values = [record_id] + list(data.values())
        
        query = f"""
            UPDATE {table}
            SET {', '.join(set_clauses)}
            WHERE id = $1
        """
        
        await self._execute_query(query, values)
        return {"affected_rows": 1}
    
    async def _delete_record(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Delete record"""
        record_id = data["id"]
        
        query = f"DELETE FROM {table} WHERE id = $1"
        await self._execute_query(query, [record_id])
        
        return {"affected_rows": 1}
    
    async def cleanup(self):
        """Cleanup database connections"""
        if self.pool:
            await self.pool.close()
```

### File Processing Pipeline

```python
import aiofiles
import aiofiles.os
from pathlib import Path
from fleet_q.models import StepPayload

class FileProcessingPipeline:
    """Multi-stage file processing with checkpoints"""
    
    def __init__(self, work_dir: str = "/tmp/fleet_q_files"):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(exist_ok=True)
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute file processing pipeline"""
        
        input_file = payload.args["input_file"]
        pipeline_stages = payload.args.get("stages", ["validate", "transform", "save"])
        
        logger.info(
            "Starting file processing pipeline",
            step_id=payload.step_id,
            input_file=input_file,
            stages=pipeline_stages
        )
        
        # Create step-specific work directory
        step_work_dir = self.work_dir / payload.step_id
        step_work_dir.mkdir(exist_ok=True)
        
        try:
            current_file = input_file
            results = {}
            
            for stage in pipeline_stages:
                logger.info(
                    "Processing pipeline stage",
                    step_id=payload.step_id,
                    stage=stage,
                    current_file=current_file
                )
                
                # Check if stage already completed (for retry scenarios)
                checkpoint_file = step_work_dir / f"{stage}_checkpoint.json"
                if checkpoint_file.exists():
                    async with aiofiles.open(checkpoint_file) as f:
                        stage_result = json.loads(await f.read())
                    logger.info(
                        "Stage already completed, using checkpoint",
                        step_id=payload.step_id,
                        stage=stage
                    )
                else:
                    # Execute stage
                    stage_result = await self._execute_stage(
                        stage, current_file, step_work_dir, payload
                    )
                    
                    # Save checkpoint
                    async with aiofiles.open(checkpoint_file, 'w') as f:
                        await f.write(json.dumps(stage_result))
                
                results[stage] = stage_result
                current_file = stage_result.get("output_file", current_file)
            
            final_result = {
                "status": "completed",
                "input_file": input_file,
                "output_file": current_file,
                "stages_completed": pipeline_stages,
                "stage_results": results
            }
            
            logger.info(
                "File processing pipeline completed",
                step_id=payload.step_id,
                output_file=current_file
            )
            
            return final_result
            
        except Exception as e:
            logger.error(
                "File processing pipeline failed",
                step_id=payload.step_id,
                error=str(e),
                exc_info=True
            )
            raise
        
        finally:
            # Cleanup work directory (optional)
            # await self._cleanup_work_dir(step_work_dir)
            pass
    
    async def _execute_stage(
        self, 
        stage: str, 
        input_file: str, 
        work_dir: Path, 
        payload: StepPayload
    ) -> Dict[str, Any]:
        """Execute a single pipeline stage"""
        
        if stage == "validate":
            return await self._validate_file(input_file, work_dir)
        elif stage == "transform":
            return await self._transform_file(input_file, work_dir, payload.args)
        elif stage == "save":
            return await self._save_file(input_file, work_dir, payload.args)
        else:
            raise ValueError(f"Unknown pipeline stage: {stage}")
    
    async def _validate_file(self, input_file: str, work_dir: Path) -> Dict[str, Any]:
        """Validate input file"""
        file_path = Path(input_file)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        file_size = await aiofiles.os.path.getsize(input_file)
        
        # Simulate validation
        await asyncio.sleep(0.5)
        
        return {
            "stage": "validate",
            "file_size": file_size,
            "is_valid": True,
            "output_file": input_file
        }
    
    async def _transform_file(
        self, 
        input_file: str, 
        work_dir: Path, 
        args: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform file content"""
        
        output_file = work_dir / f"transformed_{Path(input_file).name}"
        transformation = args.get("transformation", "uppercase")
        
        # Read input file
        async with aiofiles.open(input_file, 'r') as f:
            content = await f.read()
        
        # Apply transformation
        if transformation == "uppercase":
            transformed_content = content.upper()
        elif transformation == "lowercase":
            transformed_content = content.lower()
        else:
            transformed_content = content
        
        # Write output file
        async with aiofiles.open(output_file, 'w') as f:
            await f.write(transformed_content)
        
        # Simulate processing time
        await asyncio.sleep(1)
        
        return {
            "stage": "transform",
            "transformation": transformation,
            "input_size": len(content),
            "output_size": len(transformed_content),
            "output_file": str(output_file)
        }
    
    async def _save_file(
        self, 
        input_file: str, 
        work_dir: Path, 
        args: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Save file to final destination"""
        
        destination = args.get("destination", "/tmp/final_output")
        final_file = Path(destination) / Path(input_file).name
        
        # Ensure destination directory exists
        final_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file to destination
        async with aiofiles.open(input_file, 'rb') as src:
            async with aiofiles.open(final_file, 'wb') as dst:
                content = await src.read()
                await dst.write(content)
        
        # Simulate save time
        await asyncio.sleep(0.5)
        
        return {
            "stage": "save",
            "destination": str(final_file),
            "file_size": len(content),
            "output_file": str(final_file)
        }
```

## Step Registry Pattern

### Centralized Step Registry

```python
from typing import Dict, Type, Callable
from fleet_q.models import StepPayload

class StepRegistry:
    """Registry for step processors"""
    
    def __init__(self):
        self._processors: Dict[str, Callable] = {}
    
    def register(self, step_type: str):
        """Decorator to register step processors"""
        def decorator(processor_class):
            self._processors[step_type] = processor_class
            return processor_class
        return decorator
    
    def get_processor(self, step_type: str):
        """Get processor for step type"""
        if step_type not in self._processors:
            raise ValueError(f"No processor registered for step type: {step_type}")
        return self._processors[step_type]
    
    def list_types(self) -> list:
        """List all registered step types"""
        return list(self._processors.keys())

# Global registry instance
step_registry = StepRegistry()

# Register step processors
@step_registry.register("data_processing")
class DataProcessingStep:
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        # Implementation here
        pass

@step_registry.register("email_notification")
class EmailNotificationStep:
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        # Implementation here
        pass

@step_registry.register("file_upload")
class FileUploadStep:
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        # Implementation here
        pass

# Usage in FLEET-Q worker
async def process_step(payload: StepPayload) -> Dict[str, Any]:
    """Process a step using registered processor"""
    
    processor_class = step_registry.get_processor(payload.type)
    processor = processor_class()
    
    return await processor.execute(payload)
```

### Step Execution Framework

```python
import time
from contextlib import asynccontextmanager
from fleet_q.models import StepPayload

class StepExecutionContext:
    """Context for step execution with metrics and logging"""
    
    def __init__(self, payload: StepPayload):
        self.payload = payload
        self.start_time = None
        self.end_time = None
        self.metrics = {}
    
    @asynccontextmanager
    async def execution_context(self):
        """Context manager for step execution"""
        self.start_time = time.time()
        
        logger.info(
            "Step execution started",
            step_id=self.payload.step_id,
            step_type=self.payload.type
        )
        
        try:
            yield self
        except Exception as e:
            logger.error(
                "Step execution failed",
                step_id=self.payload.step_id,
                step_type=self.payload.type,
                error=str(e),
                execution_time=time.time() - self.start_time,
                exc_info=True
            )
            raise
        finally:
            self.end_time = time.time()
            execution_time = self.end_time - self.start_time
            
            logger.info(
                "Step execution completed",
                step_id=self.payload.step_id,
                step_type=self.payload.type,
                execution_time=execution_time,
                **self.metrics
            )
    
    def add_metric(self, key: str, value: Any):
        """Add execution metric"""
        self.metrics[key] = value

# Enhanced step processor base class
class BaseStepProcessor:
    """Base class for step processors with common functionality"""
    
    async def execute(self, payload: StepPayload) -> Dict[str, Any]:
        """Execute step with context and error handling"""
        
        context = StepExecutionContext(payload)
        
        async with context.execution_context():
            # Validate payload
            await self.validate_payload(payload)
            
            # Execute step logic
            result = await self.process(payload, context)
            
            # Validate result
            await self.validate_result(result)
            
            return result
    
    async def validate_payload(self, payload: StepPayload):
        """Validate step payload - override in subclasses"""
        required_args = getattr(self, 'REQUIRED_ARGS', [])
        
        for arg in required_args:
            if arg not in payload.args:
                raise ValueError(f"Missing required argument: {arg}")
    
    async def process(self, payload: StepPayload, context: StepExecutionContext) -> Dict[str, Any]:
        """Process step - implement in subclasses"""
        raise NotImplementedError("Subclasses must implement process method")
    
    async def validate_result(self, result: Dict[str, Any]):
        """Validate step result - override in subclasses"""
        if "status" not in result:
            raise ValueError("Step result must include 'status' field")

# Example usage
@step_registry.register("enhanced_data_processing")
class EnhancedDataProcessingStep(BaseStepProcessor):
    REQUIRED_ARGS = ["input_file", "output_format"]
    
    async def process(self, payload: StepPayload, context: StepExecutionContext) -> Dict[str, Any]:
        input_file = payload.args["input_file"]
        output_format = payload.args["output_format"]
        
        # Add metrics
        context.add_metric("input_file", input_file)
        context.add_metric("output_format", output_format)
        
        # Simulate processing
        await asyncio.sleep(2)
        
        # Add more metrics
        context.add_metric("records_processed", 1000)
        context.add_metric("processing_rate", 500)  # records per second
        
        return {
            "status": "completed",
            "output_file": f"{input_file}.{output_format}",
            "records_processed": 1000
        }
```

## Testing Step Implementations

### Unit Testing

```python
import pytest
from unittest.mock import AsyncMock, patch
from fleet_q.models import StepPayload

@pytest.mark.asyncio
async def test_data_processing_step():
    """Test data processing step"""
    
    # Create test payload
    payload = StepPayload(
        step_id="test_step_123",
        type="data_processing",
        args={
            "input_file": "test_data.csv",
            "output_format": "json"
        },
        metadata={"test": True}
    )
    
    # Create step processor
    processor = DataProcessingStep()
    
    # Mock file processing
    with patch.object(processor, '_process_file', new_callable=AsyncMock) as mock_process:
        mock_process.return_value = None
        
        # Execute step
        result = await processor.execute(payload)
        
        # Verify result
        assert result["status"] == "completed"
        assert result["output_file"] == "test_data.csv.json"
        assert result["records_processed"] == 1000
        
        # Verify mock was called
        mock_process.assert_called_once_with("test_data.csv", "json")

@pytest.mark.asyncio
async def test_idempotent_step():
    """Test idempotent step behavior"""
    
    payload = StepPayload(
        step_id="idempotent_test_123",
        type="file_processing",
        args={"input_file": "test.txt"},
        metadata={}
    )
    
    processor = IdempotentFileProcessor()
    
    # First execution
    result1 = await processor.execute(payload)
    
    # Second execution should return cached result
    result2 = await processor.execute(payload)
    
    # Results should be identical
    assert result1 == result2
    
    # Verify idempotency key generation
    key1 = processor._get_idempotency_key(payload)
    key2 = processor._get_idempotency_key(payload)
    assert key1 == key2

@pytest.mark.asyncio
async def test_step_failure_handling():
    """Test step failure scenarios"""
    
    payload = StepPayload(
        step_id="failure_test_123",
        type="data_processing",
        args={"input_file": "error_file.csv"},
        metadata={}
    )
    
    processor = DataProcessingStep()
    
    # Should raise exception for error file
    with pytest.raises(ValueError, match="Invalid file"):
        await processor.execute(payload)
```

### Integration Testing

```python
@pytest.mark.asyncio
async def test_step_with_fleet_q():
    """Test step integration with FLEET-Q"""
    
    import httpx
    
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    # Submit step
    response = await client.post("/submit", json={
        "type": "data_processing",
        "args": {
            "input_file": "integration_test.csv",
            "output_format": "json"
        },
        "metadata": {"test": "integration"}
    })
    
    step_id = response.json()["step_id"]
    
    # Wait for completion
    max_wait = 30  # seconds
    wait_time = 0
    
    while wait_time < max_wait:
        status_response = await client.get(f"/status/{step_id}")
        status = status_response.json()
        
        if status["status"] == "completed":
            # Verify step completed successfully
            assert status["retry_count"] == 0
            break
        elif status["status"] == "failed":
            pytest.fail(f"Step failed: {status}")
        
        await asyncio.sleep(1)
        wait_time += 1
    else:
        pytest.fail(f"Step did not complete within {max_wait} seconds")
    
    await client.aclose()
```

## Best Practices

### Error Handling

1. **Distinguish Transient vs Permanent Errors**
   - Raise retriable exceptions for transient failures
   - Raise non-retriable exceptions for permanent failures

2. **Provide Detailed Error Context**
   - Include step_id in all log messages
   - Add relevant payload information to error logs
   - Use structured logging for better observability

3. **Implement Graceful Degradation**
   - Handle partial failures gracefully
   - Provide meaningful error messages
   - Clean up resources on failure

### Performance Optimization

1. **Use Async/Await Properly**
   - Don't block the event loop with synchronous operations
   - Use async libraries for I/O operations
   - Implement proper connection pooling

2. **Implement Checkpointing**
   - Save intermediate results for long-running steps
   - Enable resumption after failures
   - Use idempotency keys for safe retries

3. **Resource Management**
   - Clean up resources in finally blocks
   - Use context managers for resource handling
   - Implement proper connection lifecycle management

### Monitoring and Observability

1. **Structured Logging**
   - Use consistent log formats
   - Include relevant context in logs
   - Log at appropriate levels

2. **Metrics Collection**
   - Track execution times
   - Monitor success/failure rates
   - Collect business metrics

3. **Health Checks**
   - Implement step-level health checks
   - Monitor external dependencies
   - Provide diagnostic information

These examples demonstrate how to build robust, idempotent, and observable step processors that integrate seamlessly with FLEET-Q's retry and recovery mechanisms.

## Next Steps

- **[Idempotency Patterns](idempotency.md)** - Deep dive into idempotency implementation
- **[Environment Configurations](environments.md)** - Configure steps for different environments
- **[Basic Usage](basic-usage.md)** - See these steps in action