#!/usr/bin/env python3
"""
Step Submission Demo for FLEET-Q

Demonstrates the step submission functionality including:
- Unique ID generation
- Payload validation
- Storage operations
- Idempotency key generation

This script shows how to use the step submission service programmatically.
"""

import asyncio
import logging
from datetime import datetime
from unittest.mock import AsyncMock

from fleet_q.config import FleetQConfig
from fleet_q.step_service import StepService, StepSubmissionError, StepValidationError
from fleet_q.models import StepSubmissionRequest, Step, StepStatus, StepPayload
from fleet_q.storage import generate_idempotency_key


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockStorage:
    """Mock storage for demonstration purposes."""
    
    def __init__(self):
        self.steps = {}
    
    async def create_step(self, step: Step) -> None:
        """Store a step in memory."""
        self.steps[step.step_id] = step
        logger.info(f"Stored step {step.step_id} in mock storage")
    
    async def get_step(self, step_id: str) -> Step:
        """Retrieve a step from memory."""
        return self.steps.get(step_id)
    
    async def get_queue_stats(self) -> dict:
        """Get queue statistics."""
        stats = {'pending': 0, 'claimed': 0, 'completed': 0, 'failed': 0}
        for step in self.steps.values():
            stats[step.status.value] += 1
        return stats


async def demonstrate_step_submission():
    """Demonstrate step submission functionality."""
    
    print("=" * 60)
    print("FLEET-Q Step Submission Demo")
    print("=" * 60)
    
    # Create configuration
    config = FleetQConfig(
        snowflake_account="demo",
        snowflake_user="demo",
        snowflake_password="demo",
        snowflake_database="demo"
    )
    
    # Create mock storage
    storage = MockStorage()
    
    # Create step service
    step_service = StepService(storage, config)
    
    print(f"\n1. Configuration loaded:")
    print(f"   Pod ID: {config.pod_id}")
    print(f"   Max Parallelism: {config.max_parallelism}")
    print(f"   Capacity Threshold: {config.capacity_threshold}")
    print(f"   Max Concurrent Steps: {config.max_concurrent_steps}")
    
    # Demonstrate step ID generation
    print(f"\n2. Step ID Generation:")
    for i in range(3):
        step_id = step_service.generate_step_id()
        print(f"   Generated ID {i+1}: {step_id}")
    
    # Demonstrate step submission
    print(f"\n3. Step Submission:")
    
    # Example 1: Simple task
    request1 = StepSubmissionRequest(
        type="data_processing",
        args={"input_file": "data.csv", "output_format": "json"},
        metadata={"source": "demo", "priority": "high"},
        priority=10,
        max_retries=3
    )
    
    try:
        step_id1 = await step_service.submit_step(request1)
        print(f"   ✓ Submitted step: {step_id1}")
        
        # Get step status
        step = await step_service.get_step_status(step_id1)
        print(f"     Status: {step.status}")
        print(f"     Type: {step.payload.type}")
        print(f"     Priority: {step.priority}")
        print(f"     Created: {step.created_ts}")
        
    except (StepSubmissionError, StepValidationError) as e:
        print(f"   ✗ Failed to submit step: {e}")
    
    # Example 2: Batch processing task
    request2 = StepSubmissionRequest(
        type="batch_processing",
        args={
            "batch_size": 1000,
            "input_table": "raw_data",
            "output_table": "processed_data",
            "filters": {"status": "active", "date_range": "2024-01-01:2024-01-31"}
        },
        metadata={
            "department": "analytics",
            "cost_center": "CC-1001",
            "estimated_duration": "30m"
        },
        priority=5,
        max_retries=5
    )
    
    try:
        step_id2 = await step_service.submit_step(request2)
        print(f"   ✓ Submitted step: {step_id2}")
        
    except (StepSubmissionError, StepValidationError) as e:
        print(f"   ✗ Failed to submit step: {e}")
    
    # Example 3: Machine learning task
    request3 = StepSubmissionRequest(
        type="ml_training",
        args={
            "model_type": "random_forest",
            "dataset": "customer_features.parquet",
            "hyperparameters": {
                "n_estimators": 100,
                "max_depth": 10,
                "random_state": 42
            },
            "validation_split": 0.2
        },
        metadata={
            "experiment_id": "exp_20240101_001",
            "researcher": "data_scientist_1",
            "gpu_required": False
        },
        priority=8,
        max_retries=2
    )
    
    try:
        step_id3 = await step_service.submit_step(request3)
        print(f"   ✓ Submitted step: {step_id3}")
        
    except (StepSubmissionError, StepValidationError) as e:
        print(f"   ✗ Failed to submit step: {e}")
    
    # Demonstrate idempotency keys
    print(f"\n4. Idempotency Keys:")
    for step_id in [step_id1, step_id2, step_id3]:
        idempotency_key = step_service.get_idempotency_key(step_id)
        print(f"   Step {step_id[:20]}... → {idempotency_key}")
    
    # Demonstrate validation errors
    print(f"\n5. Validation Examples:")
    
    # Valid step type formats
    valid_types = ["simple_task", "complex-task", "task123", "UPPERCASE_TASK"]
    print(f"   Valid step types:")
    for step_type in valid_types:
        try:
            payload = StepPayload(type=step_type, args={}, metadata={})
            step_service.validate_step_payload(payload)
            print(f"     ✓ {step_type}")
        except StepValidationError as e:
            print(f"     ✗ {step_type}: {e}")
    
    # Invalid step type formats
    invalid_types = ["task with spaces", "task@symbol", "task.dot", ""]
    print(f"   Invalid step types:")
    for step_type in invalid_types:
        try:
            payload = StepPayload(type=step_type, args={}, metadata={})
            step_service.validate_step_payload(payload)
            print(f"     ✓ {step_type}")
        except StepValidationError as e:
            print(f"     ✗ {step_type}: {str(e)[:50]}...")
    
    # Get queue statistics
    print(f"\n6. Queue Statistics:")
    try:
        stats = await step_service.get_queue_statistics()
        for status, count in stats.items():
            print(f"   {status.capitalize()}: {count}")
    except StepSubmissionError as e:
        print(f"   ✗ Failed to get statistics: {e}")
    
    print(f"\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)


async def demonstrate_error_handling():
    """Demonstrate error handling scenarios."""
    
    print(f"\n" + "=" * 60)
    print("Error Handling Demonstration")
    print("=" * 60)
    
    # Create configuration
    config = FleetQConfig(
        snowflake_account="demo",
        snowflake_user="demo",
        snowflake_password="demo",
        snowflake_database="demo"
    )
    
    # Create mock storage that fails
    failing_storage = AsyncMock()
    failing_storage.create_step.side_effect = Exception("Database connection failed")
    
    step_service = StepService(failing_storage, config)
    
    # Try to submit a step with storage failure
    request = StepSubmissionRequest(
        type="test_task",
        args={},
        metadata={}
    )
    
    print(f"\n1. Storage Failure Handling:")
    try:
        step_id = await step_service.submit_step(request)
        print(f"   ✗ Unexpected success: {step_id}")
    except StepSubmissionError as e:
        print(f"   ✓ Caught expected error: {e}")
    
    # Try validation errors
    print(f"\n2. Validation Error Handling:")
    
    invalid_requests = [
        StepSubmissionRequest(type="", args={}, metadata={}),  # Empty type
        StepSubmissionRequest(type="invalid@type", args={}, metadata={}),  # Invalid chars
    ]
    
    for i, invalid_request in enumerate(invalid_requests, 1):
        try:
            step_id = await step_service.submit_step(invalid_request)
            print(f"   ✗ Request {i}: Unexpected success: {step_id}")
        except StepValidationError as e:
            print(f"   ✓ Request {i}: Caught validation error: {str(e)[:50]}...")
        except StepSubmissionError as e:
            print(f"   ✓ Request {i}: Caught submission error: {str(e)[:50]}...")


if __name__ == "__main__":
    # Run the demonstrations
    asyncio.run(demonstrate_step_submission())
    asyncio.run(demonstrate_error_handling())