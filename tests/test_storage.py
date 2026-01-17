"""
Unit tests for storage interface abstractions.

Tests the VariantSerializer, utility functions, and data model creation
functions defined in the storage module.
"""

import json
import pytest
from datetime import datetime
from unittest.mock import Mock

from fleet_q.storage import (
    VariantSerializer,
    StorageError,
    TransactionConflictError,
    ConnectionError,
    generate_idempotency_key,
    create_step_from_submission,
    create_pod_health_record,
    create_dlq_record,
)
from fleet_q.models import (
    Step,
    StepPayload,
    StepStatus,
    PodHealth,
    PodStatus,
    DLQRecord,
)


class TestVariantSerializer:
    """Test cases for VariantSerializer utility class."""
    
    def test_serialize_step_payload(self):
        """Test serialization of StepPayload to JSON string."""
        payload = StepPayload(
            type="test_task",
            args={"param1": "value1", "param2": 42},
            metadata={"priority": "high", "timeout": 300}
        )
        
        result = VariantSerializer.serialize(payload)
        
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed["type"] == "test_task"
        assert parsed["args"] == {"param1": "value1", "param2": 42}
        assert parsed["metadata"] == {"priority": "high", "timeout": 300}
    
    def test_serialize_dict(self):
        """Test serialization of dictionary to JSON string."""
        data = {"key1": "value1", "key2": [1, 2, 3], "key3": {"nested": True}}
        
        result = VariantSerializer.serialize(data)
        
        # Should be valid JSON that deserializes to original dict
        parsed = json.loads(result)
        assert parsed == data
    
    def test_serialize_other_types(self):
        """Test serialization of other JSON-serializable types."""
        # Test list
        data = [1, 2, 3, "test"]
        result = VariantSerializer.serialize(data)
        assert json.loads(result) == data
        
        # Test string
        data = "simple string"
        result = VariantSerializer.serialize(data)
        assert json.loads(result) == data
        
        # Test number
        data = 42
        result = VariantSerializer.serialize(data)
        assert json.loads(result) == data
    
    def test_serialize_non_serializable_raises_error(self):
        """Test that non-JSON-serializable objects raise TypeError."""
        # Mock object that can't be serialized
        non_serializable = Mock()
        
        with pytest.raises(TypeError):
            VariantSerializer.serialize(non_serializable)
    
    def test_deserialize_step_payload_valid(self):
        """Test deserialization of valid JSON to StepPayload."""
        json_str = json.dumps({
            "type": "test_task",
            "args": {"param1": "value1"},
            "metadata": {"priority": "high"}
        })
        
        result = VariantSerializer.deserialize_step_payload(json_str)
        
        assert isinstance(result, StepPayload)
        assert result.type == "test_task"
        assert result.args == {"param1": "value1"}
        assert result.metadata == {"priority": "high"}
    
    def test_deserialize_step_payload_minimal(self):
        """Test deserialization with minimal required fields."""
        json_str = json.dumps({"type": "minimal_task"})
        
        result = VariantSerializer.deserialize_step_payload(json_str)
        
        assert isinstance(result, StepPayload)
        assert result.type == "minimal_task"
        assert result.args == {}  # Should default to empty dict
        assert result.metadata == {}  # Should default to empty dict
    
    def test_deserialize_step_payload_invalid_json(self):
        """Test that invalid JSON raises ValueError."""
        invalid_json = "{ invalid json }"
        
        with pytest.raises(ValueError, match="Invalid step payload JSON"):
            VariantSerializer.deserialize_step_payload(invalid_json)
    
    def test_deserialize_step_payload_missing_type(self):
        """Test that missing 'type' field raises ValueError."""
        json_str = json.dumps({"args": {"param1": "value1"}})
        
        with pytest.raises(ValueError, match="Invalid step payload JSON"):
            VariantSerializer.deserialize_step_payload(json_str)
    
    def test_deserialize_dict_valid(self):
        """Test deserialization of valid JSON to dictionary."""
        data = {"key1": "value1", "key2": [1, 2, 3], "nested": {"key": "value"}}
        json_str = json.dumps(data)
        
        result = VariantSerializer.deserialize_dict(json_str)
        
        assert result == data
    
    def test_deserialize_dict_invalid_json(self):
        """Test that invalid JSON raises ValueError."""
        invalid_json = "{ invalid json }"
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            VariantSerializer.deserialize_dict(invalid_json)


class TestStorageExceptions:
    """Test cases for storage exception classes."""
    
    def test_storage_error_basic(self):
        """Test basic StorageError creation."""
        error = StorageError("Test error message")
        
        assert str(error) == "Test error message"
        assert error.original_error is None
    
    def test_storage_error_with_original(self):
        """Test StorageError with original exception."""
        original = ValueError("Original error")
        error = StorageError("Wrapped error", original)
        
        assert str(error) == "Wrapped error"
        assert error.original_error is original
    
    def test_transaction_conflict_error(self):
        """Test TransactionConflictError inheritance."""
        error = TransactionConflictError("Conflict occurred")
        
        assert isinstance(error, StorageError)
        assert str(error) == "Conflict occurred"
    
    def test_connection_error(self):
        """Test ConnectionError inheritance."""
        error = ConnectionError("Connection failed")
        
        assert isinstance(error, StorageError)
        assert str(error) == "Connection failed"


class TestUtilityFunctions:
    """Test cases for utility functions."""
    
    def test_generate_idempotency_key(self):
        """Test idempotency key generation."""
        step_id = "test-step-123"
        
        result = generate_idempotency_key(step_id)
        
        assert result == "idempotent_test-step-123"
        
        # Should be consistent
        result2 = generate_idempotency_key(step_id)
        assert result == result2
        
        # Different step IDs should produce different keys
        different_result = generate_idempotency_key("different-step")
        assert result != different_result
    
    def test_create_step_from_submission(self):
        """Test step creation from submission parameters."""
        step_id = "test-step-123"
        payload = StepPayload(
            type="test_task",
            args={"param": "value"},
            metadata={"info": "test"}
        )
        priority = 5
        max_retries = 2
        
        # Capture time before creation for comparison
        before_time = datetime.utcnow()
        
        result = create_step_from_submission(step_id, payload, priority, max_retries)
        
        # Capture time after creation
        after_time = datetime.utcnow()
        
        assert isinstance(result, Step)
        assert result.step_id == step_id
        assert result.status == StepStatus.PENDING
        assert result.claimed_by is None
        assert result.payload == payload
        assert result.retry_count == 0
        assert result.priority == priority
        assert result.max_retries == max_retries
        
        # Timestamps should be reasonable
        assert before_time <= result.last_update_ts <= after_time
        assert before_time <= result.created_ts <= after_time
        assert result.created_ts == result.last_update_ts  # Should be same on creation
    
    def test_create_step_from_submission_defaults(self):
        """Test step creation with default parameters."""
        step_id = "test-step-456"
        payload = StepPayload(type="simple_task", args={}, metadata={})
        
        result = create_step_from_submission(step_id, payload)
        
        assert result.priority == 0  # Default priority
        assert result.max_retries == 3  # Default max retries
    
    def test_create_pod_health_record(self):
        """Test pod health record creation."""
        pod_id = "test-pod-123"
        metadata = {"cpu_count": 4, "version": "1.0.0"}
        
        # Capture time before creation
        before_time = datetime.utcnow()
        
        result = create_pod_health_record(pod_id, metadata)
        
        # Capture time after creation
        after_time = datetime.utcnow()
        
        assert isinstance(result, PodHealth)
        assert result.pod_id == pod_id
        assert result.status == PodStatus.UP
        assert result.meta == metadata
        
        # Timestamps should be reasonable and equal
        assert before_time <= result.birth_ts <= after_time
        assert before_time <= result.last_heartbeat_ts <= after_time
        assert result.birth_ts == result.last_heartbeat_ts  # Should be same on creation
    
    def test_create_pod_health_record_no_metadata(self):
        """Test pod health record creation without metadata."""
        pod_id = "test-pod-456"
        
        result = create_pod_health_record(pod_id)
        
        assert result.meta == {}  # Should default to empty dict
    
    def test_create_dlq_record(self):
        """Test DLQ record creation from step."""
        # Create a test step
        step = Step(
            step_id="test-step-789",
            status=StepStatus.CLAIMED,
            claimed_by="test-pod-123",
            last_update_ts=datetime(2024, 1, 1, 12, 0, 0),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=1,
            max_retries=3
        )
        
        reason = "dead_pod"
        action = "requeue"
        recovery_time = datetime(2024, 1, 1, 12, 5, 0)
        
        result = create_dlq_record(step, reason, action, recovery_time)
        
        assert isinstance(result, DLQRecord)
        assert result.step_id == step.step_id
        assert result.original_payload == step.payload
        assert result.claimed_by == step.claimed_by
        assert result.claim_timestamp == step.last_update_ts
        assert result.recovery_timestamp == recovery_time
        assert result.reason == reason
        assert result.retry_count == step.retry_count
        assert result.max_retries == step.max_retries
        assert result.action == action
    
    def test_create_dlq_record_default_timestamp(self):
        """Test DLQ record creation with default recovery timestamp."""
        step = Step(
            step_id="test-step-999",
            status=StepStatus.CLAIMED,
            claimed_by="test-pod-456",
            last_update_ts=datetime(2024, 1, 1, 12, 0, 0),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=0,
            max_retries=3
        )
        
        before_time = datetime.utcnow()
        result = create_dlq_record(step, "timeout", "terminal")
        after_time = datetime.utcnow()
        
        # Recovery timestamp should be set to current time
        assert before_time <= result.recovery_timestamp <= after_time
    
    def test_create_dlq_record_unknown_claimed_by(self):
        """Test DLQ record creation when step has no claimed_by."""
        step = Step(
            step_id="test-step-000",
            status=StepStatus.PENDING,
            claimed_by=None,  # No claimer
            last_update_ts=datetime(2024, 1, 1, 12, 0, 0),
            payload=StepPayload(type="test", args={}, metadata={}),
            retry_count=0,
            max_retries=3
        )
        
        result = create_dlq_record(step, "orphan", "requeue")
        
        assert result.claimed_by == "unknown"  # Should default to "unknown"


class TestSerializationRoundTrip:
    """Test round-trip serialization/deserialization."""
    
    def test_step_payload_round_trip(self):
        """Test that StepPayload can be serialized and deserialized correctly."""
        original = StepPayload(
            type="complex_task",
            args={
                "string_param": "test value",
                "int_param": 42,
                "list_param": [1, 2, 3],
                "dict_param": {"nested": "value"}
            },
            metadata={
                "priority": "high",
                "timeout": 300,
                "tags": ["urgent", "customer-facing"]
            }
        )
        
        # Serialize then deserialize
        serialized = VariantSerializer.serialize(original)
        deserialized = VariantSerializer.deserialize_step_payload(serialized)
        
        # Should be equivalent
        assert deserialized.type == original.type
        assert deserialized.args == original.args
        assert deserialized.metadata == original.metadata
    
    def test_dict_round_trip(self):
        """Test that dictionaries can be serialized and deserialized correctly."""
        original = {
            "simple_key": "simple_value",
            "complex_key": {
                "nested_list": [1, 2, {"deep": "value"}],
                "nested_dict": {"a": 1, "b": 2}
            },
            "list_key": ["item1", "item2", 3, True, None]
        }
        
        # Serialize then deserialize
        serialized = VariantSerializer.serialize(original)
        deserialized = VariantSerializer.deserialize_dict(serialized)
        
        # Should be equivalent
        assert deserialized == original