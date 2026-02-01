"""
Snowflake Writer Stage with Batching

This module implements a pipeline stage for writing results to Snowflake:
- Batched writes for efficiency
- Configurable batch size and timeout
- Transaction safety
- Automatic retry on transient failures

Key Insight:
    Individual row inserts are slow.
    Batching dramatically improves throughput while maintaining consistency.
"""

import time
import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, asdict
from datetime import datetime

from pipeline import PipelineStage, PipelineMessage, MessageType
from storage import SnowflakeStorage
from backoff import with_backoff


logger = logging.getLogger(__name__)


@dataclass
class WriteRecord:
    """Standard record format for Snowflake writes"""
    record_id: str
    data: Dict[str, Any]
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


@dataclass
class WriteBatch:
    """Batch of records to write"""
    records: List[WriteRecord]
    batch_id: str
    created_at: float = 0.0
    
    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()
    
    def size(self) -> int:
        return len(self.records)


@dataclass
class WriteResult:
    """Result of a batch write operation"""
    batch_id: str
    records_written: int
    success: bool
    write_time: float
    error: Optional[str] = None


class SnowflakeWriterStage(PipelineStage):
    """
    Pipeline stage for writing results to Snowflake with batching.
    
    Features:
    - Batched writes for efficiency
    - Configurable batch size and flush timeout
    - Transaction-safe operations
    - Automatic retry with backoff
    - Graceful shutdown with flush
    """
    
    def __init__(
        self,
        stage_name: str = "SnowflakeWriter",
        table_name: str = "RESULTS",
        batch_size: int = 100,
        flush_timeout_seconds: float = 5.0,
        storage: Optional[SnowflakeStorage] = None,
        use_mock: bool = True,  # Use mock by default for demo
        **kwargs
    ):
        super().__init__(stage_name, **kwargs)
        self.table_name = table_name
        self.batch_size = batch_size
        self.flush_timeout = flush_timeout_seconds
        self.storage = storage
        self.use_mock = use_mock
        
        # Batching state
        self.current_batch: List[WriteRecord] = []
        self.last_flush_time = time.time()
        self.batch_counter = 0
        
        # Metrics
        self.total_records = 0
        self.total_batches = 0
        self.failed_batches = 0
        self.total_write_time = 0.0
    
    def setup(self):
        """Initialize Snowflake connection"""
        self.logger.info(
            f"Setting up Snowflake writer "
            f"(table={self.table_name}, batch_size={self.batch_size})"
        )
        
        if self.use_mock:
            self.logger.warning("Using MOCK Snowflake storage (for demo)")
        else:
            if not self.storage:
                from config import load_config
                config = load_config()
                self.storage = SnowflakeStorage(config.snowflake)
                self.logger.info("Connected to Snowflake")
    
    def teardown(self):
        """Flush remaining records and cleanup"""
        # Flush any pending records
        if self.current_batch:
            self.logger.info(f"Flushing {len(self.current_batch)} pending records on teardown")
            self._flush_batch()
        
        # Log statistics
        avg_write_time = self.total_write_time / max(self.total_batches, 1)
        self.logger.info(
            f"Snowflake writer stats: "
            f"Records={self.total_records}, "
            f"Batches={self.total_batches}, "
            f"Failed={self.failed_batches}, "
            f"Avg write time={avg_write_time:.3f}s"
        )
    
    def process_message(self, message: PipelineMessage) -> Optional[PipelineMessage]:
        """
        Add record to batch and flush if needed.
        
        This stage acts as a sink - it doesn't forward messages downstream.
        """
        if message.msg_type != MessageType.DATA:
            return None
        
        # Extract record
        payload = message.payload
        
        # Convert various payload types to WriteRecord
        if isinstance(payload, WriteRecord):
            record = payload
        elif isinstance(payload, dict):
            record = WriteRecord(
                record_id=payload.get('record_id', f"rec-{time.time()}"),
                data=payload
            )
        else:
            # Wrap arbitrary payload
            record = WriteRecord(
                record_id=f"rec-{time.time()}",
                data={'payload': str(payload)}
            )
        
        # Add to batch
        self.current_batch.append(record)
        
        # Check if we should flush
        should_flush = (
            len(self.current_batch) >= self.batch_size or
            time.time() - self.last_flush_time >= self.flush_timeout
        )
        
        if should_flush:
            self._flush_batch()
        
        return None  # Sink stage - no downstream
    
    def _flush_batch(self):
        """Write current batch to Snowflake"""
        if not self.current_batch:
            return
        
        batch_id = f"batch-{self.batch_counter:06d}"
        self.batch_counter += 1
        
        batch = WriteBatch(
            records=self.current_batch[:],  # Copy
            batch_id=batch_id
        )
        
        self.logger.info(f"Flushing batch {batch_id} with {batch.size()} records")
        
        try:
            result = self._write_batch_with_retry(batch)
            
            if result.success:
                self.total_records += result.records_written
                self.total_batches += 1
                self.total_write_time += result.write_time
                
                self.logger.debug(
                    f"Batch {batch_id} written: "
                    f"{result.records_written} records in {result.write_time:.3f}s"
                )
            else:
                self.failed_batches += 1
                self.logger.error(f"Batch {batch_id} failed: {result.error}")
            
            # Clear batch regardless of outcome
            self.current_batch.clear()
            self.last_flush_time = time.time()
        
        except Exception as e:
            self.failed_batches += 1
            self.logger.error(f"Failed to write batch {batch_id}: {e}", exc_info=True)
            
            # Clear batch to prevent infinite retry
            self.current_batch.clear()
            self.last_flush_time = time.time()
    
    @with_backoff(max_attempts=3, base_delay_ms=1000, max_delay_ms=5000)
    def _write_batch_with_retry(self, batch: WriteBatch) -> WriteResult:
        """Write batch with automatic retry"""
        start_time = time.time()
        
        try:
            if self.use_mock:
                records_written = self._mock_write(batch)
            else:
                records_written = self._real_write(batch)
            
            write_time = time.time() - start_time
            
            return WriteResult(
                batch_id=batch.batch_id,
                records_written=records_written,
                success=True,
                write_time=write_time
            )
        
        except Exception as e:
            write_time = time.time() - start_time
            
            return WriteResult(
                batch_id=batch.batch_id,
                records_written=0,
                success=False,
                write_time=write_time,
                error=str(e)
            )
    
    def _mock_write(self, batch: WriteBatch) -> int:
        """
        Mock Snowflake write for testing.
        
        Simulates write latency.
        """
        import random
        
        # Simulate write time (10-100ms per record)
        write_time = len(batch.records) * random.uniform(0.01, 0.1)
        time.sleep(write_time)
        
        self.logger.debug(f"Mock wrote {batch.size()} records to {self.table_name}")
        
        return batch.size()
    
    def _real_write(self, batch: WriteBatch) -> int:
        """
        Real Snowflake batch write using multi-row insert.
        
        Uses parameterized INSERT for safety and efficiency.
        """
        if not self.storage:
            raise RuntimeError("Snowflake storage not initialized")
        
        # Build multi-row INSERT
        # Assumes records have consistent schema
        if not batch.records:
            return 0
        
        # Extract columns from first record
        sample_record = batch.records[0]
        columns = list(sample_record.data.keys())
        
        # Build INSERT statement
        placeholders = ", ".join([f"({', '.join(['?' for _ in columns])})" for _ in batch.records])
        column_names = ", ".join(columns)
        
        sql = f"""
            INSERT INTO {self.table_name} ({column_names})
            VALUES {placeholders}
        """
        
        # Flatten record values
        values = []
        for record in batch.records:
            values.extend([record.data.get(col) for col in columns])
        
        # Execute batch insert
        with self.storage.cursor() as cursor:
            cursor.execute(sql, values)
            rows_affected = cursor.rowcount
        
        self.logger.debug(f"Wrote {rows_affected} records to {self.table_name}")
        
        return rows_affected


class BufferedWriter:
    """
    Convenience wrapper for buffered writes to Snowflake.
    
    Usage:
        with BufferedWriter(storage, "MY_TABLE") as writer:
            for record in records:
                writer.write(record)
            # Automatic flush on context exit
    """
    
    def __init__(
        self,
        storage: SnowflakeStorage,
        table_name: str,
        batch_size: int = 100
    ):
        self.storage = storage
        self.table_name = table_name
        self.batch_size = batch_size
        self.buffer: List[Dict[str, Any]] = []
        self.total_written = 0
    
    def write(self, record: Dict[str, Any]):
        """Add record to buffer and flush if needed"""
        self.buffer.append(record)
        
        if len(self.buffer) >= self.batch_size:
            self.flush()
    
    def flush(self):
        """Write buffer to Snowflake"""
        if not self.buffer:
            return
        
        # Build multi-row insert
        columns = list(self.buffer[0].keys())
        placeholders = ", ".join([f"({', '.join(['?' for _ in columns])})" for _ in self.buffer])
        column_names = ", ".join(columns)
        
        sql = f"""
            INSERT INTO {self.table_name} ({column_names})
            VALUES {placeholders}
        """
        
        # Flatten values
        values = []
        for record in self.buffer:
            values.extend([record.get(col) for col in columns])
        
        # Execute
        with self.storage.cursor() as cursor:
            cursor.execute(sql, values)
            self.total_written += cursor.rowcount
        
        logger.info(f"Flushed {len(self.buffer)} records to {self.table_name}")
        self.buffer.clear()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        logger.info(f"BufferedWriter closed. Total written: {self.total_written}")


# Demo usage
if __name__ == "__main__":
    """
    Demonstrate Snowflake batch writing.
    """
    from pipeline import Pipeline, SourceStage
    
    print("=== Snowflake Writer Demo ===\n")
    
    # Create test records
    records = [
        WriteRecord(
            record_id=f"rec-{i:03d}",
            data={
                "id": i,
                "value": f"test_value_{i}",
                "timestamp": time.time()
            }
        )
        for i in range(1, 51)
    ]
    
    # Create pipeline
    pipeline = Pipeline(name="snowflake-demo")
    
    # Stage 1: Source (feed records)
    source = SourceStage(
        stage_name="RecordSource",
        items=records
    )
    
    # Stage 2: Snowflake writer (batched writes)
    writer = SnowflakeWriterStage(
        stage_name="SnowflakeWriter",
        table_name="TEST_RESULTS",
        batch_size=10,
        flush_timeout_seconds=2.0,
        use_mock=True  # Use mock for demo
    )
    
    # Build and run
    pipeline.add_stage(source)
    pipeline.add_stage(writer)
    pipeline.build()
    
    try:
        pipeline.start()
        pipeline.wait()
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        pipeline.stop()
    
    print("\n=== Demo Complete ===")
    print(f"Total records: {writer.total_records}")
    print(f"Total batches: {writer.total_batches}")
    print(f"Failed batches: {writer.failed_batches}")
