"""
IOHub - Central Coordination Process for In-Pod Execution

This module implements the "IOHub" pattern - a single process that coordinates:
- Shared AIMD throttle control (pod-wide pressure memory)
- Local SQLite outbox (batched writes to Snowflake/SharePoint)
- Token caching and session management
- Permit granting for Bedrock calls

Two IPC options supported:
1. Pipe-based (multiprocessing.Pipe) - simpler
2. pyzmq ROUTER/DEALER - production-grade

Key Benefits:
- Workers stay lightweight (just Bedrock calls)
- Non-pickleable clients (Snowflake, SharePoint) live in IOHub only
- Shared AIMD prevents pod-wide throttling
- Batching dramatically improves Snowflake write performance
"""

import asyncio
import time
import json
import logging
import sqlite3
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
import threading

from throttle import AdaptiveThrottle, ThrottleConfig


logger = logging.getLogger(__name__)


# ============================================================
# Message Protocol
# ============================================================

class IOHubMessageType(Enum):
    """Message types for IOHub communication"""
    # Throttle operations
    REQUEST_PERMIT = "request_permit"
    RELEASE_PERMIT = "release_permit"
    REPORT_OUTCOME = "report_outcome"
    GET_THROTTLE_STATUS = "get_throttle_status"
    
    # Outbox operations
    ENQUEUE_WRITE = "enqueue_write"
    ENQUEUE_DOWNLOAD = "enqueue_download"
    
    # Responses
    PERMIT_GRANTED = "permit_granted"
    PERMIT_DENIED = "permit_denied"
    ACK = "ack"
    STATUS = "status"


@dataclass
class IOHubMessage:
    """Standard message format for IOHub communication"""
    msg_type: IOHubMessageType
    payload: Dict[str, Any]
    request_id: Optional[str] = None
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'msg_type': self.msg_type.value,
            'payload': self.payload,
            'request_id': self.request_id,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IOHubMessage':
        """Create from dictionary"""
        return cls(
            msg_type=IOHubMessageType(data['msg_type']),
            payload=data['payload'],
            request_id=data.get('request_id'),
            timestamp=data.get('timestamp', time.time())
        )


# ============================================================
# Local SQLite Outbox
# ============================================================

class SQLiteOutbox:
    """
    Local SQLite-based outbox for batched writes.
    
    Workers append to outbox, dedicated flushers batch-write to Snowflake.
    """
    
    def __init__(self, db_path: str = "/tmp/fleet_q_outbox.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize outbox tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Snowflake write queue
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS snowflake_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                step_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_data TEXT NOT NULL,
                created_at REAL NOT NULL,
                flushed_at REAL,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0
            )
        """)
        
        # SharePoint operation queue
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sharepoint_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                file_path TEXT NOT NULL,
                sharepoint_url TEXT,
                created_at REAL NOT NULL,
                completed_at REAL,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sf_status ON snowflake_outbox(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_sp_status ON sharepoint_outbox(status)")
        
        conn.commit()
        conn.close()
        
        logger.info(f"SQLite outbox initialized at {self.db_path}")
    
    def enqueue_snowflake_write(
        self,
        step_id: str,
        table_name: str,
        record_data: Dict[str, Any]
    ):
        """Add a Snowflake write to the outbox"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO snowflake_outbox (step_id, table_name, record_data, created_at)
            VALUES (?, ?, ?, ?)
        """, (step_id, table_name, json.dumps(record_data), time.time()))
        
        conn.commit()
        conn.close()
    
    def enqueue_sharepoint_operation(
        self,
        operation: str,
        file_path: str,
        sharepoint_url: Optional[str] = None
    ):
        """Add a SharePoint operation to the outbox"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO sharepoint_outbox (operation, file_path, sharepoint_url, created_at)
            VALUES (?, ?, ?, ?)
        """, (operation, file_path, sharepoint_url, time.time()))
        
        conn.commit()
        conn.close()
    
    def get_pending_snowflake_writes(self, limit: int = 100) -> List[Tuple[int, str, str, str]]:
        """Get pending Snowflake writes for batching"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, step_id, table_name, record_data
            FROM snowflake_outbox
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT ?
        """, (limit,))
        
        results = cursor.fetchall()
        conn.close()
        
        return results
    
    def mark_flushed(self, ids: List[int], table: str = "snowflake_outbox"):
        """Mark records as flushed"""
        if not ids:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ','.join('?' * len(ids))
        cursor.execute(f"""
            UPDATE {table}
            SET status = 'flushed', flushed_at = ?
            WHERE id IN ({placeholders})
        """, [time.time()] + ids)
        
        conn.commit()
        conn.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get outbox statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Snowflake stats
        cursor.execute("SELECT status, COUNT(*) FROM snowflake_outbox GROUP BY status")
        sf_stats = dict(cursor.fetchall())
        
        # SharePoint stats
        cursor.execute("SELECT status, COUNT(*) FROM sharepoint_outbox GROUP BY status")
        sp_stats = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'snowflake': sf_stats,
            'sharepoint': sp_stats
        }


# ============================================================
# Shared AIMD Controller
# ============================================================

class SharedAIMDController:
    """
    Shared AIMD throttle controller for pod-wide coordination.
    
    All workers report outcomes here, ensuring consistent throttling.
    """
    
    def __init__(self, throttle_name: str = "bedrock", config: Optional[ThrottleConfig] = None):
        self.throttle = AdaptiveThrottle(throttle_name, config)
        self.lock = threading.Lock()
        self.waiting_workers = []
        
        logger.info(f"Shared AIMD controller initialized: {throttle_name}")
    
    def request_permit(self, worker_id: str, timeout: float = 10.0) -> bool:
        """
        Request a permit to call Bedrock.
        
        Returns:
            True if permit granted, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            with self.lock:
                if self.throttle.current_inflight < self.throttle.max_inflight:
                    # Grant permit
                    self.throttle.current_inflight += 1
                    logger.debug(
                        f"Permit granted to {worker_id} "
                        f"({self.throttle.current_inflight}/{self.throttle.max_inflight})"
                    )
                    return True
            
            # Wait briefly before retrying
            time.sleep(0.1)
        
        logger.warning(f"Permit timeout for {worker_id}")
        return False
    
    def release_permit(self, worker_id: str):
        """Release a permit after Bedrock call completes"""
        with self.lock:
            self.throttle.current_inflight = max(0, self.throttle.current_inflight - 1)
            logger.debug(
                f"Permit released by {worker_id} "
                f"({self.throttle.current_inflight}/{self.throttle.max_inflight})"
            )
    
    def report_outcome(
        self,
        worker_id: str,
        outcome: str,
        latency: Optional[float] = None
    ):
        """
        Report Bedrock call outcome for AIMD adjustment.
        
        Args:
            worker_id: Worker identifier
            outcome: 'success', 'throttle', or 'timeout'
            latency: Request latency in seconds
        """
        with self.lock:
            if outcome == 'success':
                self.throttle.record_success(latency=latency)
                logger.debug(
                    f"Success reported by {worker_id}, "
                    f"new limit: {self.throttle.max_inflight}"
                )
            
            elif outcome == 'throttle':
                self.throttle.record_throttle()
                logger.warning(
                    f"Throttle reported by {worker_id}, "
                    f"new limit: {self.throttle.max_inflight}"
                )
            
            elif outcome == 'timeout':
                self.throttle.record_timeout()
                logger.warning(
                    f"Timeout reported by {worker_id}, "
                    f"new limit: {self.throttle.max_inflight}"
                )
    
    def get_status(self) -> Dict[str, Any]:
        """Get current throttle status"""
        with self.lock:
            return {
                'max_inflight': self.throttle.max_inflight,
                'current_inflight': self.throttle.current_inflight,
                'total_requests': self.throttle.total_requests,
                'total_successes': self.throttle.total_successes,
                'total_throttles': self.throttle.total_throttles,
                'throttle_rate': self.throttle.total_throttles / max(self.throttle.total_requests, 1)
            }


# ============================================================
# IOHub Process (Option 1: Pipe-Based)
# ============================================================

class IOHubPipeBased:
    """
    IOHub using multiprocessing.Pipe for communication.
    
    Simpler than ROUTER/DEALER but works well for single parent-child.
    """
    
    def __init__(
        self,
        outbox_db_path: str = "/tmp/fleet_q_outbox.db",
        throttle_config: Optional[ThrottleConfig] = None
    ):
        self.outbox = SQLiteOutbox(outbox_db_path)
        self.throttle_controller = SharedAIMDController("bedrock", throttle_config)
        self.running = False
    
    def handle_message(self, msg: IOHubMessage) -> IOHubMessage:
        """Process incoming message and return response"""
        
        if msg.msg_type == IOHubMessageType.REQUEST_PERMIT:
            worker_id = msg.payload.get('worker_id', 'unknown')
            granted = self.throttle_controller.request_permit(worker_id, timeout=10.0)
            
            return IOHubMessage(
                msg_type=IOHubMessageType.PERMIT_GRANTED if granted else IOHubMessageType.PERMIT_DENIED,
                payload={'worker_id': worker_id},
                request_id=msg.request_id
            )
        
        elif msg.msg_type == IOHubMessageType.RELEASE_PERMIT:
            worker_id = msg.payload.get('worker_id', 'unknown')
            self.throttle_controller.release_permit(worker_id)
            
            return IOHubMessage(
                msg_type=IOHubMessageType.ACK,
                payload={},
                request_id=msg.request_id
            )
        
        elif msg.msg_type == IOHubMessageType.REPORT_OUTCOME:
            worker_id = msg.payload.get('worker_id', 'unknown')
            outcome = msg.payload.get('outcome')
            latency = msg.payload.get('latency')
            
            self.throttle_controller.report_outcome(worker_id, outcome, latency)
            
            return IOHubMessage(
                msg_type=IOHubMessageType.ACK,
                payload={},
                request_id=msg.request_id
            )
        
        elif msg.msg_type == IOHubMessageType.ENQUEUE_WRITE:
            step_id = msg.payload.get('step_id')
            table_name = msg.payload.get('table_name')
            record_data = msg.payload.get('record_data')
            
            self.outbox.enqueue_snowflake_write(step_id, table_name, record_data)
            
            return IOHubMessage(
                msg_type=IOHubMessageType.ACK,
                payload={},
                request_id=msg.request_id
            )
        
        elif msg.msg_type == IOHubMessageType.GET_THROTTLE_STATUS:
            status = self.throttle_controller.get_status()
            
            return IOHubMessage(
                msg_type=IOHubMessageType.STATUS,
                payload=status,
                request_id=msg.request_id
            )
        
        else:
            logger.warning(f"Unknown message type: {msg.msg_type}")
            return IOHubMessage(
                msg_type=IOHubMessageType.ACK,
                payload={'error': 'unknown_message_type'},
                request_id=msg.request_id
            )
    
    def run(self, pipe_conn):
        """Main IOHub loop (runs in separate process)"""
        self.running = True
        logger.info("IOHub (Pipe-based) starting")
        
        try:
            while self.running:
                # Check for incoming messages
                if pipe_conn.poll(timeout=0.1):
                    try:
                        msg_dict = pipe_conn.recv()
                        msg = IOHubMessage.from_dict(msg_dict)
                        
                        # Process message
                        response = self.handle_message(msg)
                        
                        # Send response
                        pipe_conn.send(response.to_dict())
                    
                    except Exception as e:
                        logger.error(f"Error handling message: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            logger.info("IOHub interrupted")
        
        finally:
            logger.info("IOHub shutting down")
            self.running = False


# ============================================================
# IOHub Process (Option 2: pyzmq ROUTER/DEALER)
# ============================================================

class IOHubZMQBased:
    """
    IOHub using pyzmq ROUTER/DEALER for communication.
    
    Production-grade, supports many workers, built-in routing.
    """
    
    def __init__(
        self,
        bind_address: str = "tcp://127.0.0.1:5555",
        outbox_db_path: str = "/tmp/fleet_q_outbox.db",
        throttle_config: Optional[ThrottleConfig] = None
    ):
        self.bind_address = bind_address
        self.outbox = SQLiteOutbox(outbox_db_path)
        self.throttle_controller = SharedAIMDController("bedrock", throttle_config)
        self.running = False
    
    def handle_message(self, msg: IOHubMessage) -> IOHubMessage:
        """Process incoming message and return response (same as pipe-based)"""
        # Reuse logic from IOHubPipeBased
        hub = IOHubPipeBased(throttle_config=None)
        hub.outbox = self.outbox
        hub.throttle_controller = self.throttle_controller
        return hub.handle_message(msg)
    
    def run(self):
        """Main IOHub loop using pyzmq ROUTER"""
        try:
            import zmq
        except ImportError:
            logger.error("pyzmq not installed. Run: pip install pyzmq")
            return
        
        self.running = True
        logger.info(f"IOHub (ZMQ ROUTER) starting on {self.bind_address}")
        
        context = zmq.Context()
        socket = context.socket(zmq.ROUTER)
        socket.bind(self.bind_address)
        
        try:
            while self.running:
                try:
                    # Non-blocking receive with timeout
                    if socket.poll(timeout=100):  # 100ms
                        # ROUTER receives [identity, message]
                        identity, msg_bytes = socket.recv_multipart()
                        
                        # Deserialize message
                        msg_dict = json.loads(msg_bytes.decode('utf-8'))
                        msg = IOHubMessage.from_dict(msg_dict)
                        
                        # Process message
                        response = self.handle_message(msg)
                        
                        # Send response [identity, response]
                        response_bytes = json.dumps(response.to_dict()).encode('utf-8')
                        socket.send_multipart([identity, response_bytes])
                
                except Exception as e:
                    logger.error(f"Error in IOHub loop: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            logger.info("IOHub interrupted")
        
        finally:
            socket.close()
            context.term()
            logger.info("IOHub shut down")
            self.running = False


# ============================================================
# Worker Client (Pipe-Based)
# ============================================================

class IOHubClientPipe:
    """Client for workers to communicate with IOHub via Pipe"""
    
    def __init__(self, pipe_conn, worker_id: str):
        self.pipe_conn = pipe_conn
        self.worker_id = worker_id
        self.request_counter = 0
    
    def _send_request(self, msg_type: IOHubMessageType, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send request and wait for response"""
        self.request_counter += 1
        request_id = f"{self.worker_id}-{self.request_counter}"
        
        msg = IOHubMessage(
            msg_type=msg_type,
            payload=payload,
            request_id=request_id
        )
        
        # Send request
        self.pipe_conn.send(msg.to_dict())
        
        # Wait for response
        response_dict = self.pipe_conn.recv()
        response = IOHubMessage.from_dict(response_dict)
        
        return response.payload
    
    def request_permit(self, timeout: float = 10.0) -> bool:
        """Request permit to call Bedrock"""
        response = self._send_request(
            IOHubMessageType.REQUEST_PERMIT,
            {'worker_id': self.worker_id}
        )
        return response.get('worker_id') == self.worker_id
    
    def release_permit(self):
        """Release permit after Bedrock call"""
        self._send_request(
            IOHubMessageType.RELEASE_PERMIT,
            {'worker_id': self.worker_id}
        )
    
    def report_outcome(self, outcome: str, latency: Optional[float] = None):
        """Report Bedrock call outcome"""
        self._send_request(
            IOHubMessageType.REPORT_OUTCOME,
            {
                'worker_id': self.worker_id,
                'outcome': outcome,
                'latency': latency
            }
        )
    
    def enqueue_write(self, step_id: str, table_name: str, record_data: Dict[str, Any]):
        """Enqueue a Snowflake write"""
        self._send_request(
            IOHubMessageType.ENQUEUE_WRITE,
            {
                'step_id': step_id,
                'table_name': table_name,
                'record_data': record_data
            }
        )


# ============================================================
# Worker Client (ZMQ-Based)
# ============================================================

class IOHubClientZMQ:
    """Client for workers to communicate with IOHub via ZMQ DEALER"""
    
    def __init__(self, hub_address: str, worker_id: str):
        try:
            import zmq
        except ImportError:
            raise ImportError("pyzmq required. Run: pip install pyzmq")
        
        self.hub_address = hub_address
        self.worker_id = worker_id
        self.request_counter = 0
        
        # Create DEALER socket
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(hub_address)
        
        logger.info(f"IOHub client connected to {hub_address}")
    
    def _send_request(self, msg_type: IOHubMessageType, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send request and wait for response"""
        import zmq
        
        self.request_counter += 1
        request_id = f"{self.worker_id}-{self.request_counter}"
        
        msg = IOHubMessage(
            msg_type=msg_type,
            payload=payload,
            request_id=request_id
        )
        
        # Send request
        msg_bytes = json.dumps(msg.to_dict()).encode('utf-8')
        self.socket.send(msg_bytes)
        
        # Wait for response (with timeout)
        if self.socket.poll(timeout=10000):  # 10 second timeout
            response_bytes = self.socket.recv()
            response_dict = json.loads(response_bytes.decode('utf-8'))
            response = IOHubMessage.from_dict(response_dict)
            return response.payload
        else:
            raise TimeoutError("IOHub request timeout")
    
    def request_permit(self, timeout: float = 10.0) -> bool:
        """Request permit to call Bedrock"""
        response = self._send_request(
            IOHubMessageType.REQUEST_PERMIT,
            {'worker_id': self.worker_id}
        )
        return response.get('worker_id') == self.worker_id
    
    def release_permit(self):
        """Release permit after Bedrock call"""
        self._send_request(
            IOHubMessageType.RELEASE_PERMIT,
            {'worker_id': self.worker_id}
        )
    
    def report_outcome(self, outcome: str, latency: Optional[float] = None):
        """Report Bedrock call outcome"""
        self._send_request(
            IOHubMessageType.REPORT_OUTCOME,
            {
                'worker_id': self.worker_id,
                'outcome': outcome,
                'latency': latency
            }
        )
    
    def enqueue_write(self, step_id: str, table_name: str, record_data: Dict[str, Any]):
        """Enqueue a Snowflake write"""
        self._send_request(
            IOHubMessageType.ENQUEUE_WRITE,
            {
                'step_id': step_id,
                'table_name': table_name,
                'record_data': record_data
            }
        )
    
    def close(self):
        """Close connection"""
        self.socket.close()
        self.context.term()


# Demo
if __name__ == "__main__":
    """
    Demonstrate IOHub patterns.
    """
    import multiprocessing as mp
    
    print("=== IOHub Demo ===\n")
    print("Choose implementation:")
    print("1. Pipe-based (simpler)")
    print("2. ZMQ ROUTER/DEALER (production)")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        print("\n--- Pipe-Based IOHub ---\n")
        
        # Create pipe
        parent_conn, child_conn = mp.Pipe()
        
        # Start IOHub in separate process
        hub = IOHubPipeBased()
        hub_process = mp.Process(target=hub.run, args=(child_conn,))
        hub_process.start()
        
        # Create client
        client = IOHubClientPipe(parent_conn, "worker-demo")
        
        # Test permit flow
        print("Requesting permit...")
        if client.request_permit():
            print("✓ Permit granted")
            
            time.sleep(0.5)
            
            print("Reporting success...")
            client.report_outcome('success', latency=0.3)
            print("✓ Outcome reported")
            
            print("Releasing permit...")
            client.release_permit()
            print("✓ Permit released")
        
        # Test outbox
        print("\nEnqueuing write...")
        client.enqueue_write(
            step_id="test-001",
            table_name="TEST_TABLE",
            record_data={'value': 123}
        )
        print("✓ Write enqueued")
        
        # Cleanup
        hub.running = False
        hub_process.join(timeout=2)
        hub_process.terminate()
        
        print("\n✓ Demo complete")
    
    elif choice == "2":
        print("\n--- ZMQ ROUTER/DEALER IOHub ---\n")
        
        try:
            import zmq
        except ImportError:
            print("ERROR: pyzmq not installed")
            print("Run: pip install pyzmq")
            exit(1)
        
        # Start IOHub in separate process
        hub = IOHubZMQBased(bind_address="tcp://127.0.0.1:5555")
        hub_process = mp.Process(target=hub.run)
        hub_process.start()
        
        time.sleep(1)  # Let hub start
        
        # Create client
        client = IOHubClientZMQ("tcp://127.0.0.1:5555", "worker-demo")
        
        # Test permit flow
        print("Requesting permit...")
        if client.request_permit():
            print("✓ Permit granted")
            
            time.sleep(0.5)
            
            print("Reporting success...")
            client.report_outcome('success', latency=0.3)
            print("✓ Outcome reported")
            
            print("Releasing permit...")
            client.release_permit()
            print("✓ Permit released")
        
        # Test outbox
        print("\nEnqueuing write...")
        client.enqueue_write(
            step_id="test-001",
            table_name="TEST_TABLE",
            record_data={'value': 123}
        )
        print("✓ Write enqueued")
        
        # Cleanup
        client.close()
        hub.running = False
        hub_process.join(timeout=2)
        hub_process.terminate()
        
        print("\n✓ Demo complete")
    
    else:
        print("Invalid choice")
