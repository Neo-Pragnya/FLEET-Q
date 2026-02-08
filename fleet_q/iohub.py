"""
IOHub: Central Coordinator for FLEET-Q In-Pod Execution Fabric

IOHub is the "brain" of the pod, providing:
- AIMD-based permit control for Bedrock calls
- ZeroMQ ROUTER for request/reply routing
- Centralized outbox write coordination
- SharePoint operation routing
- Feedback processing and pressure management

Architecture:
- Runs in Control Plane Runner (singleton per pod)
- Workers connect via DEALER sockets
- Shared AIMD state prevents over-parallelization
- All external writes go through outbox

Message Flow:
1. Worker requests permit → IOHub checks AIMD → grant/deny
2. Worker calls Bedrock → reports outcome → IOHub updates AIMD
3. Worker sends result intent → IOHub writes to outbox
4. Separate flushers read outbox → write to external systems
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, Set
from dataclasses import dataclass
import uuid

from fleet_q.zeromq_utils import (
    ZMQRouter,
    ZMQMessage,
    MessageType,
    create_ipc_address
)
from fleet_q.sqlite_outbox import (
    SQLiteOutbox,
    StepUpdate,
    ResultIntent,
    SharePointIntent,
    PressureState,
    SharePointOpType
)

logger = logging.getLogger(__name__)


# ============================================================================
# AIMD Configuration
# ============================================================================

@dataclass
class AIMDConfig:
    """AIMD (Additive Increase Multiplicative Decrease) configuration"""
    initial_max_inflight: int = 10
    min_max_inflight: int = 1
    max_max_inflight: int = 100
    increase_rate: float = 1.0  # Add 1 per success streak
    decrease_factor: float = 0.5  # Halve on throttle
    success_streak_threshold: int = 5  # Increase after N successes
    throttle_cooldown_seconds: float = 10.0  # Wait before increasing after throttle
    latency_threshold_ms: float = 5000.0  # Warn if latency exceeds this


# ============================================================================
# IOHub Core
# ============================================================================

class IOHub:
    """
    Central coordinator for in-pod execution fabric.
    
    Responsibilities:
    - Permit management with AIMD
    - Message routing (ROUTER pattern)
    - Outbox coordination
    - Pressure feedback processing
    """
    
    def __init__(
        self,
        bind_address: str,
        outbox: SQLiteOutbox,
        aimd_config: Optional[AIMDConfig] = None,
        hwm: int = 1000
    ):
        """
        Initialize IOHub.
        
        Args:
            bind_address: ZeroMQ address to bind (e.g., "ipc:///tmp/fleetq-iohub.ipc")
            outbox: SQLiteOutbox instance
            aimd_config: AIMD configuration
            hwm: High water mark for ZeroMQ
        """
        self.bind_address = bind_address
        self.outbox = outbox
        self.aimd_config = aimd_config or AIMDConfig()
        self.hwm = hwm
        
        # ZeroMQ router
        self.router = ZMQRouter(bind_address, hwm=hwm, use_async=True)
        
        # AIMD state
        self.max_inflight = self.aimd_config.initial_max_inflight
        self.current_inflight = 0
        self.success_streak = 0
        self.last_throttle_time = 0.0
        self.permits_granted = 0
        self.permits_denied = 0
        
        # Active permits (worker_id -> permit_id)
        self.active_permits: Dict[str, str] = {}
        
        # Statistics
        self.total_requests = 0
        self.total_successes = 0
        self.total_throttles = 0
        self.total_errors = 0
        self.latency_samples = []
        
        # Running state
        self.running = False
        self.message_loop_task: Optional[asyncio.Task] = None
        
        logger.info(
            f"IOHub initialized at {bind_address} "
            f"(max_inflight={self.max_inflight}, HWM={hwm})"
        )
        
        # Load or initialize pressure state from outbox
        self._load_pressure_state()
    
    def _load_pressure_state(self):
        """Load AIMD state from outbox (persistent across restarts)"""
        state = self.outbox.get_pressure_state()
        if state:
            self.max_inflight = state.max_inflight
            self.current_inflight = 0  # Reset on startup
            self.success_streak = state.success_streak
            self.last_throttle_time = state.last_throttle_time
            logger.info(f"Loaded pressure state: max_inflight={self.max_inflight}")
        else:
            # Initialize
            self._save_pressure_state()
    
    def _save_pressure_state(self):
        """Save AIMD state to outbox"""
        state = PressureState(
            max_inflight=self.max_inflight,
            current_inflight=self.current_inflight,
            success_streak=self.success_streak,
            last_throttle_time=self.last_throttle_time,
            updated_at=time.time()
        )
        self.outbox.update_pressure_state(state)
    
    # ========================================================================
    # AIMD Permit Control
    # ========================================================================
    
    def can_grant_permit(self) -> bool:
        """Check if permit can be granted based on current inflight"""
        return self.current_inflight < self.max_inflight
    
    def grant_permit(self, worker_id: str) -> str:
        """
        Grant permit to worker.
        
        Returns:
            Permit ID
        """
        permit_id = str(uuid.uuid4())
        self.active_permits[worker_id] = permit_id
        self.current_inflight += 1
        self.permits_granted += 1
        
        logger.debug(
            f"Granted permit to {worker_id} "
            f"(inflight={self.current_inflight}/{self.max_inflight})"
        )
        
        return permit_id
    
    def release_permit(self, worker_id: str) -> bool:
        """
        Release permit from worker.
        
        Returns:
            True if permit was active
        """
        if worker_id in self.active_permits:
            del self.active_permits[worker_id]
            self.current_inflight = max(0, self.current_inflight - 1)
            logger.debug(f"Released permit from {worker_id} (inflight={self.current_inflight})")
            return True
        return False
    
    def update_aimd_success(self):
        """Update AIMD on successful call"""
        self.total_successes += 1
        self.success_streak += 1
        
        # Check if we can increase max_inflight
        now = time.time()
        cooldown_elapsed = (now - self.last_throttle_time) > self.aimd_config.throttle_cooldown_seconds
        
        if (self.success_streak >= self.aimd_config.success_streak_threshold and 
            cooldown_elapsed):
            # Additive increase
            old_max = self.max_inflight
            self.max_inflight = min(
                self.max_inflight + int(self.aimd_config.increase_rate),
                self.aimd_config.max_max_inflight
            )
            
            if self.max_inflight > old_max:
                logger.info(
                    f"AIMD increase: {old_max} → {self.max_inflight} "
                    f"(streak={self.success_streak})"
                )
                self.success_streak = 0  # Reset streak
                self._save_pressure_state()
    
    def update_aimd_throttle(self):
        """Update AIMD on throttle (429)"""
        self.total_throttles += 1
        self.success_streak = 0
        self.last_throttle_time = time.time()
        
        # Multiplicative decrease
        old_max = self.max_inflight
        self.max_inflight = max(
            int(self.max_inflight * self.aimd_config.decrease_factor),
            self.aimd_config.min_max_inflight
        )
        
        logger.warning(
            f"AIMD decrease: {old_max} → {self.max_inflight} "
            f"(throttle detected)"
        )
        
        self._save_pressure_state()
    
    def update_aimd_error(self):
        """Update AIMD on error (non-throttle)"""
        self.total_errors += 1
        self.success_streak = 0
        # Don't change max_inflight on errors (only throttles)
    
    def record_latency(self, latency_ms: float):
        """Record latency sample"""
        self.latency_samples.append(latency_ms)
        
        # Keep only recent samples (last 100)
        if len(self.latency_samples) > 100:
            self.latency_samples.pop(0)
        
        # Warn on high latency
        if latency_ms > self.aimd_config.latency_threshold_ms:
            logger.warning(f"High latency detected: {latency_ms:.0f}ms")
    
    # ========================================================================
    # Message Handlers
    # ========================================================================
    
    async def handle_permit_request(self, identity: bytes, message: ZMQMessage):
        """Handle permit request from worker"""
        worker_id = message.sender_id
        self.total_requests += 1
        
        if self.can_grant_permit():
            # Grant permit
            permit_id = self.grant_permit(worker_id)
            
            response = ZMQMessage.create(
                MessageType.PERMIT_GRANT,
                sender_id="iohub",
                payload={"permit_id": permit_id},
                correlation_id=message.correlation_id
            )
        else:
            # Deny permit
            self.permits_denied += 1
            
            response = ZMQMessage.create(
                MessageType.PERMIT_DENY,
                sender_id="iohub",
                payload={
                    "reason": "max_inflight_reached",
                    "current": self.current_inflight,
                    "max": self.max_inflight
                },
                correlation_id=message.correlation_id
            )
            
            logger.debug(
                f"Denied permit to {worker_id} "
                f"(inflight={self.current_inflight}/{self.max_inflight})"
            )
        
        await self.router.send_message(identity, response)
    
    async def handle_permit_release(self, identity: bytes, message: ZMQMessage):
        """Handle permit release from worker"""
        worker_id = message.sender_id
        self.release_permit(worker_id)
    
    async def handle_call_success(self, identity: bytes, message: ZMQMessage):
        """Handle successful call feedback"""
        worker_id = message.sender_id
        
        # Update AIMD
        self.update_aimd_success()
        
        # Record latency if provided
        latency = message.payload.get('latency')
        if latency:
            self.record_latency(latency * 1000)  # Convert to ms
        
        # Release permit
        self.release_permit(worker_id)
    
    async def handle_call_throttle(self, identity: bytes, message: ZMQMessage):
        """Handle throttle (429) feedback"""
        worker_id = message.sender_id
        
        # Update AIMD (decrease)
        self.update_aimd_throttle()
        
        # Release permit
        self.release_permit(worker_id)
    
    async def handle_call_error(self, identity: bytes, message: ZMQMessage):
        """Handle error feedback"""
        worker_id = message.sender_id
        error = message.payload.get('error', 'unknown')
        
        logger.error(f"Worker {worker_id} reported error: {error}")
        
        # Update AIMD
        self.update_aimd_error()
        
        # Release permit
        self.release_permit(worker_id)
    
    async def handle_enqueue_write(self, identity: bytes, message: ZMQMessage):
        """Handle write intent (outbox)"""
        payload = message.payload
        
        # Determine write type and enqueue
        if 'step_id' in payload and 'status' in payload:
            # Step update
            update = StepUpdate(
                step_id=payload['step_id'],
                status=payload['status'],
                error_message=payload.get('error_message'),
                retry_count=payload.get('retry_count', 0),
                metadata=payload.get('metadata')
            )
            self.outbox.enqueue_step_update(update)
        
        elif 'table_name' in payload and 'record_data' in payload:
            # Result
            result = ResultIntent(
                step_id=payload['step_id'],
                table_name=payload['table_name'],
                record_data=payload['record_data'],
                partition_key=payload.get('partition_key')
            )
            self.outbox.enqueue_result(result)
        
        logger.debug(f"Enqueued write intent from {message.sender_id}")
    
    async def handle_sharepoint_op(self, identity: bytes, message: ZMQMessage):
        """Handle SharePoint operation intent"""
        payload = message.payload
        
        intent = SharePointIntent(
            operation_id=payload.get('operation_id', str(uuid.uuid4())),
            op_type=SharePointOpType(payload['op_type']),
            site_url=payload['site_url'],
            file_path=payload['file_path'],
            local_path=payload.get('local_path'),
            metadata=payload.get('metadata')
        )
        
        self.outbox.enqueue_sharepoint_op(intent)
        logger.debug(f"Enqueued SharePoint op: {intent.op_type} from {message.sender_id}")
    
    async def handle_status_request(self, identity: bytes, message: ZMQMessage):
        """Handle status request"""
        status = self.get_status()
        
        response = ZMQMessage.create(
            MessageType.STATUS_RESPONSE,
            sender_id="iohub",
            payload=status,
            correlation_id=message.correlation_id
        )
        
        await self.router.send_message(identity, response)
    
    # ========================================================================
    # Main Message Loop
    # ========================================================================
    
    async def message_loop(self):
        """Main message processing loop"""
        logger.info("IOHub message loop started")
        
        while self.running:
            try:
                # Receive message with timeout
                result = await self.router.recv_message(timeout_ms=1000)
                
                if not result:
                    continue  # Timeout, check running flag
                
                identity, message = result
                
                # Route based on message type
                msg_type = message.msg_type
                
                if msg_type == MessageType.PERMIT_REQUEST.value:
                    await self.handle_permit_request(identity, message)
                
                elif msg_type == MessageType.PERMIT_RELEASE.value:
                    await self.handle_permit_release(identity, message)
                
                elif msg_type == MessageType.CALL_SUCCESS.value:
                    await self.handle_call_success(identity, message)
                
                elif msg_type == MessageType.CALL_THROTTLE.value:
                    await self.handle_call_throttle(identity, message)
                
                elif msg_type == MessageType.CALL_ERROR.value:
                    await self.handle_call_error(identity, message)
                
                elif msg_type == MessageType.ENQUEUE_WRITE.value:
                    await self.handle_enqueue_write(identity, message)
                
                elif msg_type in (MessageType.SP_DOWNLOAD.value, MessageType.SP_UPLOAD.value):
                    await self.handle_sharepoint_op(identity, message)
                
                elif msg_type == MessageType.STATUS_REQUEST.value:
                    await self.handle_status_request(identity, message)
                
                elif msg_type == MessageType.SHUTDOWN.value:
                    logger.info("Received shutdown message")
                    self.running = False
                
                else:
                    logger.warning(f"Unknown message type: {msg_type}")
            
            except Exception as e:
                logger.error(f"Error in message loop: {e}", exc_info=True)
                await asyncio.sleep(0.1)  # Brief pause on error
        
        logger.info("IOHub message loop stopped")
    
    # ========================================================================
    # Lifecycle
    # ========================================================================
    
    async def start(self):
        """Start IOHub message loop"""
        if self.running:
            logger.warning("IOHub already running")
            return
        
        self.running = True
        self.message_loop_task = asyncio.create_task(self.message_loop())
        logger.info("IOHub started")
    
    async def stop(self):
        """Stop IOHub gracefully"""
        if not self.running:
            return
        
        logger.info("Stopping IOHub...")
        self.running = False
        
        if self.message_loop_task:
            await self.message_loop_task
        
        self.router.close()
        self._save_pressure_state()
        logger.info("IOHub stopped")
    
    # ========================================================================
    # Status and Monitoring
    # ========================================================================
    
    def get_status(self) -> Dict[str, Any]:
        """Get current IOHub status"""
        avg_latency = (
            sum(self.latency_samples) / len(self.latency_samples)
            if self.latency_samples else 0
        )
        
        return {
            'running': self.running,
            'aimd': {
                'max_inflight': self.max_inflight,
                'current_inflight': self.current_inflight,
                'success_streak': self.success_streak,
                'last_throttle': self.last_throttle_time
            },
            'permits': {
                'granted': self.permits_granted,
                'denied': self.permits_denied,
                'active': len(self.active_permits)
            },
            'requests': {
                'total': self.total_requests,
                'successes': self.total_successes,
                'throttles': self.total_throttles,
                'errors': self.total_errors
            },
            'latency': {
                'avg_ms': avg_latency,
                'samples': len(self.latency_samples)
            }
        }
    
    def print_status(self):
        """Print formatted status"""
        status = self.get_status()
        print("\n=== IOHub Status ===")
        print(f"Running: {status['running']}")
        print(f"\nAIMD:")
        print(f"  Max inflight: {status['aimd']['max_inflight']}")
        print(f"  Current inflight: {status['aimd']['current_inflight']}")
        print(f"  Success streak: {status['aimd']['success_streak']}")
        print(f"\nPermits:")
        print(f"  Granted: {status['permits']['granted']}")
        print(f"  Denied: {status['permits']['denied']}")
        print(f"  Active: {status['permits']['active']}")
        print(f"\nRequests:")
        print(f"  Total: {status['requests']['total']}")
        print(f"  Successes: {status['requests']['successes']}")
        print(f"  Throttles: {status['requests']['throttles']}")
        print(f"  Errors: {status['requests']['errors']}")
        print(f"\nLatency:")
        print(f"  Avg: {status['latency']['avg_ms']:.1f}ms")
        print(f"  Samples: {status['latency']['samples']}")
        print("=" * 20)


# ============================================================================
# IOHub Client (Worker Side)
# ============================================================================

class IOHubClient:
    """
    Client for workers to communicate with IOHub.
    
    Each worker process creates one client.
    """
    
    def __init__(self, connect_address: str, worker_id: str, hwm: int = 1000):
        """
        Initialize IOHub client.
        
        Args:
            connect_address: IOHub address to connect to
            worker_id: Unique worker identifier
            hwm: High water mark
        """
        from fleet_q.zeromq_utils import ZMQDealer
        
        self.connect_address = connect_address
        self.worker_id = worker_id
        self.dealer = ZMQDealer(connect_address, identity=worker_id, hwm=hwm, use_async=True)
        
        logger.info(f"IOHub client '{worker_id}' initialized")
    
    async def request_permit(self, timeout_ms: int = 5000) -> bool:
        """
        Request permit from IOHub.
        
        Args:
            timeout_ms: Timeout for response
        
        Returns:
            True if granted, False if denied
        """
        request = ZMQMessage.create(
            MessageType.PERMIT_REQUEST,
            sender_id=self.worker_id,
            payload={}
        )
        
        await self.dealer.send_message(request)
        
        response = await self.dealer.recv_message(timeout_ms=timeout_ms)
        
        if not response:
            logger.warning(f"Permit request timeout for {self.worker_id}")
            return False
        
        return response.msg_type == MessageType.PERMIT_GRANT.value
    
    async def release_permit(self):
        """Release permit back to IOHub"""
        message = ZMQMessage.create(
            MessageType.PERMIT_RELEASE,
            sender_id=self.worker_id,
            payload={}
        )
        await self.dealer.send_message(message)
    
    async def report_success(self, latency: Optional[float] = None):
        """Report successful call"""
        message = ZMQMessage.create(
            MessageType.CALL_SUCCESS,
            sender_id=self.worker_id,
            payload={'latency': latency} if latency else {}
        )
        await self.dealer.send_message(message)
    
    async def report_throttle(self):
        """Report throttle (429)"""
        message = ZMQMessage.create(
            MessageType.CALL_THROTTLE,
            sender_id=self.worker_id,
            payload={}
        )
        await self.dealer.send_message(message)
    
    async def report_error(self, error: str):
        """Report error"""
        message = ZMQMessage.create(
            MessageType.CALL_ERROR,
            sender_id=self.worker_id,
            payload={'error': error}
        )
        await self.dealer.send_message(message)
    
    async def enqueue_result(self, step_id: str, table_name: str, record_data: Dict):
        """Enqueue result write"""
        message = ZMQMessage.create(
            MessageType.ENQUEUE_WRITE,
            sender_id=self.worker_id,
            payload={
                'step_id': step_id,
                'table_name': table_name,
                'record_data': record_data
            }
        )
        await self.dealer.send_message(message)
    
    async def enqueue_sharepoint_download(self, site_url: str, file_path: str, local_path: str):
        """Enqueue SharePoint download"""
        message = ZMQMessage.create(
            MessageType.SP_DOWNLOAD,
            sender_id=self.worker_id,
            payload={
                'op_type': 'download',
                'site_url': site_url,
                'file_path': file_path,
                'local_path': local_path
            }
        )
        await self.dealer.send_message(message)
    
    def close(self):
        """Close client"""
        self.dealer.close()


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    """Example IOHub usage"""
    
    async def main():
        from fleet_q.sqlite_outbox import SQLiteOutbox
        
        # Initialize outbox
        outbox = SQLiteOutbox("/tmp/iohub_test.db")
        
        # Create IOHub
        iohub = IOHub(
            bind_address=create_ipc_address("iohub-test"),
            outbox=outbox
        )
        
        # Start IOHub
        await iohub.start()
        
        # Simulate worker requests
        async def simulate_worker():
            await asyncio.sleep(0.5)  # Let IOHub start
            
            client = IOHubClient(
                create_ipc_address("iohub-test"),
                worker_id="test-worker-1"
            )
            
            for i in range(5):
                # Request permit
                granted = await client.request_permit()
                print(f"Request {i}: {'GRANTED' if granted else 'DENIED'}")
                
                if granted:
                    # Simulate work
                    await asyncio.sleep(0.5)
                    
                    # Report success
                    await client.report_success(latency=0.3)
                
                await asyncio.sleep(0.2)
            
            client.close()
        
        # Run simulation
        await simulate_worker()
        
        # Print status
        iohub.print_status()
        
        # Stop IOHub
        await iohub.stop()
        
        outbox.close()
    
    asyncio.run(main())
