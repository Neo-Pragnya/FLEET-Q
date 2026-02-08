"""
ZeroMQ Utilities for FLEET-Q In-Pod Execution Fabric

Provides socket patterns for in-pod coordination:
- ROUTER/DEALER: Request/reply with routing (permits, control plane)
- PUSH/PULL: Pipeline distribution (streaming data)
- PUB/SUB: Broadcast patterns (optional)

Critical ZeroMQ Rules:
1. One context per process
2. Create sockets in the process that uses them (no forking)
3. Bind in one place, connect everywhere else
4. Use ipc:// for local multi-process (fast)
5. Configure HWM (high water mark) to bound queues

Design Principles:
- Patterns as architecture primitives
- Explicit message types with routing
- Backpressure-aware (HWM + outbox)
- Non-blocking operations with timeouts
"""

import zmq
import zmq.asyncio
import json
import time
import logging
from typing import Dict, Any, Optional, List, Union, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
from contextlib import contextmanager

logger = logging.getLogger(__name__)


# ============================================================================
# Message Types and Structures
# ============================================================================

class MessageType(str, Enum):
    """Standard message types for FLEET-Q ZeroMQ bus"""
    
    # Permit management
    PERMIT_REQUEST = "permit_request"
    PERMIT_GRANT = "permit_grant"
    PERMIT_DENY = "permit_deny"
    PERMIT_RELEASE = "permit_release"
    
    # Feedback and outcomes
    CALL_SUCCESS = "call_success"
    CALL_THROTTLE = "call_throttle"
    CALL_ERROR = "call_error"
    LATENCY_SAMPLE = "latency_sample"
    
    # Side effect intents (outbox)
    ENQUEUE_WRITE = "enqueue_write"
    ENQUEUE_UPDATE = "enqueue_update"
    
    # SharePoint operations
    SP_DOWNLOAD = "sp_download"
    SP_UPLOAD = "sp_upload"
    SP_RESULT = "sp_result"
    
    # Health and control
    HEARTBEAT = "heartbeat"
    SHUTDOWN = "shutdown"
    STATUS_REQUEST = "status_request"
    STATUS_RESPONSE = "status_response"


@dataclass
class ZMQMessage:
    """Standard message structure for ZeroMQ bus"""
    msg_type: str  # MessageType value
    sender_id: str
    timestamp: float
    payload: Dict[str, Any]
    correlation_id: Optional[str] = None
    
    def to_bytes(self) -> bytes:
        """Serialize to JSON bytes"""
        return json.dumps(asdict(self)).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'ZMQMessage':
        """Deserialize from JSON bytes"""
        return cls(**json.loads(data.decode('utf-8')))
    
    @classmethod
    def create(cls, msg_type: MessageType, sender_id: str, 
               payload: Dict[str, Any], correlation_id: Optional[str] = None) -> 'ZMQMessage':
        """Create a new message with current timestamp"""
        return cls(
            msg_type=msg_type.value,
            sender_id=sender_id,
            timestamp=time.time(),
            payload=payload,
            correlation_id=correlation_id
        )


# ============================================================================
# ZeroMQ Context Manager (One per process)
# ============================================================================

class ZMQContext:
    """
    Singleton ZeroMQ context per process.
    
    Critical rule: One context per process, shared by all sockets in that process.
    """
    _instance: Optional[zmq.Context] = None
    _async_instance: Optional[zmq.asyncio.Context] = None
    
    @classmethod
    def get_sync_context(cls) -> zmq.Context:
        """Get or create synchronous ZeroMQ context"""
        if cls._instance is None:
            cls._instance = zmq.Context()
            logger.info("Created synchronous ZeroMQ context")
        return cls._instance
    
    @classmethod
    def get_async_context(cls) -> zmq.asyncio.Context:
        """Get or create asynchronous ZeroMQ context"""
        if cls._async_instance is None:
            cls._async_instance = zmq.asyncio.Context()
            logger.info("Created asynchronous ZeroMQ context")
        return cls._async_instance
    
    @classmethod
    def destroy_all(cls):
        """Destroy both contexts (cleanup on shutdown)"""
        if cls._instance:
            cls._instance.term()
            cls._instance = None
        if cls._async_instance:
            cls._async_instance.term()
            cls._async_instance = None
        logger.info("Destroyed ZeroMQ contexts")


# ============================================================================
# ROUTER Socket (IOHub Side - Bind)
# ============================================================================

class ZMQRouter:
    """
    ROUTER socket for IOHub control plane.
    
    Pattern: ROUTER binds, DEALERs connect
    Use case: Request/reply with routing (permits, feedback, control)
    
    ROUTER automatically tracks identity envelopes for routing replies.
    """
    
    def __init__(self, bind_address: str, hwm: int = 1000, use_async: bool = True):
        """
        Initialize ROUTER socket.
        
        Args:
            bind_address: Address to bind (e.g., "ipc:///tmp/fleetq-iohub.ipc")
            hwm: High water mark (max queued messages)
            use_async: Use async context (default True for IOHub)
        """
        self.bind_address = bind_address
        self.hwm = hwm
        self.use_async = use_async
        
        if use_async:
            self.context = ZMQContext.get_async_context()
            self.socket = self.context.socket(zmq.ROUTER)
        else:
            self.context = ZMQContext.get_sync_context()
            self.socket = self.context.socket(zmq.ROUTER)
        
        # Configure socket
        self.socket.setsockopt(zmq.SNDHWM, hwm)
        self.socket.setsockopt(zmq.RCVHWM, hwm)
        self.socket.setsockopt(zmq.LINGER, 1000)  # 1s linger on close
        
        # Bind
        self.socket.bind(bind_address)
        logger.info(f"ROUTER bound to {bind_address} (HWM={hwm})")
    
    async def recv_message(self, timeout_ms: Optional[int] = None) -> Optional[tuple[bytes, ZMQMessage]]:
        """
        Receive message with identity envelope (async).
        
        Returns:
            Tuple of (identity, message) or None if timeout
        """
        if not self.use_async:
            raise RuntimeError("Use recv_message_sync for sync sockets")
        
        try:
            if timeout_ms:
                if await self.socket.poll(timeout_ms):
                    frames = await self.socket.recv_multipart()
                else:
                    return None
            else:
                frames = await self.socket.recv_multipart()
            
            if len(frames) < 2:
                logger.warning("Received malformed message (< 2 frames)")
                return None
            
            identity = frames[0]
            message_data = frames[1]
            message = ZMQMessage.from_bytes(message_data)
            
            return identity, message
        
        except zmq.Again:
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def recv_message_sync(self, timeout_ms: Optional[int] = None) -> Optional[tuple[bytes, ZMQMessage]]:
        """Receive message with identity envelope (sync)"""
        if self.use_async:
            raise RuntimeError("Use recv_message for async sockets")
        
        try:
            if timeout_ms:
                if self.socket.poll(timeout_ms):
                    frames = self.socket.recv_multipart()
                else:
                    return None
            else:
                frames = self.socket.recv_multipart()
            
            if len(frames) < 2:
                logger.warning("Received malformed message (< 2 frames)")
                return None
            
            identity = frames[0]
            message_data = frames[1]
            message = ZMQMessage.from_bytes(message_data)
            
            return identity, message
        
        except zmq.Again:
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    async def send_message(self, identity: bytes, message: ZMQMessage) -> bool:
        """Send message to specific identity (async)"""
        if not self.use_async:
            raise RuntimeError("Use send_message_sync for sync sockets")
        
        try:
            await self.socket.send_multipart([identity, message.to_bytes()])
            return True
        except Exception as e:
            logger.error(f"Error sending message to {identity}: {e}")
            return False
    
    def send_message_sync(self, identity: bytes, message: ZMQMessage) -> bool:
        """Send message to specific identity (sync)"""
        if self.use_async:
            raise RuntimeError("Use send_message for async sockets")
        
        try:
            self.socket.send_multipart([identity, message.to_bytes()])
            return True
        except Exception as e:
            logger.error(f"Error sending message to {identity}: {e}")
            return False
    
    def close(self):
        """Close socket"""
        self.socket.close()
        logger.info("ROUTER socket closed")


# ============================================================================
# DEALER Socket (Worker Side - Connect)
# ============================================================================

class ZMQDealer:
    """
    DEALER socket for workers.
    
    Pattern: DEALER connects to ROUTER
    Use case: Async request/reply from worker to IOHub
    
    Each worker process creates its own DEALER socket.
    """
    
    def __init__(self, connect_address: str, identity: str, hwm: int = 1000, use_async: bool = True):
        """
        Initialize DEALER socket.
        
        Args:
            connect_address: Address to connect (e.g., "ipc:///tmp/fleetq-iohub.ipc")
            identity: Unique identity for this DEALER
            hwm: High water mark
            use_async: Use async context (default True for workers)
        """
        self.connect_address = connect_address
        self.identity = identity
        self.hwm = hwm
        self.use_async = use_async
        
        if use_async:
            self.context = ZMQContext.get_async_context()
            self.socket = self.context.socket(zmq.DEALER)
        else:
            self.context = ZMQContext.get_sync_context()
            self.socket = self.context.socket(zmq.DEALER)
        
        # Set identity
        self.socket.setsockopt_string(zmq.IDENTITY, identity)
        
        # Configure socket
        self.socket.setsockopt(zmq.SNDHWM, hwm)
        self.socket.setsockopt(zmq.RCVHWM, hwm)
        self.socket.setsockopt(zmq.LINGER, 1000)
        
        # Connect
        self.socket.connect(connect_address)
        logger.info(f"DEALER '{identity}' connected to {connect_address}")
    
    async def send_message(self, message: ZMQMessage) -> bool:
        """Send message to ROUTER (async)"""
        if not self.use_async:
            raise RuntimeError("Use send_message_sync for sync sockets")
        
        try:
            await self.socket.send(message.to_bytes())
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def send_message_sync(self, message: ZMQMessage) -> bool:
        """Send message to ROUTER (sync)"""
        if self.use_async:
            raise RuntimeError("Use send_message for async sockets")
        
        try:
            self.socket.send(message.to_bytes())
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    async def recv_message(self, timeout_ms: Optional[int] = None) -> Optional[ZMQMessage]:
        """Receive message from ROUTER (async)"""
        if not self.use_async:
            raise RuntimeError("Use recv_message_sync for sync sockets")
        
        try:
            if timeout_ms:
                if await self.socket.poll(timeout_ms):
                    data = await self.socket.recv()
                else:
                    return None
            else:
                data = await self.socket.recv()
            
            return ZMQMessage.from_bytes(data)
        
        except zmq.Again:
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def recv_message_sync(self, timeout_ms: Optional[int] = None) -> Optional[ZMQMessage]:
        """Receive message from ROUTER (sync)"""
        if self.use_async:
            raise RuntimeError("Use recv_message for async sockets")
        
        try:
            if timeout_ms:
                if self.socket.poll(timeout_ms):
                    data = self.socket.recv()
                else:
                    return None
            else:
                data = self.socket.recv()
            
            return ZMQMessage.from_bytes(data)
        
        except zmq.Again:
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def close(self):
        """Close socket"""
        self.socket.close()
        logger.info(f"DEALER '{self.identity}' closed")


# ============================================================================
# PUSH Socket (Producer Side - Bind/Connect)
# ============================================================================

class ZMQPush:
    """
    PUSH socket for pipeline producers.
    
    Pattern: PUSH -> PULL (load-balanced distribution)
    Use case: Streaming data to competing consumers
    
    Example: SharePoint downloader PUSH to processor pool PULL
    """
    
    def __init__(self, address: str, bind: bool = True, hwm: int = 1000, use_async: bool = True):
        """
        Initialize PUSH socket.
        
        Args:
            address: Address to bind or connect
            bind: If True, bind; if False, connect
            hwm: High water mark
            use_async: Use async context
        """
        self.address = address
        self.bind = bind
        self.hwm = hwm
        self.use_async = use_async
        
        if use_async:
            self.context = ZMQContext.get_async_context()
            self.socket = self.context.socket(zmq.PUSH)
        else:
            self.context = ZMQContext.get_sync_context()
            self.socket = self.context.socket(zmq.PUSH)
        
        # Configure
        self.socket.setsockopt(zmq.SNDHWM, hwm)
        self.socket.setsockopt(zmq.LINGER, 1000)
        
        # Bind or connect
        if bind:
            self.socket.bind(address)
            logger.info(f"PUSH bound to {address}")
        else:
            self.socket.connect(address)
            logger.info(f"PUSH connected to {address}")
    
    async def send_message(self, message: ZMQMessage) -> bool:
        """Send message (async, load-balanced to PULLs)"""
        if not self.use_async:
            raise RuntimeError("Use send_message_sync for sync sockets")
        
        try:
            await self.socket.send(message.to_bytes())
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def send_message_sync(self, message: ZMQMessage) -> bool:
        """Send message (sync)"""
        if self.use_async:
            raise RuntimeError("Use send_message for async sockets")
        
        try:
            self.socket.send(message.to_bytes())
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def close(self):
        """Close socket"""
        self.socket.close()
        logger.info("PUSH socket closed")


# ============================================================================
# PULL Socket (Consumer Side - Bind/Connect)
# ============================================================================

class ZMQPull:
    """
    PULL socket for pipeline consumers.
    
    Pattern: PUSH -> PULL (competing consumers)
    Use case: Receive work from PUSH producers
    
    Multiple PULLs share the load from PUSH sockets.
    """
    
    def __init__(self, address: str, bind: bool = False, hwm: int = 1000, use_async: bool = True):
        """
        Initialize PULL socket.
        
        Args:
            address: Address to bind or connect
            bind: If True, bind; if False, connect (usually connect)
            hwm: High water mark
            use_async: Use async context
        """
        self.address = address
        self.bind = bind
        self.hwm = hwm
        self.use_async = use_async
        
        if use_async:
            self.context = ZMQContext.get_async_context()
            self.socket = self.context.socket(zmq.PULL)
        else:
            self.context = ZMQContext.get_sync_context()
            self.socket = self.context.socket(zmq.PULL)
        
        # Configure
        self.socket.setsockopt(zmq.RCVHWM, hwm)
        self.socket.setsockopt(zmq.LINGER, 1000)
        
        # Bind or connect
        if bind:
            self.socket.bind(address)
            logger.info(f"PULL bound to {address}")
        else:
            self.socket.connect(address)
            logger.info(f"PULL connected to {address}")
    
    async def recv_message(self, timeout_ms: Optional[int] = None) -> Optional[ZMQMessage]:
        """Receive message (async)"""
        if not self.use_async:
            raise RuntimeError("Use recv_message_sync for sync sockets")
        
        try:
            if timeout_ms:
                if await self.socket.poll(timeout_ms):
                    data = await self.socket.recv()
                else:
                    return None
            else:
                data = await self.socket.recv()
            
            return ZMQMessage.from_bytes(data)
        
        except zmq.Again:
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def recv_message_sync(self, timeout_ms: Optional[int] = None) -> Optional[ZMQMessage]:
        """Receive message (sync)"""
        if self.use_async:
            raise RuntimeError("Use recv_message for async sockets")
        
        try:
            if timeout_ms:
                if self.socket.poll(timeout_ms):
                    data = self.socket.recv()
                else:
                    return None
            else:
                data = self.socket.recv()
            
            return ZMQMessage.from_bytes(data)
        
        except zmq.Again:
            return None
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            return None
    
    def close(self):
        """Close socket"""
        self.socket.close()
        logger.info("PULL socket closed")


# ============================================================================
# Request/Reply Helper (Async)
# ============================================================================

async def send_request_and_wait(
    dealer: ZMQDealer,
    request: ZMQMessage,
    timeout_ms: int = 5000
) -> Optional[ZMQMessage]:
    """
    Send request and wait for response (async).
    
    Args:
        dealer: DEALER socket to send/receive
        request: Request message
        timeout_ms: Timeout in milliseconds
    
    Returns:
        Response message or None if timeout
    """
    if not await dealer.send_message(request):
        return None
    
    return await dealer.recv_message(timeout_ms=timeout_ms)


# ============================================================================
# Utility Functions
# ============================================================================

def create_ipc_address(name: str) -> str:
    """
    Create IPC address for local communication.
    
    Args:
        name: Unique name for the socket
    
    Returns:
        IPC address string (e.g., "ipc:///tmp/fleetq-{name}.ipc")
    """
    return f"ipc:///tmp/fleetq-{name}.ipc"


def create_tcp_address(port: int, host: str = "127.0.0.1") -> str:
    """
    Create TCP address for network communication.
    
    Args:
        port: Port number
        host: Host address (default localhost)
    
    Returns:
        TCP address string (e.g., "tcp://127.0.0.1:5555")
    """
    return f"tcp://{host}:{port}"


@contextmanager
def zmq_socket_context(socket_obj):
    """
    Context manager for automatic socket cleanup.
    
    Usage:
        with zmq_socket_context(dealer) as sock:
            sock.send_message(msg)
    """
    try:
        yield socket_obj
    finally:
        socket_obj.close()


# ============================================================================
# Example Usage and Patterns
# ============================================================================

if __name__ == "__main__":
    """
    Example: ROUTER/DEALER pattern for permits
    """
    
    async def router_example():
        """IOHub side: ROUTER receives requests"""
        router = ZMQRouter(create_ipc_address("test-router"))
        
        print("Router waiting for messages...")
        for _ in range(3):
            result = await router.recv_message(timeout_ms=5000)
            if result:
                identity, message = result
                print(f"Received from {identity}: {message.msg_type}")
                
                # Send response
                response = ZMQMessage.create(
                    MessageType.PERMIT_GRANT,
                    sender_id="router",
                    payload={"granted": True}
                )
                await router.send_message(identity, response)
        
        router.close()
    
    async def dealer_example():
        """Worker side: DEALER sends requests"""
        await asyncio.sleep(0.5)  # Let router bind
        
        dealer = ZMQDealer(
            create_ipc_address("test-router"),
            identity="worker-1"
        )
        
        for i in range(3):
            # Send request
            request = ZMQMessage.create(
                MessageType.PERMIT_REQUEST,
                sender_id="worker-1",
                payload={"attempt": i}
            )
            print(f"Sending request {i}...")
            await dealer.send_message(request)
            
            # Wait for response
            response = await dealer.recv_message(timeout_ms=2000)
            if response:
                print(f"Got response: {response.payload}")
            
            await asyncio.sleep(0.5)
        
        dealer.close()
    
    async def main():
        await asyncio.gather(
            router_example(),
            dealer_example()
        )
        
        ZMQContext.destroy_all()
    
    asyncio.run(main())
