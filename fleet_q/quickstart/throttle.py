"""
FLEET-Q Adaptive Throttling

Implements AIMD (Additive Increase, Multiplicative Decrease) based adaptive
concurrency limiting for protecting downstream APIs like Bedrock.

Key Concepts:
- Don't guess limits, learn them through feedback
- Limit in-flight requests, not just rate
- React fast to throttling (multiplicative decrease)
- Probe cautiously for capacity (additive increase)
- Optional latency-aware pressure sensing

Inspired by:
- TCP congestion control
- Netflix adaptive concurrency
- Envoy circuit breakers
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, Dict
from collections import deque
import statistics

logger = logging.getLogger(__name__)


class ThrottleOutcome(str, Enum):
    """Outcome types for throttle feedback"""
    SUCCESS = "success"
    THROTTLE_ERROR = "throttle_error"  # 429, rate limit
    TIMEOUT = "timeout"  # Capacity-related timeout
    CLIENT_ERROR = "client_error"  # 4xx (non-throttle)
    SERVER_ERROR = "server_error"  # 5xx


@dataclass
class ThrottleConfig:
    """Configuration for adaptive throttling"""
    
    # Initial and boundary values
    initial_limit: int = 10
    min_limit: int = 1
    max_limit: int = 1000
    
    # AIMD parameters
    additive_increase: int = 1  # How much to increase on success
    multiplicative_decrease: float = 0.5  # Factor to decrease on throttle
    
    # Latency awareness
    enable_latency_tracking: bool = True
    latency_window_size: int = 100  # Number of requests to track
    latency_increase_threshold: float = 1.5  # Stop increasing if latency grows by this factor
    
    # Behavior tuning
    success_threshold: int = 10  # Consecutive successes before increasing
    probe_interval: float = 1.0  # Seconds between proactive probes
    
    # Timeout for detecting stuck requests
    timeout_seconds: float = 30.0


class AdaptiveThrottle:
    """
    Adaptive concurrency limiter using AIMD algorithm.
    
    Controls in-flight requests to downstream APIs, adapting limits based on
    feedback (successes, throttle errors, latency).
    
    Example:
        throttle = AdaptiveThrottle("bedrock-api")
        
        async with throttle.acquire():
            response = await call_bedrock_api()
            throttle.record_success(latency=0.5)
    """
    
    def __init__(
        self,
        name: str,
        config: Optional[ThrottleConfig] = None
    ):
        self.name = name
        self.config = config or ThrottleConfig()
        
        # Current state
        self._max_inflight = self.config.initial_limit
        self._current_inflight = 0
        self._lock = asyncio.Lock()
        
        # Statistics
        self._consecutive_successes = 0
        self._total_requests = 0
        self._total_throttles = 0
        self._total_successes = 0
        
        # Latency tracking (for pressure sensing)
        self._latency_history: deque = deque(maxlen=self.config.latency_window_size)
        self._baseline_latency: Optional[float] = None
        
        # Time tracking
        self._last_increase_time = time.time()
        
        logger.info(
            f"AdaptiveThrottle '{name}' initialized with limit={self._max_inflight}"
        )
    
    @property
    def max_inflight(self) -> int:
        """Current maximum in-flight limit"""
        return self._max_inflight
    
    @property
    def current_inflight(self) -> int:
        """Current number of in-flight requests"""
        return self._current_inflight
    
    @property
    def available_capacity(self) -> int:
        """Available capacity for new requests"""
        return max(0, self._max_inflight - self._current_inflight)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current throttle statistics"""
        throttle_rate = (
            self._total_throttles / self._total_requests
            if self._total_requests > 0
            else 0.0
        )
        
        stats = {
            'name': self.name,
            'max_inflight': self._max_inflight,
            'current_inflight': self._current_inflight,
            'available_capacity': self.available_capacity,
            'total_requests': self._total_requests,
            'total_successes': self._total_successes,
            'total_throttles': self._total_throttles,
            'throttle_rate': throttle_rate,
            'consecutive_successes': self._consecutive_successes,
        }
        
        if self.config.enable_latency_tracking and self._latency_history:
            stats['current_p95_latency'] = statistics.quantiles(
                self._latency_history, n=20
            )[-1] if len(self._latency_history) > 1 else self._latency_history[0]
            stats['baseline_latency'] = self._baseline_latency
        
        return stats
    
    @contextmanager
    def acquire_sync(self):
        """
        Synchronous context manager for acquiring throttle slot.
        
        Blocks until capacity is available.
        """
        # Wait for capacity
        while self._current_inflight >= self._max_inflight:
            time.sleep(0.01)  # Small sleep to avoid busy waiting
        
        self._current_inflight += 1
        self._total_requests += 1
        
        try:
            yield self
        finally:
            self._current_inflight -= 1
    
    @asynccontextmanager
    async def acquire(self):
        """
        Async context manager for acquiring throttle slot.
        
        Waits until capacity is available.
        
        Example:
            async with throttle.acquire():
                result = await call_api()
        """
        # Wait for capacity
        while self._current_inflight >= self._max_inflight:
            await asyncio.sleep(0.01)
        
        async with self._lock:
            self._current_inflight += 1
            self._total_requests += 1
        
        try:
            yield self
        finally:
            async with self._lock:
                self._current_inflight -= 1
    
    def record_success(self, latency: Optional[float] = None):
        """
        Record a successful request.
        
        Args:
            latency: Request latency in seconds (optional, for pressure sensing)
        """
        self._total_successes += 1
        self._consecutive_successes += 1
        
        # Track latency if provided
        if latency is not None and self.config.enable_latency_tracking:
            self._latency_history.append(latency)
            
            # Establish baseline on first few requests
            if self._baseline_latency is None and len(self._latency_history) >= 10:
                self._baseline_latency = statistics.median(self._latency_history)
        
        # Consider increasing limit (AIMD: Additive Increase)
        self._maybe_increase_limit()
    
    def record_throttle(self):
        """
        Record a throttle error (429, rate limit exceeded).
        
        Applies multiplicative decrease to back off quickly.
        """
        self._total_throttles += 1
        self._consecutive_successes = 0
        
        # AIMD: Multiplicative Decrease
        old_limit = self._max_inflight
        self._max_inflight = max(
            self.config.min_limit,
            int(self._max_inflight * self.config.multiplicative_decrease)
        )
        
        logger.warning(
            f"Throttle '{self.name}': Throttle error detected. "
            f"Reducing limit {old_limit} → {self._max_inflight}"
        )
    
    def record_timeout(self):
        """
        Record a timeout error.
        
        Treats similar to throttle but less aggressive.
        """
        self._consecutive_successes = 0
        
        old_limit = self._max_inflight
        # Less aggressive than full throttle decrease
        self._max_inflight = max(
            self.config.min_limit,
            int(self._max_inflight * 0.7)
        )
        
        logger.warning(
            f"Throttle '{self.name}': Timeout detected. "
            f"Reducing limit {old_limit} → {self._max_inflight}"
        )
    
    def _maybe_increase_limit(self):
        """
        Consider increasing the limit based on success rate and latency.
        
        AIMD: Additive Increase (cautious probing)
        """
        # Check if we have enough consecutive successes
        if self._consecutive_successes < self.config.success_threshold:
            return
        
        # Check if enough time has passed since last increase
        now = time.time()
        if now - self._last_increase_time < self.config.probe_interval:
            return
        
        # Latency-aware check: Don't increase if latency is rising
        if self._should_pause_increase_due_to_latency():
            logger.debug(
                f"Throttle '{self.name}': Pausing increase due to rising latency"
            )
            return
        
        # AIMD: Additive Increase
        old_limit = self._max_inflight
        self._max_inflight = min(
            self.config.max_limit,
            self._max_inflight + self.config.additive_increase
        )
        
        if self._max_inflight != old_limit:
            logger.info(
                f"Throttle '{self.name}': Increasing limit {old_limit} → {self._max_inflight}"
            )
        
        self._consecutive_successes = 0
        self._last_increase_time = now
    
    def _should_pause_increase_due_to_latency(self) -> bool:
        """
        Check if we should pause limit increases due to rising latency.
        
        This is the "pressure sensing" component.
        """
        if not self.config.enable_latency_tracking:
            return False
        
        if self._baseline_latency is None or len(self._latency_history) < 10:
            return False
        
        # Calculate recent p95 latency
        recent_latencies = list(self._latency_history)[-20:]
        if len(recent_latencies) < 5:
            return False
        
        recent_p95 = statistics.quantiles(recent_latencies, n=20)[-1]
        
        # If current latency is significantly higher than baseline, pause
        if recent_p95 > self._baseline_latency * self.config.latency_increase_threshold:
            return True
        
        return False
    
    def reset(self):
        """Reset throttle state (useful for testing or manual intervention)"""
        self._max_inflight = self.config.initial_limit
        self._consecutive_successes = 0
        self._latency_history.clear()
        self._baseline_latency = None
        logger.info(f"Throttle '{self.name}' reset to initial limit={self._max_inflight}")


def with_throttle(
    throttle: AdaptiveThrottle,
    throttle_exceptions: tuple = (Exception,),
    timeout_exceptions: tuple = (TimeoutError, asyncio.TimeoutError),
):
    """
    Decorator that applies adaptive throttling to a function.
    
    Automatically acquires throttle slot and records outcomes.
    
    Args:
        throttle: AdaptiveThrottle instance
        throttle_exceptions: Exception types to treat as throttle errors
        timeout_exceptions: Exception types to treat as timeouts
    
    Example:
        bedrock_throttle = AdaptiveThrottle("bedrock")
        
        @with_throttle(bedrock_throttle, throttle_exceptions=(BotoThrottleError,))
        async def call_bedrock(prompt: str):
            response = await bedrock_client.invoke(prompt)
            return response
    """
    def decorator(func: Callable):
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                
                async with throttle.acquire():
                    try:
                        result = await func(*args, **kwargs)
                        
                        # Record success with latency
                        latency = time.time() - start_time
                        throttle.record_success(latency=latency)
                        
                        return result
                    
                    except throttle_exceptions as e:
                        logger.warning(f"Throttle error in {func.__name__}: {e}")
                        throttle.record_throttle()
                        raise
                    
                    except timeout_exceptions as e:
                        logger.warning(f"Timeout in {func.__name__}: {e}")
                        throttle.record_timeout()
                        raise
            
            return async_wrapper
        
        else:
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                
                with throttle.acquire_sync():
                    try:
                        result = func(*args, **kwargs)
                        
                        # Record success with latency
                        latency = time.time() - start_time
                        throttle.record_success(latency=latency)
                        
                        return result
                    
                    except throttle_exceptions as e:
                        logger.warning(f"Throttle error in {func.__name__}: {e}")
                        throttle.record_throttle()
                        raise
                    
                    except timeout_exceptions as e:
                        logger.warning(f"Timeout in {func.__name__}: {e}")
                        throttle.record_timeout()
                        raise
            
            return sync_wrapper
    
    return decorator


# Global throttle registry for managing multiple throttles
_throttle_registry: Dict[str, AdaptiveThrottle] = {}


def get_or_create_throttle(name: str, config: Optional[ThrottleConfig] = None) -> AdaptiveThrottle:
    """
    Get or create a named throttle instance.
    
    Useful for sharing throttle state across different parts of the application.
    
    Args:
        name: Unique name for the throttle
        config: Configuration (only used if creating new throttle)
    
    Returns:
        AdaptiveThrottle instance
    """
    if name not in _throttle_registry:
        _throttle_registry[name] = AdaptiveThrottle(name, config)
    return _throttle_registry[name]


def get_all_throttles() -> Dict[str, AdaptiveThrottle]:
    """Get all registered throttles"""
    return _throttle_registry.copy()


# Example usage
if __name__ == "__main__":
    import asyncio
    
    logging.basicConfig(level=logging.INFO)
    
    async def example_usage():
        """Demonstrate adaptive throttling"""
        
        # Create throttle
        throttle = AdaptiveThrottle("example-api", ThrottleConfig(
            initial_limit=5,
            min_limit=1,
            max_limit=20
        ))
        
        # Simulate API calls
        print("Simulating API calls with adaptive throttling...\n")
        
        async def simulate_api_call(request_id: int, should_throttle: bool = False):
            async with throttle.acquire():
                await asyncio.sleep(0.1)  # Simulate API latency
                
                if should_throttle:
                    throttle.record_throttle()
                    raise Exception("Throttled!")
                else:
                    throttle.record_success(latency=0.1)
                    return f"Success {request_id}"
        
        # Phase 1: Successful requests (limit should increase)
        print("Phase 1: Successful requests")
        for i in range(50):
            try:
                result = await simulate_api_call(i)
                if i % 10 == 0:
                    stats = throttle.get_stats()
                    print(f"  Request {i}: {stats['max_inflight']} max, "
                          f"{stats['current_inflight']} inflight")
            except:
                pass
        
        print(f"\nAfter successful phase: max_inflight = {throttle.max_inflight}\n")
        
        # Phase 2: Hit throttle errors (limit should decrease)
        print("Phase 2: Throttle errors")
        for i in range(5):
            try:
                await simulate_api_call(i + 50, should_throttle=True)
            except:
                stats = throttle.get_stats()
                print(f"  Throttle hit: max_inflight reduced to {stats['max_inflight']}")
        
        print(f"\nAfter throttle phase: max_inflight = {throttle.max_inflight}\n")
        
        # Phase 3: Recovery (limit should gradually increase again)
        print("Phase 3: Recovery")
        for i in range(30):
            try:
                result = await simulate_api_call(i + 55)
                if i % 10 == 0:
                    stats = throttle.get_stats()
                    print(f"  Request {i}: {stats['max_inflight']} max, "
                          f"{stats['current_inflight']} inflight")
            except:
                pass
        
        # Final stats
        print("\nFinal statistics:")
        stats = throttle.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    asyncio.run(example_usage())
