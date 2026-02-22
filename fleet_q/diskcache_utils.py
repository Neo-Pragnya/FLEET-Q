"""
DiskCache-based token management utilities for cross-process token refresh coordination.

This module provides a production-ready token provider pattern with:
- Automatic token refresh with configurable buffer time
- Cross-process locking (prevents refresh storms)
- TTL-based expiration
- Retry-on-401 pattern
- Observability (logging + metrics)
- **Multiprocessing support** (processes share tokens, not connections)

## Core Design Principle

**Tokens are shared via DiskCache; connections are per-process.**

Each process/worker:
- ✅ Creates its own HTTP client/connection
- ✅ Shares token cache via same DiskCache directory
- ✅ Only ONE process refreshes when token expires
- ✅ All processes reuse the refreshed token

## Multiprocessing Usage

### ✅ What Works (Token Reuse)

- **Gunicorn/Uvicorn workers** (separate processes) ✅
- **multiprocessing.Pool** child processes ✅
- **aiomultiprocess** processes ✅
- **Multiple threads** ✅

### 🔑 Critical Requirements

1. **Same cache directory path** (all processes must use identical absolute path)
2. **Same filesystem** (single VM or shared volume like EFS/NFS)
3. **Per-request token injection** (never embed token in long-lived client)

### ⚠️ Common Gotchas

**Gotcha 1: Different cache paths per process**
```python
# ❌ BAD - each process creates different cache
def worker_init():
    cache = DiskCacheBackend(f"/tmp/cache_{os.getpid()}")  # Different path!
```

**Fix:** Use canonical absolute path from environment
```python
# ✅ GOOD - all processes share same cache
def worker_init():
    cache = DiskCacheBackend(os.getenv("TOKEN_CACHE_DIR", "/var/lib/app/cache"))
```

**Gotcha 2: Token embedded in client headers**
```python
# ❌ BAD - token never refreshes
token = await provider.get_token()
client = httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"})
await client.get(url)  # Uses stale token forever!
```

**Fix:** Inject token per request
```python
# ✅ GOOD - fresh token every request
async def make_request(url):
    token = await provider.get_token()  # Always fresh
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient() as client:
        return await client.get(url, headers=headers)
```

## Basic Usage Example

```python
from fleet_q.diskcache_utils import TokenProvider, DiskCacheBackend, get_canonical_cache_path

# 1. Get canonical cache path (same across all processes)
cache_dir = get_canonical_cache_path()

# 2. Create cache backend (shared)
cache = DiskCacheBackend(cache_dir)

# 3. Define token fetcher
async def fetch_sharepoint_token():
    return {
        "access_token": "eyJ...",
        "expires_in": 3600
    }

# 4. Create provider
provider = TokenProvider(
    token_key="sharepoint:tenant_x",
    token_fetcher=fetch_sharepoint_token,
    cache_backend=cache
)

# 5. Use it (always fresh, shared across processes)
token = await provider.get_token()

# 6. Handle 401 (automatic recovery)
try:
    await make_api_call(token)
except Unauthorized:
    await provider.invalidate()
    token = await provider.get_token()
    await make_api_call(token)  # Retry once
```

## Multiprocessing Example

```python
import multiprocessing
from fleet_q.diskcache_utils import TokenProvider, DiskCacheBackend

# Shared cache path (environment variable)
CACHE_DIR = os.getenv("TOKEN_CACHE_DIR", "/var/lib/app/token_cache")

def worker_task(worker_id: int):
    '''Each worker creates own provider pointing to SAME cache'''
    cache = DiskCacheBackend(CACHE_DIR)  # Same path!
    provider = TokenProvider(
        token_key="shared:service",
        token_fetcher=fetch_token,
        cache_backend=cache
    )
    
    # All workers share token from cache
    token = asyncio.run(provider.get_token())
    
    # But each worker has own HTTP client
    client = httpx.Client()
    response = client.get(url, headers={"Authorization": f"Bearer {token}"})
    client.close()

# Spawn multiple processes
with multiprocessing.Pool(4) as pool:
    pool.map(worker_task, range(4))
    # Only ONE process refreshes token; others read from cache
```
"""

import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypedDict, Union

try:
    import diskcache
except ImportError:
    diskcache = None

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration Helpers
# ============================================================================


def get_canonical_cache_path(
    env_var: str = "TOKEN_CACHE_DIR",
    default: str = "/var/lib/app/token_cache"
) -> Path:
    """
    Get canonical cache path that is consistent across all processes.
    
    This ensures all workers/processes use the SAME cache directory,
    which is critical for token sharing.
    
    Args:
        env_var: Environment variable name (default: TOKEN_CACHE_DIR)
        default: Default path if env var not set
    
    Returns:
        Absolute Path object
    
    Example:
        ```python
        # In your config.py or main app
        CACHE_PATH = get_canonical_cache_path()
        
        # All workers use this same path
        cache = DiskCacheBackend(CACHE_PATH)
        ```
    """
    path_str = os.getenv(env_var, default)
    path = Path(path_str).resolve()  # Absolute path
    
    logger.debug(f"Canonical cache path: {path} (from {env_var}={path_str})")
    return path


def validate_cache_path(cache_path: Path) -> None:
    """
    Validate that cache path is suitable for multiprocessing use.
    
    Checks:
    - Path is absolute
    - Path is not per-process (doesn't contain PID)
    - Path is writable
    
    Raises:
        ValueError: If path is unsuitable
    
    Example:
        ```python
        cache_path = Path("/var/lib/app/cache")
        validate_cache_path(cache_path)  # OK
        
        bad_path = Path(f"/tmp/cache_{os.getpid()}")
        validate_cache_path(bad_path)  # Raises ValueError
        ```
    """
    if not cache_path.is_absolute():
        raise ValueError(
            f"Cache path must be absolute for multiprocessing: {cache_path}\n"
            f"Got relative path which may differ between processes."
        )
    
    # Check for PID in path (common mistake)
    if str(os.getpid()) in str(cache_path):
        raise ValueError(
            f"Cache path contains PID {os.getpid()}: {cache_path}\n"
            f"This will create different caches per process!\n"
            f"Use a fixed absolute path instead."
        )
    
    # Check if path contains common process-specific patterns
    path_str = str(cache_path)
    risky_patterns = ['$PID', '${PID}', '$PPID', '{pid}']
    for pattern in risky_patterns:
        if pattern.lower() in path_str.lower():
            raise ValueError(
                f"Cache path contains process-specific pattern '{pattern}': {cache_path}\n"
                f"This will create different caches per process!"
            )
    
    logger.debug(f"Cache path validation passed: {cache_path}")


# ============================================================================
# Data Models
# ============================================================================


class TokenRecord(TypedDict):
    """Structure for cached token data."""
    access_token: str
    refresh_token: Optional[str]
    expires_at: float  # epoch timestamp
    token_type: str
    metadata: Dict[str, Any]


@dataclass
class TokenProviderConfig:
    """Configuration for TokenProvider."""
    refresh_buffer_seconds: int = 120  # Refresh when < 2 min remaining
    lock_timeout_seconds: int = 30  # Max time to hold refresh lock
    retry_on_lock_wait_ms: int = 100  # Polling interval when waiting for lock
    max_lock_wait_seconds: int = 10  # Max time to wait for another refresher
    enable_metrics: bool = True
    enable_debug_logging: bool = False


# ============================================================================
# Cache Backend Interface
# ============================================================================


class CacheBackend(ABC):
    """Abstract interface for token cache storage."""
    
    @abstractmethod
    def get(self, key: str) -> Optional[TokenRecord]:
        """Retrieve token record from cache."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: TokenRecord, ttl_seconds: Optional[int] = None) -> None:
        """Store token record in cache with optional TTL."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove token record from cache."""
        pass
    
    @abstractmethod
    def acquire_lock(self, lock_key: str, ttl_seconds: int) -> bool:
        """Attempt to acquire a distributed lock. Returns True if acquired."""
        pass
    
    @abstractmethod
    def release_lock(self, lock_key: str) -> None:
        """Release a previously acquired lock."""
        pass


# ============================================================================
# DiskCache Backend Implementation
# ============================================================================


class DiskCacheBackend(CacheBackend):
    """
    DiskCache-based cache backend with cross-process locking.
    
    Features:
    - Automatic TTL expiration
    - Built-in cross-process locks
    - Thread-safe and process-safe
    - Configurable size limits
    - **Multiprocessing support** (all processes share same cache)
    
    Critical for Multiprocessing:
    - All processes MUST use the same cache_dir (absolute path)
    - Recommended: use get_canonical_cache_path() to ensure consistency
    - Each process creates own DiskCacheBackend instance
    - All instances share the same underlying cache files
    
    Args:
        cache_dir: Directory path for cache storage (must be absolute)
        size_limit: Maximum cache size in bytes (default: 100MB)
        eviction_policy: 'least-recently-stored' or 'least-recently-used'
        validate_path: Whether to validate path for multiprocessing (default: True)
    
    Example (multiprocessing-safe):
        ```python
        # In each process/worker
        CACHE_DIR = get_canonical_cache_path()  # Same for all processes
        cache = DiskCacheBackend(CACHE_DIR)
        provider = TokenProvider(..., cache_backend=cache)
        ```
    """
    
    def __init__(
        self,
        cache_dir: Union[str, Path],
        size_limit: int = 100 * 1024 * 1024,  # 100 MB
        eviction_policy: str = 'least-recently-stored',
        validate_path: bool = True
    ):
        if diskcache is None:
            raise ImportError(
                "diskcache is not installed. Install with: pip install diskcache"
            )
        
        self.cache_dir = Path(cache_dir).resolve()  # Always absolute
        
        # Validate for multiprocessing safety
        if validate_path:
            validate_cache_path(self.cache_dir)
        
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.cache = diskcache.Cache(
            str(self.cache_dir),
            size_limit=size_limit,
            eviction_policy=eviction_policy,
        )
        
        logger.info(
            f"DiskCacheBackend initialized at {self.cache_dir} "
            f"(size_limit={size_limit}, policy={eviction_policy}, pid={os.getpid()})"
        )
    
    def get(self, key: str) -> Optional[TokenRecord]:
        """Get token record from cache."""
        try:
            value = self.cache.get(key)
            if value:
                logger.debug(f"Cache HIT for key: {key}")
                return value
            logger.debug(f"Cache MISS for key: {key}")
            return None
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: TokenRecord, ttl_seconds: Optional[int] = None) -> None:
        """Set token record in cache with optional TTL."""
        try:
            self.cache.set(key, value, expire=ttl_seconds)
            logger.debug(f"Cache SET for key: {key} (ttl={ttl_seconds}s)")
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
    
    def delete(self, key: str) -> None:
        """Remove token from cache."""
        try:
            self.cache.delete(key)
            logger.debug(f"Cache DELETE for key: {key}")
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
    
    def acquire_lock(self, lock_key: str, ttl_seconds: int) -> bool:
        """
        Acquire a distributed lock using DiskCache.
        
        Returns True if lock acquired, False if already held.
        """
        try:
            # DiskCache doesn't have explicit locks, but we can use atomic operations
            # Create a lock record with expiry
            lock_value = {"owner": id(self), "acquired_at": time.time()}
            
            # Try to add lock (fails if exists)
            # Use a short expire to auto-release if holder crashes
            success = self.cache.add(lock_key, lock_value, expire=ttl_seconds)
            
            if success:
                logger.debug(f"Lock ACQUIRED: {lock_key}")
            else:
                logger.debug(f"Lock DENIED (already held): {lock_key}")
            
            return success
        except Exception as e:
            logger.error(f"Lock acquire error for {lock_key}: {e}")
            return False
    
    def release_lock(self, lock_key: str) -> None:
        """Release lock."""
        try:
            self.cache.delete(lock_key)
            logger.debug(f"Lock RELEASED: {lock_key}")
        except Exception as e:
            logger.error(f"Lock release error for {lock_key}: {e}")
    
    def close(self) -> None:
        """Close cache (cleanup)."""
        try:
            self.cache.close()
        except Exception as e:
            logger.error(f"Cache close error: {e}")


# ============================================================================
# In-Memory Backend (for testing/development)
# ============================================================================


class InMemoryCacheBackend(CacheBackend):
    """
    Simple in-memory cache backend (NOT cross-process safe).
    Use only for development/testing or single-worker deployments.
    """
    
    def __init__(self):
        self._cache: Dict[str, tuple[TokenRecord, Optional[float]]] = {}
        self._locks: Dict[str, float] = {}  # lock_key -> expires_at
        logger.warning(
            "InMemoryCacheBackend is NOT cross-process safe. "
            "Use DiskCacheBackend for production."
        )
    
    def get(self, key: str) -> Optional[TokenRecord]:
        """Get from in-memory cache."""
        if key not in self._cache:
            return None
        
        value, expires_at = self._cache[key]
        
        # Check if expired
        if expires_at and time.time() > expires_at:
            del self._cache[key]
            return None
        
        return value
    
    def set(self, key: str, value: TokenRecord, ttl_seconds: Optional[int] = None) -> None:
        """Set in-memory cache."""
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        self._cache[key] = (value, expires_at)
    
    def delete(self, key: str) -> None:
        """Delete from cache."""
        self._cache.pop(key, None)
    
    def acquire_lock(self, lock_key: str, ttl_seconds: int) -> bool:
        """Acquire in-memory lock."""
        # Clean expired locks
        now = time.time()
        self._locks = {k: v for k, v in self._locks.items() if v > now}
        
        if lock_key in self._locks:
            return False
        
        self._locks[lock_key] = now + ttl_seconds
        return True
    
    def release_lock(self, lock_key: str) -> None:
        """Release lock."""
        self._locks.pop(lock_key, None)


# ============================================================================
# Token Provider
# ============================================================================


class TokenProvider:
    """
    Manages token lifecycle with lazy refresh and cross-process coordination.
    
    Key behaviors:
    - Caches tokens until expiry (minus buffer)
    - Only one process refreshes at a time (singleflight pattern)
    - Other processes wait briefly for refresher, then recheck cache
    - Supports manual invalidation (for 401 handling)
    - **Multiprocessing-safe** when using DiskCacheBackend
    
    Multiprocessing Pattern:
    1. Each process creates its own TokenProvider instance
    2. All instances share the same DiskCache directory
    3. When token expires:
       - Process A acquires lock → refreshes token
       - Processes B, C, D wait → read token from cache
    4. Result: Only ONE refresh across all processes
    
    Critical: Never embed token in long-lived objects!
    - ✅ Call get_token() per request
    - ❌ Don't cache token in HTTP client headers at init time
    
    Args:
        token_key: Unique identifier for this token (e.g., "sharepoint:tenantX")
        token_fetcher: Async function that returns token data
        cache_backend: Cache backend instance (DiskCacheBackend recommended)
        config: Optional configuration overrides
    
    Example (multiprocessing worker):
        ```python
        # Each worker runs this
        def worker_init():
            cache = DiskCacheBackend(get_canonical_cache_path())
            provider = TokenProvider(
                token_key="service:prod",
                token_fetcher=fetch_token,
                cache_backend=cache
            )
            return provider
        
        # In request handler
        async def handle_request():
            token = await provider.get_token()  # Fresh, shared token
            headers = {"Authorization": f"Bearer {token}"}
            # Make request with token...
        ```
    """
    
    def __init__(
        self,
        token_key: str,
        token_fetcher: Callable[[], Union[Dict[str, Any], TokenRecord]],
        cache_backend: CacheBackend,
        config: Optional[TokenProviderConfig] = None,
    ):
        self.token_key = token_key
        self.token_fetcher = token_fetcher
        self.cache_backend = cache_backend
        self.config = config or TokenProviderConfig()
        
        # Metrics counters (in-memory, per instance)
        self._metrics = {
            'cache_hits': 0,
            'cache_misses': 0,
            'refreshes': 0,
            'refresh_failures': 0,
            'lock_acquires': 0,
            'lock_waits': 0,
        }
        
        logger.info(f"TokenProvider created for key: {token_key}")
    
    async def get_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            The access token string
        
        Raises:
            Exception if token refresh fails and no cached token available
        """
        # Try cache first
        cached = self.cache_backend.get(self.token_key)
        
        if cached and self._is_token_valid(cached):
            self._increment_metric('cache_hits')
            if self.config.enable_debug_logging:
                logger.debug(f"Returning cached token for {self.token_key}")
            return cached['access_token']
        
        self._increment_metric('cache_misses')
        
        # Token expired or missing - need to refresh
        return await self._refresh_token()
    
    async def invalidate(self) -> None:
        """
        Force invalidate cached token.
        Call this when you receive a 401/403 despite having a "valid" token.
        """
        logger.warning(f"Token invalidated for {self.token_key}")
        self.cache_backend.delete(self.token_key)
    
    async def get_headers(self) -> Dict[str, str]:
        """
        Convenience method to get Authorization headers.
        
        Returns:
            {"Authorization": "Bearer <token>"}
        """
        token = await self.get_token()
        return {"Authorization": f"Bearer {token}"}
    
    def _is_token_valid(self, token_record: TokenRecord) -> bool:
        """
        Check if token is still valid (accounting for refresh buffer).
        
        Returns True if: current_time < expires_at - buffer
        """
        now = time.time()
        expires_at = token_record['expires_at']
        buffer = self.config.refresh_buffer_seconds
        
        is_valid = now < (expires_at - buffer)
        
        if self.config.enable_debug_logging:
            remaining = expires_at - now
            logger.debug(
                f"Token validity check: {self.token_key} "
                f"(remaining={remaining:.0f}s, buffer={buffer}s, valid={is_valid})"
            )
        
        return is_valid
    
    async def _refresh_token(self) -> str:
        """
        Refresh token with singleflight pattern.
        
        Algorithm:
        1. Try to acquire refresh lock
        2. If acquired:
           - Fetch new token
           - Cache it
           - Release lock
        3. If not acquired:
           - Wait briefly
           - Check cache again (another worker may have refreshed)
        4. Return token
        """
        lock_key = f"lock:{self.token_key}"
        
        # Try to acquire lock
        lock_acquired = self.cache_backend.acquire_lock(
            lock_key,
            ttl_seconds=self.config.lock_timeout_seconds
        )
        
        if lock_acquired:
            self._increment_metric('lock_acquires')
            try:
                logger.info(f"Refreshing token: {self.token_key}")
                
                # Fetch new token
                start_time = time.time()
                token_data = await self._fetch_token()
                duration = time.time() - start_time
                
                # Build token record
                token_record = self._build_token_record(token_data)
                
                # Cache it
                cache_ttl = int(token_record['expires_at'] - time.time())
                self.cache_backend.set(self.token_key, token_record, ttl_seconds=cache_ttl)
                
                self._increment_metric('refreshes')
                logger.info(
                    f"Token refresh successful: {self.token_key} "
                    f"(duration={duration:.2f}s, ttl={cache_ttl}s)"
                )
                
                return token_record['access_token']
            
            except Exception as e:
                self._increment_metric('refresh_failures')
                logger.error(f"Token refresh failed for {self.token_key}: {e}", exc_info=True)
                raise
            
            finally:
                # Always release lock
                self.cache_backend.release_lock(lock_key)
        
        else:
            # Lock not acquired - another worker is refreshing
            self._increment_metric('lock_waits')
            logger.debug(f"Waiting for token refresh by another worker: {self.token_key}")
            
            # Wait for the other worker to finish
            token = await self._wait_for_refresh()
            
            if token:
                return token
            
            # Still no token - try refreshing ourselves
            logger.warning(
                f"No token after waiting for refresh: {self.token_key}. "
                f"Attempting refresh ourselves."
            )
            
            # Recursive call will try to acquire lock again
            return await self._refresh_token()
    
    async def _wait_for_refresh(self) -> Optional[str]:
        """
        Wait for another worker to complete token refresh.
        
        Polls cache for a short duration to see if token appears.
        """
        max_wait = self.config.max_lock_wait_seconds
        poll_interval_ms = self.config.retry_on_lock_wait_ms
        
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait:
            await asyncio.sleep(poll_interval_ms / 1000.0)
            
            # Check if token now in cache
            cached = self.cache_backend.get(self.token_key)
            if cached and self._is_token_valid(cached):
                logger.debug(f"Token found after waiting: {self.token_key}")
                return cached['access_token']
        
        logger.warning(f"Waited {max_wait}s for token refresh but none appeared: {self.token_key}")
        return None
    
    async def _fetch_token(self) -> Union[Dict[str, Any], TokenRecord]:
        """
        Call the token fetcher (may be sync or async).
        """
        if asyncio.iscoroutinefunction(self.token_fetcher):
            return await self.token_fetcher()
        else:
            # Run sync function in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.token_fetcher)
    
    def _build_token_record(self, token_data: Union[Dict[str, Any], TokenRecord]) -> TokenRecord:
        """
        Build standardized TokenRecord from fetcher response.
        
        Handles both OAuth2 response format and pre-built TokenRecord.
        """
        # If already a TokenRecord, use it
        if all(k in token_data for k in ['access_token', 'expires_at']):
            return TokenRecord(**token_data)
        
        # Otherwise, build from OAuth2 response format
        now = time.time()
        
        # Extract expires_in (seconds) or expires_at (timestamp)
        if 'expires_at' in token_data:
            expires_at = token_data['expires_at']
        elif 'expires_in' in token_data:
            expires_at = now + token_data['expires_in']
        else:
            # Default: 1 hour
            logger.warning(f"No expiry info in token response for {self.token_key}, defaulting to 3600s")
            expires_at = now + 3600
        
        return TokenRecord(
            access_token=token_data['access_token'],
            refresh_token=token_data.get('refresh_token'),
            expires_at=expires_at,
            token_type=token_data.get('token_type', 'Bearer'),
            metadata=token_data.get('metadata', {})
        )
    
    def _increment_metric(self, metric_name: str) -> None:
        """Increment metrics counter."""
        if self.config.enable_metrics:
            self._metrics[metric_name] = self._metrics.get(metric_name, 0) + 1
    
    def get_metrics(self) -> Dict[str, int]:
        """Get metrics snapshot."""
        return self._metrics.copy()


# ============================================================================
# Retry Helpers
# ============================================================================


async def retry_on_401(
    func: Callable,
    token_provider: TokenProvider,
    max_retries: int = 1,
    *args,
    **kwargs
) -> Any:
    """
    Wrapper that automatically retries on 401 by invalidating and refreshing token.
    
    Usage:
        ```python
        provider = TokenProvider(...)
        
        async def make_request(token):
            headers = {"Authorization": f"Bearer {token}"}
            response = await http_client.get(url, headers=headers)
            if response.status_code == 401:
                raise Unauthorized()
            return response
        
        # Automatically retries once on 401
        result = await retry_on_401(make_request, provider, await provider.get_token())
        ```
    
    Args:
        func: Async function to call
        token_provider: TokenProvider instance
        max_retries: Max retry attempts (default: 1)
        *args, **kwargs: Passed to func
    
    Returns:
        Result from func
    
    Raises:
        Last exception if all retries exhausted
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        
        except Exception as e:
            # Check if it's an auth error (customize this based on your exceptions)
            error_str = str(e).lower()
            is_auth_error = (
                '401' in error_str or
                'unauthorized' in error_str or
                'invalid_token' in error_str or
                'token expired' in error_str
            )
            
            if is_auth_error and attempt < max_retries:
                logger.warning(
                    f"Auth error on attempt {attempt + 1}, "
                    f"invalidating token and retrying: {e}"
                )
                await token_provider.invalidate()
                # Token will be refreshed on next get_token() call
                last_exception = e
            else:
                # Not an auth error, or out of retries
                raise
    
    # Should not reach here, but for safety
    raise last_exception or Exception("Retry exhausted")


# ============================================================================
# Example Usage (for testing this module standalone)
# ============================================================================


async def example_basic_usage():
    """Example demonstrating basic token provider pattern."""
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create cache backend
    cache_dir = "/tmp/fleet_q_token_cache_example"
    cache_backend = DiskCacheBackend(cache_dir)
    
    # Simulate token fetcher
    refresh_count = 0
    
    async def fetch_mock_token() -> Dict[str, Any]:
        nonlocal refresh_count
        refresh_count += 1
        
        logger.info(f"Fetching token (refresh #{refresh_count})...")
        await asyncio.sleep(0.5)  # Simulate API call
        
        return {
            'access_token': f'mock_token_{refresh_count}',
            'expires_in': 10,  # Very short for demo (10 seconds)
            'token_type': 'Bearer'
        }
    
    # Create provider
    config = TokenProviderConfig(
        refresh_buffer_seconds=3,  # Refresh when < 3s remaining
        enable_debug_logging=True
    )
    
    provider = TokenProvider(
        token_key="example:service",
        token_fetcher=fetch_mock_token,
        cache_backend=cache_backend,
        config=config
    )
    
    # Get token multiple times
    print("\n=== First request ===")
    token1 = await provider.get_token()
    print(f"Token: {token1}")
    
    print("\n=== Second request (should use cache) ===")
    token2 = await provider.get_token()
    print(f"Token: {token2}")
    assert token1 == token2, "Should be cached"
    
    print("\n=== Wait for expiry... ===")
    await asyncio.sleep(8)  # Wait 8 seconds (past refresh buffer)
    
    print("\n=== Third request (should refresh) ===")
    token3 = await provider.get_token()
    print(f"Token: {token3}")
    assert token3 != token1, "Should have refreshed"
    
    print("\n=== Metrics ===")
    print(provider.get_metrics())
    
    # Cleanup
    cache_backend.close()
    print("\n✅ Example completed successfully!")


async def example_multiprocessing():
    """
    Example demonstrating multiprocessing with shared token cache.
    
    Shows how multiple processes share tokens but have separate connections.
    """
    import multiprocessing
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [PID:%(process)d] - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Canonical cache path (same for all processes)
    CACHE_DIR = get_canonical_cache_path(
        env_var="EXAMPLE_CACHE_DIR",
        default="/tmp/fleet_q_multiprocess_example"
    )
    
    print(f"\n=== Multiprocessing Token Sharing Demo ===")
    print(f"Cache directory: {CACHE_DIR}")
    print(f"Number of workers: 4\n")
    
    # Shared token fetcher (simulated)
    refresh_count = multiprocessing.Value('i', 0)
    
    def fetch_token_sync():
        """Simulated token fetcher (runs in worker process)."""
        with refresh_count.get_lock():
            refresh_count.value += 1
            count = refresh_count.value
        
        logger.info(f"Fetching token (refresh #{count})...")
        time.sleep(0.5)  # Simulate API call
        
        return {
            'access_token': f'token_{count}_{os.getpid()}',
            'expires_in': 60,  # 1 minute
            'token_type': 'Bearer'
        }
    
    def worker_task(worker_id: int):
        """
        Worker task - each process creates own provider.
        
        Critical: All workers use SAME cache directory!
        """
        try:
            # Each process creates its own cache backend
            # BUT points to the SAME directory
            cache = DiskCacheBackend(CACHE_DIR)
            
            # Each process creates its own provider
            provider = TokenProvider(
                token_key="shared:multiprocess:example",
                token_fetcher=fetch_token_sync,
                cache_backend=cache,
                config=TokenProviderConfig(
                    refresh_buffer_seconds=30,
                    enable_debug_logging=False
                )
            )
            
            logger.info(f"Worker {worker_id} starting (PID: {os.getpid()})")
            
            # All workers try to get token simultaneously
            token = asyncio.run(provider.get_token())
            
            logger.info(
                f"Worker {worker_id} got token: {token[:20]}... "
                f"(metrics: {provider.get_metrics()})"
            )
            
            # Simulate using token with own HTTP client
            # (In real code: each worker would have its own httpx.AsyncClient)
            logger.info(f"Worker {worker_id} making API call with token...")
            time.sleep(0.1)  # Simulate request
            
            cache.close()
            return worker_id
            
        except Exception as e:
            logger.error(f"Worker {worker_id} failed: {e}", exc_info=True)
            raise
    
    # Spawn multiple processes
    print("Spawning 4 workers...\n")
    
    with multiprocessing.Pool(4) as pool:
        results = pool.map(worker_task, range(4))
    
    print(f"\n✅ All workers completed: {results}")
    print(f"\n📊 Total token refreshes: {refresh_count.value}")
    print(f"Expected: 1 (only ONE process should refresh)\n")
    
    # Cleanup
    import shutil
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
        print(f"Cleaned up cache directory: {CACHE_DIR}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--multiprocess":
        # Run multiprocessing example
        asyncio.run(example_multiprocessing())
    else:
        # Run basic example
        print("\nRunning basic example...")
        print("(Use --multiprocess flag for multiprocessing demo)\n")
        asyncio.run(example_basic_usage())
