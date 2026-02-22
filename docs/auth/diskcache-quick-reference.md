# DiskCache Token Management - Quick Reference

## Installation

```bash
pip install diskcache
```

For specific integrations:
```bash
# SharePoint
pip install msal

# Snowflake key-pair auth
pip install PyJWT cryptography

# HTTP requests
pip install httpx
```

---

## Basic Usage (3 Steps)

### 1. Create Cache Backend

```python
from fleet_q.diskcache_utils import DiskCacheBackend

# For production (multi-worker safe)
cache_backend = DiskCacheBackend("/var/lib/app/token_cache")

# For development/testing
from fleet_q.diskcache_utils import InMemoryCacheBackend
cache_backend = InMemoryCacheBackend()
```

### 2. Create Token Provider

```python
from fleet_q.diskcache_utils import TokenProvider

# Define your token fetcher
async def fetch_my_token():
    # Your actual token fetch logic
    return {
        "access_token": "eyJ...",
        "expires_in": 3600  # seconds
    }

# Create provider
provider = TokenProvider(
    token_key="my_service:env:scope",
    token_fetcher=fetch_my_token,
    cache_backend=cache_backend,
    config=TokenProviderConfig(refresh_buffer_seconds=120)
)
```

### 3. Use Token in Your Code

```python
# Get token (automatically refreshes if needed)
token = await provider.get_token()

# Or get as headers
headers = await provider.get_headers()
# Returns: {"Authorization": "Bearer eyJ..."}

# Manual invalidation (on 401)
await provider.invalidate()
```

---

## Complete Example: SharePoint Writer

```python
from fleet_q.diskcache_utils import (
    DiskCacheBackend,
    TokenProvider,
    retry_on_401
)
import httpx

# 1. Setup cache
cache = DiskCacheBackend("/var/lib/app/cache")

# 2. Token fetcher
async def fetch_sharepoint_token():
    # Using MSAL
    import msal
    app = msal.ConfidentialClientApplication(
        client_id=SHAREPOINT_CLIENT_ID,
        client_credential=SHAREPOINT_CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )
    result = app.acquire_token_for_client(scopes=[SCOPE])
    return result

# 3. Token provider
provider = TokenProvider(
    token_key=f"sharepoint:{TENANT_ID}",
    token_fetcher=fetch_sharepoint_token,
    cache_backend=cache
)

# 4. Writer class
class SharePointWriter:
    def __init__(self, provider):
        self.provider = provider
    
    async def write_file(self, url: str, content: bytes):
        """Write with automatic retry on 401"""
        
        async def _write(token: str):
            headers = {"Authorization": f"Bearer {token}"}
            async with httpx.AsyncClient() as client:
                response = await client.put(url, content=content, headers=headers)
                if response.status_code == 401:
                    raise Exception("401 Unauthorized")
                response.raise_for_status()
                return response.json()
        
        # Automatic retry on 401
        token = await self.provider.get_token()
        return await retry_on_401(_write, self.provider, token)

# 5. Use it
writer = SharePointWriter(provider)
await writer.write_file("https://...", b"data")
```

---

## Key Patterns

### Pattern 1: Lazy Refresh (Automatic)

```python
# First call: fetches token
token1 = await provider.get_token()

# Subsequent calls: uses cache (until near expiry)
token2 = await provider.get_token()  # From cache
token3 = await provider.get_token()  # From cache

# After expiry - buffer: automatically refreshes
token4 = await provider.get_token()  # Auto-refresh
```

### Pattern 2: Retry on 401

```python
try:
    token = await provider.get_token()
    response = await make_api_call(token)
except Unauthorized:
    # Invalidate and retry once
    await provider.invalidate()
    token = await provider.get_token()
    response = await make_api_call(token)
```

### Pattern 3: Multi-Provider (Different Services)

```python
# Shared cache, different token keys
cache = DiskCacheBackend("/var/lib/app/cache")

sharepoint_provider = TokenProvider(
    token_key="sharepoint:tenant_x",
    token_fetcher=fetch_sharepoint_token,
    cache_backend=cache
)

snowflake_provider = TokenProvider(
    token_key="snowflake:account_y",
    token_fetcher=fetch_snowflake_token,
    cache_backend=cache
)

# Both use same cache, different keys
sp_token = await sharepoint_provider.get_token()
sf_token = await snowflake_provider.get_token()
```

---

## Configuration Options

```python
from fleet_q.diskcache_utils import TokenProviderConfig

config = TokenProviderConfig(
    refresh_buffer_seconds=120,      # Refresh when < 2 min remaining
    lock_timeout_seconds=30,         # Max time to hold refresh lock
    retry_on_lock_wait_ms=100,       # Poll interval when waiting
    max_lock_wait_seconds=10,        # Max wait for another refresher
    enable_metrics=True,             # Track cache hits/misses
    enable_debug_logging=False       # Detailed logs
)

provider = TokenProvider(..., config=config)
```

---

## Multiprocessing & Multi-Worker Deployment

### 🎯 Core Pattern: Shared Tokens, Separate Connections

**Critical Principle:**
- ✅ Tokens: SHARED via DiskCache (same directory)
- ✅ Connections: PER-PROCESS (each has own HTTP client)
- ✅ Refresh: Only ONE process refreshes at a time
- ✅ Result: All processes reuse valid tokens

### ✅ What Works (Cross-Process Token Sharing)

All these scenarios work perfectly with DiskCache:

| Scenario | Works? | Notes |
|---|---|---|
| Gunicorn/Uvicorn workers | ✅ Yes | Same host, shared filesystem |
| `multiprocessing.Pool` | ✅ Yes | Processes share cache directory |
| `aiomultiprocess` | ✅ Yes | Async multiprocessing |
| Multiple threads | ✅ Yes | Same process, shared cache |
| Celery workers | ✅ Yes | If on same host or shared volume |

### 🔑 Critical Requirements

**1. Same Absolute Cache Path**

```python
# ✅ GOOD - canonical path from environment
from fleet_q.diskcache_utils import get_canonical_cache_path

CACHE_PATH = get_canonical_cache_path()  # Same for all processes
cache = DiskCacheBackend(CACHE_PATH)
```

```python
# ❌ BAD - different path per process
cache = DiskCacheBackend(f"/tmp/cache_{os.getpid()}")  # Each process different!
```

**2. Same Filesystem**

- ✅ Single VM/container: Works automatically
- ✅ Shared volume (EFS/NFS): Works if mounted at same path
- ❌ Separate containers without shared volume: Each has own cache

### Multiprocessing Example

```python
import multiprocessing
from fleet_q.diskcache_utils import (
    DiskCacheBackend,
    TokenProvider,
    get_canonical_cache_path
)

# Shared configuration (before spawning processes)
CACHE_DIR = get_canonical_cache_path()

def worker_task(worker_id: int):
    """Each worker creates own provider, shares cache."""
    
    # 1. Create cache backend (same path for all!)
    cache = DiskCacheBackend(CACHE_DIR)
    
    # 2. Create provider
    provider = TokenProvider(
        token_key="service:shared",
        token_fetcher=fetch_token,
        cache_backend=cache
    )
    
    # 3. Get token (shared across all workers)
    token = asyncio.run(provider.get_token())
    
    # 4. Create own HTTP client (NOT shared)
    client = httpx.Client()
    
    # 5. Make request
    response = client.get(
        url,
        headers={"Authorization": f"Bearer {token}"}
    )
    
    client.close()
    cache.close()

# Spawn processes
with multiprocessing.Pool(4) as pool:
    pool.map(worker_task, range(4))
    # Expected: Only 1 token refresh total!
```

### Same Host (Gunicorn/Uvicorn workers)

✅ **Works perfectly with DiskCache**

```python
# config.py or app startup
from fleet_q.diskcache_utils import get_canonical_cache_path

CACHE_PATH = get_canonical_cache_path()

# In each worker
cache = DiskCacheBackend(CACHE_PATH)
provider = TokenProvider(..., cache_backend=cache)
```

**How it works:**
- Worker A refreshes token → writes to disk
- Worker B checks cache → finds valid token
- Worker C checks cache → finds valid token
- Built-in locking prevents refresh storms

**Verification:**
```bash
# Check that all workers use same cache
ls -la /var/lib/app/token_cache/
# Should see cache.db with recent timestamp
```

### Multiple Pods (Kubernetes)

**Option 1: Shared Volume (EFS/NFS)**

```yaml
# kubernetes volume
volumes:
  - name: token-cache
    persistentVolumeClaim:
      claimName: shared-token-cache-efs

volumeMounts:
  - name: token-cache
    mountPath: /var/lib/app/cache
```

```python
cache = DiskCacheBackend("/var/lib/app/cache")
```

**Option 2: Per-Pod Cache (acceptable)**

```python
# Each pod has its own cache
cache = DiskCacheBackend("/tmp/token_cache")
```

**Trade-off:**
- ✅ Still safe (retry-on-401 prevents stale tokens)
- ⚠️ Each pod refreshes independently (slightly more API calls)

---

## Observability

### Metrics

```python
metrics = provider.get_metrics()
print(metrics)
# {
#   'cache_hits': 150,
#   'cache_misses': 3,
#   'refreshes': 3,
#   'refresh_failures': 0,
#   'lock_acquires': 3,
#   'lock_waits': 12
# }
```

### Logging

```python
import logging

# Enable debug logging
logging.getLogger("fleet_q.diskcache_utils").setLevel(logging.DEBUG)

# Or via config
config = TokenProviderConfig(enable_debug_logging=True)
```

**Log events:**
- `token_refresh_started`
- `token_refresh_succeeded`
- `token_refresh_failed`
- `token_invalidated`
- `cache_hit` / `cache_miss`
- `lock_acquired` / `lock_denied`

---

## Common Pitfalls & Solutions

### ❌ Pitfall 1: Storing Token in Writer Init

```python
# DON'T DO THIS
class BadWriter:
    def __init__(self, provider):
        self.token = await provider.get_token()  # ❌ Stale
    
    async def write(self, data):
        headers = {"Authorization": f"Bearer {self.token}"}  # ❌ Never refreshes
```

### ✅ Solution: Get Token Per Request

```python
class GoodWriter:
    def __init__(self, provider):
        self.provider = provider
    
    async def write(self, data):
        token = await self.provider.get_token()  # ✅ Always fresh
        headers = {"Authorization": f"Bearer {token}"}
```

---

### ❌ Pitfall 2: No 401 Handling

```python
# Risky - edge expiry can still happen
token = await provider.get_token()
response = await make_call(token)  # ❌ If 401, fails immediately
```

### ✅ Solution: Always Retry Once on 401

```python
# Safe - handles edge cases
try:
    token = await provider.get_token()
    response = await make_call(token)
except Unauthorized:
    await provider.invalidate()
    token = await provider.get_token()
    response = await make_call(token)  # ✅ Retry once
```

---

### ❌ Pitfall 3: APScheduler for Token Refresh

```python
# DON'T DO THIS
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

@scheduler.scheduled_job('interval', minutes=50)
async def refresh_tokens():
    token = await fetch_token()  # ❌ Not cross-worker safe
    GLOBAL_TOKEN = token  # ❌ Only in this worker's memory
```

### ✅ Solution: Use TokenProvider (Lazy Refresh)

```python
# TokenProvider handles refresh automatically
provider = TokenProvider(...)  # ✅ Cross-worker safe
token = await provider.get_token()  # ✅ Always valid
```

---

## Testing

### Unit Test

```python
import pytest
from fleet_q.diskcache_utils import TokenProvider, InMemoryCacheBackend

@pytest.mark.asyncio
async def test_token_caching():
    refresh_count = 0
    
    async def fetch_token():
        nonlocal refresh_count
        refresh_count += 1
        return {"access_token": f"token_{refresh_count}", "expires_in": 3600}
    
    cache = InMemoryCacheBackend()
    provider = TokenProvider("test", fetch_token, cache)
    
    # First call: fetches
    token1 = await provider.get_token()
    assert refresh_count == 1
    
    # Second call: cached
    token2 = await provider.get_token()
    assert refresh_count == 1  # Still 1, not 2
    assert token1 == token2
```

### Integration Test (Multi-Process)

```python
import asyncio
import multiprocessing
from fleet_q.diskcache_utils import TokenProvider, DiskCacheBackend

async def worker_task(worker_id: int):
    cache = DiskCacheBackend("/tmp/test_cache")
    provider = TokenProvider("shared_key", fetch_token, cache)
    
    # All workers try to get token simultaneously
    token = await provider.get_token()
    print(f"Worker {worker_id}: {token}")

# Run multiple processes
processes = [
    multiprocessing.Process(target=lambda i=i: asyncio.run(worker_task(i)))
    for i in range(4)
]

for p in processes:
    p.start()
for p in processes:
    p.join()

# Only ONE refresh should have occurred (singleflight)
```

---

## Comparison: DiskCache vs Alternatives

| Feature | DiskCache | Redis | In-Memory | SQLite (custom) |
|---|---|---|---|---|
| Cross-process safe | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |
| External dependency | ❌ No | ✅ Yes | ❌ No | ❌ No |
| Setup complexity | Low | Medium | Low | Medium |
| Performance | Good | Excellent | Excellent | Good |
| Multi-pod (K8s) | ⚠️ Needs EFS | ✅ Native | ❌ No | ⚠️ Needs EFS |
| Built-in locking | ✅ Yes | ✅ Yes | ⚠️ Manual | ⚠️ Manual |
| **Recommendation** | ✅ **Best for most** | Multi-pod only | Dev only | Audit needs |

---

## Production Checklist

- [ ] Use `DiskCacheBackend` (not `InMemoryCacheBackend`)
- [ ] Set `refresh_buffer_seconds` appropriately (default: 120s)
- [ ] Implement retry-on-401 pattern in all writers
- [ ] Never store tokens in init/global variables
- [ ] Configure cache directory with proper permissions
- [ ] Add metrics collection for observability
- [ ] Set up alerts for `refresh_failures` > 0
- [ ] Test multi-worker behavior (Gunicorn/Uvicorn)
- [ ] Document token key naming convention
- [ ] Encrypt cache directory if storing sensitive tokens

---

## Quick Reference Card

```python
# 1. Setup
from fleet_q.diskcache_utils import DiskCacheBackend, TokenProvider

cache = DiskCacheBackend("/var/lib/app/cache")

# 2. Define fetcher
async def fetch_token():
    return {"access_token": "...", "expires_in": 3600}

# 3. Create provider
provider = TokenProvider("service:key", fetch_token, cache)

# 4. Use it
token = await provider.get_token()  # Always fresh
headers = await provider.get_headers()  # {"Authorization": "Bearer ..."}

# 5. Handle 401
try:
    await api_call(token)
except Unauthorized401:
    await provider.invalidate()
    token = await provider.get_token()
    await api_call(token)  # Retry once
```

---

## Need Help?

- **Examples:** See `examples/token_provider_example.py`
- **Source:** `fleet_q/diskcache_utils.py`
- **Architecture:** `docs/ideation/Token-Management.md`
