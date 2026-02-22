"""
Multiprocessing Example: Token Sharing Across Processes

This demonstrates how multiple processes share tokens via DiskCache
while maintaining their own HTTP connections.

Key Pattern:
- ✅ Tokens: Shared via DiskCache (same directory)
- ✅ Connections: Per-process (each has own HTTP client)
- ✅ Refresh: Only ONE process refreshes when token expires
- ✅ Result: All processes reuse the same valid token

Run this example:
    python examples/multiprocessing_token_example.py
"""

import asyncio
import logging
import multiprocessing
import os
import time
from pathlib import Path

# Setup path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from fleet_q.diskcache_utils import (
    DiskCacheBackend,
    TokenProvider,
    TokenProviderConfig,
    get_canonical_cache_path,
)

# Configure logging to show process IDs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PID:%(process)5d] %(name)-20s %(levelname)-8s %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Shared Configuration
# ============================================================================

# CRITICAL: All processes must use the SAME cache directory
CACHE_DIR = get_canonical_cache_path(
    env_var="TOKEN_CACHE_DIR",
    default="/tmp/multiprocess_token_demo"
)

# Simulate a shared counter for tracking refreshes
# (In production, this would be the actual auth API call count)
REFRESH_COUNTER = multiprocessing.Value('i', 0)


# ============================================================================
# Token Fetcher (Simulates OAuth2 API)
# ============================================================================

def fetch_demo_token() -> dict:
    """
    Simulated token fetcher (would be real OAuth2 in production).
    
    This demonstrates that only ONE process calls this function
    even when multiple processes request tokens simultaneously.
    """
    with REFRESH_COUNTER.get_lock():
        REFRESH_COUNTER.value += 1
        count = REFRESH_COUNTER.value
    
    pid = os.getpid()
    logger.warning(f"🔐 FETCHING TOKEN (refresh #{count}) - PID {pid}")
    
    # Simulate network latency
    time.sleep(0.5)
    
    token = f"token_v{count}_pid{pid}_{int(time.time())}"
    
    logger.info(f"✅ Token fetched: {token}")
    
    return {
        'access_token': token,
        'expires_in': 30,  # Short TTL for demo (30 seconds)
        'token_type': 'Bearer'
    }


# ============================================================================
# Worker Process
# ============================================================================

def worker_process(worker_id: int, num_requests: int = 3):
    """
    Worker process that makes multiple API calls.
    
    Each worker:
    1. Creates its own TokenProvider (pointing to shared cache)
    2. Creates its own HTTP client (not shared)
    3. Makes requests using fresh tokens
    4. Shares tokens with other workers via DiskCache
    
    Args:
        worker_id: Worker identifier
        num_requests: Number of API calls to make
    """
    logger.info(f"Worker {worker_id} starting...")
    
    try:
        # Each process creates its own cache backend
        # CRITICAL: All point to the SAME directory!
        cache = DiskCacheBackend(
            CACHE_DIR,
            validate_path=True  # Validates path is multiprocessing-safe
        )
        
        # Each process creates its own provider
        provider = TokenProvider(
            token_key="demo:shared:service",
            token_fetcher=fetch_demo_token,
            cache_backend=cache,
            config=TokenProviderConfig(
                refresh_buffer_seconds=10,  # Refresh when < 10s remaining
                enable_metrics=True
            )
        )
        
        # Simulate making multiple API calls
        for i in range(num_requests):
            logger.info(f"Worker {worker_id} - Request {i+1}/{num_requests}")
            
            # Get token (may be from cache or freshly refreshed)
            token = asyncio.run(provider.get_token())
            
            # In production, you'd use the token here
            # Each worker would have its own HTTP client
            logger.info(
                f"Worker {worker_id} - Got token: {token[:30]}..."
            )
            
            # Simulate API call with own HTTP client
            # (In real code: async with httpx.AsyncClient() as client: ...)
            time.sleep(0.2)  # Simulate request latency
        
        # Report metrics
        metrics = provider.get_metrics()
        logger.info(
            f"Worker {worker_id} complete - "
            f"Cache hits: {metrics['cache_hits']}, "
            f"Refreshes: {metrics['refreshes']}, "
            f"Lock waits: {metrics['lock_waits']}"
        )
        
        cache.close()
        return {
            'worker_id': worker_id,
            'metrics': metrics,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"Worker {worker_id} failed: {e}", exc_info=True)
        return {
            'worker_id': worker_id,
            'error': str(e),
            'success': False
        }


# ============================================================================
# Demo Scenarios
# ============================================================================

def demo_concurrent_startup():
    """
    Demo: Multiple workers start simultaneously and request tokens.
    
    Expected behavior:
    - Only 1 token refresh occurs
    - All workers get the same token
    - No "refresh storm"
    """
    print("\n" + "="*70)
    print("DEMO 1: Concurrent Startup (Refresh Storm Prevention)")
    print("="*70)
    print("Scenario: 4 workers start simultaneously, all need token")
    print("Expected: Only 1 refresh (not 4)")
    print()
    
    REFRESH_COUNTER.value = 0
    
    # Spawn 4 workers that all start at once
    with multiprocessing.Pool(4) as pool:
        results = pool.starmap(worker_process, [(i, 1) for i in range(4)])
    
    print("\n📊 Results:")
    print(f"   Total token refreshes: {REFRESH_COUNTER.value}")
    print(f"   Expected: 1")
    print(f"   ✅ Refresh storm prevented!" if REFRESH_COUNTER.value == 1 else "❌ Multiple refreshes!")
    
    for result in results:
        print(f"   Worker {result['worker_id']}: {result['metrics']}")
    
    return results


def demo_sustained_load():
    """
    Demo: Workers make multiple requests over time.
    
    Expected behavior:
    - Token refreshed when near expiry
    - All workers share refreshed token
    - High cache hit ratio
    """
    print("\n" + "="*70)
    print("DEMO 2: Sustained Load (Cache Efficiency)")
    print("="*70)
    print("Scenario: 4 workers making 3 requests each")
    print("Expected: High cache hit ratio")
    print()
    
    REFRESH_COUNTER.value = 0
    
    # Each worker makes 3 requests
    with multiprocessing.Pool(4) as pool:
        results = pool.starmap(worker_process, [(i, 3) for i in range(4)])
    
    print("\n📊 Results:")
    print(f"   Total token refreshes: {REFRESH_COUNTER.value}")
    
    total_requests = sum(r['metrics']['cache_hits'] + r['metrics']['refreshes'] for r in results)
    total_hits = sum(r['metrics']['cache_hits'] for r in results)
    hit_ratio = (total_hits / total_requests * 100) if total_requests > 0 else 0
    
    print(f"   Total requests: {total_requests}")
    print(f"   Cache hits: {total_hits}")
    print(f"   Cache hit ratio: {hit_ratio:.1f}%")
    
    for result in results:
        m = result['metrics']
        print(
            f"   Worker {result['worker_id']}: "
            f"hits={m['cache_hits']}, refreshes={m['refreshes']}, waits={m['lock_waits']}"
        )
    
    return results


def demo_token_expiry():
    """
    Demo: Token expires mid-execution.
    
    Expected behavior:
    - First batch: All use same token
    - Wait for expiry
    - Second batch: Token refreshed once, all workers get new token
    """
    print("\n" + "="*70)
    print("DEMO 3: Token Expiry & Refresh")
    print("="*70)
    print("Scenario: Workers run, wait for expiry, run again")
    print("Expected: 1 refresh before expiry, 1 refresh after")
    print()
    
    REFRESH_COUNTER.value = 0
    
    # First batch
    print("🔵 Batch 1: Initial requests")
    with multiprocessing.Pool(2) as pool:
        results1 = pool.starmap(worker_process, [(i, 1) for i in range(2)])
    
    print(f"\n   Token refreshes after batch 1: {REFRESH_COUNTER.value}")
    
    # Wait for token to expire (TTL=30s, buffer=10s, so ~21s to expiry)
    wait_time = 22
    print(f"\n⏳ Waiting {wait_time}s for token to expire...")
    time.sleep(wait_time)
    
    # Second batch
    print("\n🔵 Batch 2: After expiry")
    with multiprocessing.Pool(2) as pool:
        results2 = pool.starmap(worker_process, [(i+2, 1) for i in range(2)])
    
    print(f"\n   Token refreshes after batch 2: {REFRESH_COUNTER.value}")
    print(f"   Expected: 2 (one per batch)")
    
    return results1, results2


# ============================================================================
# Main Demo Runner
# ============================================================================

def cleanup_cache():
    """Clean up demo cache directory."""
    import shutil
    if Path(CACHE_DIR).exists():
        shutil.rmtree(CACHE_DIR)
        logger.info(f"Cleaned up cache: {CACHE_DIR}")


def run_all_demos():
    """Run all demonstration scenarios."""
    print("\n" + "="*70)
    print("MULTIPROCESSING TOKEN SHARING DEMONSTRATION")
    print("="*70)
    print(f"Cache Directory: {CACHE_DIR}")
    print(f"Python Version: {sys.version}")
    print(f"Process ID: {os.getpid()}")
    print()
    
    try:
        # Run demos
        demo_concurrent_startup()
        
        # Clean cache between demos
        cleanup_cache()
        
        demo_sustained_load()
        
        # Clean cache between demos
        cleanup_cache()
        
        demo_token_expiry()
        
        # Final summary
        print("\n" + "="*70)
        print("✅ ALL DEMOS COMPLETED SUCCESSFULLY")
        print("="*70)
        print()
        print("Key Takeaways:")
        print("1. ✅ Tokens are shared across processes via DiskCache")
        print("2. ✅ Only ONE process refreshes when token expires")
        print("3. ✅ All processes reuse the same valid token")
        print("4. ✅ Each process has its own connections/clients")
        print("5. ✅ High cache hit ratio (> 80% typically)")
        print()
        
    finally:
        # Cleanup
        cleanup_cache()


if __name__ == "__main__":
    # Ensure we're running as main process
    multiprocessing.set_start_method('spawn', force=True)
    
    run_all_demos()
