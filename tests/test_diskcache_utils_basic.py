"""
Quick test to verify diskcache_utils implementation.

Run this to ensure everything works:
    python tests/test_diskcache_utils_basic.py
"""

import asyncio
import logging
import sys
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fleet_q.diskcache_utils import (
    DiskCacheBackend,
    InMemoryCacheBackend,
    TokenProvider,
    TokenProviderConfig,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_in_memory_cache():
    """Test basic functionality with in-memory cache."""
    logger.info("=== Test 1: In-Memory Cache ===\n")
    
    refresh_count = 0
    
    async def fetch_token():
        nonlocal refresh_count
        refresh_count += 1
        logger.info(f"Fetching token (call #{refresh_count})...")
        await asyncio.sleep(0.1)
        return {
            'access_token': f'token_{refresh_count}',
            'expires_in': 5,  # 5 seconds for quick test
        }
    
    cache = InMemoryCacheBackend()
    config = TokenProviderConfig(
        refresh_buffer_seconds=2,  # Refresh when < 2s remaining
        enable_debug_logging=False
    )
    
    provider = TokenProvider(
        token_key="test:service",
        token_fetcher=fetch_token,
        cache_backend=cache,
        config=config
    )
    
    # First call - should fetch
    logger.info("Call 1: Should fetch token...")
    token1 = await provider.get_token()
    assert refresh_count == 1, f"Expected 1 refresh, got {refresh_count}"
    logger.info(f"✅ Token: {token1}")
    
    # Second call - should use cache
    logger.info("\nCall 2: Should use cached token...")
    token2 = await provider.get_token()
    assert refresh_count == 1, f"Expected 1 refresh, got {refresh_count}"
    assert token1 == token2, "Tokens should match"
    logger.info(f"✅ Token: {token2} (from cache)")
    
    # Wait for expiry
    logger.info("\nWaiting 4 seconds (past refresh buffer)...")
    await asyncio.sleep(4)
    
    # Third call - should refresh
    logger.info("\nCall 3: Should refresh token...")
    token3 = await provider.get_token()
    assert refresh_count == 2, f"Expected 2 refreshes, got {refresh_count}"
    assert token3 != token1, "Token should be different (refreshed)"
    logger.info(f"✅ Token: {token3} (refreshed)")
    
    # Test invalidation
    logger.info("\nManual invalidation...")
    await provider.invalidate()
    
    logger.info("Call 4: Should refresh after invalidation...")
    token4 = await provider.get_token()
    assert refresh_count == 3, f"Expected 3 refreshes, got {refresh_count}"
    logger.info(f"✅ Token: {token4} (refreshed after invalidation)")
    
    # Check metrics
    metrics = provider.get_metrics()
    logger.info(f"\n📊 Metrics: {metrics}")
    
    logger.info("\n✅ In-Memory Cache Test PASSED\n")
    return True


async def test_diskcache():
    """Test DiskCache backend (requires diskcache installed)."""
    logger.info("=== Test 2: DiskCache Backend ===\n")
    
    try:
        import diskcache
    except ImportError:
        logger.warning("⚠️  diskcache not installed, skipping DiskCache test")
        logger.info("Install with: pip install diskcache\n")
        return True
    
    # Create temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        logger.info(f"Using temp cache dir: {tmpdir}")
        
        refresh_count = 0
        
        async def fetch_token():
            nonlocal refresh_count
            refresh_count += 1
            logger.info(f"Fetching token (call #{refresh_count})...")
            await asyncio.sleep(0.1)
            return {
                'access_token': f'disk_token_{refresh_count}',
                'expires_in': 5,
            }
        
        cache = DiskCacheBackend(tmpdir)
        config = TokenProviderConfig(refresh_buffer_seconds=2)
        
        provider = TokenProvider(
            token_key="test:diskcache",
            token_fetcher=fetch_token,
            cache_backend=cache,
            config=config
        )
        
        # First call
        logger.info("Call 1: Should fetch token...")
        token1 = await provider.get_token()
        assert refresh_count == 1
        logger.info(f"✅ Token: {token1}")
        
        # Second call (cached)
        logger.info("\nCall 2: Should use cached token...")
        token2 = await provider.get_token()
        assert refresh_count == 1
        assert token1 == token2
        logger.info(f"✅ Token: {token2} (from cache)")
        
        # Test cross-"process" (simulate another provider instance)
        logger.info("\nCreating second provider (simulating another worker)...")
        provider2 = TokenProvider(
            token_key="test:diskcache",  # Same key!
            token_fetcher=fetch_token,
            cache_backend=DiskCacheBackend(tmpdir),  # Same cache dir!
            config=config
        )
        
        logger.info("Call 3: Second provider should use same cached token...")
        token3 = await provider2.get_token()
        assert refresh_count == 1, "Should still be 1 (no new refresh)"
        assert token3 == token1, "Should be same token (from shared cache)"
        logger.info(f"✅ Token: {token3} (from shared cache)")
        
        # Cleanup
        cache.close()
        
        logger.info("\n✅ DiskCache Test PASSED\n")
        return True


async def test_concurrent_requests():
    """Test singleflight pattern with concurrent requests."""
    logger.info("=== Test 3: Concurrent Requests (Singleflight) ===\n")
    
    refresh_count = 0
    
    async def fetch_token():
        nonlocal refresh_count
        refresh_count += 1
        logger.info(f"Fetching token (call #{refresh_count})...")
        await asyncio.sleep(0.5)  # Simulate slow API call
        return {
            'access_token': f'concurrent_token_{refresh_count}',
            'expires_in': 60,
        }
    
    cache = InMemoryCacheBackend()
    provider = TokenProvider(
        token_key="test:concurrent",
        token_fetcher=fetch_token,
        cache_backend=cache
    )
    
    # Simulate 10 concurrent requests
    logger.info("Launching 10 concurrent token requests...")
    tokens = await asyncio.gather(*[
        provider.get_token() for _ in range(10)
    ])
    
    # All should be the same token
    assert len(set(tokens)) == 1, "All tokens should be identical"
    # Should only refresh once (singleflight)
    assert refresh_count == 1, f"Expected 1 refresh, got {refresh_count}"
    
    logger.info(f"✅ All 10 requests got same token: {tokens[0]}")
    logger.info(f"✅ Only {refresh_count} API call made (singleflight working)")
    
    logger.info("\n✅ Concurrent Requests Test PASSED\n")
    return True


async def test_headers_helper():
    """Test the get_headers() convenience method."""
    logger.info("=== Test 4: Headers Helper ===\n")
    
    async def fetch_token():
        return {'access_token': 'test_token_123', 'expires_in': 3600}
    
    cache = InMemoryCacheBackend()
    provider = TokenProvider(
        token_key="test:headers",
        token_fetcher=fetch_token,
        cache_backend=cache
    )
    
    headers = await provider.get_headers()
    
    assert 'Authorization' in headers, "Should have Authorization key"
    assert headers['Authorization'] == 'Bearer test_token_123'
    
    logger.info(f"✅ Headers: {headers}")
    logger.info("\n✅ Headers Helper Test PASSED\n")
    return True


async def run_all_tests():
    """Run all tests."""
    logger.info("\n" + "="*70)
    logger.info("FLEET-Q DiskCache Utils - Test Suite")
    logger.info("="*70 + "\n")
    
    tests = [
        ("In-Memory Cache", test_in_memory_cache),
        ("DiskCache Backend", test_diskcache),
        ("Concurrent Requests", test_concurrent_requests),
        ("Headers Helper", test_headers_helper),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = await test_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"❌ {name} FAILED: {e}", exc_info=True)
            results.append((name, False))
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("TEST SUMMARY")
    logger.info("="*70)
    
    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{status}: {name}")
    
    all_passed = all(result for _, result in results)
    
    if all_passed:
        logger.info("\n🎉 ALL TESTS PASSED! 🎉\n")
        return 0
    else:
        logger.error("\n❌ SOME TESTS FAILED\n")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(run_all_tests())
    sys.exit(exit_code)
