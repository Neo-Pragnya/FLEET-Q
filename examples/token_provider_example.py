"""
Real-world examples of using TokenProvider with different integrations.

This demonstrates:
- SharePoint (MSAL)
- Snowflake (key-pair JWT)
- Generic OAuth2 client credentials
- Retry-on-401 pattern
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from fleet_q.diskcache_utils import (
    DiskCacheBackend,
    InMemoryCacheBackend,
    TokenProvider,
    TokenProviderConfig,
    retry_on_401,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Example 1: SharePoint Token Provider (using MSAL)
# ============================================================================

class SharePointTokenProvider:
    """
    SharePoint token provider using MSAL (Microsoft Authentication Library).
    
    Requires:
        pip install msal
    
    Environment variables:
        SHAREPOINT_TENANT_ID
        SHAREPOINT_CLIENT_ID
        SHAREPOINT_CLIENT_SECRET
        SHAREPOINT_SCOPE (e.g., "https://graph.microsoft.com/.default")
    """
    
    def __init__(self, cache_backend):
        try:
            import msal
            self.msal = msal
        except ImportError:
            raise ImportError("MSAL not installed. Install with: pip install msal")
        
        self.tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
        self.client_id = os.getenv("SHAREPOINT_CLIENT_ID")
        self.client_secret = os.getenv("SHAREPOINT_CLIENT_SECRET")
        self.scope = os.getenv("SHAREPOINT_SCOPE", "https://graph.microsoft.com/.default")
        
        if not all([self.tenant_id, self.client_id, self.client_secret]):
            raise ValueError("Missing SharePoint credentials in environment")
        
        # MSAL confidential client
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.msal_app = self.msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority
        )
        
        # Token provider
        self.provider = TokenProvider(
            token_key=f"sharepoint:{self.tenant_id}:{self.scope}",
            token_fetcher=self._fetch_sharepoint_token,
            cache_backend=cache_backend,
            config=TokenProviderConfig(refresh_buffer_seconds=300)  # 5 min buffer
        )
    
    def _fetch_sharepoint_token(self) -> dict:
        """Fetch token from MSAL."""
        logger.info("Fetching SharePoint token via MSAL...")
        
        result = self.msal_app.acquire_token_for_client(scopes=[self.scope])
        
        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "Unknown error"))
            raise Exception(f"MSAL token acquisition failed: {error}")
        
        # Convert MSAL response to standard format
        return {
            'access_token': result['access_token'],
            'expires_in': result.get('expires_in', 3600),
            'token_type': result.get('token_type', 'Bearer'),
            'metadata': {
                'scope': self.scope,
                'tenant_id': self.tenant_id
            }
        }
    
    async def get_token(self) -> str:
        """Get valid SharePoint access token."""
        return await self.provider.get_token()
    
    async def get_headers(self) -> dict:
        """Get Authorization headers."""
        return await self.provider.get_headers()


# ============================================================================
# Example 2: Snowflake Token Provider (Key-Pair JWT)
# ============================================================================

class SnowflakeTokenProvider:
    """
    Snowflake token provider using key-pair authentication.
    
    Requires:
        pip install cryptography PyJWT
    
    Environment variables:
        SNOWFLAKE_ACCOUNT
        SNOWFLAKE_USER
        SNOWFLAKE_PRIVATE_KEY_PATH (path to RSA private key)
        SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional)
    """
    
    def __init__(self, cache_backend):
        try:
            import jwt
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
            self.jwt = jwt
        except ImportError:
            raise ImportError(
                "JWT/cryptography not installed. "
                "Install with: pip install PyJWT cryptography"
            )
        
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        
        if not all([self.account, self.user, key_path]):
            raise ValueError("Missing Snowflake credentials in environment")
        
        # Load private key
        with open(key_path, 'rb') as f:
            private_key_data = f.read()
        
        self.private_key = serialization.load_pem_private_key(
            private_key_data,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
        
        # Token provider
        self.provider = TokenProvider(
            token_key=f"snowflake:{self.account}:{self.user}",
            token_fetcher=self._fetch_snowflake_token,
            cache_backend=cache_backend,
            config=TokenProviderConfig(refresh_buffer_seconds=300)
        )
    
    def _fetch_snowflake_token(self) -> dict:
        """Generate Snowflake JWT token."""
        import time
        
        logger.info("Generating Snowflake JWT token...")
        
        # Snowflake expects qualified user name
        qualified_username = f"{self.account}.{self.user}".upper()
        
        # JWT lifetime (Snowflake max: 59 minutes)
        now = int(time.time())
        lifetime = 3300  # 55 minutes
        
        payload = {
            'iss': qualified_username,
            'sub': qualified_username,
            'iat': now,
            'exp': now + lifetime,
        }
        
        # Sign JWT
        token = self.jwt.encode(
            payload,
            self.private_key,
            algorithm='RS256'
        )
        
        return {
            'access_token': token,
            'expires_in': lifetime,
            'token_type': 'Bearer',
            'metadata': {
                'account': self.account,
                'user': self.user
            }
        }
    
    async def get_token(self) -> str:
        """Get valid Snowflake JWT token."""
        return await self.provider.get_token()


# ============================================================================
# Example 3: Generic OAuth2 Client Credentials
# ============================================================================

class OAuth2TokenProvider:
    """
    Generic OAuth2 client credentials token provider.
    
    Requires:
        pip install httpx
    
    Args:
        token_url: OAuth2 token endpoint URL
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        scope: Optional scope string
        cache_backend: Cache backend instance
    """
    
    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str = None,
        cache_backend = None
    ):
        try:
            import httpx
            self.httpx = httpx
        except ImportError:
            raise ImportError("httpx not installed. Install with: pip install httpx")
        
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        
        cache_backend = cache_backend or InMemoryCacheBackend()
        
        self.provider = TokenProvider(
            token_key=f"oauth2:{token_url}:{client_id}",
            token_fetcher=self._fetch_oauth2_token,
            cache_backend=cache_backend,
            config=TokenProviderConfig(refresh_buffer_seconds=120)
        )
    
    async def _fetch_oauth2_token(self) -> dict:
        """Fetch token from OAuth2 endpoint."""
        logger.info(f"Fetching OAuth2 token from {self.token_url}...")
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }
        
        if self.scope:
            data['scope'] = self.scope
        
        async with self.httpx.AsyncClient() as client:
            response = await client.post(
                self.token_url,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            if response.status_code != 200:
                raise Exception(
                    f"OAuth2 token request failed: {response.status_code} - {response.text}"
                )
            
            return response.json()
    
    async def get_token(self) -> str:
        """Get valid OAuth2 access token."""
        return await self.provider.get_token()


# ============================================================================
# Example 4: Writer with Automatic Retry-on-401
# ============================================================================

class SharePointWriter:
    """
    Example writer that uses TokenProvider with automatic retry on 401.
    """
    
    def __init__(self, token_provider: SharePointTokenProvider):
        try:
            import httpx
            self.httpx = httpx
        except ImportError:
            raise ImportError("httpx not installed. Install with: pip install httpx")
        
        self.token_provider = token_provider
    
    async def write_file(self, site_id: str, file_path: str, content: bytes):
        """
        Write file to SharePoint with automatic token refresh on 401.
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}:/content"
        
        async def _do_write(token: str):
            """Inner function that performs the actual write."""
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/octet-stream'
            }
            
            async with self.httpx.AsyncClient() as client:
                response = await client.put(url, content=content, headers=headers)
                
                if response.status_code == 401:
                    raise Exception("401 Unauthorized")
                
                response.raise_for_status()
                return response.json()
        
        # Use retry_on_401 helper
        token = await self.token_provider.get_token()
        result = await retry_on_401(_do_write, self.token_provider.provider, token)
        
        logger.info(f"File written successfully: {file_path}")
        return result


# ============================================================================
# Example 5: Complete Integration Demo
# ============================================================================

async def demo_sharepoint_integration():
    """
    Complete demo of SharePoint integration with token management.
    
    Note: This requires actual SharePoint credentials to run.
    """
    logger.info("=== SharePoint Integration Demo ===\n")
    
    # Setup cache
    cache_dir = "/tmp/fleet_q_sharepoint_cache"
    cache_backend = DiskCacheBackend(cache_dir)
    
    try:
        # Create token provider
        sharepoint_provider = SharePointTokenProvider(cache_backend)
        
        # Get token (first time - will fetch)
        logger.info("Getting token (first call)...")
        token1 = await sharepoint_provider.get_token()
        logger.info(f"Token: {token1[:20]}...")
        
        # Get token again (should use cache)
        logger.info("\nGetting token (second call - should use cache)...")
        token2 = await sharepoint_provider.get_token()
        assert token1 == token2, "Tokens should match (from cache)"
        logger.info("✅ Cache working correctly")
        
        # Get metrics
        logger.info("\nMetrics:")
        metrics = sharepoint_provider.provider.get_metrics()
        for key, value in metrics.items():
            logger.info(f"  {key}: {value}")
        
        # Demo writer with retry-on-401
        logger.info("\n=== Testing Writer with Retry-on-401 ===")
        writer = SharePointWriter(sharepoint_provider)
        
        # This would write a file (commented out to avoid actual API calls)
        # site_id = "your-site-id"
        # await writer.write_file(site_id, "test.txt", b"Hello, World!")
        
        logger.info("✅ Demo completed successfully!")
    
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        logger.info("\nTo run this demo, set these environment variables:")
        logger.info("  SHAREPOINT_TENANT_ID")
        logger.info("  SHAREPOINT_CLIENT_ID")
        logger.info("  SHAREPOINT_CLIENT_SECRET")
        logger.info("  SHAREPOINT_SCOPE (optional)")
    
    finally:
        cache_backend.close()


async def demo_multi_provider():
    """
    Demonstrate using multiple token providers simultaneously.
    """
    logger.info("=== Multi-Provider Demo ===\n")
    
    # Shared cache backend
    cache_dir = "/tmp/fleet_q_multi_provider_cache"
    cache_backend = DiskCacheBackend(cache_dir)
    
    # Mock token fetchers
    async def fetch_service_a_token():
        await asyncio.sleep(0.1)
        return {'access_token': 'token_service_a', 'expires_in': 3600}
    
    async def fetch_service_b_token():
        await asyncio.sleep(0.1)
        return {'access_token': 'token_service_b', 'expires_in': 1800}
    
    # Create providers
    provider_a = TokenProvider(
        token_key="service_a",
        token_fetcher=fetch_service_a_token,
        cache_backend=cache_backend
    )
    
    provider_b = TokenProvider(
        token_key="service_b",
        token_fetcher=fetch_service_b_token,
        cache_backend=cache_backend
    )
    
    # Get tokens concurrently
    logger.info("Fetching tokens for both services concurrently...")
    token_a, token_b = await asyncio.gather(
        provider_a.get_token(),
        provider_b.get_token()
    )
    
    logger.info(f"Service A token: {token_a}")
    logger.info(f"Service B token: {token_b}")
    
    # Get again (should use cache)
    logger.info("\nFetching again (should use cache)...")
    token_a2, token_b2 = await asyncio.gather(
        provider_a.get_token(),
        provider_b.get_token()
    )
    
    assert token_a == token_a2 and token_b == token_b2
    logger.info("✅ Both services using cached tokens")
    
    # Metrics
    logger.info("\nMetrics:")
    logger.info(f"Service A: {provider_a.get_metrics()}")
    logger.info(f"Service B: {provider_b.get_metrics()}")
    
    cache_backend.close()


if __name__ == "__main__":
    # Run demos
    logger.info("Token Provider Examples\n")
    
    # Demo 1: Multi-provider (works without credentials)
    asyncio.run(demo_multi_provider())
    
    print("\n" + "="*70 + "\n")
    
    # Demo 2: SharePoint (requires credentials)
    # Uncomment to run:
    # asyncio.run(demo_sharepoint_integration())
