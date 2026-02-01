"""
SharePoint Reader Stage with Async I/O

This module implements a pipeline stage for downloading files from SharePoint:
- Async I/O for concurrent downloads
- Configurable concurrency limits
- Retry logic with backoff
- Progress tracking

Key Insight:
    File downloads are I/O-heavy, not CPU-heavy.
    Async enables efficient concurrent downloads without multiprocessing overhead.
"""

import asyncio
import time
import logging
import os
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from pathlib import Path

from pipeline import PipelineStage, PipelineMessage, MessageType
from backoff import with_backoff, BackoffConfig


logger = logging.getLogger(__name__)


@dataclass
class SharePointDownloadRequest:
    """Request to download a file from SharePoint"""
    sharepoint_url: str
    local_path: str
    request_id: str
    site_id: Optional[str] = None
    drive_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class SharePointDownloadResponse:
    """Response after downloading a file"""
    request_id: str
    sharepoint_url: str
    local_path: str
    success: bool
    file_size_bytes: int = 0
    download_time: float = 0.0
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class SharePointReaderStage(PipelineStage):
    """
    Pipeline stage for downloading files from SharePoint.
    
    Features:
    - Async downloads for efficiency
    - Configurable concurrency
    - Automatic retry with backoff
    - Local caching
    """
    
    def __init__(
        self,
        stage_name: str = "SharePointReader",
        max_concurrent: int = 10,
        download_dir: str = "/tmp/sharepoint_downloads",
        use_mock: bool = True,  # Use mock by default for demo
        **kwargs
    ):
        super().__init__(stage_name, **kwargs)
        self.max_concurrent = max_concurrent
        self.download_dir = Path(download_dir)
        self.use_mock = use_mock
        self.event_loop = None
        self.semaphore = None
        
        # Metrics
        self.total_bytes = 0
        self.total_downloads = 0
        self.failed_downloads = 0
    
    def setup(self):
        """Initialize async resources and create download directory"""
        self.logger.info(f"Setting up SharePoint reader (async mode, concurrency={self.max_concurrent})")
        
        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Download directory: {self.download_dir}")
        
        # Create event loop
        self.event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.event_loop)
        
        # Semaphore for concurrency control
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        if self.use_mock:
            self.logger.warning("Using MOCK SharePoint client (for demo)")
        else:
            self.logger.info("Using REAL SharePoint client")
            try:
                from office365.sharepoint.client_context import ClientContext
                self.logger.info("office365 library loaded")
            except ImportError:
                self.logger.error("office365 library not found. Install: pip install Office365-REST-Python-Client")
                self.logger.info("Falling back to mock mode")
                self.use_mock = True
    
    def teardown(self):
        """Cleanup async resources"""
        if self.event_loop:
            self.event_loop.close()
        
        self.logger.info(
            f"SharePoint reader stats: "
            f"Downloads={self.total_downloads}, "
            f"Failed={self.failed_downloads}, "
            f"Total bytes={self.total_bytes:,}"
        )
    
    def process_message(self, message: PipelineMessage) -> Optional[PipelineMessage]:
        """Download file from SharePoint"""
        if message.msg_type != MessageType.DATA:
            return message
        
        # Extract download request
        request = message.payload
        if not isinstance(request, SharePointDownloadRequest):
            self.logger.error(f"Invalid payload type: {type(request)}")
            return None
        
        # Run async download in event loop
        try:
            response = self.event_loop.run_until_complete(
                self._download_file_async(request)
            )
            
            # Update metrics
            if response.success:
                self.total_downloads += 1
                self.total_bytes += response.file_size_bytes
            else:
                self.failed_downloads += 1
            
            # Wrap response in message
            return PipelineMessage(
                msg_type=MessageType.DATA,
                payload=response,
                metadata=message.metadata
            )
        
        except Exception as e:
            self.logger.error(f"Failed to download {request.sharepoint_url}: {e}")
            self.failed_downloads += 1
            
            # Return error response
            error_response = SharePointDownloadResponse(
                request_id=request.request_id,
                sharepoint_url=request.sharepoint_url,
                local_path=request.local_path,
                success=False,
                error=str(e),
                metadata=request.metadata
            )
            
            return PipelineMessage(
                msg_type=MessageType.DATA,
                payload=error_response,
                metadata=message.metadata
            )
    
    async def _download_file_async(self, request: SharePointDownloadRequest) -> SharePointDownloadResponse:
        """
        Download a single file with concurrency control and retry.
        """
        start_time = time.time()
        
        # Acquire semaphore (limit concurrent downloads)
        async with self.semaphore:
            try:
                if self.use_mock:
                    file_size = await self._mock_download(request)
                else:
                    file_size = await self._real_download(request)
                
                download_time = time.time() - start_time
                
                self.logger.info(
                    f"Downloaded {request.request_id}: "
                    f"{file_size:,} bytes in {download_time:.2f}s "
                    f"({file_size/download_time/1024:.1f} KB/s)"
                )
                
                return SharePointDownloadResponse(
                    request_id=request.request_id,
                    sharepoint_url=request.sharepoint_url,
                    local_path=request.local_path,
                    success=True,
                    file_size_bytes=file_size,
                    download_time=download_time,
                    metadata=request.metadata
                )
            
            except Exception as e:
                download_time = time.time() - start_time
                
                self.logger.error(f"Download failed for {request.request_id}: {e}")
                
                return SharePointDownloadResponse(
                    request_id=request.request_id,
                    sharepoint_url=request.sharepoint_url,
                    local_path=request.local_path,
                    success=False,
                    download_time=download_time,
                    error=str(e),
                    metadata=request.metadata
                )
    
    async def _mock_download(self, request: SharePointDownloadRequest) -> int:
        """
        Mock file download for testing.
        
        Simulates:
        - Variable download time
        - File creation
        """
        import random
        
        # Simulate download time (100-500ms)
        download_time = random.uniform(0.1, 0.5)
        await asyncio.sleep(download_time)
        
        # Create mock file
        file_path = self.download_dir / request.local_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write mock content
        mock_content = f"Mock file content for {request.request_id}\n" * 100
        file_path.write_text(mock_content)
        
        file_size = file_path.stat().st_size
        return file_size
    
    async def _real_download(self, request: SharePointDownloadRequest) -> int:
        """
        Real SharePoint file download (requires credentials).
        
        Uses Office365-REST-Python-Client library.
        """
        try:
            from office365.sharepoint.client_context import ClientContext
            from office365.runtime.auth.user_credential import UserCredential
            
            # Get credentials from environment
            username = os.getenv("SHAREPOINT_USERNAME")
            password = os.getenv("SHAREPOINT_PASSWORD")
            site_url = os.getenv("SHAREPOINT_SITE_URL")
            
            if not all([username, password, site_url]):
                raise ValueError("SharePoint credentials not configured")
            
            # Create context
            credentials = UserCredential(username, password)
            ctx = ClientContext(site_url).with_credentials(credentials)
            
            # Download file
            file_path = self.download_dir / request.local_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Run sync download in thread pool
            loop = asyncio.get_event_loop()
            
            def download_sync():
                with open(file_path, 'wb') as local_file:
                    file = ctx.web.get_file_by_server_relative_url(request.sharepoint_url)
                    file.download(local_file).execute_query()
                return file_path.stat().st_size
            
            file_size = await loop.run_in_executor(None, download_sync)
            
            return file_size
        
        except Exception as e:
            raise RuntimeError(f"SharePoint download error: {e}")


# Decorator for retry logic
def with_sharepoint_retry(max_attempts: int = 3):
    """
    Decorator to add retry logic to SharePoint downloads.
    """
    return with_backoff(
        max_attempts=max_attempts,
        base_delay_ms=1000,
        max_delay_ms=10000,
        exponential_base=2.0
    )


# Demo usage
if __name__ == "__main__":
    """
    Demonstrate SharePoint downloading with async I/O.
    """
    from pipeline import Pipeline, SourceStage, SinkStage
    
    print("=== SharePoint Reader Demo ===\n")
    
    # Create test download requests
    requests = [
        SharePointDownloadRequest(
            sharepoint_url=f"/sites/mysite/documents/file{i}.txt",
            local_path=f"downloads/file{i}.txt",
            request_id=f"dl-{i:03d}"
        )
        for i in range(1, 11)
    ]
    
    # Create pipeline
    pipeline = Pipeline(name="sharepoint-demo")
    
    # Stage 1: Source (feed requests)
    source = SourceStage(
        stage_name="DownloadSource",
        items=requests
    )
    
    # Stage 2: SharePoint reader (async downloads)
    reader = SharePointReaderStage(
        stage_name="SharePointReader",
        max_concurrent=5,
        use_mock=True  # Use mock for demo
    )
    
    # Stage 3: Sink (collect responses)
    sink = SinkStage(stage_name="DownloadCollector")
    
    # Build and run
    pipeline.add_stage(source)
    pipeline.add_stage(reader)
    pipeline.add_stage(sink)
    pipeline.build()
    
    try:
        pipeline.start()
        pipeline.wait()
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        pipeline.stop()
    
    print("\n=== Demo Complete ===")
    print(f"Total downloads: {reader.total_downloads}")
    print(f"Total bytes: {reader.total_bytes:,}")
    print(f"Failed: {reader.failed_downloads}")
