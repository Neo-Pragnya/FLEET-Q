"""
Control Plane Runner for FLEET-Q In-Pod Execution Fabric

The Control Plane Runner is the singleton "brain" of each pod:
- Acquires and maintains SQLite lease (prevents duplication across FastAPI workers)
- Runs IOHub (ZeroMQ ROUTER + AIMD permit control)
- Manages APScheduler (time-based triggers)
- Coordinates outbox flushers (Snowflake + SharePoint)
- Runs claim loop and heartbeat loop
- Provides graceful startup/shutdown

Lifecycle:
1. Try to acquire lease (exits if another process holds it)
2. Start IOHub message loop
3. Start APScheduler with common jobs
4. Start outbox flushers
5. Optionally start claim/heartbeat loops
6. Run until shutdown signal

This ensures only ONE control plane per pod, regardless of FastAPI worker count.
"""

import asyncio
import logging
import signal
import os
import time
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass

from fleet_q.sqlite_outbox import SQLiteOutbox
from fleet_q.iohub import IOHub, AIMDConfig
from fleet_q.apscheduler_utils import APSchedulerManager, CommonJobs
from fleet_q.zeromq_utils import create_ipc_address

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ControlPlaneConfig:
    """Control Plane Runner configuration"""
    # Identity
    pod_id: str = os.environ.get('POD_ID', 'pod-unknown')
    process_id: int = os.getpid()
    lease_holder_id: str = None  # Auto-generated if None
    
    # SQLite paths
    outbox_db_path: str = "/tmp/fleetq_outbox.db"
    
    # ZeroMQ
    iohub_address: str = None  # Auto-generated if None
    zmq_hwm: int = 1000
    
    # Lease
    lease_ttl_seconds: int = 30
    lease_renewal_interval_seconds: int = 10
    
    # AIMD
    aimd_config: Optional[AIMDConfig] = None
    
    # Scheduler
    use_persistent_jobstore: bool = False
    jobstore_url: Optional[str] = None
    timezone: str = "UTC"
    
    # Flushers
    outbox_flush_interval_seconds: int = 30
    outbox_flush_batch_size: int = 100
    
    # Cleanup
    cleanup_interval_hours: int = 24
    cleanup_retention_hours: int = 48
    
    # Features (optional integration points)
    enable_claim_loop: bool = False
    enable_heartbeat_loop: bool = False
    enable_metrics_logging: bool = True
    
    def __post_init__(self):
        if self.lease_holder_id is None:
            self.lease_holder_id = f"control-plane-{self.pod_id}-{self.process_id}"
        
        if self.iohub_address is None:
            self.iohub_address = create_ipc_address(f"iohub-{self.pod_id}")


# ============================================================================
# Control Plane Runner
# ============================================================================

class ControlPlaneRunner:
    """
    Singleton control plane runner for FLEET-Q pod.
    
    This runs all centralized orchestration:
    - IOHub (permit control + routing)
    - APScheduler (time triggers)
    - Outbox flushers
    - Optional claim/heartbeat loops
    """
    
    def __init__(self, config: Optional[ControlPlaneConfig] = None):
        """
        Initialize Control Plane Runner.
        
        Args:
            config: Configuration (uses defaults if None)
        """
        self.config = config or ControlPlaneConfig()
        
        # State
        self.running = False
        self.lease_acquired = False
        
        # Components (initialized in start())
        self.outbox: Optional[SQLiteOutbox] = None
        self.iohub: Optional[IOHub] = None
        self.scheduler: Optional[APSchedulerManager] = None
        
        # Tasks
        self.flusher_task: Optional[asyncio.Task] = None
        self.claim_loop_task: Optional[asyncio.Task] = None
        self.heartbeat_loop_task: Optional[asyncio.Task] = None
        
        # Custom jobs
        self.custom_jobs: List[Dict[str, Any]] = []
        
        logger.info(
            f"Control Plane Runner initialized "
            f"(lease_holder={self.config.lease_holder_id}, pod={self.config.pod_id})"
        )
    
    # ========================================================================
    # Lease Management
    # ========================================================================
    
    def try_acquire_lease(self) -> bool:
        """
        Try to acquire control plane lease.
        
        Returns:
            True if acquired, False if held by another process
        """
        acquired = self.outbox.try_acquire_lease(
            lease_holder=self.config.lease_holder_id,
            pod_id=self.config.pod_id,
            process_id=self.config.process_id,
            ttl_seconds=self.config.lease_ttl_seconds
        )
        
        if acquired:
            self.lease_acquired = True
            logger.info(f"✅ Lease acquired by {self.config.lease_holder_id}")
        else:
            current_lease = self.outbox.get_current_lease()
            if current_lease:
                logger.info(
                    f"❌ Lease held by {current_lease.lease_holder} "
                    f"(expires in {current_lease.expires_at - time.time():.1f}s)"
                )
            else:
                logger.warning("❌ Failed to acquire lease (unknown reason)")
        
        return acquired
    
    def release_lease(self):
        """Release control plane lease"""
        if self.lease_acquired:
            self.outbox.release_lease(self.config.lease_holder_id)
            self.lease_acquired = False
            logger.info(f"Released lease: {self.config.lease_holder_id}")
    
    # ========================================================================
    # Component Initialization
    # ========================================================================
    
    def initialize_outbox(self):
        """Initialize SQLite outbox"""
        self.outbox = SQLiteOutbox(
            db_path=self.config.outbox_db_path,
            wal_mode=True
        )
        logger.info("Outbox initialized")
    
    def initialize_iohub(self):
        """Initialize IOHub"""
        self.iohub = IOHub(
            bind_address=self.config.iohub_address,
            outbox=self.outbox,
            aimd_config=self.config.aimd_config,
            hwm=self.config.zmq_hwm
        )
        logger.info("IOHub initialized")
    
    def initialize_scheduler(self):
        """Initialize APScheduler"""
        self.scheduler = APSchedulerManager(
            lease_holder_id=self.config.lease_holder_id,
            outbox=self.outbox,
            use_jobstore=self.config.use_persistent_jobstore,
            jobstore_url=self.config.jobstore_url,
            timezone=self.config.timezone
        )
        logger.info("Scheduler initialized")
    
    def register_default_jobs(self):
        """Register default scheduled jobs"""
        # Lease renewal (critical - runs frequently)
        self.scheduler.add_interval_job(
            lambda: CommonJobs.lease_renewal_job(
                self.outbox,
                self.config.lease_holder_id,
                ttl_seconds=self.config.lease_ttl_seconds
            ),
            job_id="lease-renewal",
            seconds=self.config.lease_renewal_interval_seconds,
            protect_with_lease=False  # Don't protect renewal itself
        )
        logger.info(
            f"Registered lease renewal job "
            f"(every {self.config.lease_renewal_interval_seconds}s)"
        )
        
        # Outbox flush (frequent)
        self.scheduler.add_interval_job(
            lambda: CommonJobs.outbox_flush_job(
                self.outbox,
                batch_size=self.config.outbox_flush_batch_size
            ),
            job_id="outbox-flush",
            seconds=self.config.outbox_flush_interval_seconds
        )
        logger.info(
            f"Registered outbox flush job "
            f"(every {self.config.outbox_flush_interval_seconds}s)"
        )
        
        # Cleanup (daily)
        self.scheduler.add_interval_job(
            lambda: CommonJobs.cleanup_job(
                self.outbox,
                retention_hours=self.config.cleanup_retention_hours
            ),
            job_id="cleanup",
            hours=self.config.cleanup_interval_hours
        )
        logger.info(
            f"Registered cleanup job "
            f"(every {self.config.cleanup_interval_hours}h)"
        )
        
        # Metrics logging (optional)
        if self.config.enable_metrics_logging:
            self.scheduler.add_interval_job(
                lambda: CommonJobs.stats_logging_job(self.outbox),
                job_id="stats-logging",
                minutes=5
            )
            logger.info("Registered stats logging job (every 5 minutes)")
    
    def add_custom_job(
        self,
        func: Callable,
        job_id: str,
        job_type: str = "interval",
        **kwargs
    ):
        """
        Add custom scheduled job.
        
        Args:
            func: Job function (async or sync)
            job_id: Unique identifier
            job_type: "interval", "cron", or "delayed"
            **kwargs: Job-specific parameters
        
        Example:
            runner.add_custom_job(
                my_custom_task,
                job_id="custom-task",
                job_type="interval",
                minutes=10
            )
        """
        self.custom_jobs.append({
            'func': func,
            'job_id': job_id,
            'job_type': job_type,
            'kwargs': kwargs
        })
        logger.info(f"Custom job '{job_id}' queued for registration")
    
    def register_custom_jobs(self):
        """Register all custom jobs with scheduler"""
        for job_info in self.custom_jobs:
            job_type = job_info['job_type']
            
            if job_type == "interval":
                self.scheduler.add_interval_job(
                    job_info['func'],
                    job_info['job_id'],
                    **job_info['kwargs']
                )
            elif job_type == "cron":
                self.scheduler.add_cron_job(
                    job_info['func'],
                    job_info['job_id'],
                    **job_info['kwargs']
                )
            elif job_type == "delayed":
                self.scheduler.add_delayed_job(
                    job_info['func'],
                    job_info['job_id'],
                    **job_info['kwargs']
                )
            else:
                logger.warning(f"Unknown job type: {job_type}")
            
            logger.info(f"Registered custom job: {job_info['job_id']}")
    
    # ========================================================================
    # Optional Loops (Claim, Heartbeat)
    # ========================================================================
    
    async def claim_loop(self):
        """
        Optional claim loop (integrate with FLEET-Q claim service).
        
        This would call claim_service.claim_pending_steps() periodically.
        """
        logger.info("Claim loop started")
        
        while self.running:
            try:
                if self.outbox.is_lease_holder(self.config.lease_holder_id):
                    logger.debug("Running claim tick...")
                    # TODO: Call claim service
                    # steps = await claim_service.claim_pending_steps(...)
                else:
                    logger.warning("Claim loop: lease lost")
                    break
                
                await asyncio.sleep(5)  # Claim interval
            
            except Exception as e:
                logger.error(f"Error in claim loop: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        logger.info("Claim loop stopped")
    
    async def heartbeat_loop(self):
        """
        Optional heartbeat loop (integrate with FLEET-Q health service).
        
        This would send pod heartbeats to Snowflake.
        """
        logger.info("Heartbeat loop started")
        
        while self.running:
            try:
                if self.outbox.is_lease_holder(self.config.lease_holder_id):
                    logger.debug("Sending heartbeat...")
                    # TODO: Call health service
                    # await health_service.send_heartbeat(...)
                else:
                    logger.warning("Heartbeat loop: lease lost")
                    break
                
                await asyncio.sleep(10)  # Heartbeat interval
            
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        logger.info("Heartbeat loop stopped")
    
    # ========================================================================
    # Lifecycle
    # ========================================================================
    
    async def start(self) -> bool:
        """
        Start Control Plane Runner.
        
        Returns:
            True if started successfully, False if lease not acquired
        """
        if self.running:
            logger.warning("Control Plane Runner already running")
            return True
        
        logger.info("🚀 Starting Control Plane Runner...")
        
        # 1. Initialize outbox
        self.initialize_outbox()
        
        # 2. Try to acquire lease
        if not self.try_acquire_lease():
            logger.info("Another process holds the lease. Exiting.")
            return False
        
        # 3. Initialize components
        self.initialize_iohub()
        self.initialize_scheduler()
        
        # 4. Start IOHub
        await self.iohub.start()
        
        # 5. Register and start scheduler jobs
        self.register_default_jobs()
        self.register_custom_jobs()
        self.scheduler.start()
        
        # 6. Start optional loops
        if self.config.enable_claim_loop:
            self.claim_loop_task = asyncio.create_task(self.claim_loop())
        
        if self.config.enable_heartbeat_loop:
            self.heartbeat_loop_task = asyncio.create_task(self.heartbeat_loop())
        
        self.running = True
        logger.info("✅ Control Plane Runner started successfully")
        
        return True
    
    async def stop(self):
        """Stop Control Plane Runner gracefully"""
        if not self.running:
            return
        
        logger.info("🛑 Stopping Control Plane Runner...")
        self.running = False
        
        # Stop scheduler (wait for running jobs)
        if self.scheduler:
            self.scheduler.shutdown(wait=True)
        
        # Stop optional loops
        if self.claim_loop_task:
            self.claim_loop_task.cancel()
            try:
                await self.claim_loop_task
            except asyncio.CancelledError:
                pass
        
        if self.heartbeat_loop_task:
            self.heartbeat_loop_task.cancel()
            try:
                await self.heartbeat_loop_task
            except asyncio.CancelledError:
                pass
        
        # Stop IOHub
        if self.iohub:
            await self.iohub.stop()
        
        # Release lease
        self.release_lease()
        
        # Close outbox
        if self.outbox:
            self.outbox.close()
        
        logger.info("✅ Control Plane Runner stopped")
    
    async def run_forever(self):
        """
        Start and run until interrupted.
        
        Handles SIGINT and SIGTERM gracefully.
        """
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        
        def signal_handler():
            logger.info("Received shutdown signal")
            asyncio.create_task(self.stop())
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
        
        # Start
        started = await self.start()
        if not started:
            return
        
        # Run until stopped
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            await self.stop()
    
    # ========================================================================
    # Status and Monitoring
    # ========================================================================
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive status"""
        status = {
            'running': self.running,
            'lease_acquired': self.lease_acquired,
            'config': {
                'pod_id': self.config.pod_id,
                'process_id': self.config.process_id,
                'lease_holder_id': self.config.lease_holder_id
            }
        }
        
        if self.iohub:
            status['iohub'] = self.iohub.get_status()
        
        if self.scheduler:
            status['scheduler'] = {
                'running': self.scheduler.is_running(),
                'jobs': self.scheduler.get_jobs()
            }
        
        if self.outbox:
            status['outbox'] = self.outbox.get_stats()
        
        return status
    
    def print_status(self):
        """Print formatted status"""
        status = self.get_status()
        
        print("\n" + "=" * 50)
        print("FLEET-Q Control Plane Runner Status")
        print("=" * 50)
        print(f"Running: {status['running']}")
        print(f"Lease Acquired: {status['lease_acquired']}")
        print(f"Pod ID: {status['config']['pod_id']}")
        print(f"Process ID: {status['config']['process_id']}")
        print(f"Lease Holder: {status['config']['lease_holder_id']}")
        
        if 'iohub' in status:
            print("\n--- IOHub ---")
            iohub = status['iohub']
            print(f"Max Inflight: {iohub['aimd']['max_inflight']}")
            print(f"Current Inflight: {iohub['aimd']['current_inflight']}")
            print(f"Permits Granted: {iohub['permits']['granted']}")
            print(f"Total Requests: {iohub['requests']['total']}")
            print(f"Successes: {iohub['requests']['successes']}")
            print(f"Throttles: {iohub['requests']['throttles']}")
        
        if 'scheduler' in status:
            print("\n--- Scheduler ---")
            sched = status['scheduler']
            print(f"Running: {sched['running']}")
            print(f"Jobs: {len(sched['jobs'])}")
            for job in sched['jobs']:
                print(f"  - {job['id']}: next run {job['next_run']}")
        
        print("=" * 50 + "\n")


# ============================================================================
# Convenience Functions
# ============================================================================

async def run_control_plane(config: Optional[ControlPlaneConfig] = None):
    """
    Convenience function to run control plane with default setup.
    
    Args:
        config: Optional configuration
    
    Example:
        asyncio.run(run_control_plane())
    """
    runner = ControlPlaneRunner(config)
    await runner.run_forever()


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    """Example Control Plane Runner usage"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create configuration
    config = ControlPlaneConfig(
        pod_id="test-pod-1",
        outbox_db_path="/tmp/test_control_plane.db",
        lease_ttl_seconds=30,
        lease_renewal_interval_seconds=10,
        outbox_flush_interval_seconds=15,
        enable_metrics_logging=True
    )
    
    # Run control plane
    asyncio.run(run_control_plane(config))
