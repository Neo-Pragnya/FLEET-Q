"""
APScheduler Utilities for FLEET-Q In-Pod Execution Fabric

Provides scheduled task execution with lease-based singleton control:
- Time triggers (cron-like scheduling)
- Interval-based execution
- One-time delayed tasks
- Lease-aware execution (only runs if lease held)

Why APScheduler:
- Perfect for "start run at 2:00 AM" use cases
- Periodic maintenance (outbox flush, metrics, DLQ sweep)
- NOT for work queue (use claim loop instead)
- Must run ONLY in Control Plane Runner (singleton)

Patterns:
- All jobs check lease before executing
- Graceful shutdown with job completion
- Job persistence (optional, uses SQLite)
- Misfire handling for missed schedules
"""

import logging
from typing import Callable, Optional, Dict, Any, List
from datetime import datetime, timedelta
from functools import wraps
import asyncio
import time

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.date import DateTrigger
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
    from apscheduler.events import (
        EVENT_JOB_EXECUTED,
        EVENT_JOB_ERROR,
        EVENT_JOB_MISSED,
        JobExecutionEvent
    )
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    print("APScheduler not available. Install with: pip install apscheduler")

from fleet_q.sqlite_outbox import SQLiteOutbox

logger = logging.getLogger(__name__)


# ============================================================================
# Lease-Aware Job Decorator
# ============================================================================

def lease_required(lease_holder_id: str, outbox: SQLiteOutbox):
    """
    Decorator to ensure job only runs if lease is held.
    
    Args:
        lease_holder_id: Identity of expected lease holder
        outbox: SQLiteOutbox instance for lease checking
    
    Usage:
        @lease_required("control-plane-1", outbox)
        async def my_scheduled_job():
            # This only runs if lease is held
            pass
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not outbox.is_lease_holder(lease_holder_id):
                logger.warning(
                    f"Job '{func.__name__}' skipped: lease not held by {lease_holder_id}"
                )
                return None
            
            logger.debug(f"Executing lease-protected job: {func.__name__}")
            return await func(*args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not outbox.is_lease_holder(lease_holder_id):
                logger.warning(
                    f"Job '{func.__name__}' skipped: lease not held by {lease_holder_id}"
                )
                return None
            
            logger.debug(f"Executing lease-protected job: {func.__name__}")
            return func(*args, **kwargs)
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# ============================================================================
# Scheduler Manager
# ============================================================================

class APSchedulerManager:
    """
    APScheduler manager for FLEET-Q control plane.
    
    Features:
    - Async scheduler (compatible with asyncio event loop)
    - Lease-aware job execution
    - Job persistence (optional)
    - Event logging
    - Graceful shutdown
    """
    
    def __init__(
        self,
        lease_holder_id: str,
        outbox: SQLiteOutbox,
        use_jobstore: bool = False,
        jobstore_url: Optional[str] = None,
        timezone: str = "UTC"
    ):
        """
        Initialize APScheduler manager.
        
        Args:
            lease_holder_id: Identity for lease checking
            outbox: SQLiteOutbox instance
            use_jobstore: Enable persistent job storage
            jobstore_url: SQLite URL for job storage (e.g., "sqlite:///fleetq_jobs.db")
            timezone: Timezone for scheduling
        """
        if not APSCHEDULER_AVAILABLE:
            raise RuntimeError("APScheduler not installed")
        
        self.lease_holder_id = lease_holder_id
        self.outbox = outbox
        self.timezone = timezone
        
        # Configure job stores
        jobstores = {}
        if use_jobstore:
            if not jobstore_url:
                jobstore_url = "sqlite:///fleetq_jobs.db"
            jobstores['default'] = SQLAlchemyJobStore(url=jobstore_url)
        
        # Configure executors
        executors = {
            'default': ThreadPoolExecutor(10),
        }
        
        # Job defaults
        job_defaults = {
            'coalesce': True,  # Combine multiple missed runs into one
            'max_instances': 1,  # Only one instance of job at a time
            'misfire_grace_time': 60  # Allow 60s grace for missed jobs
        }
        
        # Create scheduler
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=timezone
        )
        
        # Add event listeners
        self.scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED
        )
        
        logger.info(f"APScheduler initialized for {lease_holder_id} (timezone={timezone})")
    
    def _job_executed_listener(self, event: JobExecutionEvent):
        """Listen to job execution events"""
        if event.exception:
            logger.error(
                f"Job '{event.job_id}' failed: {event.exception}",
                exc_info=event.exception
            )
        elif event.code == EVENT_JOB_MISSED:
            logger.warning(f"Job '{event.job_id}' missed scheduled time")
        else:
            logger.info(f"Job '{event.job_id}' executed successfully")
    
    # ========================================================================
    # Job Registration
    # ========================================================================
    
    def add_cron_job(
        self,
        func: Callable,
        job_id: str,
        cron_expr: Optional[str] = None,
        hour: Optional[int] = None,
        minute: Optional[int] = None,
        second: Optional[int] = None,
        day_of_week: Optional[str] = None,
        protect_with_lease: bool = True,
        **kwargs
    ) -> str:
        """
        Add cron-style scheduled job.
        
        Args:
            func: Function to execute (async or sync)
            job_id: Unique job identifier
            cron_expr: Cron expression (alternative to hour/minute/second)
            hour: Hour (0-23)
            minute: Minute (0-59)
            second: Second (0-59)
            day_of_week: Day of week (mon, tue, wed, thu, fri, sat, sun)
            protect_with_lease: Wrap with lease check
            **kwargs: Additional trigger kwargs
        
        Returns:
            Job ID
        
        Examples:
            # Every day at 2:00 AM
            manager.add_cron_job(my_job, "daily-job", hour=2, minute=0)
            
            # Every Monday at 8:30 AM
            manager.add_cron_job(my_job, "weekly-job", day_of_week="mon", hour=8, minute=30)
            
            # Using cron expression
            manager.add_cron_job(my_job, "cron-job", cron_expr="0 2 * * *")
        """
        if protect_with_lease:
            func = lease_required(self.lease_holder_id, self.outbox)(func)
        
        if cron_expr:
            # Parse cron expression
            trigger = CronTrigger.from_crontab(cron_expr, timezone=self.timezone)
        else:
            trigger = CronTrigger(
                hour=hour,
                minute=minute,
                second=second,
                day_of_week=day_of_week,
                timezone=self.timezone,
                **kwargs
            )
        
        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            replace_existing=True
        )
        
        logger.info(f"Added cron job '{job_id}'")
        return job_id
    
    def add_interval_job(
        self,
        func: Callable,
        job_id: str,
        seconds: Optional[int] = None,
        minutes: Optional[int] = None,
        hours: Optional[int] = None,
        start_date: Optional[datetime] = None,
        protect_with_lease: bool = True
    ) -> str:
        """
        Add interval-based job.
        
        Args:
            func: Function to execute
            job_id: Unique job identifier
            seconds: Interval in seconds
            minutes: Interval in minutes
            hours: Interval in hours
            start_date: When to start (default: now)
            protect_with_lease: Wrap with lease check
        
        Returns:
            Job ID
        
        Examples:
            # Every 30 seconds
            manager.add_interval_job(flush_outbox, "flush-job", seconds=30)
            
            # Every 5 minutes
            manager.add_interval_job(metrics_job, "metrics-job", minutes=5)
        """
        if protect_with_lease:
            func = lease_required(self.lease_holder_id, self.outbox)(func)
        
        trigger = IntervalTrigger(
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            start_date=start_date,
            timezone=self.timezone
        )
        
        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            replace_existing=True
        )
        
        logger.info(f"Added interval job '{job_id}'")
        return job_id
    
    def add_delayed_job(
        self,
        func: Callable,
        job_id: str,
        delay_seconds: int,
        protect_with_lease: bool = True
    ) -> str:
        """
        Add one-time delayed job.
        
        Args:
            func: Function to execute
            job_id: Unique job identifier
            delay_seconds: Delay before execution
            protect_with_lease: Wrap with lease check
        
        Returns:
            Job ID
        
        Example:
            # Run once after 60 seconds
            manager.add_delayed_job(cleanup_job, "cleanup-once", delay_seconds=60)
        """
        if protect_with_lease:
            func = lease_required(self.lease_holder_id, self.outbox)(func)
        
        run_date = datetime.now() + timedelta(seconds=delay_seconds)
        trigger = DateTrigger(run_date=run_date, timezone=self.timezone)
        
        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            replace_existing=True
        )
        
        logger.info(f"Added delayed job '{job_id}' (runs in {delay_seconds}s)")
        return job_id
    
    # ========================================================================
    # Job Management
    # ========================================================================
    
    def remove_job(self, job_id: str) -> bool:
        """Remove job by ID"""
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed job '{job_id}'")
            return True
        except Exception as e:
            logger.error(f"Failed to remove job '{job_id}': {e}")
            return False
    
    def pause_job(self, job_id: str) -> bool:
        """Pause job execution"""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Paused job '{job_id}'")
            return True
        except Exception as e:
            logger.error(f"Failed to pause job '{job_id}': {e}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """Resume paused job"""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Resumed job '{job_id}'")
            return True
        except Exception as e:
            logger.error(f"Failed to resume job '{job_id}': {e}")
            return False
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get list of all scheduled jobs"""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'trigger': str(job.trigger),
                'next_run': job.next_run_time,
                'executor': job.executor,
                'pending': job.pending
            })
        return jobs
    
    def modify_job(self, job_id: str, **changes) -> bool:
        """
        Modify job parameters.
        
        Args:
            job_id: Job to modify
            **changes: Parameters to change (trigger, next_run_time, etc.)
        
        Returns:
            True if successful
        """
        try:
            self.scheduler.modify_job(job_id, **changes)
            logger.info(f"Modified job '{job_id}'")
            return True
        except Exception as e:
            logger.error(f"Failed to modify job '{job_id}': {e}")
            return False
    
    # ========================================================================
    # Scheduler Lifecycle
    # ========================================================================
    
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info(f"Scheduler started (lease holder: {self.lease_holder_id})")
    
    def shutdown(self, wait: bool = True):
        """
        Shutdown the scheduler.
        
        Args:
            wait: Wait for running jobs to complete
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("Scheduler shut down")
    
    def is_running(self) -> bool:
        """Check if scheduler is running"""
        return self.scheduler.running


# ============================================================================
# Common Job Templates
# ============================================================================

class CommonJobs:
    """Pre-built job templates for common FLEET-Q tasks"""
    
    @staticmethod
    async def outbox_flush_job(outbox: SQLiteOutbox, batch_size: int = 100):
        """
        Flush pending outbox entries.
        
        This job should run frequently (e.g., every 10-30 seconds).
        """
        start_time = time.time()
        
        # Flush step updates
        updates = outbox.get_pending_step_updates(limit=batch_size)
        logger.info(f"Flushing {len(updates)} step updates")
        
        # Flush results
        results = outbox.get_pending_results(limit=batch_size)
        logger.info(f"Flushing {len(results)} results")
        
        # Flush SharePoint ops
        sp_ops = outbox.get_pending_sharepoint_ops(limit=batch_size)
        logger.info(f"Flushing {len(sp_ops)} SharePoint operations")
        
        elapsed = time.time() - start_time
        logger.info(f"Outbox flush completed in {elapsed:.2f}s")
    
    @staticmethod
    async def lease_renewal_job(
        outbox: SQLiteOutbox,
        lease_holder_id: str,
        ttl_seconds: int = 30
    ):
        """
        Renew control plane lease.
        
        This job should run more frequently than TTL (e.g., TTL/3).
        """
        renewed = outbox.renew_lease(lease_holder_id, ttl_seconds=ttl_seconds)
        if renewed:
            logger.debug(f"Lease renewed for {lease_holder_id}")
        else:
            logger.error(f"Failed to renew lease for {lease_holder_id}")
    
    @staticmethod
    async def cleanup_job(outbox: SQLiteOutbox, retention_hours: int = 24):
        """
        Clean up old outbox records.
        
        This job should run periodically (e.g., daily).
        """
        logger.info(f"Starting outbox cleanup (retention={retention_hours}h)")
        outbox.cleanup_old_records(retention_hours=retention_hours)
    
    @staticmethod
    async def stats_logging_job(outbox: SQLiteOutbox):
        """
        Log outbox statistics.
        
        This job provides observability into outbox health.
        """
        stats = outbox.get_stats()
        logger.info(f"Outbox stats: {stats}")
    
    @staticmethod
    async def heartbeat_job(pod_id: str, custom_metadata: Optional[Dict] = None):
        """
        Send pod heartbeat.
        
        This keeps pod registered as alive in the system.
        """
        logger.info(f"Heartbeat from pod {pod_id}")
        # Implementation would call health service or update Snowflake


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    """Example usage of APScheduler utilities"""
    
    import asyncio
    from fleet_q.sqlite_outbox import SQLiteOutbox
    
    async def example_job():
        """Example scheduled job"""
        print(f"Job executed at {datetime.now()}")
    
    async def main():
        # Initialize outbox
        outbox = SQLiteOutbox("/tmp/scheduler_test.db")
        
        # Acquire lease
        lease_acquired = outbox.try_acquire_lease(
            lease_holder="test-worker",
            pod_id="test-pod",
            process_id=12345
        )
        
        if not lease_acquired:
            print("Failed to acquire lease")
            return
        
        # Create scheduler
        manager = APSchedulerManager(
            lease_holder_id="test-worker",
            outbox=outbox
        )
        
        # Add jobs
        manager.add_interval_job(
            example_job,
            job_id="example-job",
            seconds=5
        )
        
        manager.add_interval_job(
            lambda: CommonJobs.lease_renewal_job(outbox, "test-worker", ttl_seconds=30),
            job_id="lease-renewal",
            seconds=10
        )
        
        manager.add_interval_job(
            lambda: CommonJobs.outbox_flush_job(outbox),
            job_id="outbox-flush",
            seconds=30
        )
        
        # Start scheduler
        manager.start()
        print("Scheduler started. Press Ctrl+C to stop.")
        
        try:
            # Run for 60 seconds
            await asyncio.sleep(60)
        except KeyboardInterrupt:
            print("\nStopping scheduler...")
        finally:
            # Shutdown
            manager.shutdown(wait=True)
            outbox.release_lease("test-worker")
            outbox.close()
            print("Scheduler stopped.")
    
    asyncio.run(main())
