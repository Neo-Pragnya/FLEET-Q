#!/usr/bin/env python3
"""
Leader Election and Health Management Demo

This script demonstrates the leader election and health management functionality
of FLEET-Q, showing how pods register themselves, send heartbeats, elect leaders,
and detect dead pods.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

from fleet_q.config import FleetQConfig
from fleet_q.health_service import HealthService
from fleet_q.leader_service import LeaderElectionService
from fleet_q.models import PodHealth, PodStatus
from fleet_q.sqlite_storage import SQLiteStorage


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockConfig:
    """Mock configuration for demo purposes."""
    
    def __init__(self, pod_id: str):
        self.pod_id = pod_id
        self.max_parallelism = 10
        self.capacity_threshold = 0.8
        self.max_concurrent_steps = 8
        self.heartbeat_interval_seconds = 2  # Fast for demo
        self.dead_pod_threshold_seconds = 10  # Short for demo


async def simulate_pod_lifecycle(pod_id: str, storage: SQLiteStorage, duration_seconds: int = 15):
    """
    Simulate a complete pod lifecycle with health tracking and leader election.
    
    Args:
        pod_id: Unique identifier for this pod
        storage: Shared storage backend
        duration_seconds: How long to run the simulation
    """
    config = MockConfig(pod_id)
    
    # Create health and leader services
    health_service = HealthService(storage, config)
    leader_service = LeaderElectionService(storage, config)
    
    logger.info(f"🚀 Starting pod {pod_id}")
    
    try:
        # Start health service (registers pod and starts heartbeat loop)
        await health_service.start()
        
        # Run for the specified duration
        start_time = datetime.utcnow()
        heartbeat_count = 0
        
        while (datetime.utcnow() - start_time).total_seconds() < duration_seconds:
            # Check leader status
            is_leader = await leader_service.am_i_leader()
            leader_info = await leader_service.get_leader_info()
            
            status_emoji = "👑" if is_leader else "👤"
            logger.info(
                f"{status_emoji} Pod {pod_id}: Leader={is_leader}, "
                f"Current Leader={leader_info.get('current_leader', 'none')}, "
                f"Active Pods={leader_info.get('active_pods_count', 0)}, "
                f"Uptime={health_service.uptime_seconds:.1f}s"
            )
            
            # If we're the leader, demonstrate dead pod detection
            if is_leader:
                dead_pods = await health_service.get_dead_pods()
                if dead_pods:
                    logger.warning(f"👑 Leader {pod_id} detected {len(dead_pods)} dead pods: {[p.pod_id for p in dead_pods]}")
                    # Mark them as dead
                    await health_service.detect_and_mark_dead_pods()
            
            heartbeat_count += 1
            await asyncio.sleep(3)  # Check every 3 seconds
        
        logger.info(f"✅ Pod {pod_id} completed lifecycle ({heartbeat_count} status checks)")
        
    except Exception as e:
        logger.error(f"❌ Pod {pod_id} encountered error: {e}")
    finally:
        # Graceful shutdown
        await health_service.stop()
        logger.info(f"🛑 Pod {pod_id} shut down gracefully")


async def demonstrate_leader_election():
    """
    Demonstrate leader election with multiple pods.
    """
    logger.info("🎯 Starting Leader Election and Health Management Demo")
    
    # Create shared storage
    storage = SQLiteStorage(":memory:")
    await storage.initialize()
    
    try:
        # Scenario 1: Single pod becomes leader
        logger.info("\n📍 Scenario 1: Single Pod Leadership")
        pod1_task = asyncio.create_task(simulate_pod_lifecycle("pod-alpha", storage, 8))
        await asyncio.sleep(4)  # Let it establish leadership
        
        # Scenario 2: Multiple pods, eldest becomes leader
        logger.info("\n📍 Scenario 2: Multi-Pod Election (Eldest Wins)")
        pod2_task = asyncio.create_task(simulate_pod_lifecycle("pod-beta", storage, 12))
        await asyncio.sleep(2)
        pod3_task = asyncio.create_task(simulate_pod_lifecycle("pod-gamma", storage, 10))
        
        # Let them run together
        await asyncio.sleep(6)
        
        # Scenario 3: Leader failure and re-election
        logger.info("\n📍 Scenario 3: Leader Failure and Re-election")
        logger.info("🔥 Simulating pod-alpha failure (oldest pod)")
        pod1_task.cancel()  # Simulate pod failure
        
        try:
            await pod1_task
        except asyncio.CancelledError:
            logger.info("💀 Pod-alpha failed (cancelled)")
        
        # Wait for re-election
        await asyncio.sleep(8)
        
        # Scenario 4: Add new pod after leader failure
        logger.info("\n📍 Scenario 4: New Pod Joins After Leader Failure")
        pod4_task = asyncio.create_task(simulate_pod_lifecycle("pod-delta", storage, 8))
        
        # Wait for all remaining pods to complete
        await asyncio.gather(pod2_task, pod3_task, pod4_task, return_exceptions=True)
        
        # Final status check
        logger.info("\n📊 Final System Status")
        all_pods = await storage.get_all_pod_health()
        active_pods = await storage.get_active_pods(10)  # 10 second threshold
        dead_pods = await storage.get_dead_pods(10)
        
        logger.info(f"Total pods registered: {len(all_pods)}")
        logger.info(f"Active pods: {len(active_pods)} - {[p.pod_id for p in active_pods]}")
        logger.info(f"Dead pods: {len(dead_pods)} - {[p.pod_id for p in dead_pods]}")
        
        # Show final leader
        final_leader = await storage.determine_leader(10)
        if final_leader:
            logger.info(f"👑 Final leader: {final_leader}")
        else:
            logger.info("👑 No leader remaining")
        
    finally:
        await storage.close()
        logger.info("🏁 Demo completed")


async def demonstrate_health_tracking():
    """
    Demonstrate detailed health tracking features.
    """
    logger.info("\n💓 Health Tracking Feature Demo")
    
    storage = SQLiteStorage(":memory:")
    await storage.initialize()
    
    try:
        config = MockConfig("health-demo-pod")
        health_service = HealthService(storage, config)
        
        # Start health service
        await health_service.start()
        logger.info(f"✅ Health service started for {config.pod_id}")
        
        # Demonstrate metadata collection
        metadata = health_service.collect_pod_metadata()
        logger.info(f"📋 Pod metadata collected: {list(metadata.keys())}")
        
        # Show uptime tracking
        await asyncio.sleep(3)
        logger.info(f"⏱️  Pod uptime: {health_service.uptime_seconds:.1f} seconds")
        
        # Demonstrate health record retrieval
        pod_health = await health_service.get_pod_health()
        if pod_health:
            logger.info(f"💓 Health record: Status={pod_health.status.value}, Last heartbeat age={(datetime.utcnow() - pod_health.last_heartbeat_ts).total_seconds():.1f}s")
        
        # Demonstrate graceful shutdown
        await health_service.stop()
        logger.info(f"🛑 Health service stopped gracefully")
        
        # Verify final status
        final_health = await storage.get_pod_health(config.pod_id)
        if final_health:
            logger.info(f"📋 Final status: {final_health.status.value}")
        
    finally:
        await storage.close()


async def main():
    """
    Main demo function.
    """
    print("🎪 FLEET-Q Leader Election and Health Management Demo")
    print("=" * 60)
    
    try:
        # Run leader election demo
        await demonstrate_leader_election()
        
        # Run health tracking demo
        await demonstrate_health_tracking()
        
        print("\n🎉 All demos completed successfully!")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())