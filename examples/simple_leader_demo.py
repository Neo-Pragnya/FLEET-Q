#!/usr/bin/env python3
"""
Simple Leader Election Demo

A minimal demonstration of the leader election and health management
functionality without complex dependencies.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import asyncio
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mock the problematic imports
class MockStorage:
    def __init__(self):
        self.pods = {}
        
    async def initialize(self):
        pass
        
    async def close(self):
        pass
        
    async def upsert_pod_health(self, pod_health):
        self.pods[pod_health.pod_id] = pod_health
        
    async def determine_leader(self, threshold_seconds):
        current_time = datetime.utcnow()
        threshold = current_time - timedelta(seconds=threshold_seconds)
        
        # Find active pods with fresh heartbeats
        active_pods = []
        for pod in self.pods.values():
            if pod.status.value == 'up' and pod.last_heartbeat_ts > threshold:
                active_pods.append(pod)
        
        if active_pods:
            # Return eldest pod (earliest birth_ts)
            eldest = min(active_pods, key=lambda p: p.birth_ts)
            return eldest.pod_id
        return None
        
    async def am_i_leader(self, pod_id, threshold_seconds):
        leader = await self.determine_leader(threshold_seconds)
        return leader == pod_id
        
    async def get_leader_info(self, threshold_seconds):
        current_time = datetime.utcnow()
        threshold = current_time - timedelta(seconds=threshold_seconds)
        
        active_pods = []
        for pod in self.pods.values():
            if pod.status.value == 'up' and pod.last_heartbeat_ts > threshold:
                active_pods.append(pod)
        
        leader_id = None
        if active_pods:
            eldest = min(active_pods, key=lambda p: p.birth_ts)
            leader_id = eldest.pod_id
            
        return {
            'current_leader': leader_id,
            'active_pods_count': len(active_pods),
            'active_pod_ids': [p.pod_id for p in active_pods],
            'election_timestamp': current_time
        }

# Simple data models
class PodStatus:
    UP = 'up'
    DOWN = 'down'
    
    def __init__(self, value):
        self.value = value

class PodHealth:
    def __init__(self, pod_id, birth_ts, last_heartbeat_ts, status, meta):
        self.pod_id = pod_id
        self.birth_ts = birth_ts
        self.last_heartbeat_ts = last_heartbeat_ts
        self.status = status
        self.meta = meta

async def demonstrate_leader_election():
    """
    Demonstrate the core leader election logic.
    """
    logger.info("🎯 Starting Simple Leader Election Demo")
    
    storage = MockStorage()
    await storage.initialize()
    
    current_time = datetime.utcnow()
    
    # Create three pods with different birth times
    pods = [
        PodHealth(
            pod_id="pod-alpha",
            birth_ts=current_time - timedelta(minutes=10),  # Oldest
            last_heartbeat_ts=current_time - timedelta(seconds=5),
            status=PodStatus('up'),
            meta={"version": "1.0.0"}
        ),
        PodHealth(
            pod_id="pod-beta", 
            birth_ts=current_time - timedelta(minutes=5),   # Middle
            last_heartbeat_ts=current_time - timedelta(seconds=3),
            status=PodStatus('up'),
            meta={"version": "1.0.0"}
        ),
        PodHealth(
            pod_id="pod-gamma",
            birth_ts=current_time - timedelta(minutes=1),   # Newest
            last_heartbeat_ts=current_time - timedelta(seconds=2),
            status=PodStatus('up'),
            meta={"version": "1.0.0"}
        )
    ]
    
    # Register all pods
    for pod in pods:
        await storage.upsert_pod_health(pod)
        logger.info(f"📝 Registered pod {pod.pod_id} (birth: {pod.birth_ts.strftime('%H:%M:%S')})")
    
    # Check leader election
    logger.info("\n👑 Leader Election Results:")
    leader_info = await storage.get_leader_info(60)  # 60 second threshold
    
    logger.info(f"Current leader: {leader_info['current_leader']}")
    logger.info(f"Active pods: {leader_info['active_pods_count']}")
    logger.info(f"Pod IDs: {leader_info['active_pod_ids']}")
    
    # Check each pod's leader status
    for pod in pods:
        is_leader = await storage.am_i_leader(pod.pod_id, 60)
        status_emoji = "👑" if is_leader else "👤"
        logger.info(f"{status_emoji} {pod.pod_id}: Leader = {is_leader}")
    
    # Simulate oldest pod going down
    logger.info(f"\n💀 Simulating {pods[0].pod_id} going down...")
    pods[0].status = PodStatus('down')
    await storage.upsert_pod_health(pods[0])
    
    # Check new leader election
    logger.info("\n👑 New Leader Election Results:")
    new_leader_info = await storage.get_leader_info(60)
    
    logger.info(f"New leader: {new_leader_info['current_leader']}")
    logger.info(f"Active pods: {new_leader_info['active_pods_count']}")
    
    # Check each remaining pod's leader status
    for pod in pods[1:]:  # Skip the down pod
        is_leader = await storage.am_i_leader(pod.pod_id, 60)
        status_emoji = "👑" if is_leader else "👤"
        logger.info(f"{status_emoji} {pod.pod_id}: Leader = {is_leader}")
    
    await storage.close()
    logger.info("\n✅ Demo completed successfully!")

async def demonstrate_heartbeat_logic():
    """
    Demonstrate heartbeat and dead pod detection logic.
    """
    logger.info("\n💓 Heartbeat and Dead Pod Detection Demo")
    
    storage = MockStorage()
    await storage.initialize()
    
    current_time = datetime.utcnow()
    
    # Create pods with different heartbeat ages
    pods = [
        PodHealth(
            pod_id="fresh-pod",
            birth_ts=current_time - timedelta(minutes=5),
            last_heartbeat_ts=current_time - timedelta(seconds=10),  # Fresh
            status=PodStatus('up'),
            meta={}
        ),
        PodHealth(
            pod_id="stale-pod",
            birth_ts=current_time - timedelta(minutes=10),
            last_heartbeat_ts=current_time - timedelta(minutes=5),   # Stale
            status=PodStatus('up'),
            meta={}
        ),
        PodHealth(
            pod_id="down-pod",
            birth_ts=current_time - timedelta(minutes=15),
            last_heartbeat_ts=current_time - timedelta(minutes=2),
            status=PodStatus('down'),  # Already down
            meta={}
        )
    ]
    
    # Register all pods
    for pod in pods:
        await storage.upsert_pod_health(pod)
        heartbeat_age = (current_time - pod.last_heartbeat_ts).total_seconds()
        logger.info(f"📝 {pod.pod_id}: Status={pod.status.value}, Heartbeat age={heartbeat_age:.1f}s")
    
    # Test with different thresholds
    thresholds = [30, 120, 300]  # 30s, 2min, 5min
    
    for threshold in thresholds:
        logger.info(f"\n🔍 Testing with {threshold}s threshold:")
        leader_info = await storage.get_leader_info(threshold)
        
        logger.info(f"  Leader: {leader_info['current_leader']}")
        logger.info(f"  Active pods: {leader_info['active_pods_count']} - {leader_info['active_pod_ids']}")
    
    await storage.close()
    logger.info("\n✅ Heartbeat demo completed!")

async def main():
    """
    Main demo function.
    """
    print("🎪 FLEET-Q Leader Election and Health Management Demo")
    print("=" * 60)
    
    try:
        await demonstrate_leader_election()
        await demonstrate_heartbeat_logic()
        
        print("\n🎉 All demos completed successfully!")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())