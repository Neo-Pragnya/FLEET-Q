#!/usr/bin/env python3
"""
FastAPI Application Demo for FLEET-Q

Demonstrates the FastAPI application with all endpoints including:
- Health endpoint
- Step submission
- Step status queries
- Administrative endpoints (leader info, queue stats, manual recovery)

This example shows how to use the FLEET-Q API endpoints programmatically.
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Any

import httpx


class FleetQAPIDemo:
    """Demo client for FLEET-Q FastAPI application."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the demo client.
        
        Args:
            base_url: Base URL of the FLEET-Q API server
        """
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, timeout=30.0)
    
    def check_health(self) -> Dict[str, Any]:
        """Check the health status of the FLEET-Q pod."""
        print("🏥 Checking pod health...")
        
        response = self.client.get("/health")
        
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ Pod {health_data['pod_id']} is {health_data['status']}")
            print(f"   Uptime: {health_data['uptime_seconds']:.1f}s")
            print(f"   Capacity: {health_data['current_capacity']}/{health_data['max_capacity']}")
            print(f"   Leader: {'Yes' if health_data['is_leader'] else 'No'}")
            print(f"   Version: {health_data['version']}")
            return health_data
        else:
            print(f"❌ Health check failed: {response.status_code} - {response.text}")
            return {}
    
    def submit_step(self, step_type: str, args: Dict[str, Any] = None, 
                   metadata: Dict[str, Any] = None, priority: int = 0) -> str:
        """Submit a new step to the queue."""
        print(f"📤 Submitting step of type '{step_type}'...")
        
        payload = {
            "type": step_type,
            "args": args or {},
            "metadata": metadata or {},
            "priority": priority,
            "max_retries": 3
        }
        
        response = self.client.post("/submit", json=payload)
        
        if response.status_code == 200:
            result = response.json()
            step_id = result["step_id"]
            print(f"✅ Step submitted successfully: {step_id}")
            return step_id
        else:
            print(f"❌ Step submission failed: {response.status_code} - {response.text}")
            return ""
    
    def get_step_status(self, step_id: str) -> Dict[str, Any]:
        """Get the status of a specific step."""
        print(f"📊 Checking status of step {step_id}...")
        
        response = self.client.get(f"/status/{step_id}")
        
        if response.status_code == 200:
            status_data = response.json()
            print(f"✅ Step {step_id}:")
            print(f"   Status: {status_data['status']}")
            print(f"   Claimed by: {status_data['claimed_by'] or 'None'}")
            print(f"   Retry count: {status_data['retry_count']}/{status_data['max_retries']}")
            print(f"   Priority: {status_data['priority']}")
            print(f"   Created: {status_data['created_ts']}")
            print(f"   Last update: {status_data['last_update_ts']}")
            return status_data
        elif response.status_code == 404:
            print(f"❌ Step {step_id} not found")
            return {}
        else:
            print(f"❌ Status check failed: {response.status_code} - {response.text}")
            return {}
    
    def get_idempotency_key(self, step_id: str) -> str:
        """Get the idempotency key for a step."""
        print(f"🔑 Getting idempotency key for step {step_id}...")
        
        response = self.client.get(f"/idempotency/{step_id}")
        
        if response.status_code == 200:
            result = response.json()
            key = result["idempotency_key"]
            print(f"✅ Idempotency key: {key}")
            return key
        else:
            print(f"❌ Idempotency key retrieval failed: {response.status_code} - {response.text}")
            return ""
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get current queue statistics."""
        print("📈 Getting queue statistics...")
        
        response = self.client.get("/admin/queue")
        
        if response.status_code == 200:
            stats = response.json()
            print("✅ Queue Statistics:")
            print(f"   Pending steps: {stats['pending_steps']}")
            print(f"   Claimed steps: {stats['claimed_steps']}")
            print(f"   Completed steps: {stats['completed_steps']}")
            print(f"   Failed steps: {stats['failed_steps']}")
            print(f"   Active pods: {stats['active_pods']}")
            print(f"   Current leader: {stats['current_leader'] or 'None'}")
            return stats
        else:
            print(f"❌ Queue stats retrieval failed: {response.status_code} - {response.text}")
            return {}
    
    def get_leader_info(self) -> Dict[str, Any]:
        """Get leader election information."""
        print("👑 Getting leader election information...")
        
        response = self.client.get("/admin/leader")
        
        if response.status_code == 200:
            leader_info = response.json()
            print("✅ Leader Election Info:")
            print(f"   Current leader: {leader_info['current_leader'] or 'None'}")
            print(f"   Leader birth time: {leader_info['leader_birth_ts'] or 'N/A'}")
            print(f"   Active pods: {leader_info['active_pods']}")
            print(f"   Last election check: {leader_info['last_election_check']}")
            return leader_info
        else:
            print(f"❌ Leader info retrieval failed: {response.status_code} - {response.text}")
            return {}
    
    def trigger_manual_recovery(self) -> Dict[str, Any]:
        """Trigger manual recovery cycle (leader only)."""
        print("🔄 Triggering manual recovery...")
        
        response = self.client.post("/admin/recovery/run")
        
        if response.status_code == 200:
            result = response.json()
            summary = result["recovery_summary"]
            print("✅ Manual recovery completed:")
            print(f"   Cycle ID: {summary['cycle_id']}")
            print(f"   Dead pods found: {summary['dead_pods_found']}")
            print(f"   Orphaned steps found: {summary['orphaned_steps_found']}")
            print(f"   Steps requeued: {summary['steps_requeued']}")
            print(f"   Steps marked terminal: {summary['steps_marked_terminal']}")
            print(f"   Duration: {summary['duration_ms']}ms")
            if summary['dead_pods']:
                print(f"   Dead pods: {', '.join(summary['dead_pods'])}")
            return result
        elif response.status_code == 403:
            print("❌ Manual recovery can only be triggered by the leader pod")
            return {}
        else:
            print(f"❌ Manual recovery failed: {response.status_code} - {response.text}")
            return {}
    
    def run_demo(self):
        """Run a complete demonstration of the API."""
        print("🚀 Starting FLEET-Q FastAPI Demo")
        print("=" * 50)
        
        try:
            # 1. Check health
            health = self.check_health()
            if not health:
                print("❌ Cannot connect to FLEET-Q API. Make sure the server is running.")
                return
            
            print("\n" + "-" * 50)
            
            # 2. Get leader info
            self.get_leader_info()
            
            print("\n" + "-" * 50)
            
            # 3. Get queue stats
            self.get_queue_stats()
            
            print("\n" + "-" * 50)
            
            # 4. Submit some test steps
            step_ids = []
            
            # Submit a high priority step
            step_id = self.submit_step(
                "data_processing",
                args={"input_file": "data.csv", "output_format": "json"},
                metadata={"user": "demo", "timestamp": datetime.utcnow().isoformat()},
                priority=10
            )
            if step_id:
                step_ids.append(step_id)
            
            # Submit a normal priority step
            step_id = self.submit_step(
                "email_notification",
                args={"recipient": "user@example.com", "subject": "Demo notification"},
                metadata={"source": "demo_script"}
            )
            if step_id:
                step_ids.append(step_id)
            
            # Submit a low priority step
            step_id = self.submit_step(
                "cleanup_task",
                args={"directory": "/tmp/demo"},
                priority=-5
            )
            if step_id:
                step_ids.append(step_id)
            
            print("\n" + "-" * 50)
            
            # 5. Check status of submitted steps
            for step_id in step_ids:
                self.get_step_status(step_id)
                print()
            
            print("-" * 50)
            
            # 6. Get idempotency keys
            for step_id in step_ids[:2]:  # Just first two
                self.get_idempotency_key(step_id)
            
            print("\n" + "-" * 50)
            
            # 7. Get updated queue stats
            self.get_queue_stats()
            
            print("\n" + "-" * 50)
            
            # 8. Try manual recovery (will fail if not leader)
            self.trigger_manual_recovery()
            
            print("\n" + "=" * 50)
            print("✅ Demo completed successfully!")
            
        except Exception as e:
            print(f"❌ Demo failed with error: {e}")
        finally:
            self.client.close()


def main():
    """Main function to run the demo."""
    import argparse
    
    parser = argparse.ArgumentParser(description="FLEET-Q FastAPI Demo")
    parser.add_argument(
        "--url", 
        default="http://localhost:8000",
        help="Base URL of the FLEET-Q API server (default: http://localhost:8000)"
    )
    
    args = parser.parse_args()
    
    demo = FleetQAPIDemo(args.url)
    demo.run_demo()


if __name__ == "__main__":
    main()