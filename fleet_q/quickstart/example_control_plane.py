"""
FLEET-Q Control Plane Example Usage

This example demonstrates:
1. Submitting bulk write operations
2. Monitoring control plane statistics
3. Manual flushing
4. Database maintenance
5. ORM-agnostic batching
6. Dynamic scaling observation
"""

import asyncio
import json
import time
from typing import List, Dict, Any
import httpx


# ============================================================================
# Configuration
# ============================================================================
BASE_URL = "http://localhost:8000"


# ============================================================================
# Helper Functions
# ============================================================================

async def submit_write(
    client: httpx.AsyncClient,
    writer_type: str,
    destination: str,
    data: Dict[str, Any],
    orm_type: str = None,
    priority: int = 0
) -> Dict[str, Any]:
    """Submit a single write operation"""
    response = await client.post(
        f"{BASE_URL}/control-plane/write",
        json={
            "writer_type": writer_type,
            "destination": destination,
            "data": data,
            "orm_type": orm_type,
            "priority": priority
        }
    )
    return response.json()


async def get_stats(client: httpx.AsyncClient) -> Dict[str, Any]:
    """Get control plane statistics"""
    response = await client.get(f"{BASE_URL}/control-plane/stats")
    return response.json()


async def trigger_flush(client: httpx.AsyncClient) -> Dict[str, Any]:
    """Manually trigger flush"""
    response = await client.post(f"{BASE_URL}/control-plane/flush")
    return response.json()


async def trigger_maintenance(client: httpx.AsyncClient) -> Dict[str, Any]:
    """Manually trigger maintenance"""
    response = await client.post(f"{BASE_URL}/control-plane/maintenance")
    return response.json()


def print_stats(stats: Dict[str, Any]):
    """Pretty print control plane statistics"""
    print("\n" + "=" * 80)
    print("CONTROL PLANE STATISTICS")
    print("=" * 80)
    
    if not stats['enabled']:
        print("⚠️  Control Plane is NOT enabled")
        return
    
    print(f"\n📊 Status:")
    print(f"   Pod ID: {stats['pod_id']}")
    print(f"   Running: {'✅ Yes' if stats['running'] else '❌ No'}")
    
    buffer_stats = stats['buffer_stats']
    print(f"\n📦 Buffer Statistics:")
    print(f"   Total Buffered: {buffer_stats['total_buffered']}")
    print(f"   Buffer Count: {buffer_stats['buffer_count']}")
    print(f"   Last Flush: {buffer_stats['last_flush_ago']:.1f}s ago")
    
    if buffer_stats['buffers']:
        print(f"\n   Buffers by Destination:")
        for key, count in buffer_stats['buffers'].items():
            print(f"      {key}: {count} operations")
    
    storage_stats = stats['storage_stats']
    print(f"\n💾 Storage Statistics:")
    print(f"   Pending Operations: {storage_stats['pending_operations']}")
    print(f"   Completed Operations: {storage_stats['completed_operations']}")
    print(f"   Database Size: {storage_stats['database_size_bytes'] / 1024:.2f} KB")
    print(f"   Database Path: {storage_stats['database_path']}")
    
    if storage_stats['destination_counts']:
        print(f"\n   Operations by Destination:")
        for dest, count in storage_stats['destination_counts'].items():
            print(f"      {dest}: {count} operations")
    
    writer_pool = stats['writer_pool']
    print(f"\n⚙️  Writer Pool:")
    print(f"   Active Workers: {writer_pool['active_workers']}")
    print(f"   Queue Depth: {writer_pool['queue_depth']}")
    
    print("=" * 80)


# ============================================================================
# Example 1: Basic Bulk Writes
# ============================================================================

async def example_basic_bulk_writes():
    """Submit basic bulk write operations to Snowflake"""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Basic Bulk Writes")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Submit 10 operations to Snowflake
        print("\n📝 Submitting 10 write operations to Snowflake...")
        
        for i in range(10):
            result = await submit_write(
                client,
                writer_type="snowflake",
                destination="EVENTS_TABLE",
                data={
                    "event_id": i,
                    "event_type": "user_action",
                    "user_id": 123,
                    "timestamp": time.time(),
                    "metadata": {"action": f"action_{i}"}
                },
                orm_type="sqlalchemy"
            )
            print(f"   ✅ {result['operation_id']}: {result['message']}")
        
        # Check stats
        print("\n📊 Current statistics:")
        stats = await get_stats(client)
        print_stats(stats)


# ============================================================================
# Example 2: ORM-Agnostic Batching
# ============================================================================

async def example_orm_batching():
    """Demonstrate ORM-agnostic batching"""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: ORM-Agnostic Batching")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Submit operations with different ORM types
        print("\n📝 Submitting operations with different ORM types...")
        
        # SQLAlchemy operations
        print("\n   SQLAlchemy operations:")
        for i in range(5):
            result = await submit_write(
                client,
                writer_type="snowflake",
                destination="USERS_TABLE",
                data={"user_id": i, "name": f"User {i}"},
                orm_type="sqlalchemy"
            )
            print(f"      ✅ {result['operation_id']}")
        
        # Django ORM operations
        print("\n   Django ORM operations:")
        for i in range(5):
            result = await submit_write(
                client,
                writer_type="snowflake",
                destination="USERS_TABLE",
                data={"user_id": i + 100, "name": f"Django User {i}"},
                orm_type="django"
            )
            print(f"      ✅ {result['operation_id']}")
        
        # Raw SQL operations
        print("\n   Raw SQL operations:")
        for i in range(5):
            result = await submit_write(
                client,
                writer_type="snowflake",
                destination="USERS_TABLE",
                data={"user_id": i + 200, "name": f"Raw User {i}"},
                orm_type="raw"
            )
            print(f"      ✅ {result['operation_id']}")
        
        # Check stats - should see 3 separate buffers
        print("\n📊 Statistics (should show 3 separate buffers):")
        stats = await get_stats(client)
        print_stats(stats)
        
        print("\n💡 Note: Operations are grouped by (writer_type, destination, orm_type)")
        print("   This creates 3 separate batches that will be written independently.")


# ============================================================================
# Example 3: Multi-Destination Writes
# ============================================================================

async def example_multi_destination():
    """Write to multiple destinations"""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Multi-Destination Writes")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        print("\n📝 Submitting operations to multiple destinations...")
        
        # Snowflake writes
        print("\n   Snowflake → EVENTS_TABLE:")
        for i in range(10):
            result = await submit_write(
                client,
                writer_type="snowflake",
                destination="EVENTS_TABLE",
                data={"event_id": i, "type": "event"}
            )
            print(f"      ✅ {result['operation_id']}")
        
        # SharePoint writes
        print("\n   SharePoint → documents:")
        for i in range(5):
            result = await submit_write(
                client,
                writer_type="sharepoint",
                destination="documents/reports",
                data={"file_name": f"report_{i}.pdf", "content": "base64..."},
                priority=1
            )
            print(f"      ✅ {result['operation_id']}")
        
        # Bedrock writes
        print("\n   Bedrock → inference:")
        for i in range(8):
            result = await submit_write(
                client,
                writer_type="bedrock",
                destination="inference/batch",
                data={"prompt": f"Question {i}", "model": "claude-3-sonnet"},
                priority=2
            )
            print(f"      ✅ {result['operation_id']}")
        
        # Check stats
        print("\n📊 Statistics (should show buffers for each destination):")
        stats = await get_stats(client)
        print_stats(stats)


# ============================================================================
# Example 4: Manual Flush
# ============================================================================

async def example_manual_flush():
    """Demonstrate manual flush"""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Manual Flush")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Submit some operations
        print("\n📝 Submitting 20 operations...")
        for i in range(20):
            await submit_write(
                client,
                writer_type="snowflake",
                destination="MANUAL_FLUSH_TABLE",
                data={"id": i, "value": f"data_{i}"}
            )
        
        # Check stats before flush
        print("\n📊 Statistics BEFORE flush:")
        stats_before = await get_stats(client)
        print_stats(stats_before)
        
        # Trigger manual flush
        print("\n🔄 Triggering manual flush...")
        flush_result = await trigger_flush(client)
        print(f"   Message: {flush_result['message']}")
        print(f"   Batches Flushed: {flush_result['batches_flushed']}")
        print(f"   Total Operations: {flush_result['total_operations']}")
        
        # Wait a moment for processing
        await asyncio.sleep(2)
        
        # Check stats after flush
        print("\n📊 Statistics AFTER flush:")
        stats_after = await get_stats(client)
        print_stats(stats_after)


# ============================================================================
# Example 5: Observe Dynamic Scaling
# ============================================================================

async def example_dynamic_scaling():
    """Observe dynamic scaling of writer pool"""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Dynamic Scaling Observation")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        print("\n📝 Submitting operations in batches to trigger scaling...")
        
        # Batch 1: 50 operations (should keep 1 writer)
        print("\n   Batch 1: 50 operations")
        for i in range(50):
            await submit_write(
                client,
                writer_type="snowflake",
                destination="SCALING_TABLE",
                data={"batch": 1, "id": i}
            )
        
        stats = await get_stats(client)
        print(f"   Active Writers: {stats['writer_pool']['active_workers']}")
        print(f"   Queue Depth: {stats['writer_pool']['queue_depth']}")
        
        # Batch 2: 150 more operations (should scale to 2-3 writers)
        print("\n   Batch 2: 150 more operations (total: 200)")
        for i in range(150):
            await submit_write(
                client,
                writer_type="snowflake",
                destination="SCALING_TABLE",
                data={"batch": 2, "id": i}
            )
        
        # Trigger flush to create batches
        await trigger_flush(client)
        await asyncio.sleep(1)
        
        stats = await get_stats(client)
        print(f"   Active Writers: {stats['writer_pool']['active_workers']}")
        print(f"   Queue Depth: {stats['writer_pool']['queue_depth']}")
        
        # Batch 3: 800 more operations (should scale to 4-5 writers)
        print("\n   Batch 3: 800 more operations (total: 1000)")
        for i in range(800):
            await submit_write(
                client,
                writer_type="snowflake",
                destination="SCALING_TABLE",
                data={"batch": 3, "id": i}
            )
        
        # Trigger flush
        await trigger_flush(client)
        await asyncio.sleep(1)
        
        stats = await get_stats(client)
        print(f"   Active Workers: {stats['writer_pool']['active_workers']}")
        print(f"   Queue Depth: {stats['writer_pool']['queue_depth']}")
        
        print("\n📊 Final statistics:")
        print_stats(stats)
        
        print("\n💡 Scaling behavior:")
        print("   Queue < 100: 1 writer")
        print("   Queue 100-500: 2-3 writers")
        print("   Queue 500-1000: 4-5 writers")
        print("   Queue > 1000: 6-8 workers (max)")


# ============================================================================
# Example 6: Database Maintenance
# ============================================================================

async def example_maintenance():
    """Demonstrate database maintenance"""
    print("\n" + "=" * 80)
    print("EXAMPLE 6: Database Maintenance")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Check stats before maintenance
        print("\n📊 Statistics BEFORE maintenance:")
        stats_before = await get_stats(client)
        print_stats(stats_before)
        
        # Trigger maintenance
        print("\n🧹 Triggering database maintenance...")
        maintenance_result = await trigger_maintenance(client)
        print(f"   Message: {maintenance_result['message']}")
        
        # Check stats after maintenance
        print("\n📊 Statistics AFTER maintenance:")
        stats_after = await get_stats(client)
        print_stats(stats_after)
        
        print("\n💡 Maintenance actions:")
        print("   - Deletes completed operations older than 24 hours")
        print("   - Deletes processed batch history")
        print("   - Logs maintenance actions")
        print("   - Optimizes database file")


# ============================================================================
# Example 7: Complete Workflow
# ============================================================================

async def example_complete_workflow():
    """Complete workflow example"""
    print("\n" + "=" * 80)
    print("EXAMPLE 7: Complete Workflow")
    print("=" * 80)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Step 1: Submit bulk operations
        print("\n📝 Step 1: Submitting bulk operations...")
        for i in range(50):
            await submit_write(
                client,
                writer_type="snowflake",
                destination="WORKFLOW_TABLE",
                data={"workflow_id": i, "step": "process", "status": "pending"},
                orm_type="sqlalchemy"
            )
        print(f"   ✅ Submitted 50 operations")
        
        # Step 2: Check statistics
        print("\n📊 Step 2: Checking statistics...")
        stats = await get_stats(client)
        print(f"   Buffered: {stats['buffer_stats']['total_buffered']}")
        print(f"   Active Writers: {stats['writer_pool']['active_workers']}")
        
        # Step 3: Wait for automatic flush (20 seconds by default)
        print("\n⏳ Step 3: Waiting for automatic flush (20 seconds)...")
        for seconds in range(20, 0, -1):
            print(f"   Flushing in {seconds}s...", end="\r")
            await asyncio.sleep(1)
        print("\n")
        
        # Step 4: Check if flushed
        print("\n📊 Step 4: Checking if operations were flushed...")
        stats = await get_stats(client)
        print(f"   Buffered: {stats['buffer_stats']['total_buffered']}")
        print(f"   Queue Depth: {stats['writer_pool']['queue_depth']}")
        
        # Step 5: Manual flush if needed
        if stats['buffer_stats']['total_buffered'] > 0:
            print("\n🔄 Step 5: Manually flushing remaining operations...")
            flush_result = await trigger_flush(client)
            print(f"   Flushed: {flush_result['total_operations']} operations")
        
        # Step 6: Final statistics
        print("\n📊 Step 6: Final statistics...")
        final_stats = await get_stats(client)
        print_stats(final_stats)
        
        print("\n✅ Workflow complete!")


# ============================================================================
# Main Runner
# ============================================================================

async def run_all_examples():
    """Run all examples"""
    try:
        await example_basic_bulk_writes()
        await asyncio.sleep(2)
        
        await example_orm_batching()
        await asyncio.sleep(2)
        
        await example_multi_destination()
        await asyncio.sleep(2)
        
        await example_manual_flush()
        await asyncio.sleep(2)
        
        await example_dynamic_scaling()
        await asyncio.sleep(2)
        
        await example_maintenance()
        await asyncio.sleep(2)
        
        await example_complete_workflow()
        
    except httpx.ConnectError:
        print("\n❌ ERROR: Could not connect to FLEET-Q server")
        print("   Make sure the server is running at http://localhost:8000")
        print("\n   Start the server with:")
        print("   uvicorn fleet_q.quickstart.main:app --reload")
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main entry point"""
    print("\n" + "=" * 80)
    print("FLEET-Q CONTROL PLANE EXAMPLES")
    print("=" * 80)
    print("\nThis script demonstrates all control plane features:")
    print("1. Basic bulk writes")
    print("2. ORM-agnostic batching")
    print("3. Multi-destination writes")
    print("4. Manual flush")
    print("5. Dynamic scaling")
    print("6. Database maintenance")
    print("7. Complete workflow")
    print("\nMake sure FLEET-Q is running at http://localhost:8000")
    print("=" * 80)
    
    # Run all examples
    asyncio.run(run_all_examples())
    
    print("\n" + "=" * 80)
    print("ALL EXAMPLES COMPLETED!")
    print("=" * 80)


if __name__ == "__main__":
    main()
