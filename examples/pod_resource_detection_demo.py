"""
Pod Resource Detection Demo

Demonstrates how FLEET-Q automatically detects Kubernetes pod resources
and configures optimal worker counts for different workload types.

This script shows:
1. Detecting CPU and memory limits from cgroups
2. Getting recommended worker counts for different workload types
3. Comparing adaptive vs. hard-coded configurations
4. Simulating different pod sizes (1 core, 4 cores, 8 cores)

Requirements:
- Python 3.11+
- fleet_q package installed

Usage:
    python pod_resource_detection_demo.py

In Kubernetes:
    kubectl exec -it fleet-q-worker-001 -- python examples/pod_resource_detection_demo.py
"""

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Add parent directory to path for local development
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import pod resource utilities
try:
    from fleet_q.cgroup_aware_resources import (
        effective_cpu_cores,
        effective_memory_gb,
        recommended_fleet_claim_workers,
        recommended_aiomultiprocess_workers,
        recommended_iohub_flush_threads,
        recommended_async_concurrency,
        get_pod_resources,
        print_pod_resources,
    )
    DETECTION_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  fleet_q.cgroup_aware_resources not available: {e}")
    print("Make sure fleet_q package is installed or run from FLEET-Q directory")
    DETECTION_AVAILABLE = False


@dataclass
class WorkloadProfile:
    """Configuration profile for different workload types"""
    name: str
    description: str
    fleet_workers: int
    aiomultiprocess_workers: int
    iohub_flush_threads: int
    async_concurrency_per_process: int


def get_adaptive_profile() -> Optional[WorkloadProfile]:
    """Get adaptive configuration based on detected pod resources"""
    if not DETECTION_AVAILABLE:
        return None
    
    resources = get_pod_resources()
    
    return WorkloadProfile(
        name="Adaptive (Auto-Detected)",
        description=f"Optimized for {resources.cpu_cores:.1f} cores, {resources.memory_gb:.1f} GB",
        fleet_workers=resources.recommended_fleet_workers,
        aiomultiprocess_workers=resources.recommended_aiomultiprocess,
        iohub_flush_threads=resources.recommended_iohub_flush_threads,
        async_concurrency_per_process=resources.recommended_async_concurrency,
    )


def get_hardcoded_profile() -> WorkloadProfile:
    """Get hard-coded configuration (no adaptation)"""
    return WorkloadProfile(
        name="Hard-Coded (Default)",
        description="Fixed values, no adaptation",
        fleet_workers=8,
        aiomultiprocess_workers=4,
        iohub_flush_threads=4,
        async_concurrency_per_process=40,
    )


def calculate_total_concurrency(profile: WorkloadProfile) -> int:
    """Calculate total concurrent operations possible"""
    # Total = (aiomultiprocess workers) × (async concurrency per process)
    return profile.aiomultiprocess_workers * profile.async_concurrency_per_process


def compare_profiles(adaptive: WorkloadProfile, hardcoded: WorkloadProfile):
    """Compare adaptive vs. hard-coded configuration"""
    print("\n" + "="*80)
    print("CONFIGURATION COMPARISON")
    print("="*80)
    
    print(f"\n📊 {adaptive.name}")
    print(f"   {adaptive.description}")
    print(f"   Fleet Workers: {adaptive.fleet_workers}")
    print(f"   AIOMultiprocess Workers: {adaptive.aiomultiprocess_workers}")
    print(f"   IOHub Flush Threads: {adaptive.iohub_flush_threads}")
    print(f"   Async Concurrency (per process): {adaptive.async_concurrency_per_process}")
    print(f"   Total HTTP Concurrency: {calculate_total_concurrency(adaptive)}")
    
    print(f"\n📦 {hardcoded.name}")
    print(f"   {hardcoded.description}")
    print(f"   Fleet Workers: {hardcoded.fleet_workers}")
    print(f"   AIOMultiprocess Workers: {hardcoded.aiomultiprocess_workers}")
    print(f"   IOHub Flush Threads: {hardcoded.iohub_flush_threads}")
    print(f"   Async Concurrency (per process): {hardcoded.async_concurrency_per_process}")
    print(f"   Total HTTP Concurrency: {calculate_total_concurrency(hardcoded)}")
    
    # Calculate differences
    print("\n📈 Impact Analysis:")
    
    fleet_diff = adaptive.fleet_workers - hardcoded.fleet_workers
    if fleet_diff > 0:
        print(f"   ✅ Fleet Workers: +{fleet_diff} ({fleet_diff/hardcoded.fleet_workers*100:.1f}% more parallelism)")
    elif fleet_diff < 0:
        print(f"   ✅ Fleet Workers: {fleet_diff} ({abs(fleet_diff)/hardcoded.fleet_workers*100:.1f}% less thrashing)")
    else:
        print(f"   ➖ Fleet Workers: No change")
    
    aio_diff = adaptive.aiomultiprocess_workers - hardcoded.aiomultiprocess_workers
    if aio_diff != 0:
        change_pct = aio_diff / hardcoded.aiomultiprocess_workers * 100
        if aio_diff > 0:
            print(f"   ✅ AIOMultiprocess: +{aio_diff} ({change_pct:.1f}% more processes)")
        else:
            print(f"   ✅ AIOMultiprocess: {aio_diff} ({abs(change_pct):.1f}% less overhead)")
    
    concurrency_diff = calculate_total_concurrency(adaptive) - calculate_total_concurrency(hardcoded)
    if concurrency_diff != 0:
        change_pct = concurrency_diff / calculate_total_concurrency(hardcoded) * 100
        if concurrency_diff > 0:
            print(f"   ✅ Total HTTP Concurrency: +{concurrency_diff} ({change_pct:.1f}% more throughput)")
        else:
            print(f"   ✅ Total HTTP Concurrency: {concurrency_diff} ({abs(change_pct):.1f}% less memory usage)")


def simulate_pod_sizes():
    """Simulate different pod sizes and show recommendations"""
    print("\n" + "="*80)
    print("POD SIZE SIMULATION")
    print("="*80)
    
    pod_sizes = [
        ("Small", 1.0, 2.0),    # 1 core, 2 GB
        ("Medium", 4.0, 8.0),   # 4 cores, 8 GB
        ("Large", 8.0, 16.0),   # 8 cores, 16 GB
        ("XLarge", 16.0, 32.0), # 16 cores, 32 GB
    ]
    
    print("\n| Pod Size | Cores | Memory | Fleet Workers | AIOMulti | Flush Threads | HTTP Concurrency |")
    print("|----------|-------|--------|---------------|----------|---------------|------------------|")
    
    for size_name, cores, memory_gb in pod_sizes:
        # Manually calculate recommendations
        fleet = max(1, int(cores * 0.85))
        aio = max(2, min(8, int(cores * 0.75)))
        flush = max(2, int(2 + cores * 0.5))
        concurrency = min(200, max(20, int(cores * 10)))
        total_http = aio * concurrency
        
        print(f"| {size_name:8} | {cores:5.1f} | {memory_gb:4.1f} GB | {fleet:13} | {aio:8} | {flush:13} | {total_http:16} |")


def demo_manual_detection():
    """Demonstrate manual resource detection"""
    if not DETECTION_AVAILABLE:
        return
    
    print("\n" + "="*80)
    print("MANUAL RESOURCE DETECTION")
    print("="*80)
    
    cores = effective_cpu_cores()
    memory = effective_memory_gb()
    
    print(f"\n🔍 Raw Detection:")
    print(f"   CPU Cores: {cores:.2f}")
    if memory:
        print(f"   Memory: {memory:.2f} GB")
    else:
        print(f"   Memory: Unlimited (no cgroup limit detected)")
    
    print(f"\n🎯 Recommendations by Workload Type:")
    
    # Fleet claim workers (CPU-bound with I/O)
    fleet = recommended_fleet_claim_workers(reserve_fraction=0.15, minimum=1)
    print(f"   Fleet Claim Workers: {fleet}")
    print(f"      Use for: Step claiming and execution")
    print(f"      Reserve: 15% CPU for heartbeats, leader checks")
    
    # AIOMultiprocess workers (HTTP-heavy)
    aio = recommended_aiomultiprocess_workers(reserve_fraction=0.25, minimum=2, maximum=8)
    print(f"   AIOMultiprocess Workers: {aio}")
    print(f"      Use for: High-concurrency HTTP requests")
    print(f"      Reserve: 25% CPU for FastAPI, IOHub, background tasks")
    
    # IOHub flush threads (I/O-bound)
    flush = recommended_iohub_flush_threads(base=2, per_core=0.5)
    print(f"   IOHub Flush Threads: {flush}")
    print(f"      Use for: Flushing SQLite outbox to Snowflake")
    print(f"      Base: 2 threads + 0.5 per core")
    
    # Async concurrency (per process)
    async_conc = recommended_async_concurrency(factor=10, min_value=20, max_value=200)
    print(f"   Async Concurrency (per process): {async_conc}")
    print(f"      Use for: Concurrent HTTP requests in single event loop")
    print(f"      Formula: cores × 10 (I/O can handle 10x CPU cores)")


def demo_environment_variables():
    """Show how to configure via environment variables"""
    print("\n" + "="*80)
    print("ENVIRONMENT VARIABLE CONFIGURATION")
    print("="*80)
    
    if not DETECTION_AVAILABLE:
        print("\n⚠️  Detection unavailable, showing example only")
    
    resources = get_pod_resources() if DETECTION_AVAILABLE else None
    
    print("\n💡 Use these environment variables to override auto-detection:")
    print()
    print("# Enable/disable adaptive configuration")
    print("export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=true")
    print()
    print("# Override specific values")
    if resources:
        print(f"export FLEET_Q_MAX_PARALLELISM={resources.recommended_fleet_workers}")
        print(f"export FLEET_Q_AIOMULTIPROCESS_WORKERS={resources.recommended_aiomultiprocess}")
        print(f"export FLEET_Q_IOHUB_FLUSH_THREADS={resources.recommended_iohub_flush_threads}")
    else:
        print("export FLEET_Q_MAX_PARALLELISM=3")
        print("export FLEET_Q_AIOMULTIPROCESS_WORKERS=3")
        print("export FLEET_Q_IOHUB_FLUSH_THREADS=4")
    print()
    print("# Or disable adaptive config and use hard-coded defaults")
    print("export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=false")
    print("export FLEET_Q_MAX_PARALLELISM=8")


def main():
    """Main demo function"""
    print("\n" + "="*80)
    print("FLEET-Q POD RESOURCE DETECTION DEMO")
    print("="*80)
    
    if not DETECTION_AVAILABLE:
        print("\n❌ Pod resource detection not available")
        print("Make sure the fleet_q package is installed")
        print()
        print("Installation:")
        print("  pip install fleet-q")
        print()
        sys.exit(1)
    
    # 1. Show current pod resources
    print("\n1️⃣  CURRENT POD RESOURCES")
    print("-" * 80)
    print_pod_resources()
    
    # 2. Manual detection demo
    demo_manual_detection()
    
    # 3. Compare adaptive vs. hard-coded
    print("\n2️⃣  ADAPTIVE vs. HARD-CODED CONFIGURATION")
    print("-" * 80)
    adaptive = get_adaptive_profile()
    hardcoded = get_hardcoded_profile()
    if adaptive:
        compare_profiles(adaptive, hardcoded)
    
    # 4. Simulate different pod sizes
    print("\n3️⃣  POD SIZE RECOMMENDATIONS")
    print("-" * 80)
    simulate_pod_sizes()
    
    # 5. Environment variable configuration
    demo_environment_variables()
    
    # Final summary
    print("\n" + "="*80)
    print("KEY TAKEAWAYS")
    print("="*80)
    print("""
✅ Adaptive configuration automatically adjusts to pod resources
✅ Prevents over-subscription in small pods (1-2 cores)
✅ Maximizes throughput in large pods (8+ cores)
✅ Reserves CPU for overhead (FastAPI, heartbeats, IOHub)
✅ Can be overridden via environment variables

📖 Learn more:
   docs/POD_RESOURCES_GUIDE.md
   docs/AIOMULTIPROCESS_GUIDE.md
""")


if __name__ == "__main__":
    main()
