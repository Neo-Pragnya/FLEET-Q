"""
cgroup-aware CPU detection for Kubernetes / EKS pods (Python 3.11)

This module returns the *effective CPU cores* available to the container,
using cgroups (v2 preferred, v1 fallback). It also provides a safe fallback
to CPU affinity / os.cpu_count when the container has no CPU quota.

Why not os.cpu_count()?
- In containers, it may reflect the node, not the pod's CPU limit.

Test inside a pod:
- print(effective_cpu_cores())

Notes:
- Works on Linux containers (EKS).
- Handles both cgroup v2 (/sys/fs/cgroup/cpu.max) and v1 (cpu.cfs_* files).
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class CpuLimitInfo:
    effective_cores: float
    source: str
    quota_us: Optional[int] = None
    period_us: Optional[int] = None


def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except PermissionError:
        return None


def _cpu_from_cgroup_v2() -> Optional[CpuLimitInfo]:
    """
    cgroup v2: /sys/fs/cgroup/cpu.max
    content examples:
      - "max 100000"
      - "200000 100000"
    where quota and period are in microseconds.
    """
    cpu_max = Path("/sys/fs/cgroup/cpu.max")
    raw = _read_text(cpu_max)
    if not raw:
        return None

    parts = raw.split()
    if len(parts) != 2:
        return None

    quota_str, period_str = parts
    try:
        period_us = int(period_str)
    except ValueError:
        return None

    if quota_str == "max":
        # No CPU quota limit
        return CpuLimitInfo(
            effective_cores=float("inf"),
            source="cgroup_v2_unlimited",
            quota_us=None,
            period_us=period_us,
        )

    try:
        quota_us = int(quota_str)
    except ValueError:
        return None

    if quota_us <= 0 or period_us <= 0:
        return None

    return CpuLimitInfo(
        effective_cores=quota_us / period_us,
        source="cgroup_v2_cpu.max",
        quota_us=quota_us,
        period_us=period_us,
    )


def _cpu_from_cgroup_v1() -> Optional[CpuLimitInfo]:
    """
    cgroup v1:
      - /sys/fs/cgroup/cpu/cpu.cfs_quota_us  (quota in us; -1 means unlimited)
      - /sys/fs/cgroup/cpu/cpu.cfs_period_us (period in us)
    """
    quota_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")

    quota_raw = _read_text(quota_path)
    period_raw = _read_text(period_path)
    if quota_raw is None or period_raw is None:
        return None

    try:
        quota_us = int(quota_raw)
        period_us = int(period_raw)
    except ValueError:
        return None

    if period_us <= 0:
        return None

    if quota_us == -1:
        # No CPU quota limit
        return CpuLimitInfo(
            effective_cores=float("inf"),
            source="cgroup_v1_unlimited",
            quota_us=quota_us,
            period_us=period_us,
        )

    if quota_us <= 0:
        return None

    return CpuLimitInfo(
        effective_cores=quota_us / period_us,
        source="cgroup_v1_cfs_quota",
        quota_us=quota_us,
        period_us=period_us,
    )


def _cpu_from_affinity_or_os() -> CpuLimitInfo:
    """
    Fallback when no cgroup quota exists:
    - Prefer CPU affinity count (sched_getaffinity) if available
    - Else os.cpu_count()
    """
    try:
        # Linux-only; returns set of allowed CPU ids for this process
        affinity = os.sched_getaffinity(0)  # type: ignore[attr-defined]
        n = len(affinity)
        if n > 0:
            return CpuLimitInfo(effective_cores=float(n), source="sched_getaffinity")
    except Exception:
        pass

    n = os.cpu_count() or 1
    return CpuLimitInfo(effective_cores=float(n), source="os.cpu_count")


def cpu_limit_info() -> CpuLimitInfo:
    """
    Returns CPU limit info, preferring cgroups.

    If cgroups indicate "unlimited" CPU, we fall back to affinity/os count.
    """
    info = _cpu_from_cgroup_v2() or _cpu_from_cgroup_v1()
    if info is None:
        return _cpu_from_affinity_or_os()

    if math.isinf(info.effective_cores):
        # Unlimited quota: use allowed CPUs as "effective"
        return _cpu_from_affinity_or_os()

    # Some clusters allow fractional CPU, e.g. 1500m => 1.5 cores
    return info


def effective_cpu_cores(min_cores: float = 1.0) -> float:
    """
    Convenience: returns effective cores with a floor.
    """
    info = cpu_limit_info()
    return max(min_cores, info.effective_cores)


def recommended_process_count(reserve_fraction: float = 0.2, minimum: int = 1) -> int:
    """
    Compute a safe number of worker processes based on effective CPU cores.

    reserve_fraction:
      keep some CPU headroom for:
      - FastAPI runtime
      - IOHub (ZMQ + SQLite)
      - background threads
      - burst handling

    Example:
      8 cores, reserve 20% => 6.4 => 6 processes
    """
    cores = effective_cpu_cores()
    usable = cores * max(0.0, min(1.0, 1.0 - reserve_fraction))
    return max(minimum, int(math.floor(usable)))


# ============================================================================
# Memory Detection (Kubernetes pod memory limits)
# ============================================================================

@dataclass(frozen=True)
class MemoryLimitInfo:
    """Memory limit information from cgroups."""
    limit_bytes: Optional[int]
    usage_bytes: Optional[int]
    source: str


def _memory_from_cgroup_v2() -> Optional[MemoryLimitInfo]:
    """
    cgroup v2: /sys/fs/cgroup/memory.max and memory.current
    """
    max_path = Path("/sys/fs/cgroup/memory.max")
    current_path = Path("/sys/fs/cgroup/memory.current")
    
    max_raw = _read_text(max_path)
    current_raw = _read_text(current_path)
    
    if max_raw is None:
        return None
    
    limit_bytes = None if max_raw == "max" else int(max_raw)
    usage_bytes = int(current_raw) if current_raw else None
    
    return MemoryLimitInfo(
        limit_bytes=limit_bytes,
        usage_bytes=usage_bytes,
        source="cgroup_v2_memory"
    )


def _memory_from_cgroup_v1() -> Optional[MemoryLimitInfo]:
    """
    cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
    """
    limit_path = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    usage_path = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    
    limit_raw = _read_text(limit_path)
    usage_raw = _read_text(usage_path)
    
    if limit_raw is None:
        return None
    
    try:
        limit_bytes = int(limit_raw)
        usage_bytes = int(usage_raw) if usage_raw else None
    except ValueError:
        return None
    
    # cgroup v1 uses very large values to indicate "unlimited"
    if limit_bytes > 9223372036854771712:  # ~8 EiB
        limit_bytes = None
    
    return MemoryLimitInfo(
        limit_bytes=limit_bytes,
        usage_bytes=usage_bytes,
        source="cgroup_v1_memory"
    )


def memory_limit_info() -> MemoryLimitInfo:
    """Get memory limit information from cgroups."""
    info = _memory_from_cgroup_v2() or _memory_from_cgroup_v1()
    if info:
        return info
    
    return MemoryLimitInfo(
        limit_bytes=None,
        usage_bytes=None,
        source="no_cgroup_limit"
    )


def effective_memory_bytes() -> Optional[int]:
    """Get effective memory limit in bytes, or None if unlimited."""
    info = memory_limit_info()
    return info.limit_bytes


def effective_memory_gb() -> Optional[float]:
    """Get effective memory limit in GB, or None if unlimited."""
    bytes_limit = effective_memory_bytes()
    return bytes_limit / (1024**3) if bytes_limit else None


# ============================================================================
# Adaptive Configuration Helpers
# ============================================================================

def recommended_aiomultiprocess_workers(
    reserve_fraction: float = 0.25,
    minimum: int = 2,
    maximum: int = 8
) -> int:
    """
    Recommend number of aiomultiprocess worker processes.
    
    For HTTP-heavy workloads (Bedrock):
    - Reserve more CPU (25%) for FastAPI + IOHub + overhead
    - Cap at 8 to avoid too many processes competing for permits
    
    Example:
      8 cores, reserve 25% => 6 workers
      2 cores, reserve 25% => 2 workers (minimum)
    """
    cores = effective_cpu_cores()
    usable = cores * max(0.0, min(1.0, 1.0 - reserve_fraction))
    recommended = int(math.floor(usable))
    return max(minimum, min(maximum, recommended))


def recommended_fleet_claim_workers(
    reserve_fraction: float = 0.15,
    minimum: int = 1
) -> int:
    """
    Recommend number of workers for FLEET-Q claim loop.
    
    Claim loops are lightweight (mostly DB queries), so:
    - Reserve less CPU (15%)
    - Allow more workers than aiomultiprocess
    
    Example:
      8 cores, reserve 15% => 6 workers
      2 cores, reserve 15% => 1 worker
    """
    cores = effective_cpu_cores()
    usable = cores * max(0.0, min(1.0, 1.0 - reserve_fraction))
    return max(minimum, int(math.floor(usable)))


def recommended_iohub_flush_threads(
    base_threads: int = 2,
    per_core_threads: float = 0.5
) -> int:
    """
    Recommend number of threads for IOHub background flushers.
    
    Flushing (SQLite → Snowflake) is I/O-heavy:
    - Start with 2 base threads
    - Add 0.5 threads per CPU core
    
    Example:
      8 cores => 2 + (8 * 0.5) = 6 threads
      2 cores => 2 + (2 * 0.5) = 3 threads
    """
    cores = effective_cpu_cores()
    return base_threads + int(math.floor(cores * per_core_threads))


def recommended_async_concurrency(
    per_core_factor: float = 10.0,
    minimum: int = 20,
    maximum: int = 200
) -> int:
    """
    Recommend async concurrency limit per process.
    
    For HTTP-heavy async operations:
    - Use 10x CPU cores as a starting point
    - This is the *per-process* concurrency
    - IOHub will coordinate across processes
    
    Example:
      8 cores => 80 concurrent per process
      2 cores => 20 concurrent per process (minimum)
    """
    cores = effective_cpu_cores()
    recommended = int(cores * per_core_factor)
    return max(minimum, min(maximum, recommended))


# ============================================================================
# Resource Summary
# ============================================================================

@dataclass
class PodResourceSummary:
    """Complete pod resource summary."""
    # CPU
    cpu_cores: float
    cpu_source: str
    cpu_quota_us: Optional[int]
    cpu_period_us: Optional[int]
    
    # Memory
    memory_limit_bytes: Optional[int]
    memory_limit_gb: Optional[float]
    memory_usage_bytes: Optional[int]
    memory_source: str
    
    # Recommendations
    recommended_aiomultiprocess: int
    recommended_fleet_workers: int
    recommended_flush_threads: int
    recommended_async_per_process: int


def get_pod_resources() -> PodResourceSummary:
    """Get complete pod resource information with recommendations."""
    cpu_info = cpu_limit_info()
    mem_info = memory_limit_info()
    
    return PodResourceSummary(
        # CPU
        cpu_cores=cpu_info.effective_cores,
        cpu_source=cpu_info.source,
        cpu_quota_us=cpu_info.quota_us,
        cpu_period_us=cpu_info.period_us,
        
        # Memory
        memory_limit_bytes=mem_info.limit_bytes,
        memory_limit_gb=mem_info.limit_bytes / (1024**3) if mem_info.limit_bytes else None,
        memory_usage_bytes=mem_info.usage_bytes,
        memory_source=mem_info.source,
        
        # Recommendations
        recommended_aiomultiprocess=recommended_aiomultiprocess_workers(),
        recommended_fleet_workers=recommended_fleet_claim_workers(),
        recommended_flush_threads=recommended_iohub_flush_threads(),
        recommended_async_per_process=recommended_async_concurrency()
    )


def print_pod_resources():
    """Pretty-print pod resource information."""
    summary = get_pod_resources()
    
    print("\n" + "=" * 70)
    print("Pod Resource Summary")
    print("=" * 70)
    
    # CPU
    print("\n[CPU]")
    print(f"  Effective cores: {summary.cpu_cores:.2f}")
    print(f"  Source: {summary.cpu_source}")
    if summary.cpu_quota_us and summary.cpu_period_us:
        print(f"  Quota: {summary.cpu_quota_us}µs / {summary.cpu_period_us}µs")
    
    # Memory
    print("\n[Memory]")
    if summary.memory_limit_gb:
        print(f"  Limit: {summary.memory_limit_gb:.2f} GB ({summary.memory_limit_bytes:,} bytes)")
    else:
        print("  Limit: Unlimited")
    print(f"  Source: {summary.memory_source}")
    if summary.memory_usage_bytes:
        usage_gb = summary.memory_usage_bytes / (1024**3)
        print(f"  Current usage: {usage_gb:.2f} GB ({summary.memory_usage_bytes:,} bytes)")
    
    # Recommendations
    print("\n[Recommended Configuration]")
    print(f"  aiomultiprocess workers: {summary.recommended_aiomultiprocess}")
    print(f"  FLEET-Q claim workers: {summary.recommended_fleet_workers}")
    print(f"  IOHub flush threads: {summary.recommended_flush_threads}")
    print(f"  Async concurrency (per process): {summary.recommended_async_per_process}")
    
    print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    # Show detailed pod resource summary
    print_pod_resources()
    
    # Example usage
    print("\n" + "=" * 70)
    print("Example Configuration")
    print("=" * 70 + "\n")
    
    print("# In your main.py or config:")
    print(f"AIOMULTIPROCESS_WORKERS = {recommended_aiomultiprocess_workers()}")
    print(f"FLEET_CLAIM_WORKERS = {recommended_fleet_claim_workers()}")
    print(f"IOHUB_FLUSH_THREADS = {recommended_iohub_flush_threads()}")
    print(f"ASYNC_CONCURRENCY_PER_PROCESS = {recommended_async_concurrency()}")
    print()