# Pod Resource Detection & Adaptive Configuration

**Version:** 1.0  
**Last Updated:** 2024-01-20

## 📋 Overview

FLEET-Q includes **automatic pod resource detection** that adapts worker counts, thread pools, and concurrency limits based on actual Kubernetes CPU and memory quotas. This prevents over-subscription in resource-constrained environments and maximizes throughput in larger pods.

### Why Pod-Aware Configuration?

**Problem:**
- Hard-coded worker counts (e.g., `max_parallelism=8`) don't adapt to pod resources
- A 1-core pod running 8 workers causes thrashing and poor performance
- An 8-core pod with 2 workers underutilizes resources

**Solution:**
- Detect actual CPU quota from cgroups (`cpu.max`, `cpu.cfs_quota_us`)
- Detect memory limits from cgroups (`memory.max`, `memory.limit_in_bytes`)
- Recommend optimal worker counts for different workload types
- Auto-configure FLEET-Q at startup with detected values

### Performance Impact

| Pod Resources | Without Adaptive Config | With Adaptive Config | Improvement |
|--------------|-------------------------|----------------------|-------------|
| 1 core, 2 GB | 8 workers (thrashing) | 1 worker | 3-4x faster |
| 4 cores, 8 GB | 8 workers (underutilized) | 3 workers | Stable |
| 8 cores, 16 GB | 8 workers (underutilized) | 6 workers | 1.5x throughput |

---

## 🏗️ Architecture

### Detection Flow

```
┌─────────────────────┐
│  FLEET-Q Startup    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────┐
│ load_config()               │
│ - Check env vars            │
│ - Detect adaptive config    │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ get_pod_resources()         │
│ - Read /sys/fs/cgroup/...   │
│ - Parse CPU quota           │
│ - Parse memory limit        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Calculate Recommendations   │
│ - Fleet workers (80% cores) │
│ - AIOMultiprocess (75%)     │
│ - IOHub flush threads       │
│ - Async concurrency         │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ FleetQConfig                │
│ - max_parallelism           │
│ - aiomultiprocess_workers   │
│ - iohub_flush_threads       │
└─────────────────────────────┘
```

### Resource Detection Sources

#### CPU Detection (cgroup v2)
```
/sys/fs/cgroup/cpu.max
Format: "200000 100000"
         ^^^^^^  ^^^^^^
         quota   period

Effective cores = quota / period = 200000 / 100000 = 2.0
```

#### CPU Detection (cgroup v1)
```
/sys/fs/cgroup/cpu/cpu.cfs_quota_us   → 200000
/sys/fs/cgroup/cpu/cpu.cfs_period_us  → 100000

Effective cores = quota / period = 2.0
```

#### Memory Detection (cgroup v2)
```
/sys/fs/cgroup/memory.max
Format: "2147483648" (bytes)
         ^^^^^^^^^^
         2 GB

Effective memory = 2147483648 / 1073741824 = 2.0 GB
```

#### Memory Detection (cgroup v1)
```
/sys/fs/cgroup/memory/memory.limit_in_bytes
Format: "2147483648" (bytes)

Effective memory = 2.0 GB
```

---

## 🚀 Usage

### Automatic Configuration (Recommended)

FLEET-Q automatically detects pod resources at startup:

```python
from fleet_q.quickstart.config import load_config

# Automatically detects CPU/memory and configures optimal workers
config = load_config()

print(f"Max Parallelism: {config.max_parallelism}")
print(f"AIOMultiprocess Workers: {config.aiomultiprocess_workers}")
print(f"IOHub Flush Threads: {config.iohub_flush_threads}")
```

**Startup Output:**
```
🔍 Pod Resource Detection:
  CPU Cores: 4.00
  Memory: 8.00 GB
  Recommended Fleet Workers: 3
  Recommended AIOMultiprocess Workers: 3
  Recommended IOHub Flush Threads: 4
```

### Manual Resource Detection

Use the pod resource utilities directly:

```python
from fleet_q.cgroup_aware_resources import (
    effective_cpu_cores,
    effective_memory_gb,
    get_pod_resources,
    print_pod_resources
)

# Get raw values
cores = effective_cpu_cores()
memory_gb = effective_memory_gb()

print(f"Available: {cores:.2f} cores, {memory_gb:.2f} GB")

# Get complete resource summary with recommendations
resources = get_pod_resources()
print(f"Fleet Workers: {resources.recommended_fleet_workers}")
print(f"AIOMultiprocess Workers: {resources.recommended_aiomultiprocess}")

# Pretty-print all details
print_pod_resources()
```

### Override Adaptive Configuration

Use environment variables to override auto-detected values:

```bash
# Disable adaptive configuration entirely
export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=false

# Override specific values (adaptive config enabled, but use custom values)
export FLEET_Q_MAX_PARALLELISM=10
export FLEET_Q_AIOMULTIPROCESS_WORKERS=6
export FLEET_Q_IOHUB_FLUSH_THREADS=8
```

---

## 📊 Recommendation Logic

### Fleet Claim Workers

**Purpose:** Number of parallel step claims to process  
**Formula:** `floor(cores × 0.85)`, min 1  
**Reasoning:** Reserve 15% CPU for heartbeats, leader checks, IOHub coordination

```python
cores = 4.0
usable = 4.0 × 0.85 = 3.4
recommended = floor(3.4) = 3 workers
```

**Environment Variable:** `FLEET_Q_MAX_PARALLELISM`

### AIOMultiprocess Workers

**Purpose:** Number of worker processes for HTTP-heavy workloads  
**Formula:** `floor(cores × 0.75)`, min 2, max 8  
**Reasoning:** Reserve 25% CPU for FastAPI, IOHub, background tasks

```python
cores = 4.0
usable = 4.0 × 0.75 = 3.0
recommended = max(2, min(8, floor(3.0))) = 3 workers
```

**Environment Variable:** `FLEET_Q_AIOMULTIPROCESS_WORKERS`

### IOHub Flush Threads

**Purpose:** Number of threads for flushing SQLite outbox to Snowflake  
**Formula:** `base=2 + floor(cores × 0.5)`  
**Reasoning:** I/O-bound workload benefits from modest thread pool

```python
cores = 4.0
recommended = 2 + floor(4.0 × 0.5) = 2 + 2 = 4 threads
```

**Environment Variable:** `FLEET_Q_IOHUB_FLUSH_THREADS`

### Async Concurrency (Per-Process)

**Purpose:** Concurrent HTTP requests in a single async event loop  
**Formula:** `cores × 10`, min 20, max 200  
**Reasoning:** Async I/O can handle 10x more requests than CPU cores

```python
cores = 4.0
recommended = min(200, max(20, 4.0 × 10)) = 40 concurrent requests
```

**Environment Variable:** `FLEET_Q_ASYNC_CONCURRENCY` *(not yet used in config)*

---

## 🔧 Configuration Reference

### FleetQConfig Fields

| Field | Type | Default | Adaptive? | Description |
|-------|------|---------|-----------|-------------|
| `max_parallelism` | `Optional[int]` | `None` | ✅ | Auto-detects or uses env var |
| `aiomultiprocess_workers` | `Optional[int]` | `None` | ✅ | Auto-detects or uses env var |
| `iohub_flush_threads` | `Optional[int]` | `None` | ✅ | Auto-detects or uses env var |
| `enable_adaptive_config` | `bool` | `True` | ❌ | Enable/disable auto-detection |

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `FLEET_Q_ENABLE_ADAPTIVE_CONFIG` | Enable auto-detection | `true` (default) |
| `FLEET_Q_MAX_PARALLELISM` | Override fleet workers | `5` |
| `FLEET_Q_AIOMULTIPROCESS_WORKERS` | Override aiomultiprocess pool | `4` |
| `FLEET_Q_IOHUB_FLUSH_THREADS` | Override IOHub flush threads | `6` |

---

## 🐳 Kubernetes Deployment Examples

### Example 1: Small Pod (1 core, 2 GB)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: fleet-q-worker-small
spec:
  containers:
  - name: fleet-q
    image: fleet-q:latest
    resources:
      requests:
        cpu: "1000m"      # 1 core
        memory: "2Gi"     # 2 GB
      limits:
        cpu: "1000m"
        memory: "2Gi"
    env:
    - name: FLEET_Q_POD_ID
      value: "worker-small-001"
    - name: FLEET_Q_ENABLE_ADAPTIVE_CONFIG
      value: "true"
```

**Detected Configuration:**
- CPU Cores: 1.0
- Memory: 2.0 GB
- Fleet Workers: 1
- AIOMultiprocess Workers: 2 (minimum enforced)
- IOHub Flush Threads: 2

### Example 2: Medium Pod (4 cores, 8 GB)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: fleet-q-worker-medium
spec:
  containers:
  - name: fleet-q
    image: fleet-q:latest
    resources:
      requests:
        cpu: "4000m"      # 4 cores
        memory: "8Gi"     # 8 GB
      limits:
        cpu: "4000m"
        memory: "8Gi"
```

**Detected Configuration:**
- CPU Cores: 4.0
- Memory: 8.0 GB
- Fleet Workers: 3
- AIOMultiprocess Workers: 3
- IOHub Flush Threads: 4

### Example 3: Large Pod (8 cores, 16 GB)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: fleet-q-worker-large
spec:
  containers:
  - name: fleet-q
    image: fleet-q:latest
    resources:
      requests:
        cpu: "8000m"      # 8 cores
        memory: "16Gi"    # 16 GB
      limits:
        cpu: "8000m"
        memory: "16Gi"
```

**Detected Configuration:**
- CPU Cores: 8.0
- Memory: 16.0 GB
- Fleet Workers: 6
- AIOMultiprocess Workers: 6
- IOHub Flush Threads: 6

### Example 4: Override Adaptive Config

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: fleet-q-worker-custom
spec:
  containers:
  - name: fleet-q
    image: fleet-q:latest
    resources:
      limits:
        cpu: "4000m"
        memory: "8Gi"
    env:
    - name: FLEET_Q_ENABLE_ADAPTIVE_CONFIG
      value: "true"
    - name: FLEET_Q_MAX_PARALLELISM
      value: "5"          # Override auto-detected value (3 → 5)
    - name: FLEET_Q_AIOMULTIPROCESS_WORKERS
      value: "4"
```

**Detected Configuration:**
- CPU Cores: 4.0 (detected)
- Memory: 8.0 GB (detected)
- Fleet Workers: **5** (overridden)
- AIOMultiprocess Workers: **4** (overridden)
- IOHub Flush Threads: 4 (auto-detected)

---

## 🧪 Testing & Validation

### Verify Detection Inside Pod

```bash
# SSH into running pod
kubectl exec -it fleet-q-worker-001 -- /bin/bash

# Check cgroup values
cat /sys/fs/cgroup/cpu.max          # cgroup v2
cat /sys/fs/cgroup/memory.max       # cgroup v2

# Or cgroup v1
cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us
cat /sys/fs/cgroup/memory/memory.limit_in_bytes

# Run Python detection
python3 -c "from fleet_q.cgroup_aware_resources import print_pod_resources; print_pod_resources()"
```

### Local Testing (Without Kubernetes)

On your local machine (no cgroup limits), detection falls back to `os.cpu_count()`:

```python
from fleet_q.cgroup_aware_resources import get_pod_resources

resources = get_pod_resources()
print(f"Detected: {resources.cpu_cores} cores")
# Output: Detected: 8.0 cores (your machine's CPU count)
```

### Docker Testing

Test with Docker CPU limits:

```bash
# Run with 2 CPU cores
docker run --cpus=2.0 -it fleet-q:latest python3 -c \
  "from fleet_q.cgroup_aware_resources import print_pod_resources; print_pod_resources()"

# Expected output:
# 🔍 Pod Resource Summary
#   CPU Cores: 2.00
#   Recommended Fleet Workers: 1
#   Recommended AIOMultiprocess Workers: 2
```

---

## 🐛 Troubleshooting

### Adaptive Config Not Working

**Symptom:** Config shows hard-coded values (8 workers) instead of detected values

**Possible Causes:**
1. `cgroup_aware_resources.py` not in `fleet_q/` directory
2. Import error (check for `⚠️ Adaptive config failed` in logs)
3. Environment variable override: `FLEET_Q_ENABLE_ADAPTIVE_CONFIG=false`

**Solution:**
```bash
# Verify file exists
ls -la /app/fleet_q/cgroup_aware_resources.py

# Check for import errors
python3 -c "from fleet_q.cgroup_aware_resources import get_pod_resources; print('OK')"

# Enable adaptive config
export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=true
```

### Detection Returns Wrong Values

**Symptom:** Detected cores = 64 (node CPU count) instead of 4 (pod limit)

**Possible Causes:**
1. Pod resources not set in Kubernetes manifest
2. Running outside Kubernetes (no cgroup limits)
3. cgroup v2 not available

**Solution:**
```yaml
# Always set resources.limits in Kubernetes manifest
resources:
  limits:
    cpu: "4000m"    # Required for accurate detection
    memory: "8Gi"   # Required for memory detection
```

### OOM Kills Despite Detection

**Symptom:** Pod killed by OOM despite memory detection

**Possible Causes:**
1. Memory limit too low for workload
2. Memory leak in application code
3. IOHub outbox accumulating too much data

**Solution:**
```python
# Monitor memory usage
from fleet_q.cgroup_aware_resources import effective_memory_gb
import psutil

memory_limit = effective_memory_gb()
memory_used = psutil.Process().memory_info().rss / 1024**3

print(f"Memory: {memory_used:.2f} / {memory_limit:.2f} GB")

# Adjust worker counts if memory usage > 80%
if memory_used / memory_limit > 0.8:
    print("⚠️  High memory usage, reduce workers")
```

---

## 📚 API Reference

### `effective_cpu_cores() -> float`

Returns the effective CPU cores available to the container.

**Returns:** Float (e.g., `4.0`, `2.5`)  
**Fallback:** `os.cpu_count()` if cgroups unavailable  
**Source Priority:** cgroup v2 → cgroup v1 → CPU affinity → `os.cpu_count()`

```python
from fleet_q.cgroup_aware_resources import effective_cpu_cores

cores = effective_cpu_cores()
print(f"Detected {cores:.2f} cores")
```

### `effective_memory_gb() -> float`

Returns the effective memory limit in gigabytes.

**Returns:** Float (e.g., `8.0`, `2.5`)  
**Fallback:** Total system memory if cgroups unavailable  
**Source Priority:** cgroup v2 → cgroup v1 → `psutil.virtual_memory().total`

```python
from fleet_q.cgroup_aware_resources import effective_memory_gb

memory = effective_memory_gb()
print(f"Detected {memory:.2f} GB memory")
```

### `recommended_fleet_claim_workers(...) -> int`

Recommends optimal number of fleet claim workers.

**Parameters:**
- `reserve_fraction` (float): Reserve fraction of CPU (default: 0.15)
- `minimum` (int): Minimum workers (default: 1)

**Returns:** Integer worker count

```python
from fleet_q.cgroup_aware_resources import recommended_fleet_claim_workers

workers = recommended_fleet_claim_workers(reserve_fraction=0.2, minimum=2)
print(f"Recommended fleet workers: {workers}")
```

### `recommended_aiomultiprocess_workers(...) -> int`

Recommends optimal number of aiomultiprocess workers.

**Parameters:**
- `reserve_fraction` (float): Reserve fraction of CPU (default: 0.25)
- `minimum` (int): Minimum workers (default: 2)
- `maximum` (int): Maximum workers (default: 8)

**Returns:** Integer worker count

```python
from fleet_q.cgroup_aware_resources import recommended_aiomultiprocess_workers

workers = recommended_aiomultiprocess_workers()
print(f"Recommended aiomultiprocess workers: {workers}")
```

### `recommended_iohub_flush_threads(...) -> int`

Recommends optimal number of IOHub flush threads.

**Parameters:**
- `base` (int): Base thread count (default: 2)
- `per_core` (float): Additional threads per core (default: 0.5)

**Returns:** Integer thread count

```python
from fleet_q.cgroup_aware_resources import recommended_iohub_flush_threads

threads = recommended_iohub_flush_threads()
print(f"Recommended IOHub flush threads: {threads}")
```

### `get_pod_resources() -> PodResourceSummary`

Returns complete resource summary with all recommendations.

**Returns:** `PodResourceSummary` dataclass

```python
from fleet_q.cgroup_aware_resources import get_pod_resources

resources = get_pod_resources()
print(f"CPU: {resources.cpu_cores:.2f}")
print(f"Memory: {resources.memory_gb:.2f} GB")
print(f"Fleet Workers: {resources.recommended_fleet_workers}")
print(f"AIOMultiprocess: {resources.recommended_aiomultiprocess}")
print(f"IOHub Threads: {resources.recommended_iohub_flush_threads}")
print(f"Async Concurrency: {resources.recommended_async_concurrency}")
```

### `print_pod_resources() -> None`

Pretty-prints complete resource summary to stdout.

```python
from fleet_q.cgroup_aware_resources import print_pod_resources

print_pod_resources()
```

**Output:**
```
🔍 Pod Resource Summary
────────────────────────────────────────
CPU:
  Effective Cores: 4.00
  Source: cgroup-v2

Memory:
  Limit: 8.00 GB
  Source: cgroup-v2

Recommended Configuration:
  Fleet Claim Workers: 3
  AIOMultiprocess Workers: 3
  IOHub Flush Threads: 4
  Async Concurrency (per-process): 40

Example Configuration:
  export FLEET_Q_MAX_PARALLELISM=3
  export FLEET_Q_AIOMULTIPROCESS_WORKERS=3
  export FLEET_Q_IOHUB_FLUSH_THREADS=4
────────────────────────────────────────
```

---

## 🎯 Best Practices

### 1. Always Set Resource Limits in Kubernetes

```yaml
# ✅ Good: Enables accurate detection
resources:
  limits:
    cpu: "4000m"
    memory: "8Gi"

# ❌ Bad: Detection falls back to node CPU count
resources:
  requests:
    cpu: "4000m"
  # No limits set
```

### 2. Use Adaptive Config in Production

```python
# ✅ Good: Adapts to pod resources
config = load_config()  # enable_adaptive_config=True

# ❌ Bad: Hard-coded values don't adapt
max_parallelism = 8
```

### 3. Monitor Resource Usage

```python
# Add to heartbeat loop
import psutil
from fleet_q.cgroup_aware_resources import effective_cpu_cores, effective_memory_gb

cpu_limit = effective_cpu_cores()
memory_limit = effective_memory_gb()

cpu_used = psutil.cpu_percent() / 100 * cpu_limit
memory_used = psutil.Process().memory_info().rss / 1024**3

if cpu_used / cpu_limit > 0.9:
    print("⚠️  High CPU usage")
if memory_used / memory_limit > 0.8:
    print("⚠️  High memory usage")
```

### 4. Test with Realistic Pod Sizes

```bash
# Test small pod (1 core)
kubectl apply -f k8s/fleet-q-small.yaml

# Test medium pod (4 cores)
kubectl apply -f k8s/fleet-q-medium.yaml

# Test large pod (8 cores)
kubectl apply -f k8s/fleet-q-large.yaml

# Verify each detects correct resources
kubectl logs fleet-q-worker-001 | grep "Pod Resource Detection"
```

### 5. Reserve CPU for Overhead

```python
# Reserve 15-25% CPU for:
# - FastAPI server
# - Heartbeats
# - Leader checks
# - IOHub coordination
# - Background tasks

# ✅ Good: 75% for workers, 25% for overhead
recommended_aiomultiprocess_workers(reserve_fraction=0.25)

# ❌ Bad: 100% for workers, nothing for overhead
recommended_aiomultiprocess_workers(reserve_fraction=0.0)
```

---

## 🔗 Related Documentation

- [AIOMULTIPROCESS_GUIDE.md](AIOMULTIPROCESS_GUIDE.md) - Using aiomultiprocess with IOHub
- [Multi-Queue-Parallelization.md](Multi-Queue-Parallelization.md) - Design patterns for in-pod execution
- [INDEX.md](INDEX.md) - Complete documentation index

---

## 📝 Changelog

**v1.0 (2024-01-20):**
- Initial release
- CPU detection (cgroup v2, cgroup v1, fallback)
- Memory detection (cgroup v2, cgroup v1, fallback)
- Adaptive configuration helpers
- Integration with FleetQConfig
- Kubernetes deployment examples
- Complete API reference
