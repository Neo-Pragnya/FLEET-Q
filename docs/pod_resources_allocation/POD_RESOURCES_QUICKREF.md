# Pod Resource Detection - Quick Reference

**Last Updated:** 2024-01-20

## 🚀 Quick Start

### Enable Adaptive Configuration (Default)

Just set `FLEET_Q_POD_ID` and Snowflake credentials. Worker counts auto-detect:

```bash
export FLEET_Q_POD_ID="worker-001"
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_DATABASE="your-db"
export SNOWFLAKE_SCHEMA="your-schema"
export SNOWFLAKE_WAREHOUSE="your-warehouse"

# That's it! Worker counts auto-detect from pod resources
python -m fleet_q.main
```

### Override Specific Values

```bash
# Keep adaptive config on, but override fleet workers
export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=true
export FLEET_Q_MAX_PARALLELISM=10

# Other values still auto-detect
python -m fleet_q.main
```

### Disable Adaptive Configuration

```bash
# Use hard-coded defaults
export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=false
export FLEET_Q_MAX_PARALLELISM=8
export FLEET_Q_AIOMULTIPROCESS_WORKERS=4
export FLEET_Q_IOHUB_FLUSH_THREADS=4
```

---

## 📊 Resource Detection

### Check Detected Resources

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
────────────────────────────────────────
```

### Get Raw Values

```python
from fleet_q.cgroup_aware_resources import (
    effective_cpu_cores,
    effective_memory_gb,
    recommended_fleet_claim_workers,
)

cores = effective_cpu_cores()
memory = effective_memory_gb()
workers = recommended_fleet_claim_workers()

print(f"Cores: {cores}, Memory: {memory} GB, Workers: {workers}")
```

---

## 🎯 Recommendation Formulas

| Component | Formula | Example (4 cores) |
|-----------|---------|-------------------|
| **Fleet Workers** | `floor(cores × 0.85)` | floor(4 × 0.85) = **3** |
| **AIOMultiprocess** | `floor(cores × 0.75)` | floor(4 × 0.75) = **3** |
| **IOHub Flush Threads** | `2 + floor(cores × 0.5)` | 2 + floor(4 × 0.5) = **4** |
| **Async Concurrency** | `cores × 10` | 4 × 10 = **40** |

### Why Reserve CPU?

- **Fleet Workers (15% reserve):** Heartbeats, leader checks, IOHub coordination
- **AIOMultiprocess (25% reserve):** FastAPI server, IOHub, SQLite, background tasks
- **IOHub Threads (50% per core):** I/O-bound workload benefits from moderate threading
- **Async Concurrency (10x cores):** I/O can handle much more than CPU cores

---

## 🐳 Kubernetes Manifests

### Small Pod (1 core, 2 GB)

```yaml
resources:
  limits:
    cpu: "1000m"
    memory: "2Gi"
```

**Detects:** 1 fleet worker, 2 aiomultiprocess workers

### Medium Pod (4 cores, 8 GB)

```yaml
resources:
  limits:
    cpu: "4000m"
    memory: "8Gi"
```

**Detects:** 3 fleet workers, 3 aiomultiprocess workers

### Large Pod (8 cores, 16 GB)

```yaml
resources:
  limits:
    cpu: "8000m"
    memory: "16Gi"
```

**Detects:** 6 fleet workers, 6 aiomultiprocess workers

---

## 🔍 Debugging

### Check Detection Inside Pod

```bash
kubectl exec -it fleet-q-worker-001 -- bash

# Check cgroup v2
cat /sys/fs/cgroup/cpu.max
cat /sys/fs/cgroup/memory.max

# Check cgroup v1
cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us
cat /sys/fs/cgroup/memory/memory.limit_in_bytes

# Run Python detection
python -c "from fleet_q.cgroup_aware_resources import print_pod_resources; print_pod_resources()"
```

### Verify Configuration

```bash
# Check logs for detection output
kubectl logs fleet-q-worker-001 | grep "Pod Resource Detection"
```

Expected output:
```
🔍 Pod Resource Detection:
  CPU Cores: 4.00
  Memory: 8.00 GB
  Recommended Fleet Workers: 3
  Recommended AIOMultiprocess Workers: 3
  Recommended IOHub Flush Threads: 4
```

---

## ⚠️ Common Issues

### Issue: Detection shows 64 cores (node count) instead of 4 (pod limit)

**Cause:** Pod resources not set in Kubernetes manifest

**Fix:**
```yaml
# ❌ Wrong: No limits set
resources:
  requests:
    cpu: "4000m"

# ✅ Correct: Limits enable detection
resources:
  limits:
    cpu: "4000m"
    memory: "8Gi"
```

### Issue: Adaptive config not working

**Cause:** Import error or disabled

**Fix:**
```bash
# Verify file exists
ls -la /app/fleet_q/cgroup_aware_resources.py

# Check imports
python -c "from fleet_q.cgroup_aware_resources import get_pod_resources; print('OK')"

# Enable adaptive config
export FLEET_Q_ENABLE_ADAPTIVE_CONFIG=true
```

### Issue: Workers still using hard-coded 8

**Cause:** Environment variable override

**Fix:**
```bash
# Remove override
unset FLEET_Q_MAX_PARALLELISM

# Or set to empty to use auto-detection
export FLEET_Q_MAX_PARALLELISM=""
```

---

## 📚 API Quick Reference

### Detection Functions

```python
# CPU detection
effective_cpu_cores() -> float  # Returns cores (e.g., 4.0)

# Memory detection
effective_memory_gb() -> Optional[float]  # Returns GB or None

# Recommendations
recommended_fleet_claim_workers(reserve_fraction=0.15, minimum=1) -> int
recommended_aiomultiprocess_workers(reserve_fraction=0.25, minimum=2, maximum=8) -> int
recommended_iohub_flush_threads(base=2, per_core=0.5) -> int
recommended_async_concurrency(factor=10, min_value=20, max_value=200) -> int

# Complete summary
get_pod_resources() -> PodResourceSummary
print_pod_resources() -> None  # Pretty-print to stdout
```

### PodResourceSummary Fields

```python
@dataclass
class PodResourceSummary:
    cpu_cores: float
    cpu_source: str
    memory_gb: Optional[float]
    memory_source: str
    recommended_aiomultiprocess: int
    recommended_fleet_workers: int
    recommended_iohub_flush_threads: int
    recommended_async_concurrency: int
```

---

## 🧪 Testing

### Run Demo

```bash
# Local (uses os.cpu_count)
python examples/pod_resource_detection_demo.py

# Docker (uses cgroup limits)
docker run --cpus=2.0 --memory=4g fleet-q \
  python examples/pod_resource_detection_demo.py

# Kubernetes (uses pod limits)
kubectl exec -it fleet-q-worker-001 -- \
  python examples/pod_resource_detection_demo.py
```

### Expected Demo Output

```
================================================================================
FLEET-Q POD RESOURCE DETECTION DEMO
================================================================================

1️⃣  CURRENT POD RESOURCES
--------------------------------------------------------------------------------
[Shows detected CPU, memory, recommendations]

2️⃣  ADAPTIVE vs. HARD-CODED CONFIGURATION
--------------------------------------------------------------------------------
[Compares adaptive (3 workers) vs hard-coded (8 workers)]

3️⃣  POD SIZE RECOMMENDATIONS
--------------------------------------------------------------------------------
[Table showing 1, 4, 8, 16 core recommendations]
```

---

## 📖 Full Documentation

- **[POD_RESOURCES_GUIDE.md](POD_RESOURCES_GUIDE.md)** - Complete guide (~1400 lines)
- **[AIOMULTIPROCESS_GUIDE.md](AIOMULTIPROCESS_GUIDE.md)** - HTTP concurrency patterns
- **[Multi-Queue-Parallelization.md](Multi-Queue-Parallelization.md)** - Design patterns

---

## 🎯 Best Practices

1. ✅ **Always set `resources.limits` in Kubernetes**
2. ✅ **Use adaptive config in production** (default: enabled)
3. ✅ **Monitor resource usage** in heartbeat loop
4. ✅ **Test with realistic pod sizes** (1, 4, 8 cores)
5. ✅ **Reserve CPU for overhead** (15-25%)

---

## 🆘 Get Help

**Documentation:**
- [POD_RESOURCES_GUIDE.md](POD_RESOURCES_GUIDE.md) - Comprehensive guide
- [INDEX.md](INDEX.md) - Documentation index

**Examples:**
- [pod_resource_detection_demo.py](../examples/pod_resource_detection_demo.py) - Interactive demo

**Issues:**
- Check `kubectl logs` for detection output
- Verify `resources.limits` in Kubernetes manifest
- Run demo inside pod for validation
