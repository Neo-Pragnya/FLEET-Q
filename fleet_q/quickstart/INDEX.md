# FLEET-Q Quickstart Documentation Index

## 📖 Documentation Roadmap

This index helps you navigate the complete FLEET-Q documentation based on your needs.

---

## 🎯 I Want To...

### Get Started with FLEET-Q

👉 **[README.md](README.md)** (Main documentation)
- What is FLEET-Q?
- Quick start guide
- Basic task execution
- Schema setup
- Configuration

**Time:** 10-15 minutes  
**Best for:** First-time users, basic understanding

---

### Understand the Complete System

👉 **[SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md)** (Architecture guide)
- Three-tier architecture (Cluster → Pod → In-Pod)
- Capability matrix (Basic → Pipeline → IOHub)
- When to use each tier
- Performance comparison
- Decision tree

**Time:** 15-20 minutes  
**Best for:** Architects, system designers, choosing the right approach

---

### Optimize HTTP-Heavy Workloads

👉 **[PIPELINE_QUICKSTART.md](PIPELINE_QUICKSTART.md)** (Pipeline system - Tier 2)
- In-pod multi-stage pipelines
- Async I/O optimization
- AIMD adaptive throttling
- Bedrock/SharePoint/Snowflake stages
- 10 comprehensive Mermaid diagrams
- Integration patterns

**Time:** 30-45 minutes  
**Best for:** Bedrock/OpenAI workloads, HTTP-heavy tasks, 2-4x performance gains

---

### Coordinate Many Workers in Production

👉 **[IOHUB_PATTERN.md](IOHUB_PATTERN.md)** (IOHub pattern - Tier 3)
- Pod-wide shared AIMD control
- Local SQLite outbox (batched writes)
- Pipe vs ZMQ ROUTER/DEALER
- Complete usage examples
- Production integration
- Monitoring and troubleshooting

**Time:** 30-45 minutes  
**Best for:** Production deployments with 10+ workers, strict rate limits, high stability requirements

---

### See Implementation Details

👉 **[IOHUB_SUMMARY.md](IOHUB_SUMMARY.md)** (Implementation reference)
- What was built (1,962 lines)
- Key features with code examples
- Performance impact (before/after)
- Message protocol
- Architecture diagrams
- File reference

**Time:** 20-30 minutes  
**Best for:** Developers implementing IOHub, understanding internals

---

### Run Demos and Examples

👉 **Demo Files:**
1. **[pipeline_demo.py](pipeline_demo.py)** - Complete pipeline demo
   - SharePoint → Bedrock → Snowflake
   - Mock mode (no credentials)
   - Stress test with 50 documents
   
2. **[iohub_worker_demo.py](iohub_worker_demo.py)** - IOHub examples
   - Interactive menu (Pipe or ZMQ)
   - Single worker demo
   - Multi-worker demo (3 workers)
   
3. **[iohub_integration.py](iohub_integration.py)** - FLEET-Q integration
   - Simple executor pattern
   - Production executor with metrics
   - FastAPI lifespan example

**Time:** 10-20 minutes per demo  
**Best for:** Learning by doing, seeing real examples

---

## 📚 By Topic

### Core FLEET-Q (Tier 1)

| Topic | File | Description |
|-------|------|-------------|
| **Overview** | [README.md](README.md) | Main documentation, quick start |
| **Schema** | [schema.sql](schema.sql) | Snowflake tables (POD_HEALTH, STEP_TRACKER) |
| **Config** | [config.py](config.py) | Environment variables, FleetQConfig |
| **Storage** | [storage.py](storage.py) | SnowflakeStorage, SQLiteStorage |
| **Queue Ops** | [queue.py](queue.py) | submit_step, claim_step, complete_step |
| **Workers** | [worker.py](worker.py) | Heartbeat, claim, execute loops |
| **Leader** | [leader.py](leader.py) | Leader election, recovery |
| **Main** | [main.py](main.py) | FastAPI application |

**Total:** 2,092 lines

---

### Pipeline System (Tier 2)

| Topic | File | Description |
|-------|------|-------------|
| **Guide** | [PIPELINE_QUICKSTART.md](PIPELINE_QUICKSTART.md) | Complete pipeline documentation |
| **Core** | [pipeline.py](pipeline.py) | PipelineStage, Pipeline, message-driven |
| **SharePoint** | [sharepoint_reader.py](sharepoint_reader.py) | Async file download stage |
| **Bedrock** | [bedrock_processor.py](bedrock_processor.py) | Bedrock API + AIMD throttling |
| **Snowflake** | [snowflake_writer.py](snowflake_writer.py) | Batched write stage |
| **Demo** | [pipeline_demo.py](pipeline_demo.py) | End-to-end demo |
| **Integration** | [pipeline_integration.py](pipeline_integration.py) | FLEET-Q integration patterns |
| **Throttle** | [throttle.py](throttle.py) | AIMD algorithm |
| **Backoff** | [backoff.py](backoff.py) | Exponential backoff decorator |

**Total:** 2,935 lines

**Performance:** 2-4x faster for HTTP workloads

---

### IOHub Pattern (Tier 3)

| Topic | File | Description |
|-------|------|-------------|
| **Guide** | [IOHUB_PATTERN.md](IOHUB_PATTERN.md) | Complete usage guide |
| **Summary** | [IOHUB_SUMMARY.md](IOHUB_SUMMARY.md) | Implementation details |
| **Core** | [iohub.py](iohub.py) | IOHub, SharedAIMD, SQLite outbox |
| **Demo** | [iohub_worker_demo.py](iohub_worker_demo.py) | Worker examples (Pipe/ZMQ) |
| **Integration** | [iohub_integration.py](iohub_integration.py) | FLEET-Q integration |
| **aiomultiprocess** | [aiomultiprocess_iohub.py](aiomultiprocess_iohub.py) | Async multiprocessing (NEW!) |
| **aiomultiprocess Guide** | [AIOMULTIPROCESS_GUIDE.md](AIOMULTIPROCESS_GUIDE.md) | Complete aiomultiprocess guide (NEW!) |
| **Throttle** | [throttle.py](throttle.py) | AIMD algorithm (shared) |

**Total:** 2,134 lines (including aiomultiprocess)

**Performance:** 3x better throughput, 13% fewer errors, 50x faster writes, 4-5x with aiomultiprocess

---

### Design Documents

| Topic | File | Description |
|-------|------|-------------|
| **Architecture** | [SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md) | Three-tier system overview |
| **Parallelization** | [Multi-Queue-Parallelization.md](../../docs/ideation/Multi-Queue-Parallelization.md) | Original design doc |
| **Patterns** | [raquel_patterns.py](raquel_patterns.py) | Raquel-inspired patterns |
| **Implementation** | [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) | Pipeline design decisions |

---

## 🎓 Learning Paths

### Path 1: Beginner (Basic FLEET-Q)

1. **[README.md](README.md)** - Understand core concepts (15 min)
2. **[schema.sql](schema.sql)** - Review database schema (5 min)
3. **[main.py](main.py)** - See FastAPI integration (10 min)
4. **Deploy** - Get basic system running (30 min)

**Total:** ~1 hour  
**Outcome:** Working FLEET-Q deployment

---

### Path 2: Intermediate (Add Pipeline)

1. **[PIPELINE_QUICKSTART.md](PIPELINE_QUICKSTART.md)** - Understand pipeline system (30 min)
2. **[pipeline_demo.py](pipeline_demo.py)** - Run demo (10 min)
3. **[pipeline.py](pipeline.py)** - Study core implementation (20 min)
4. **[pipeline_integration.py](pipeline_integration.py)** - Integration patterns (20 min)
5. **Integrate** - Add to your deployment (1 hour)

**Total:** ~2 hours  
**Outcome:** Optimized HTTP-heavy workload execution (2-4x faster)

---

### Path 3: Advanced (Add IOHub)

1. **[SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md)** - Understand three-tier architecture (20 min)
2. **[IOHUB_PATTERN.md](IOHUB_PATTERN.md)** - Study IOHub pattern (30 min)
3. **[iohub_worker_demo.py](iohub_worker_demo.py)** - Run demos (20 min)
4. **[iohub_integration.py](iohub_integration.py)** - Integration example (20 min)
5. **[IOHUB_SUMMARY.md](IOHUB_SUMMARY.md)** - Implementation details (20 min)
6. **Integrate** - Add to your deployment (2 hours)

**Total:** ~3-4 hours  
**Outcome:** Production-ready system with pod-wide coordination (3x throughput, 50x faster writes)

---

## 🔍 By Use Case

### Use Case 1: Simple Task Queue

**Need:** Distributed task queue without broker

**Read:**
- [README.md](README.md) - Quick start
- [schema.sql](schema.sql) - Schema setup

**Run:**
```bash
python main.py
```

**Result:** Basic FLEET-Q running

---

### Use Case 2: Bedrock API Processing

**Need:** Process documents with Bedrock at scale

**Read:**
- [README.md](README.md) - Foundation
- [PIPELINE_QUICKSTART.md](PIPELINE_QUICKSTART.md) - Pipeline details

**Run:**
```bash
python pipeline_demo.py
```

**Result:** 2-4x faster processing with automatic throttling

---

### Use Case 3: High-Scale Production

**Need:** Many workers, strict rate limits, stability

**Read:**
- [SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md) - Architecture
- [IOHUB_PATTERN.md](IOHUB_PATTERN.md) - IOHub guide

**Run:**
```bash
python iohub_worker_demo.py  # Choose option 3
```

**Result:** Production-ready with 3x throughput, minimal errors

---

## 📊 Quick Reference

### Architecture Tiers

| Tier | Purpose | Coordination | Files | Lines |
|------|---------|-------------|-------|-------|
| **1: Cluster** | Task distribution | Snowflake | 8 files | 2,092 |
| **2: Pipeline** | HTTP optimization | Message queues | 9 files | 2,935 |
| **3: IOHub** | Pod-wide control | IPC (Pipe/ZMQ) | 6 files | 1,962 |

**Total:** 23 files, 6,989 lines of implementation

---

### Performance Gains

| Metric | Basic | + Pipeline | + IOHub |
|--------|-------|-----------|---------|
| **Throughput** | 5.5/s | 13.3/s (2.4x) | 16.7/s (3x) |
| **Error Rate** | 15% | 5% (-10%) | 1-2% (-13%) |
| **Write Speed** | 1x | 10x (batching) | 50x (outbox) |
| **Stability** | Low | Medium | High |

---

### When to Use Each Tier

| Scenario | Tier 1 | Tier 2 | Tier 3 |
|----------|--------|--------|--------|
| Simple tasks (DB, compute) | ✅ | ❌ | ❌ |
| HTTP-heavy (Bedrock, APIs) | ❌ | ✅ | ✅ |
| 1-5 workers per pod | ✅ | ✅ | ❌ |
| 10+ workers per pod | ✅ | ✅ | ✅ |
| Relaxed rate limits | ✅ | ✅ | ❌ |
| Strict rate limits | ❌ | ✅ | ✅ |
| Development/testing | ✅ | ✅ | ❌ |
| Production at scale | ✅ | ✅ | ✅ |

---

## 🚀 Quick Start Commands

```bash
# Clone repository
cd fleet_q/quickstart/

# Basic FLEET-Q
python main.py

# Pipeline demo
python pipeline_demo.py

# IOHub demo (interactive)
python iohub_worker_demo.py

# Integration demo
python iohub_integration.py
```

---

## 📞 Getting Help

### Troubleshooting

1. **Basic issues** → [README.md](README.md) (Troubleshooting section)
2. **Pipeline issues** → [PIPELINE_QUICKSTART.md](PIPELINE_QUICKSTART.md) (Troubleshooting section)
3. **IOHub issues** → [IOHUB_PATTERN.md](IOHUB_PATTERN.md) (Troubleshooting section)

### Understanding Concepts

1. **Architecture** → [SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md)


### Code Examples

1. **Basic execution** → [main.py](main.py)
2. **Pipeline usage** → [pipeline_demo.py](pipeline_demo.py)
3. **IOHub usage** → [iohub_worker_demo.py](iohub_worker_demo.py)
4. **Integration** → [iohub_integration.py](iohub_integration.py)

---

## 🎉 Summary

FLEET-Q provides a **complete distributed task queue system** with three tiers:

1. **Tier 1 (Basic):** Snowflake-coordinated queue
2. **Tier 2 (Pipeline):** HTTP-optimized execution
3. **Tier 3 (IOHub):** Pod-wide coordination

**Choose the tier that matches your workload complexity.**

---

**Start here:** [README.md](README.md) → [SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md) → Choose your path

**Questions?** Check the relevant guide's troubleshooting section.
