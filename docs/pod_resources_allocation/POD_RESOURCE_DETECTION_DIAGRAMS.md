# Pod Resource Detection Flow

This document provides visual diagrams showing how FLEET-Q detects and adapts to Kubernetes pod resources.

## Detection Flow

```mermaid
flowchart TD
    Start([FLEET-Q Startup]) --> LoadConfig[load_config]
    LoadConfig --> CheckAdaptive{Adaptive\nConfig\nEnabled?}
    
    CheckAdaptive -->|No| UseDefaults[Use Environment Variables\nor Hard-Coded Defaults]
    CheckAdaptive -->|Yes| ImportCheck{cgroup_aware_resources\nAvailable?}
    
    ImportCheck -->|No| UseDefaults
    ImportCheck -->|Yes| DetectResources[get_pod_resources]
    
    DetectResources --> CheckCPU[Check CPU Limits]
    
    CheckCPU --> CgroupV2CPU{/sys/fs/cgroup/\ncpu.max\nexists?}
    CgroupV2CPU -->|Yes| ReadV2CPU[Read cgroup v2\nquota/period]
    CgroupV2CPU -->|No| CgroupV1CPU{/sys/fs/cgroup/cpu/\ncpu.cfs_quota_us\nexists?}
    
    CgroupV1CPU -->|Yes| ReadV1CPU[Read cgroup v1\nquota/period]
    CgroupV1CPU -->|No| FallbackCPU[Fallback:\nos.cpu_count]
    
    ReadV2CPU --> CalcCPU[Calculate:\neffective_cores = quota/period]
    ReadV1CPU --> CalcCPU
    FallbackCPU --> CalcCPU
    
    CalcCPU --> CheckMemory[Check Memory Limits]
    
    CheckMemory --> CgroupV2Mem{/sys/fs/cgroup/\nmemory.max\nexists?}
    CgroupV2Mem -->|Yes| ReadV2Mem[Read cgroup v2\nmemory limit]
    CgroupV2Mem -->|No| CgroupV1Mem{/sys/fs/cgroup/memory/\nmemory.limit_in_bytes\nexists?}
    
    CgroupV1Mem -->|Yes| ReadV1Mem[Read cgroup v1\nmemory limit]
    CgroupV1Mem -->|No| UnlimitedMem[Memory: Unlimited]
    
    ReadV2Mem --> CalcMem[Memory: limit_bytes / 1GB]
    ReadV1Mem --> CalcMem
    UnlimitedMem --> CalcRecs
    CalcMem --> CalcRecs[Calculate Recommendations]
    
    CalcRecs --> FleetCalc[Fleet Workers:\nfloor cores × 0.85]
    CalcRecs --> AIOCalc[AIOMultiprocess:\nfloor cores × 0.75]
    CalcRecs --> FlushCalc[IOHub Threads:\n2 + floor cores × 0.5]
    CalcRecs --> AsyncCalc[Async Concurrency:\ncores × 10]
    
    FleetCalc --> CheckEnv{Environment\nVariable\nSet?}
    AIOCalc --> CheckEnv
    FlushCalc --> CheckEnv
    AsyncCalc --> CheckEnv
    
    CheckEnv -->|Yes| UseEnv[Use Environment Value]
    CheckEnv -->|No| UseDetected[Use Detected Value]
    
    UseDefaults --> CreateConfig[Create FleetQConfig]
    UseEnv --> CreateConfig
    UseDetected --> CreateConfig
    
    CreateConfig --> PrintSummary[Print Detection Summary]
    PrintSummary --> End([Start Worker Loops])
    
    style Start fill:#e1f5e1
    style End fill:#e1f5e1
    style DetectResources fill:#fff3cd
    style CalcRecs fill:#d1ecf1
    style CreateConfig fill:#f8d7da
```

## CPU Detection Priority

```mermaid
flowchart LR
    A[CPU Detection] --> B{cgroup v2?}
    B -->|Yes| C[/sys/fs/cgroup/cpu.max]
    B -->|No| D{cgroup v1?}
    D -->|Yes| E[/sys/fs/cgroup/cpu/\ncpu.cfs_quota_us]
    D -->|No| F{CPU Affinity?}
    F -->|Yes| G[os.sched_getaffinity]
    F -->|No| H[os.cpu_count]
    
    C --> I[effective_cores]
    E --> I
    G --> I
    H --> I
    
    style A fill:#e1f5e1
    style I fill:#d1ecf1
    style C fill:#fff3cd
    style E fill:#fff3cd
    style G fill:#ffeeba
    style H fill:#f8d7da
```

## Memory Detection Priority

```mermaid
flowchart LR
    A[Memory Detection] --> B{cgroup v2?}
    B -->|Yes| C[/sys/fs/cgroup/memory.max]
    B -->|No| D{cgroup v1?}
    D -->|Yes| E[/sys/fs/cgroup/memory/\nmemory.limit_in_bytes]
    D -->|No| F[Unlimited]
    
    C --> G{Value = 'max'?}
    G -->|Yes| F
    G -->|No| H[effective_memory_gb]
    
    E --> I{Value > 8 EiB?}
    I -->|Yes| F
    I -->|No| H
    
    F --> H
    
    style A fill:#e1f5e1
    style H fill:#d1ecf1
    style C fill:#fff3cd
    style E fill:#fff3cd
    style F fill:#f8d7da
```

## Recommendation Calculation

```mermaid
flowchart TD
    Cores[Detected CPU Cores\ne.g., 4.0] --> Fleet[Fleet Workers]
    Cores --> AIO[AIOMultiprocess]
    Cores --> Flush[IOHub Flush]
    Cores --> Async[Async Concurrency]
    
    Fleet --> FleetCalc[cores × 0.85\n= 4.0 × 0.85\n= 3.4]
    FleetCalc --> FleetFloor[floor = 3]
    FleetFloor --> FleetMin[max min=1 = 3]
    
    AIO --> AIOCalc[cores × 0.75\n= 4.0 × 0.75\n= 3.0]
    AIOCalc --> AIOFloor[floor = 3]
    AIOFloor --> AIOClamp[max min=2 = 3\nmax max=8 = 3]
    
    Flush --> FlushCalc[base=2 + cores × 0.5\n= 2 + 4.0 × 0.5\n= 2 + 2.0]
    FlushCalc --> FlushFloor[floor = 4]
    
    Async --> AsyncCalc[cores × 10\n= 4.0 × 10\n= 40]
    AsyncCalc --> AsyncClamp[max min=20 = 40\nmax max=200 = 40]
    
    FleetMin --> Result[Fleet: 3\nAIO: 3\nFlush: 4\nAsync: 40]
    AIOClamp --> Result
    FlushFloor --> Result
    AsyncClamp --> Result
    
    style Cores fill:#e1f5e1
    style Result fill:#d1ecf1
```

## Pod Size Examples

```mermaid
graph TB
    subgraph "Small Pod: 1 core, 2 GB"
        S1[CPU: 1.0 core] --> S2[Fleet: 1 worker]
        S1 --> S3[AIO: 2 workers min]
        S1 --> S4[Flush: 2 threads base]
        S1 --> S5[Async: 20 min]
    end
    
    subgraph "Medium Pod: 4 cores, 8 GB"
        M1[CPU: 4.0 cores] --> M2[Fleet: 3 workers]
        M1 --> M3[AIO: 3 workers]
        M1 --> M4[Flush: 4 threads]
        M1 --> M5[Async: 40]
    end
    
    subgraph "Large Pod: 8 cores, 16 GB"
        L1[CPU: 8.0 cores] --> L2[Fleet: 6 workers]
        L1 --> L3[AIO: 6 workers]
        L1 --> L4[Flush: 6 threads]
        L1 --> L5[Async: 80]
    end
    
    subgraph "XLarge Pod: 16 cores, 32 GB"
        X1[CPU: 16.0 cores] --> X2[Fleet: 13 workers]
        X1 --> X3[AIO: 8 workers max]
        X1 --> X4[Flush: 10 threads]
        X1 --> X5[Async: 160]
    end
    
    style S1 fill:#fff3cd
    style M1 fill:#fff3cd
    style L1 fill:#fff3cd
    style X1 fill:#fff3cd
```

## Environment Variable Override Flow

```mermaid
flowchart TD
    Start[Detected Values] --> CheckFleet{FLEET_Q_MAX_PARALLELISM\nset?}
    
    CheckFleet -->|Yes| UseFleetEnv[Use Environment Value]
    CheckFleet -->|No| UseFleetDetected[Use Detected Value]
    
    Start --> CheckAIO{FLEET_Q_AIOMULTIPROCESS_WORKERS\nset?}
    CheckAIO -->|Yes| UseAIOEnv[Use Environment Value]
    CheckAIO -->|No| UseAIODetected[Use Detected Value]
    
    Start --> CheckFlush{FLEET_Q_IOHUB_FLUSH_THREADS\nset?}
    CheckFlush -->|Yes| UseFlushEnv[Use Environment Value]
    CheckFlush -->|No| UseFlushDetected[Use Detected Value]
    
    UseFleetEnv --> Config[FleetQConfig]
    UseFleetDetected --> Config
    UseAIOEnv --> Config
    UseAIODetected --> Config
    UseFlushEnv --> Config
    UseFlushDetected --> Config
    
    Config --> Summary[Print Configuration Summary]
    Summary --> Run[Start FLEET-Q]
    
    style Start fill:#e1f5e1
    style Config fill:#d1ecf1
    style Run fill:#e1f5e1
```

## Kubernetes Integration

```mermaid
graph TB
    subgraph "Kubernetes Pod Spec"
        PodSpec[Pod YAML] --> ResourceLimits[resources.limits]
        ResourceLimits --> CPULimit[cpu: '4000m']
        ResourceLimits --> MemLimit[memory: '8Gi']
    end
    
    subgraph "Container Runtime cgroup Setup"
        CPULimit --> CgroupCPU[/sys/fs/cgroup/cpu.max\n'400000 100000']
        MemLimit --> CgroupMem[/sys/fs/cgroup/memory.max\n'8589934592']
    end
    
    subgraph "FLEET-Q Detection"
        CgroupCPU --> DetectCPU[effective_cpu_cores\n= 400000/100000\n= 4.0]
        CgroupMem --> DetectMem[effective_memory_gb\n= 8589934592/1073741824\n= 8.0]
    end
    
    subgraph "FLEET-Q Configuration"
        DetectCPU --> CalcWorkers[Calculate Recommendations]
        DetectMem --> CalcWorkers
        CalcWorkers --> FleetWorkers[max_parallelism: 3]
        CalcWorkers --> AIOWorkers[aiomultiprocess_workers: 3]
        CalcWorkers --> FlushThreads[iohub_flush_threads: 4]
    end
    
    subgraph "Worker Loops"
        FleetWorkers --> ClaimLoop[Claim Loop:\n3 parallel claims]
        AIOWorkers --> HTTPPool[HTTP Pool:\n3 processes × 40 async\n= 120 concurrent]
        FlushThreads --> FlushPool[Flush Pool:\n4 threads]
    end
    
    style PodSpec fill:#e1f5e1
    style ResourceLimits fill:#fff3cd
    style DetectCPU fill:#d1ecf1
    style DetectMem fill:#d1ecf1
    style CalcWorkers fill:#f8d7da
```

## Reserve Fraction Explanation

```mermaid
graph LR
    Total[Total CPU: 4.0 cores] --> Reserved[Reserved: 1.0 core\n25%]
    Total --> Available[Available: 3.0 cores\n75%]
    
    Reserved --> FastAPI[FastAPI Server\n~0.3 cores]
    Reserved --> Heartbeat[Heartbeats\n~0.1 cores]
    Reserved --> Leader[Leader Checks\n~0.1 cores]
    Reserved --> IOHub[IOHub Coordination\n~0.2 cores]
    Reserved --> Background[Background Tasks\n~0.3 cores]
    
    Available --> Workers[AIOMultiprocess Workers\n3 workers × 1.0 core]
    
    style Total fill:#e1f5e1
    style Reserved fill:#f8d7da
    style Available fill:#d1ecf1
    style Workers fill:#fff3cd
```

## Throughput Comparison

```mermaid
graph TB
    subgraph "Hard-Coded (8 workers on 4 cores)"
        HC[8 Workers] --> HCThrash[Context Switching\nThrashing]
        HCThrash --> HCPerf[Poor Performance\n~50% CPU utilized]
    end
    
    subgraph "Adaptive (3 workers on 4 cores)"
        AC[3 Workers] --> ACMatch[Matches CPU\nNo Thrashing]
        ACMatch --> ACPerf[Good Performance\n~75% CPU utilized\n25% reserved]
    end
    
    subgraph "Adaptive (6 workers on 8 cores)"
        AL[6 Workers] --> ALScale[Scales with CPU\nNo Thrashing]
        ALScale --> ALPerf[Excellent Performance\n~75% CPU utilized\n25% reserved]
    end
    
    style HCPerf fill:#f8d7da
    style ACPerf fill:#d1ecf1
    style ALPerf fill:#e1f5e1
```

## Complete System Architecture with Adaptive Config

```mermaid
graph TB
    subgraph "Kubernetes"
        Pod[Pod Spec\nCPU: 4 cores\nMemory: 8 GB]
    end
    
    subgraph "Container Runtime"
        Cgroups[cgroups\ncpu.max\nmemory.max]
    end
    
    subgraph "FLEET-Q Startup"
        Config[load_config] --> Detect[get_pod_resources]
        Detect --> Print[Print Detection Summary]
    end
    
    subgraph "FLEET-Q Runtime"
        ClaimLoop[Claim Loop\n3 parallel]
        ExecuteLoop[Execute Loop\n3 AIO workers]
        FlushLoop[Flush Loop\n4 threads]
        HeartbeatLoop[Heartbeat Loop]
        LeaderLoop[Leader Loop]
    end
    
    subgraph "Snowflake"
        PodHealth[(POD_HEALTH)]
        StepTracker[(STEP_TRACKER)]
    end
    
    Pod --> Cgroups
    Cgroups --> Detect
    Print --> ClaimLoop
    Print --> ExecuteLoop
    Print --> FlushLoop
    Print --> HeartbeatLoop
    Print --> LeaderLoop
    
    ClaimLoop --> StepTracker
    ExecuteLoop --> StepTracker
    HeartbeatLoop --> PodHealth
    LeaderLoop --> StepTracker
    FlushLoop --> StepTracker
    
    style Pod fill:#fff3cd
    style Detect fill:#d1ecf1
    style ClaimLoop fill:#e1f5e1
    style ExecuteLoop fill:#e1f5e1
    style FlushLoop fill:#e1f5e1
```

---

## Key Takeaways

1. **Detection is automatic** - No manual configuration needed
2. **Priority system** - cgroup v2 → v1 → fallback
3. **Reserve CPU** - 15-25% for overhead (heartbeats, FastAPI, IOHub)
4. **Environment overrides** - Can override any detected value
5. **Graceful degradation** - Falls back to defaults if detection fails
6. **Kubernetes integration** - Respects `resources.limits` from pod spec

---

## Related Documentation

- [POD_RESOURCES_GUIDE.md](POD_RESOURCES_GUIDE.md) - Complete guide
- [POD_RESOURCES_QUICKREF.md](POD_RESOURCES_QUICKREF.md) - Quick reference
- [AIOMULTIPROCESS_GUIDE.md](AIOMULTIPROCESS_GUIDE.md) - HTTP concurrency
