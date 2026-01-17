# Architecture Overview

FLEET-Q implements a unique distributed task queue architecture designed for environments where traditional message brokers cannot be used and pods cannot communicate directly with each other.

## Design Philosophy

FLEET-Q is built on several key principles:

### Database-Mediated Coordination
All inter-pod communication occurs through Snowflake database transactions, eliminating the need for message brokers or direct networking between pods.

### Atomic State Transitions
Every step state change uses database transactions to prevent race conditions and ensure data consistency across the distributed system.

### Elastic Capacity Management
Pods dynamically adjust their workload based on available resources, automatically scaling up or down as needed.

### Resilient Recovery
A leader-based recovery system handles pod failures without data loss, ensuring all tasks are eventually processed.

### Operational Simplicity
Minimal configuration, clear observability, and easy troubleshooting make FLEET-Q simple to operate in production.

## System Architecture

```mermaid
graph TB
    subgraph "EKS Cluster"
        subgraph "Pod 1 (Leader + Worker)"
            P1[FastAPI Server]
            P1H[Heartbeat Loop]
            P1C[Claim Loop]
            P1E[Execute Loop]
            P1R[Recovery Loop]
            P1L[Local SQLite]
        end
        
        subgraph "Pod 2 (Worker)"
            P2[FastAPI Server]
            P2H[Heartbeat Loop]
            P2C[Claim Loop]
            P2E[Execute Loop]
            P2L[Local SQLite]
        end
        
        subgraph "Pod N (Worker)"
            PN[FastAPI Server]
            PNH[Heartbeat Loop]
            PNC[Claim Loop]
            PNE[Execute Loop]
            PNL[Local SQLite]
        end
    end
    
    subgraph "Snowflake"
        PH[(POD_HEALTH)]
        ST[(STEP_TRACKER)]
    end
    
    P1H --> PH
    P2H --> PH
    PNH --> PH
    
    P1C --> ST
    P2C --> ST
    PNC --> ST
    
    P1E --> ST
    P2E --> ST
    PNE --> ST
    
    P1R --> ST
    P1R --> P1L
```

## Core Components

### Pods
Individual EKS containers running the FLEET-Q service. Each pod contains:

- **FastAPI Server**: REST API for external interaction
- **Background Loops**: Heartbeat, claiming, execution, and recovery processes
- **Local SQLite**: Ephemeral storage for recovery operations
- **Storage Clients**: Connections to Snowflake and local SQLite

### Snowflake Coordination Store
The single source of truth for all system coordination:

- **POD_HEALTH Table**: Tracks pod status and enables leader election
- **STEP_TRACKER Table**: Manages the complete task lifecycle
- **Transactional Guarantees**: Ensures atomic operations across pods

### Leader Election System
Deterministic leader selection based on:

- **Eldest Pod Rule**: Pod with earliest birth timestamp becomes leader
- **Health Validation**: Only pods with fresh heartbeats are eligible
- **Automatic Failover**: New leader elected when current leader fails

## Architectural Patterns

### Event Loop Architecture
Each pod runs multiple concurrent loops:

```mermaid
graph LR
    subgraph "Pod Process"
        API[FastAPI Server]
        HB[Heartbeat Loop]
        CL[Claim Loop]
        EL[Execute Loop]
        RL[Recovery Loop]
        LE[Leader Election]
    end
    
    API --> |HTTP Requests| API
    HB --> |30s interval| HB
    CL --> |5s interval| CL
    EL --> |Continuous| EL
    RL --> |5m interval| RL
    LE --> |1m interval| LE
```

### Database-First Design
The system treats Snowflake as the single source of truth:

- **No In-Memory State**: All critical state is persisted in the database
- **Atomic Operations**: All coordination happens through database transactions
- **Local Ephemeral Data**: SQLite used only for temporary recovery data

### Hybrid Leadership Model
Combines the benefits of leaderless and leader-coordinated architectures:

| Aspect | Leaderless | FLEET-Q Hybrid |
|--------|------------|----------------|
| Task Distribution | Fully distributed | Distributed claiming + leader cleanup |
| Coordination Overhead | Low | Low-medium (only leader cleanup) |
| Failure Handling | Heuristic timeouts | Explicit detection with leader |
| Race Conditions | Higher risk | Lower risk (single recovery leader) |
| Complexity | Harder to reason about failures | Easier - single responsibility leader |

## Data Flow

### Step Submission Flow

```mermaid
sequenceDiagram
    participant Client
    participant Pod
    participant Snowflake
    
    Client->>Pod: POST /submit
    Pod->>Snowflake: INSERT INTO STEP_TRACKER
    Snowflake-->>Pod: step_id
    Pod-->>Client: {"step_id": "..."}
```

### Step Claiming Flow

```mermaid
sequenceDiagram
    participant Worker1
    participant Worker2
    participant Snowflake
    
    Worker1->>Snowflake: BEGIN TRANSACTION
    Worker2->>Snowflake: BEGIN TRANSACTION
    Worker1->>Snowflake: SELECT ... FOR UPDATE
    Worker2->>Snowflake: SELECT ... FOR UPDATE (blocked)
    Worker1->>Snowflake: UPDATE claimed_by = worker1
    Worker1->>Snowflake: COMMIT
    Worker2->>Snowflake: UPDATE (conflict - retry with backoff)
```

### Recovery Flow

```mermaid
sequenceDiagram
    participant Leader
    participant SQLite
    participant Snowflake
    
    Leader->>Snowflake: SELECT dead pods
    Leader->>Snowflake: SELECT orphaned steps
    Leader->>SQLite: CREATE DLQ table
    Leader->>SQLite: INSERT orphan records
    Leader->>Snowflake: UPDATE steps to pending
    Leader->>SQLite: DROP DLQ table
```

## Scalability Characteristics

### Horizontal Scaling
- **Linear Throughput**: Adding pods increases processing capacity linearly
- **No Central Bottleneck**: Distributed claiming prevents single points of failure
- **Elastic Capacity**: Pods automatically adjust to available resources

### Database Scaling
- **Snowflake Elasticity**: Warehouse scaling handles increased load
- **Minimal Schema**: Only two tables reduce complexity
- **Efficient Queries**: Optimized for common operations

### Network Efficiency
- **No Pod-to-Pod Communication**: Eliminates network complexity
- **Batch Operations**: Multiple steps claimed in single transactions
- **Heartbeat Optimization**: Configurable intervals balance freshness and overhead

## Fault Tolerance

### Pod Failures
- **Automatic Detection**: Stale heartbeats trigger dead pod detection
- **Work Recovery**: Orphaned tasks automatically requeued
- **Leader Failover**: New leader elected within one heartbeat interval

### Database Failures
- **Connection Retry**: Exponential backoff for transient failures
- **Circuit Breaker**: Prevents cascade failures during outages
- **Graceful Degradation**: Continue operation with reduced functionality

### Network Partitions
- **Database-Only Communication**: No direct pod communication to partition
- **Heartbeat Tolerance**: Configurable thresholds handle temporary issues
- **Recovery Resilience**: Leader recovery handles extended partitions

## Performance Characteristics

### Latency
- **Claim Latency**: Typically 10-50ms for database round-trip
- **Recovery Latency**: 3-5 minutes for dead pod detection
- **API Response**: Sub-millisecond for cached operations

### Throughput
- **Per-Pod Throughput**: Limited by configured parallelism
- **System Throughput**: Scales linearly with pod count
- **Database Throughput**: Limited by Snowflake warehouse capacity

### Resource Usage
- **Memory**: ~100MB base + 10MB per max parallelism
- **CPU**: Low baseline, scales with task execution
- **Storage**: Minimal (ephemeral SQLite only)

## Comparison with Traditional Architectures

### vs. Message Broker Architectures

| Aspect | Message Broker | FLEET-Q |
|--------|----------------|---------|
| Infrastructure | Requires Redis/RabbitMQ | Only needs database |
| Networking | Pod-to-broker communication | Database-only communication |
| Failure Modes | Broker single point of failure | Database handles failures |
| Operational Complexity | Broker + workers | Workers only |
| Consistency | Eventually consistent | Strongly consistent |

### vs. Fully Leaderless Systems

| Aspect | Leaderless | FLEET-Q |
|--------|------------|---------|
| Task Assignment | Distributed consensus | Distributed claiming |
| Failure Recovery | Timeout-based | Leader-coordinated |
| Split Brain Risk | Higher | Lower |
| Operational Clarity | Complex failure scenarios | Clear recovery process |

## Next Steps

Learn more about specific architectural components:

- **[Components](components.md)** - Detailed component descriptions
- **[Data Flow](data-flow.md)** - Complete data flow diagrams
- **[State Management](state-management.md)** - State transition details