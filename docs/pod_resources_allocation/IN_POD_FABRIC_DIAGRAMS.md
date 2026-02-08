# In-Pod Execution Fabric - Architecture Diagrams

## Complete System Architecture

```mermaid
flowchart TB
    subgraph POD["EKS Pod - FLEET-Q In-Pod Execution Fabric"]
        subgraph API["FastAPI Application (Multiple Workers)"]
            W1["Worker 1<br/>PID 101"]
            W2["Worker 2<br/>PID 102"]
            W3["Worker 3<br/>PID 103"]
            W4["Worker 4<br/>PID 104"]
        end
        
        DB["SQLite File<br/>/tmp/fleetq_outbox.db<br/>WAL Mode"]
        
        subgraph CP["Control Plane Runner<br/>(Lease Holder Only)"]
            LEASE["Lease Manager<br/>TTL: 30s<br/>Renew: 10s"]
            SCHED["APScheduler<br/>AsyncIOScheduler"]
            IOHUB["IOHub<br/>ZMQ ROUTER<br/>AIMD Brain"]
            OUTBOX["Outbox Writer<br/>SQLite ORM"]
            
            subgraph JOBS["Scheduled Jobs"]
                J1["Lease Renewal<br/>Every 10s"]
                J2["Outbox Flush<br/>Every 30s"]
                J3["Cleanup<br/>Daily"]
                J4["Stats Log<br/>Every 5min"]
            end
            
            subgraph FLUSHERS["Flushers"]
                F1["Snowflake Writer<br/>Batch: 100"]
                F2["SharePoint Handler<br/>Async I/O"]
            end
        end
        
        subgraph EXEC["Execution Tier"]
            POOL["aiomultiprocess Pool<br/>Workers: 4"]
            E1["Worker E1<br/>ZMQ DEALER<br/>PID 201"]
            E2["Worker E2<br/>ZMQ DEALER<br/>PID 202"]
            E3["Worker E3<br/>ZMQ DEALER<br/>PID 203"]
            E4["Worker E4<br/>ZMQ DEALER<br/>PID 204"]
        end
    end
    
    SF["Snowflake<br/>STEP_TRACKER<br/>POD_HEALTH"]
    BR["Bedrock API<br/>Claude/Titan"]
    SP["SharePoint<br/>Documents"]
    
    W1 --> DB
    W2 --> DB
    W3 --> DB
    W4 --> DB
    
    DB -.SQLite Lease.-> LEASE
    LEASE --> CP
    
    SCHED --> J1
    SCHED --> J2
    SCHED --> J3
    SCHED --> J4
    
    J1 --> LEASE
    J2 --> OUTBOX
    
    IOHUB <--> E1
    IOHUB <--> E2
    IOHUB <--> E3
    IOHUB <--> E4
    
    POOL --> E1
    POOL --> E2
    POOL --> E3
    POOL --> E4
    
    E1 --> BR
    E2 --> BR
    E3 --> BR
    E4 --> BR
    
    IOHUB --> OUTBOX
    OUTBOX --> F1
    OUTBOX --> F2
    
    F1 --> SF
    F2 --> SP
```

## Message Flow - Permit Request/Response

```mermaid
sequenceDiagram
    participant W as Worker<br/>(aiomultiprocess)
    participant D as ZMQ DEALER<br/>(Worker Client)
    participant R as ZMQ ROUTER<br/>(IOHub)
    participant A as AIMD Brain
    participant O as SQLite Outbox
    
    Note over W,O: Phase 1: Permit Request
    W->>D: request_permit()
    D->>R: PERMIT_REQUEST<br/>{sender_id: "worker-1"}
    R->>A: Check inflight vs max_inflight
    
    alt Permit Available
        A->>A: current < max_inflight
        A->>A: Grant permit<br/>inflight += 1
        R->>D: PERMIT_GRANT<br/>{permit_id: "uuid"}
        D->>W: True
    else Max Inflight Reached
        A->>A: current >= max_inflight
        R->>D: PERMIT_DENY<br/>{reason: "max_reached"}
        D->>W: False
    end
    
    Note over W,O: Phase 2: Execute Work
    W->>W: call_bedrock()
    
    Note over W,O: Phase 3: Report Outcome
    alt Success
        W->>D: report_success(latency=0.5)
        D->>R: CALL_SUCCESS<br/>{latency: 0.5}
        R->>A: Update AIMD<br/>success_streak += 1
        A->>A: Check if should increase<br/>max_inflight
        A->>O: Save pressure state
    else Throttle (429)
        W->>D: report_throttle()
        D->>R: CALL_THROTTLE
        R->>A: Update AIMD<br/>max_inflight *= 0.5
        A->>O: Save pressure state
    else Error
        W->>D: report_error(error)
        D->>R: CALL_ERROR<br/>{error: "..."}
        R->>A: Update AIMD<br/>reset streak
    end
    
    R->>A: Release permit<br/>inflight -= 1
    
    Note over W,O: Phase 4: Enqueue Result
    W->>D: enqueue_result(...)
    D->>R: ENQUEUE_WRITE<br/>{step_id, table, data}
    R->>O: Insert into outbox_results
```

## AIMD State Machine

```mermaid
stateDiagram-v2
    [*] --> Normal: Initialize<br/>max_inflight=10
    
    Normal --> Increasing: Success streak ≥ 5<br/>AND cooldown elapsed
    Increasing --> Normal: max_inflight += 1
    
    Normal --> Throttled: Throttle (429)
    Throttled --> Decreasing: max_inflight *= 0.5
    Decreasing --> Cooldown: Set cooldown timer
    Cooldown --> Normal: After 10 seconds
    
    Normal --> Error: Other error
    Error --> Normal: Reset streak
    
    note right of Normal
        Current State:
        - max_inflight: 10-100
        - current_inflight: 0-max
        - success_streak: 0-N
    end note
    
    note right of Throttled
        React Fast:
        - Halve max_inflight
        - Reset streak
        - Start cooldown
    end note
    
    note right of Increasing
        Cautious Growth:
        - Add 1 per streak
        - Never exceed max
        - Reset streak
    end note
```

## SQLite Outbox Tables

```mermaid
erDiagram
    OUTBOX_STEP_UPDATES {
        int id PK
        string step_id
        string status
        string error_message
        int retry_count
        text metadata
        real created_at
        string outbox_status
        real processed_at
    }
    
    OUTBOX_RESULTS {
        int id PK
        string step_id
        string table_name
        text record_data
        string partition_key
        real created_at
        string outbox_status
        real processed_at
        string error_message
    }
    
    OUTBOX_SHAREPOINT_OPS {
        int id PK
        string operation_id UK
        string op_type
        string site_url
        string file_path
        string local_path
        text metadata
        real created_at
        string outbox_status
        real processed_at
        string error_message
        text result_data
    }
    
    PRESSURE_STATE {
        int id PK "CHECK(id=1)"
        int max_inflight
        int current_inflight
        int success_streak
        real last_throttle_time
        real updated_at
    }
    
    CONTROL_PLANE_LEASE {
        int id PK "CHECK(id=1)"
        string lease_holder
        real acquired_at
        real expires_at
        real heartbeat_at
        string pod_id
        int process_id
    }
```

## Control Plane Startup Sequence

```mermaid
sequenceDiagram
    participant F as FastAPI Worker
    participant L as Lease Manager
    participant DB as SQLite Outbox
    participant S as APScheduler
    participant I as IOHub
    participant E as Execution Pool
    
    Note over F,E: Pod Startup (Multiple Workers)
    
    F->>L: Try acquire lease
    L->>DB: SELECT expires_at FROM lease
    
    alt Lease Available
        DB-->>L: No lease or expired
        L->>DB: INSERT/UPDATE lease<br/>expires_at = now + 30s
        DB-->>L: Success
        L-->>F: Lease Acquired ✅
        
        Note over F,E: This Worker Becomes Control Plane
        
        F->>I: Initialize IOHub
        I->>I: Create ZMQ ROUTER<br/>Bind to IPC
        I->>DB: Load pressure state
        
        F->>S: Initialize APScheduler
        S->>S: Register default jobs
        
        F->>S: Start scheduler
        S->>S: Begin job execution
        
        F->>I: Start IOHub
        I->>I: Begin message loop
        
        Note over F: Ready to coordinate workers
        
    else Lease Held
        DB-->>L: Lease held by another<br/>expires_at > now
        L-->>F: Lease Denied ❌
        
        Note over F: Regular API Worker<br/>No control plane duties
    end
```

## Worker Execution Flow

```mermaid
flowchart TD
    START([Worker Receives Task])
    CONNECT[Create IOHub Client<br/>ZMQ DEALER]
    REQUEST{Request<br/>Permit?}
    WAIT[Wait 500ms<br/>Retry]
    GRANTED{Granted?}
    EXECUTE[Execute Bedrock Call<br/>Async I/O]
    SUCCESS{Success?}
    REPORT_OK[Report Success<br/>+ Latency]
    REPORT_THROTTLE[Report Throttle]
    REPORT_ERROR[Report Error]
    ENQUEUE[Enqueue Result<br/>to Outbox]
    CLOSE[Close Client]
    END([Done])
    
    START --> CONNECT
    CONNECT --> REQUEST
    REQUEST --> GRANTED
    GRANTED -->|No| WAIT
    WAIT --> REQUEST
    GRANTED -->|Yes| EXECUTE
    EXECUTE --> SUCCESS
    SUCCESS -->|Yes| REPORT_OK
    SUCCESS -->|Throttle| REPORT_THROTTLE
    SUCCESS -->|Error| REPORT_ERROR
    REPORT_OK --> ENQUEUE
    REPORT_THROTTLE --> CLOSE
    REPORT_ERROR --> CLOSE
    ENQUEUE --> CLOSE
    CLOSE --> END
```

## Scheduled Jobs Timeline

```mermaid
gantt
    title APScheduler Jobs Timeline (First Minute)
    dateFormat mm:ss
    axisFormat %M:%S
    
    section Lease
    Renewal :00:00, 10s
    Renewal :00:10, 10s
    Renewal :00:20, 10s
    Renewal :00:30, 10s
    Renewal :00:40, 10s
    Renewal :00:50, 10s
    
    section Outbox
    Flush :00:00, 30s
    Flush :00:30, 30s
    
    section Monitoring
    Stats :00:00, 5m
```

## Resource Allocation

```mermaid
pie title Pod Resource Allocation
    "FastAPI Workers (4×)" : 30
    "Control Plane Runner" : 15
    "IOHub Message Loop" : 10
    "aiomultiprocess Pool (4×)" : 35
    "SQLite Outbox" : 5
    "APScheduler" : 5
```

## Component Relationships

```mermaid
graph TB
    subgraph External
        SF[Snowflake]
        BR[Bedrock]
        SP[SharePoint]
    end
    
    subgraph "Control Plane Runner"
        CPR[ControlPlaneRunner]
        
        subgraph Scheduler
            APS[APScheduler]
            J1[Lease Renewal]
            J2[Outbox Flush]
            J3[Cleanup]
        end
        
        subgraph Coordinator
            IOH[IOHub]
            AIMD[AIMD Brain]
            ROUTER[ZMQ ROUTER]
        end
        
        subgraph Storage
            OUT[SQLite Outbox]
            LEASE[Lease Table]
            PRESS[Pressure State]
        end
    end
    
    subgraph Workers
        DEALER1[DEALER 1]
        DEALER2[DEALER 2]
        DEALER3[DEALER 3]
    end
    
    CPR --> APS
    CPR --> IOH
    CPR --> OUT
    
    APS --> J1
    APS --> J2
    APS --> J3
    
    J1 --> LEASE
    J2 --> OUT
    J3 --> OUT
    
    IOH --> AIMD
    IOH --> ROUTER
    IOH --> OUT
    
    AIMD --> PRESS
    
    ROUTER <--> DEALER1
    ROUTER <--> DEALER2
    ROUTER <--> DEALER3
    
    DEALER1 --> BR
    DEALER2 --> BR
    DEALER3 --> BR
    
    OUT --> SF
    OUT --> SP
```

## Error Recovery Flow

```mermaid
flowchart TD
    ERROR([Error Occurs])
    TYPE{Error<br/>Type?}
    
    THROTTLE[Throttle 429]
    AIMD_DEC[AIMD Decrease<br/>max_inflight *= 0.5]
    COOLDOWN[Set Cooldown<br/>10 seconds]
    SAVE1[Save Pressure State]
    RETRY1[Worker Retries<br/>After Backoff]
    
    NETWORK[Network Error]
    RETRY2[Retry with<br/>Exponential Backoff]
    MAX_RETRY{Max<br/>Retries?}
    FAIL[Mark Failed]
    
    OTHER[Other Error]
    RESET_STREAK[Reset Success Streak]
    LOG[Log Error]
    SAVE2[Save to Outbox]
    
    ERROR --> TYPE
    
    TYPE -->|429| THROTTLE
    THROTTLE --> AIMD_DEC
    AIMD_DEC --> COOLDOWN
    COOLDOWN --> SAVE1
    SAVE1 --> RETRY1
    
    TYPE -->|Network| NETWORK
    NETWORK --> RETRY2
    RETRY2 --> MAX_RETRY
    MAX_RETRY -->|No| RETRY2
    MAX_RETRY -->|Yes| FAIL
    
    TYPE -->|Other| OTHER
    OTHER --> RESET_STREAK
    RESET_STREAK --> LOG
    LOG --> SAVE2
```

---

**Diagrams show:**
1. Complete system architecture with all components
2. Message flow for permit request/response cycle
3. AIMD state machine with transitions
4. SQLite outbox table relationships
5. Control plane startup sequence
6. Worker execution flow
7. Scheduled jobs timeline
8. Resource allocation breakdown
9. Component relationships and dependencies
10. Error recovery flows

These diagrams provide a comprehensive visual reference for understanding the In-Pod Execution Fabric architecture.
