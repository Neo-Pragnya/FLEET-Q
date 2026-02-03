# DuckDB Integration Architecture

This document shows how DuckDB utilities integrate with FLEET-Q's distributed execution framework.

## System Architecture

```mermaid
graph TB
    subgraph "Kubernetes Pod"
        subgraph "FLEET-Q Core"
            API[FastAPI Server]
            Worker[Worker Loops]
            Executor[Step Executor]
        end
        
        subgraph "DuckDB Utilities"
            Manager[DuckDBDataManager]
            Fuzzy[Fuzzy Matching Engine]
            DuckDB[(DuckDB<br/>In-Memory)]
            Polars[Polars DataFrames]
        end
        
        subgraph "IOHub & Storage"
            IOHub[IOHub Coordinator]
            SQLite[(SQLite<br/>Outbox)]
        end
    end
    
    subgraph "External Systems"
        SF[(Snowflake<br/>Data Warehouse)]
        S3[(S3/Storage<br/>Parquet Files)]
    end
    
    API --> Executor
    Executor --> Manager
    Manager --> DuckDB
    Manager --> Polars
    Manager --> Fuzzy
    Manager --> SF
    Manager --> S3
    
    Executor --> IOHub
    IOHub --> SQLite
    SQLite --> SF
    
    style Manager fill:#fff3cd
    style DuckDB fill:#d1ecf1
    style Fuzzy fill:#f8d7da
```

## Data Flow: Matching Task Execution

```mermaid
sequenceDiagram
    participant API as FastAPI API
    participant Worker as FLEET-Q Worker
    participant Executor as Step Executor
    participant Manager as DuckDBDataManager
    participant SF as Snowflake
    participant IOHub as IOHub
    
    API->>Worker: Submit matching task
    Worker->>Worker: Claim task
    Worker->>Executor: Execute step
    
    Executor->>Manager: Initialize (auto-configure)
    Manager->>Manager: Detect pod resources
    Manager->>Manager: Configure DuckDB (memory/threads)
    
    Executor->>Manager: load_from_snowflake(source_query)
    Manager->>SF: Execute query
    SF-->>Manager: Stream data (chunks)
    Manager->>Manager: Convert to Polars DataFrame
    
    Executor->>Manager: load_from_snowflake(target_query)
    Manager->>SF: Execute query
    SF-->>Manager: Stream data (chunks)
    Manager->>Manager: Convert to Polars DataFrame
    
    Executor->>Manager: fuzzy_match(source, target, method="jaro_winkler")
    Manager->>Manager: Calculate similarity scores
    Manager->>Manager: Filter by threshold
    Manager->>Manager: Return top matches
    
    Manager-->>Executor: List[MatchResult]
    Executor->>Executor: Convert to DataFrame
    
    Executor->>Manager: upload_to_snowflake(results)
    Manager->>SF: Create table (if needed)
    Manager->>SF: Insert data (chunks)
    
    Executor->>IOHub: Report SUCCESS
    IOHub->>SF: Update STEP_TRACKER
    
    API-->>API: Task complete
```

## Component Interaction

```mermaid
graph LR
    subgraph "FLEET-Q Step Execution"
        A[Claim Step] --> B[Load Data]
        B --> C[Process/Match]
        C --> D[Save Results]
        D --> E[Report Status]
    end
    
    subgraph "DuckDB Operations"
        B --> F[load_from_snowflake]
        B --> G[load_parquet]
        B --> H[load_json]
        
        C --> I[fuzzy_match]
        C --> J[exact_match]
        C --> K[deduplicate]
        C --> L[SQL query]
        
        D --> M[upload_to_snowflake]
        D --> N[save_parquet]
        D --> O[save_json]
    end
    
    F --> P[(Snowflake)]
    G --> Q[(S3/Parquet)]
    H --> Q
    M --> P
    N --> Q
    O --> Q
    
    style B fill:#e1f5e1
    style C fill:#fff3cd
    style D fill:#d1ecf1
```

## Fuzzy Matching Pipeline

```mermaid
flowchart TD
    Start[Start Fuzzy Match Task] --> LoadSource[Load Source Data]
    LoadSource --> LoadTarget[Load Target Data]
    
    LoadTarget --> CheckSize{Target Size<br/>< 10K?}
    
    CheckSize -->|Yes| DirectMatch[Direct Fuzzy Matching]
    CheckSize -->|No| BuildIndex[Build N-Gram Index]
    
    BuildIndex --> IndexedMatch[Indexed Fuzzy Matching]
    IndexedMatch --> GetCandidates[Get Candidates for Each Source]
    GetCandidates --> PreciseMatch[Precise Match on Candidates]
    
    DirectMatch --> FilterThreshold[Filter by Threshold]
    PreciseMatch --> FilterThreshold
    
    FilterThreshold --> TopK[Select Top-K Matches]
    TopK --> FormatResults[Format as MatchResult]
    FormatResults --> SaveResults[Save to Parquet/Snowflake]
    SaveResults --> End[Report Complete]
    
    style BuildIndex fill:#fff3cd
    style IndexedMatch fill:#d1ecf1
    style DirectMatch fill:#f8d7da
```

## Resource Management

```mermaid
graph TB
    subgraph "Pod Resources (4 cores, 8GB)"
        Total[Total Resources]
    end
    
    Total --> FLEETQ[FLEET-Q Core<br/>1 core, 2GB]
    Total --> DuckDB[DuckDB<br/>2.25 cores, 4GB]
    Total --> Reserve[Reserved<br/>0.75 cores, 2GB]
    
    FLEETQ --> API[FastAPI: 0.25 cores]
    FLEETQ --> Workers[Workers: 0.5 cores]
    FLEETQ --> IOHub[IOHub: 0.25 cores]
    
    DuckDB --> Threads[DuckDB Threads: 3]
    DuckDB --> Memory[Memory Limit: 4GB]
    DuckDB --> Cache[Object Cache: Enabled]
    
    Reserve --> Background[Background Tasks]
    Reserve --> Burst[Burst Handling]
    Reserve --> OS[OS Overhead]
    
    style DuckDB fill:#fff3cd
    style FLEETQ fill:#e1f5e1
    style Reserve fill:#d1ecf1
```

## Data Format Support

```mermaid
graph LR
    subgraph "Input Formats"
        SF[Snowflake<br/>Tables]
        Parquet[Parquet<br/>Files]
        JSON[JSON/JSONL<br/>Files]
    end
    
    subgraph "DuckDBDataManager"
        Manager[Data Manager]
        DuckDB[(DuckDB Engine)]
        Polars[Polars DataFrames]
    end
    
    subgraph "Output Formats"
        SFOut[Snowflake<br/>Tables]
        ParquetOut[Parquet<br/>Files]
        JSONOut[JSON/JSONL<br/>Files]
    end
    
    SF -->|load_from_snowflake| Manager
    Parquet -->|load_parquet| Manager
    JSON -->|load_json| Manager
    
    Manager --> DuckDB
    Manager --> Polars
    
    Manager -->|upload_to_snowflake| SFOut
    Manager -->|save_parquet| ParquetOut
    Manager -->|save_json| JSONOut
    
    style Manager fill:#fff3cd
```

## Matching Algorithm Selection

```mermaid
graph TD
    Start[Start Fuzzy Match] --> DataType{Data Type?}
    
    DataType -->|Person Names| Jaro[Jaro-Winkler<br/>threshold: 0.85-0.90]
    DataType -->|Company Names| JaroComp[Jaro-Winkler<br/>threshold: 0.80-0.85]
    DataType -->|Addresses| Token[Token Set Ratio<br/>threshold: 0.70-0.80]
    DataType -->|Products| TokenSort[Token Sort Ratio<br/>threshold: 0.75-0.85]
    DataType -->|Descriptions| Cosine[Cosine Similarity<br/>threshold: 0.60-0.70]
    DataType -->|Codes/SKUs| Lev[Levenshtein<br/>threshold: 0.90-0.95]
    
    Jaro --> Execute[Execute Matching]
    JaroComp --> Execute
    Token --> Execute
    TokenSort --> Execute
    Cosine --> Execute
    Lev --> Execute
    
    Execute --> Results[Return Matches]
    
    style Execute fill:#fff3cd
    style Results fill:#d1ecf1
```

## Performance Optimization Flow

```mermaid
flowchart TD
    Start[Matching Task] --> DetectResources[Detect Pod Resources]
    
    DetectResources --> ConfigDuckDB[Configure DuckDB<br/>Memory: 50% pod<br/>Threads: 75% cores]
    
    ConfigDuckDB --> EstimateSize{Dataset Size?}
    
    EstimateSize -->|Small<br/>< 10K rows| InMemory[In-Memory Matching<br/>All-to-All Comparison]
    
    EstimateSize -->|Medium<br/>10K-100K| Batched[Batch Processing<br/>Process in 10K chunks]
    
    EstimateSize -->|Large<br/>> 100K| Indexed[N-Gram Indexing<br/>Candidate Selection]
    
    InMemory --> ExecuteFast[Execute: 0.3-1s]
    Batched --> ExecuteMedium[Execute: 5-30s]
    Indexed --> ExecuteSlow[Execute: 30-300s]
    
    ExecuteFast --> Report[Report Results]
    ExecuteMedium --> Report
    ExecuteSlow --> Report
    
    style ConfigDuckDB fill:#e1f5e1
    style Indexed fill:#fff3cd
    style Report fill:#d1ecf1
```

## Example: Customer Matching Workflow

```mermaid
sequenceDiagram
    participant User as User/API
    participant FLEETQ as FLEET-Q Worker
    participant DuckDB as DuckDB Manager
    participant SF as Snowflake
    participant S3 as S3/Parquet
    
    User->>FLEETQ: Submit customer matching task
    FLEETQ->>FLEETQ: Claim task from STEP_TRACKER
    
    FLEETQ->>DuckDB: Initialize (auto-configure)
    Note over DuckDB: Detect 4 cores, 8GB<br/>Config: 3 threads, 4GB memory
    
    FLEETQ->>DuckDB: load_from_snowflake("SELECT * FROM customers")
    DuckDB->>SF: Execute query
    SF-->>DuckDB: Stream 10K customer records
    
    FLEETQ->>DuckDB: load_parquet("vendors.parquet")
    DuckDB->>S3: Read Parquet file
    S3-->>DuckDB: Stream 50K vendor records
    
    FLEETQ->>DuckDB: fuzzy_match(customers, vendors,<br/>method="jaro_winkler", threshold=0.85)
    
    Note over DuckDB: 10K × 50K = 500M comparisons<br/>Batch size: 10K<br/>Time: ~8 seconds
    
    DuckDB-->>FLEETQ: 7,500 matches (75% match rate)
    
    FLEETQ->>FLEETQ: Filter high confidence (> 0.9)
    FLEETQ->>DuckDB: save_parquet(high_conf, "matched.parquet")
    DuckDB->>S3: Write Parquet (compressed)
    
    FLEETQ->>DuckDB: upload_to_snowflake(all_matches, "customer_matches")
    DuckDB->>SF: Create table
    DuckDB->>SF: Insert 7,500 rows (chunks)
    
    FLEETQ->>SF: Update STEP_TRACKER (status=COMPLETED)
    FLEETQ-->>User: Task complete (7,500 matches)
```

## Memory Management

```mermaid
graph TB
    subgraph "Pod Memory: 8GB"
        Total[Total: 8GB]
    end
    
    Total --> DuckDB[DuckDB: 4GB<br/>50% allocation]
    Total --> FLEETQ[FLEET-Q: 2GB<br/>Workers + API]
    Total --> OS[OS + Buffers: 1GB]
    Total --> Free[Free: 1GB<br/>Burst handling]
    
    DuckDB --> InMem[In-Memory Tables<br/>~2GB]
    DuckDB --> Temp[Temp Buffers<br/>~1GB]
    DuckDB --> Cache[Object Cache<br/>~1GB]
    
    FLEETQ --> Workers[Worker State<br/>~1GB]
    FLEETQ --> IOHub[IOHub Queue<br/>~0.5GB]
    FLEETQ --> FastAPI[FastAPI<br/>~0.5GB]
    
    style DuckDB fill:#fff3cd
    style Free fill:#d1ecf1
```

## Integration Points

```mermaid
graph TB
    subgraph "FLEET-Q Core Services"
        API[FastAPI API]
        Claim[Claim Loop]
        Execute[Execute Loop]
        IOHub[IOHub]
    end
    
    subgraph "DuckDB Module"
        Manager[DuckDBDataManager]
        Fuzzy[Fuzzy Matching]
        DDB[(DuckDB)]
    end
    
    subgraph "Step Types"
        Match[Matching Steps]
        Dedupe[Deduplication Steps]
        Transform[Transform Steps]
        Load[Data Loading Steps]
    end
    
    API -->|Submit| Claim
    Claim -->|Assign| Execute
    
    Execute -->|matching| Match
    Execute -->|deduplication| Dedupe
    Execute -->|transformation| Transform
    Execute -->|loading| Load
    
    Match --> Manager
    Dedupe --> Manager
    Transform --> Manager
    Load --> Manager
    
    Manager --> Fuzzy
    Manager --> DDB
    
    Execute --> IOHub
    
    style Manager fill:#fff3cd
    style Fuzzy fill:#f8d7da
```

## End-to-End Example

```mermaid
flowchart LR
    subgraph "Input"
        SF1[(Snowflake<br/>Customers)]
        Parquet1[(S3<br/>Vendors)]
    end
    
    subgraph "FLEET-Q Pod"
        Load[Load Data]
        Match[Fuzzy Match<br/>Jaro-Winkler]
        Filter[Filter<br/>threshold > 0.85]
        Dedupe[Deduplicate<br/>Results]
    end
    
    subgraph "Output"
        Parquet2[(S3<br/>Matched.parquet)]
        SF2[(Snowflake<br/>Results Table)]
    end
    
    SF1 --> Load
    Parquet1 --> Load
    
    Load --> Match
    Match --> Filter
    Filter --> Dedupe
    
    Dedupe --> Parquet2
    Dedupe --> SF2
    
    style Match fill:#fff3cd
    style Filter fill:#d1ecf1
```

---

## Key Integration Benefits

1. **Seamless Data Flow**: Snowflake → DuckDB → Polars → Snowflake
2. **Pod-Aware Resources**: Auto-configures based on Kubernetes limits
3. **High Performance**: 3-12M comparisons/second for fuzzy matching
4. **Memory Efficient**: Streaming operations, lazy loading
5. **Format Flexibility**: Parquet, JSON, JSONL, Snowflake tables
6. **Algorithm Selection**: 7 fuzzy matching methods for different use cases
7. **Scalable**: N-gram indexing for large datasets (90%+ reduction)
8. **Robust**: Error handling, graceful degradation, retry logic

---

## Related Documentation

- [DuckDB Utils Guide](DUCKDB_UTILS_GUIDE.md) - Complete documentation
- [DuckDB Quick Reference](DUCKDB_QUICKREF.md) - Cheat sheet
- [Integration Summary](DUCKDB_INTEGRATION_SUMMARY.md) - Implementation details
- [Pod Resources Guide](POD_RESOURCES_GUIDE.md) - Resource detection
