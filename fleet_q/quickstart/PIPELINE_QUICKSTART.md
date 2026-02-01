# FLEET-Q In-Pod Pipeline Quickstart

Get started with the FLEET-Q parallelization pipeline in 5 minutes.

## 🚀 Quick Start

### 1. Run the Demo

```bash
cd fleet_q/quickstart/

# Basic demo with 20 files (mock mode)
python pipeline_demo.py

# Process more files
python pipeline_demo.py --files 100

# Stress test for 60 seconds
python pipeline_demo.py --mode stress --duration 60
```

### 2. Understand the Output

```
======================================================================
FLEET-Q In-Pod Pipeline Demo
======================================================================

Pipeline stages:
  1. SharePoint Reader  (async downloads)
  2. Bedrock Processor  (async + AIMD throttling)
  3. Snowflake Writer   (batched writes)

[INFO] Downloaded file-001: 2,300 bytes in 0.15s (15.3 KB/s)
[INFO] Request req-001 completed in 0.23s (throttle limit: 6)
[INFO] Flushing batch batch-000001 with 10 records

Pipeline Complete!
Total time: 8.45s
Throughput: 2.37 files/sec
```

**Key metrics to watch:**
- `throttle limit: 6` - AIMD is adapting concurrency dynamically
- `Flushing batch batch-000001` - Batching is working
- Individual stage timing

## 📐 Pipeline Architecture

### System Overview

```mermaid
graph TB
    subgraph "FLEET-Q Cluster Level"
        SF[Snowflake STEP_TRACKER]
        CLAIM[Pod Claims Task]
    end
    
    subgraph "In-Pod Pipeline"
        subgraph "Stage 1: SharePoint Reader"
            SR_SETUP[setup: Create async loop]
            SR_PROC[process_message: Download file]
            SR_TEAR[teardown: Close loop]
        end
        
        subgraph "Queue 1"
            Q1[Bounded Queue maxsize=100]
        end
        
        subgraph "Stage 2: Bedrock Processor"
            BP_SETUP[setup: Init throttle + event loop]
            BP_PROC[process_message: Call API with throttle]
            BP_TEAR[teardown: Flush + close]
        end
        
        subgraph "Queue 2"
            Q2[Bounded Queue maxsize=100]
        end
        
        subgraph "Stage 3: Snowflake Writer"
            SW_SETUP[setup: Connect to Snowflake]
            SW_PROC[process_message: Add to batch]
            SW_FLUSH[_flush_batch: Multi-row INSERT]
            SW_TEAR[teardown: Final flush]
        end
    end
    
    SF --> CLAIM
    CLAIM --> SR_SETUP
    SR_SETUP --> SR_PROC
    SR_PROC --> Q1
    Q1 --> BP_SETUP
    BP_SETUP --> BP_PROC
    BP_PROC --> Q2
    Q2 --> SW_SETUP
    SW_SETUP --> SW_PROC
    SW_PROC --> SW_FLUSH
    SW_FLUSH --> SW_TEAR
    
    style SF fill:#e1f5ff
    style CLAIM fill:#fff3cd
    style Q1 fill:#d4edda
    style Q2 fill:#d4edda
    style SR_PROC fill:#cfe2ff
    style BP_PROC fill:#cfe2ff
    style SW_PROC fill:#cfe2ff
```

### File Structure & Dependencies

```mermaid
graph LR
    subgraph "Core Files"
        PIPE[pipeline.py<br/>493 lines]
        CONFIG[config.py]
        STORAGE[storage.py]
        BACKOFF[backoff.py]
        THROTTLE[throttle.py]
    end
    
    subgraph "Stage Files"
        SP[sharepoint_reader.py<br/>366 lines]
        BR[bedrock_processor.py<br/>455 lines]
        SF[snowflake_writer.py<br/>422 lines]
    end
    
    subgraph "Demo & Integration"
        DEMO[pipeline_demo.py<br/>448 lines]
        INTEG[pipeline_integration.py<br/>404 lines]
    end
    
    subgraph "Main FLEET-Q"
        MAIN[main.py]
        WORKER[worker.py]
    end
    
    PIPE --> SP
    PIPE --> BR
    PIPE --> SF
    BACKOFF --> SP
    BACKOFF --> SF
    THROTTLE --> BR
    STORAGE --> SF
    CONFIG --> STORAGE
    
    SP --> DEMO
    BR --> DEMO
    SF --> DEMO
    PIPE --> DEMO
    
    DEMO --> INTEG
    PIPE --> INTEG
    INTEG --> MAIN
    INTEG --> WORKER
    
    style PIPE fill:#ffcccc
    style SP fill:#cce5ff
    style BR fill:#cce5ff
    style SF fill:#cce5ff
    style DEMO fill:#d4edda
    style INTEG fill:#d4edda
```

## 🏗️ Build Your Own Pipeline

### Minimal Example

```python
from pipeline import Pipeline, PipelineStage, PipelineMessage, MessageType

# Define a simple processing stage
class MyProcessor(PipelineStage):
    def process_message(self, message: PipelineMessage):
        # Your logic here
        result = process(message.payload)
        return PipelineMessage(MessageType.DATA, result)

# Create and run pipeline
pipeline = Pipeline("my-pipeline")
pipeline.add_stage(MyProcessor("processor"))
pipeline.build()
pipeline.start()

# Feed work
input_queue = pipeline.get_first_queue()
input_queue.put(PipelineMessage(MessageType.DATA, "work item"))

# Cleanup
pipeline.wait()
pipeline.stop()
```

### Three-Stage Pipeline

```python
from pipeline import Pipeline
from sharepoint_reader import SharePointReaderStage
from bedrock_processor import BedrockProcessorStage
from snowflake_writer import SnowflakeWriterStage

# Create pipeline
pipeline = Pipeline("document-processing")

# Stage 1: Download
pipeline.add_stage(SharePointReaderStage(
    stage_name="Download",
    max_concurrent=10
))

# Stage 2: Process with Bedrock
pipeline.add_stage(BedrockProcessorStage(
    stage_name="Analyze",
    throttle_config=ThrottleConfig(
        initial_limit=5,
        max_limit=20
    )
))

# Stage 3: Write to Snowflake
pipeline.add_stage(SnowflakeWriterStage(
    stage_name="Store",
    table_name="RESULTS",
    batch_size=50
))

# Build and start
pipeline.build()
pipeline.start()

# Your code to feed work...
# pipeline.wait()
# pipeline.stop()
```

## 🔧 Integration with FLEET-Q

### Integration Patterns Overview

```mermaid
graph TB
    subgraph "Pattern A: Pipeline Per Task"
        A1[Task Claimed] --> A2[Create Pipeline]
        A2 --> A3[pipeline.build]
        A3 --> A4[pipeline.start]
        A4 --> A5[Feed work items]
        A5 --> A6[pipeline.wait]
        A6 --> A7[pipeline.stop]
        A7 --> A8[Return result]
        
        note_a[Overhead: 2-3s per task<br/>Use for: Low volume]
    end
    
    subgraph "Pattern B: Persistent Pipeline"
        B1[App Startup] --> B2[Create Pipeline]
        B2 --> B3[pipeline.build]
        B3 --> B4[pipeline.start]
        B4 --> B5[Pipeline Running]
        
        B6[Task Claimed] --> B7[Feed into existing pipeline]
        B7 --> B5
        
        B8[App Shutdown] --> B9[pipeline.stop]
        B5 --> B9
        
        note_b[Overhead: 2-3s once<br/>Use for: High volume]
    end
    
    style A2 fill:#fff3cd
    style A7 fill:#fff3cd
    style B2 fill:#d4edda
    style B5 fill:#d4edda
    style B9 fill:#f8d7da
```

### Function Call Flow: Persistent Pipeline

```mermaid
sequenceDiagram
    participant App as FastAPI App
    participant LF as lifespan()
    participant PM as PipelineManager
    participant PL as Pipeline
    participant Stages as Stage Processes
    participant Task as execute_task()
    
    Note over App,Stages: Application Startup
    App->>LF: Context enter
    LF->>PM: startup_pipeline()
    PM->>PM: __init__()<br/>self.pipeline = None
    PM->>PL: Pipeline('persistent')
    PL->>PL: add_stage(Reader)<br/>add_stage(Processor)<br/>add_stage(Writer)
    PL->>PL: build()<br/>Create queues
    PM->>PL: start()
    PL->>Stages: spawn Process 1, 2, 3
    Stages->>Stages: setup() in each
    PM->>PM: self.is_running = True
    
    Note over App,Stages: Task Processing (repeated)
    App->>Task: FLEET-Q claims task
    Task->>PM: get_pipeline_manager()
    PM-->>Task: return manager
    Task->>PM: process_task(step)
    PM->>PL: get_first_queue()
    PL-->>PM: return input_queue
    PM->>PL: input_queue.put(message)
    PL->>Stages: Message flows through
    Stages->>Stages: Process in stages
    PM-->>Task: {"status": "accepted"}
    Task-->>App: Task result
    
    Note over App,Stages: Application Shutdown
    App->>LF: Context exit
    LF->>PM: shutdown_pipeline()
    PM->>PL: stop(timeout=30)
    PL->>Stages: Send POISON pills
    Stages->>Stages: teardown() in each
    Stages->>Stages: Exit processes
    PL->>PL: join all processes
    PM->>PM: self.is_running = False
```

### Option A: Persistent Pipeline (Recommended)

Add to your `main.py`:

```python
from pipeline_integration import startup_pipeline, shutdown_pipeline, get_pipeline_manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Existing startup
    worker = WorkerLoops(config, storage, queue_ops)
    worker_task = asyncio.create_task(worker.start_all())
    
    # Start persistent pipeline
    startup_pipeline()
    
    yield
    
    # Shutdown
    worker.stop_all()
    shutdown_pipeline()
    await worker_task

# In your execute_task function
async def execute_task(step):
    task_type = step['PAYLOAD']['task_type']
    
    if task_type == 'document_processing':
        # Use persistent pipeline
        manager = get_pipeline_manager()
        return manager.process_task(step)
    else:
        # Regular execution
        return await process_regular_task(step)
```

### Option B: Pipeline Per Task

```python
async def execute_task(step):
    if step['PAYLOAD']['task_type'] == 'large_batch':
        # Create dedicated pipeline
        pipeline = create_pipeline()
        pipeline.start()
        
        feed_work(pipeline, step['PAYLOAD']['items'])
        
        pipeline.wait()
        pipeline.stop()
        
        return {"status": "complete"}
```

## 📊 Monitoring

### Check Throttle Status

```bash
# Get throttle statistics
curl http://localhost:8000/admin/throttle

# Response:
{
  "throttles": {
    "bedrock": {
      "max_inflight": 15,
      "current_inflight": 8,
      "total_requests": 1000,
      "throttle_rate": 0.05
    }
  }
}
```

### Stage Metrics

```python
# After pipeline completes
for stage in pipeline.stages:
    print(f"{stage.stage_name}:")
    print(f"  Processed: {stage.processed_count}")
    print(f"  Errors: {stage.error_count}")
```

## 🎯 Key Concepts

### 1. Message-Driven

Stages communicate via messages:

```python
message = PipelineMessage(
    msg_type=MessageType.DATA,
    payload=your_data,
    metadata={'task_id': '123'}
)
```

#### Message Flow Diagram

```mermaid
sequenceDiagram
    participant Input as Input Queue
    participant Stage1 as Stage 1<br/>(Reader)
    participant Queue1 as Queue 1
    participant Stage2 as Stage 2<br/>(Processor)
    participant Queue2 as Queue 2
    participant Stage3 as Stage 3<br/>(Writer)
    
    Note over Input,Stage3: Normal Message Flow
    Input->>Stage1: PipelineMessage(DATA, payload)
    Stage1->>Stage1: process_message(msg)
    Stage1->>Queue1: send_downstream(result)
    Queue1->>Stage2: get(timeout=1.0)
    Stage2->>Stage2: process_message(msg)
    Stage2->>Queue2: send_downstream(result)
    Queue2->>Stage3: get(timeout=1.0)
    Stage3->>Stage3: process_message(msg)
    
    Note over Input,Stage3: Shutdown Flow
    Input->>Stage1: PipelineMessage(POISON, None)
    Stage1->>Queue1: Forward POISON
    Queue1->>Stage2: POISON received
    Stage2->>Queue2: Forward POISON
    Queue2->>Stage3: POISON received
    Stage3->>Stage3: teardown() and exit
```

### 2. Automatic Backpressure

When downstream stage is slow, upstream stage automatically throttles. No configuration needed.

### 3. AIMD Throttling

Bedrock processor adapts concurrency:
- Success → Slowly increase limit
- Throttle error → Quickly decrease limit
- Rising latency → Pause increases

#### AIMD Algorithm Flow

```mermaid
stateDiagram-v2
    [*] --> Probing: Start at initial_limit
    
    Probing --> CheckingLatency: After N successes
    CheckingLatency --> Increasing: Latency stable
    CheckingLatency --> Paused: Latency rising
    
    Increasing --> Probing: limit += additive_increase
    Paused --> Probing: Wait for latency to drop
    
    Probing --> ThrottleError: 429 error received
    Increasing --> ThrottleError: 429 error received
    Paused --> ThrottleError: 429 error received
    
    ThrottleError --> Backing: limit *= multiplicative_decrease
    Backing --> Probing: Recovery complete
    
    note right of Increasing
        Additive Increase
        limit = limit + 1
        (cautious growth)
    end note
    
    note right of ThrottleError
        Multiplicative Decrease
        limit = limit * 0.5
        (aggressive backoff)
    end note
```

#### Throttle State Tracking

```mermaid
flowchart LR
    subgraph "AdaptiveThrottle State"
        LIMIT[max_inflight: 10]
        CURRENT[current_inflight: 3]
        SUCCESS[success_count: 150]
        THROTTLE[throttle_count: 2]
        LATENCY[p95_latency: 0.45s]
    end
    
    subgraph "Decision Logic"
        CHECK{Check conditions}
        INC[Increase limit]
        DEC[Decrease limit]
        PAUSE[Pause increases]
    end
    
    LIMIT --> CHECK
    CURRENT --> CHECK
    SUCCESS --> CHECK
    THROTTLE --> CHECK
    LATENCY --> CHECK
    
    CHECK -->|success_count >= threshold<br/>AND latency stable| INC
    CHECK -->|throttle_count > 0| DEC
    CHECK -->|latency > baseline * 1.5| PAUSE
    
    INC --> LIMIT
    DEC --> LIMIT
    PAUSE --> SUCCESS
    
    style LIMIT fill:#ffeb99
    style CHECK fill:#cce5ff
    style INC fill:#d4edda
    style DEC fill:#f8d7da
    style PAUSE fill:#fff3cd
```

### 4. Graceful Shutdown

```python
# Poison pill signals completion
poison = PipelineMessage(MessageType.POISON, None)
input_queue.put(poison)

# All stages flush and cleanup
pipeline.wait()
```

## 🔍 Troubleshooting

### Stage Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Created: PipelineStage.__init__()
    Created --> Setup: run() called in new process
    Setup --> Running: setup() completes
    
    Running --> Processing: get message from input_queue
    Processing --> Running: send_downstream(result)
    
    Running --> Checking: Check shutdown_event
    Checking --> Running: Not set, continue
    Checking --> Teardown: shutdown_event.is_set()
    
    Running --> PoisonReceived: POISON message
    PoisonReceived --> ForwardPoison: output_queue.put(POISON)
    ForwardPoison --> Teardown: Break loop
    
    Running --> ErrorHandling: Exception in process_message
    ErrorHandling --> Running: Log error, increment error_count
    
    Teardown --> Cleanup: teardown() called
    Cleanup --> [*]: Process exits
    
    note right of Setup
        Initialize resources:
        - Async event loop
        - Database connections
        - API clients
    end note
    
    note right of Processing
        Core work:
        - process_message()
        - Transform data
        - Call APIs
    end note
    
    note right of Teardown
        Cleanup:
        - Flush buffers
        - Close connections
        - Log final metrics
    end note
```

### Pipeline Hangs

**Symptom:** Pipeline doesn't complete

**Causes:**
1. Forgot to send poison pill
2. Stage raised unhandled exception
3. Queue is full (backpressure)

**Solution:**
```python
# Always send poison pill
poison = PipelineMessage(MessageType.POISON, None)
input_queue.put(poison)

# Use timeout
pipeline.wait()  # Blocks until complete
# or
pipeline.stop(timeout=30)  # Force stop after 30s
```

### High Error Rate

**Symptom:** Many errors in stage logs

**Check:**
```python
for stage in pipeline.stages:
    if stage.error_count > 0:
        print(f"{stage.stage_name}: {stage.error_count} errors")
```

**Common causes:**
- API credentials not configured
- Network issues
- Invalid payload format

### Throttle Limit Too Low

**Symptom:** Throttle stays at minimum

**Causes:**
1. Downstream API is actually throttling
2. Latency threshold too sensitive
3. Not enough successful requests

**Solution:**
```python
throttle_config = ThrottleConfig(
    initial_limit=10,  # Start higher
    min_limit=5,       # Increase minimum
    latency_increase_threshold=2.0,  # Less sensitive
    success_threshold=5  # Increase sooner
)
```

## 📚 Examples

### Custom Stage

```python
from pipeline import PipelineStage, PipelineMessage, MessageType

class EmailSenderStage(PipelineStage):
    def setup(self):
        self.smtp_client = create_smtp_client()
    
    def process_message(self, message: PipelineMessage):
        if message.msg_type != MessageType.DATA:
            return message
        
        email_data = message.payload
        self.smtp_client.send(
            to=email_data['recipient'],
            subject=email_data['subject'],
            body=email_data['body']
        )
        
        return PipelineMessage(
            MessageType.DATA,
            {"sent": True, "recipient": email_data['recipient']}
        )
    
    def teardown(self):
        self.smtp_client.close()
```

### Adapter Stage

Convert between formats:

```python
from pipeline import TransformStage

def adapt_format(message):
    if not isinstance(message, PipelineMessage):
        return message
    
    # Convert format
    old_data = message.payload
    new_data = {
        'id': old_data['legacy_id'],
        'value': transform(old_data['legacy_value'])
    }
    
    return PipelineMessage(
        MessageType.DATA,
        new_data,
        metadata=message.metadata
    )

adapter = TransformStage(
    stage_name="FormatAdapter",
    transform_fn=adapt_format
)
```

## 🎓 Next Steps

1. **Run the demo** - See it in action
2. **Read pipeline.py** - Understand base classes
3. **Check pipeline_integration.py** - See integration patterns
4. **Build custom stages** - For your specific workload
5. **Monitor metrics** - Watch AIMD adapt

## 💡 Tips

✅ **DO:**
- Use persistent pipeline for high-volume workloads
- Monitor throttle metrics
- Let AIMD adapt naturally
- Use batching for writes
- Send poison pill for graceful shutdown

❌ **DON'T:**
- Create pipeline per small task
- Hardcode concurrency limits
- Skip error handling in stages
- Forget to call teardown
- Block in async stages

## � Class Relationships

### Core Classes

```mermaid
classDiagram
    class PipelineMessage {
        +MessageType msg_type
        +Any payload
        +Dict metadata
        +float timestamp
    }
    
    class MessageType {
        <<enumeration>>
        DATA
        POISON
        HEARTBEAT
    }
    
    class PipelineStage {
        +str stage_name
        +Queue input_queue
        +Queue output_queue
        +Event shutdown_event
        +int processed_count
        +int error_count
        +setup()
        +teardown()
        +process_message(msg) PipelineMessage
        +send_downstream(msg)
        +run()
    }
    
    class Pipeline {
        +str name
        +List~PipelineStage~ stages
        +List~Queue~ queues
        +List~Process~ processes
        +Event shutdown_event
        +add_stage(stage)
        +build()
        +start()
        +stop(timeout)
        +wait()
        +get_first_queue() Queue
    }
    
    class SharePointReaderStage {
        +int max_concurrent
        +Path download_dir
        +Semaphore semaphore
        +EventLoop event_loop
        -_download_file_async(request)
        -_mock_download(request)
        -_real_download(request)
    }
    
    class BedrockProcessorStage {
        +AdaptiveThrottle throttle
        +EventLoop event_loop
        +int throttle_errors
        +float total_latency
        -_process_request_async(request)
        -_mock_bedrock_call(request)
        -_real_bedrock_call(request)
    }
    
    class SnowflakeWriterStage {
        +str table_name
        +int batch_size
        +float flush_timeout
        +List~WriteRecord~ current_batch
        +int batch_counter
        -_flush_batch()
        -_write_batch_with_retry(batch)
        -_mock_write(batch)
        -_real_write(batch)
    }
    
    class AdaptiveThrottle {
        +str name
        +int max_inflight
        +int current_inflight
        +ThrottleConfig config
        +acquire() AsyncContextManager
        +record_success(latency)
        +record_throttle()
        +record_timeout()
        -_maybe_increase_limit()
        -_should_pause_increase_due_to_latency()
    }
    
    class PipelineManager {
        +Pipeline pipeline
        +bool is_running
        +start()
        +stop()
        +process_task(step) Dict
    }
    
    Pipeline "1" *-- "*" PipelineStage : contains
    Pipeline "1" *-- "*" Queue : manages
    Pipeline "1" *-- "*" Process : spawns
    
    PipelineStage <|-- SharePointReaderStage : inherits
    PipelineStage <|-- BedrockProcessorStage : inherits
    PipelineStage <|-- SnowflakeWriterStage : inherits
    
    PipelineStage ..> PipelineMessage : uses
    PipelineMessage ..> MessageType : uses
    
    BedrockProcessorStage *-- AdaptiveThrottle : contains
    
    PipelineManager *-- Pipeline : manages
```

### Key Method Interactions

```mermaid
flowchart TD
    subgraph "Pipeline Orchestration"
        A[Pipeline.build] --> B[Create Queue objects]
        B --> C[Wire stages with queues]
        C --> D[Set shutdown_event]
        
        E[Pipeline.start] --> F[Create Process for each stage]
        F --> G[Process.start]
        G --> H[Stage.run in new process]
    end
    
    subgraph "Stage Execution Loop"
        H --> I[Stage.setup]
        I --> J{shutdown_event?}
        J -->|No| K[input_queue.get timeout=1]
        K --> L{Message type?}
        L -->|DATA| M[Stage.process_message]
        L -->|POISON| N[Break loop]
        M --> O[Stage.send_downstream]
        O --> J
        N --> P[Stage.teardown]
    end
    
    subgraph "Graceful Shutdown"
        Q[Pipeline.stop] --> R[shutdown_event.set]
        R --> S[Send POISON to first queue]
        S --> T[Wait for processes timeout=30]
        T --> U[Terminate if needed]
    end
    
    P --> T
    
    style A fill:#e1f5ff
    style E fill:#e1f5ff
    style H fill:#d4edda
    style M fill:#cfe2ff
    style Q fill:#f8d7da
```

## �🔗 References

- [Parallelization.md](../../docs/Parallelization.md) - Full design document
- [README.md](README.md) - Complete FLEET-Q documentation
- Individual stage files for implementation details

## ❓ Questions?

Common questions:

**Q: When should I use the pipeline?**  
A: When tasks involve multiple API calls, file I/O, or need adaptive throttling.

**Q: Pipeline vs regular execution?**  
A: Pipeline for HTTP-heavy, regular for CPU-heavy.

**Q: How many stages?**  
A: Typically 3-5. More stages = more isolation but more overhead.

**Q: Can I use real APIs in demo?**  
A: Yes, use `--real` flag and configure credentials.

**Q: How to tune throttle config?**  
A: Start conservative (low initial limit), let AIMD adapt. Monitor `/admin/throttle`.
