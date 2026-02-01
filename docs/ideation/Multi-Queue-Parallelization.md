🔗 A Custom In-Pod Pipeline for Bedrock-Heavy Workloads

(Async + Multiprocessing + Adaptive Throttling)

As FLEET-Q evolved, one practical challenge became impossible to ignore:

Not all work is CPU-heavy — some work is overwhelmingly HTTP-heavy.

In our case, the processing stage is dominated by Bedrock API calls:
	•	minimal CPU usage
	•	significant network latency
	•	strict downstream capacity limits
	•	frequent throttling (429 Too Many Requests) when overloaded

Running this kind of workload with a naïve multiprocessing pool leads to two problems:
	1.	Over-parallelization — too many processes hitting Bedrock simultaneously
	2.	Client management issues — expensive initialization, non-pickleable HTTP clients, repeated auth overhead

To solve this cleanly, we introduced a custom in-pod pipeline, built entirely with:
	•	Python multiprocessing
	•	async I/O
	•	message queues (no external broker)
	•	adaptive throttling (AIMD)

This pipeline acts as a micro-broker inside each pod, complementing FLEET-Q’s Snowflake-backed global queue.

⸻

🧠 Why a Custom Pipeline?

Before diving into the design, it’s important to understand why this pattern exists at all.

The Core Constraints

Constraint	Implication
Bedrock calls are HTTP-heavy	Async I/O is far more efficient than CPU multiprocessing
Bedrock enforces soft & hard limits	We must adapt dynamically, not statically
Snowflake & SharePoint clients are non-pickleable	They cannot be freely passed across processes
Pods must remain self-contained	No Redis, Kafka, or external brokers

The solution is not “more processes” or “bigger pools”.

The solution is flow control.

⸻

🧩 Two-Tier Execution Model

FLEET-Q now operates in two clear tiers:

🧱 Tier 1 — Cluster-Level Coordination (Already Covered)
	•	Tasks are claimed atomically from Snowflake (STEP_TRACKER)
	•	Pods scale horizontally
	•	Leader handles recovery
	•	This layer decides what work exists

⚙️ Tier 2 — In-Pod Execution Pipeline (New)
	•	A custom pipeline handles how work is executed
	•	Optimized specifically for Bedrock-heavy workloads
	•	No external broker required

This section focuses on Tier 2.

⸻

🛠 The In-Pod Pipeline: Stages & Responsibilities

Inside each pod, task execution is decomposed into specialized stages, each running in its own process or pool.

📊 Pipeline Stages

Stage	Role	Concurrency Model
SharePoint Reader	Download inputs	Async I/O
Bedrock Processor	Call Bedrock APIs	Async + adaptive throttling
Snowflake Writer	Persist results	Sync batching
Coordination	Message passing	In-pod queues

Each stage is decoupled and communicates through queues, forming a true pipeline.

⸻

🔌 Message-Driven Execution (No External Broker)

Instead of Redis or Kafka, we use in-pod messaging:
	•	Messages flow forward only
	•	Each stage blocks naturally when the next stage is slow
	•	Backpressure is automatic

Conceptually:

SharePoint Path
   ↓
[ Download Queue ]
   ↓
Local File Path
   ↓
[ Processing Queue ]
   ↓
Processed Object
   ↓
[ Snowflake Write Queue ]

The queues act as:
	•	Triggers (new message → work starts)
	•	Boundaries (slow stages naturally throttle upstream stages)
	•	Buffers (smooth short bursts)

⸻

⚡ Why Async for the Processing Stage?

The processing stage is Bedrock HTTP heavy and CPU light.

This is the ideal scenario for async execution.

Async Benefits Here
	•	One process can manage many in-flight Bedrock calls
	•	While one request waits on the network, others proceed
	•	Far fewer processes are needed
	•	Memory footprint is smaller
	•	Throttling can be enforced precisely

Instead of:

“One process per Bedrock call”

We get:

“One event loop managing controlled concurrency”

⸻

🧠 Adaptive Throttling with AIMD (Critical Piece)

Async alone is not enough.

If we simply fire off async calls without restraint, we will still overwhelm Bedrock.

This is where AIMD-based adaptive throttling becomes essential.

⸻

🚀 AIMD in the Processing Stage

Each processing worker maintains a local throttle controller that governs how many Bedrock calls may be in flight.

Two Key Numbers

Variable	Meaning
current_inflight	Active Bedrock calls
max_inflight	Adaptive concurrency limit

Before a Bedrock call:
	•	The worker must acquire a permit
	•	If permits are exhausted, it waits

⸻

📈 Additive Increase (Probe Carefully)

When Bedrock responses are healthy:
	•	Requests succeed
	•	Latency is stable

We slowly increase concurrency:

max_inflight += 1

This gently probes for available capacity.

⸻

📉 Multiplicative Decrease (Protect Fast)

When Bedrock returns:
	•	429 Too Many Requests
	•	or capacity-related timeouts

We react immediately:

max_inflight = max_inflight * 0.5

This sharp reduction:
	•	protects Bedrock
	•	prevents cascading retries
	•	stabilizes the system quickly

⸻

⏱ Latency Awareness (Early Warning)

In addition to explicit throttling errors, we track rolling latency (e.g., p95).

Simple Rule

If latency is rising, pause further increases.

This prevents pushing the system right up to the breaking point.

The result is a pressure-sensitive nozzle:
	•	opens when capacity is available
	•	closes when pressure builds

⸻

🔁 How Throttling Interacts with the Pipeline

The beauty of this design is that throttling affects the entire pod naturally.

When Bedrock slows down:
	•	Processing stage accepts fewer messages
	•	The processing queue fills
	•	Upstream stages slow automatically
	•	No explicit coordination required

This is flow control by design, not by configuration.

⸻

🧱 Why This Is Better Than a Traditional Worker Pool

Traditional Pool	Custom Pipeline
Fixed concurrency	Adaptive concurrency
Blind to downstream pressure	Pressure-aware
Overloads APIs easily	Self-stabilizing
Client init per worker	Long-lived clients
Hard to debug throttling	Explicit control loop

Instead of reacting after failure, the pipeline continuously adapts.

⸻

🧠 Relationship to FLEET-Q’s Global Design

This pipeline does not replace FLEET-Q.

It complements it.

Layer	Responsibility
Snowflake / FLEET-Q	Distributed task ownership
In-Pod Pipeline	Efficient, safe execution
AIMD Throttling	Respect downstream capacity

Together, they form a distributed execution fabric:
	•	globally coordinated
	•	locally adaptive
	•	resilient under load

⸻

✨ Key Takeaway

This custom in-pod pipeline turns a hard problem —

“How do we safely scale Bedrock-heavy workloads without melting the API?”

— into a controlled, observable, and adaptive system.

By combining:
	•	async I/O
	•	multiprocessing isolation
	•	message-driven pipelines
	•	AIMD-based throttling

FLEET-Q doesn’t just execute tasks.

It learns how fast it is allowed to execute them — and adjusts continuously.

⸻

1) 🧭 End-to-End Architecture: FLEET-Q + In-Pod Micro-Broker Pipeline

flowchart TB
    %% =========================
    %% Tier 1: Cluster Coordination
    %% =========================
    subgraph Tier1["Tier 1  Cluster Coordination  Snowflake"]
        S1["STEP_TRACKER<br/>Global Queue Table"]
        S2["POD_HEALTH<br/>Heartbeats  Leader Election"]
    end

    subgraph Pod["One EKS Pod  FLEET-Q Worker"]
        P0["FastAPI Service<br/>FLEET-Q Runtime"]
        P1["Claim Loop<br/>Atomic Claim Transaction"]
        P2["Execute Step Orchestrator<br/>Dispatch into In-Pod Pipeline"]
        P3["Heartbeat Loop<br/>Update POD_HEALTH"]

        P0 --> P3
        P0 --> P1
        P1 --> P2
    end

    %% =========================
    %% Tier 2: In-Pod Micro Broker
    %% =========================
    subgraph Tier2["Tier 2  In-Pod Micro Broker  No External Broker"]
        Z0["pyzmq Local Messaging<br/>PUSH PULL Channels"]

        Q1["Channel A<br/>SharePoint Paths"]
        Q2["Channel B<br/>Local File Paths"]
        Q3["Channel C<br/>Processed Records"]

        Z0 --> Q1
        Z0 --> Q2
        Z0 --> Q3
    end

    %% =========================
    %% Stage A: SharePoint IO
    %% =========================
    subgraph StageA["Stage A  SharePoint IO  One Process"]
        A1["SharePoint Downloader<br/>Async IO Client"]
        A2["Download File to Local Disk<br/>Emit Local Path"]
        A1 --> A2
    end

    %% =========================
    %% Stage B: Bedrock Processing
    %% =========================
    subgraph StageB["Stage B  Bedrock Processing  Async Across Processes"]
        B0["aiomultiprocess Pool<br/>Async Workers"]
        B1["Bedrock Call Runner<br/>HTTP Heavy Work"]
        B2["AIMD Throttle Controller<br/>Adaptive Concurrency"]
        B3["Latency Watch<br/>Rolling p95 p99"]
        B4["Backoff Retry Wrapper<br/>Exponential Backoff"]

        B0 --> B1
        B1 --> B2
        B2 --> B4
        B2 --> B3
    end

    %% =========================
    %% Stage C: Snowflake Writer
    %% =========================
    subgraph StageC["Stage C  Snowflake Writer  One Process"]
        C1["Snowflake Writer<br/>Sync Batching"]
        C2["Batch Commit<br/>Update STEP_TRACKER Status"]
        C1 --> C2
    end

    %% External Systems
    X1["SharePoint<br/>External"]
    X2["Bedrock API<br/>External"]
    X3["Local Disk<br/>Pod Volume"]

    %% Connections Tier1
    P1 --> S1
    P3 --> S2

    %% Execute Orchestrator to micro broker
    P2 --> Q1

    %% Pipeline wiring via pyzmq channels
    Q1 --> A1
    A2 --> X3
    A2 --> Q2

    Q2 --> B0
    B1 --> X2
    B0 --> Q3

    Q3 --> C1
    C2 --> S1

    %% SharePoint external
    A1 --> X1

How to read this diagram
	•	Tier 1: Snowflake is the global coordination store for tasks and pod health
	•	Tier 2: pyzmq acts as an in-pod broker for staging work
	•	Stage A downloads inputs (async I/O)
	•	Stage B performs Bedrock-heavy work using aiomultiprocess + AIMD throttling
	•	Stage C writes results in a controlled, batched way to Snowflake

⸻

2) 🚀 AIMD Adaptive Throttling Loop for Bedrock (Detailed Control Flow)

This diagram zooms into the Bedrock processing stage and explains exactly how concurrency is adjusted based on outcomes.

flowchart TD
    S0["Async Worker Wants Bedrock Call"] --> S1["Acquire Permit<br/>current_inflight < max_inflight"]
    S1 -->|Permit Granted| S2["Send Bedrock Request"]
    S1 -->|No Permit| S3["Wait<br/>Semaphore Queue"]

    S2 --> S4{Bedrock Response}
    S4 -->|Success| S5["Record Success<br/>Latency Value"]
    S4 -->|Throttle 429| S6["Record Throttle Error"]
    S4 -->|Timeout 5xx| S7["Record Capacity Error"]

    S5 --> S8["Release Permit"]
    S6 --> S8
    S7 --> S8

    S8 --> S9["AIMD Update<br/>Adjust max_inflight"]
    S9 --> S10["Additive Increase<br/>If stable success and latency ok"]
    S9 --> S11["Multiplicative Decrease<br/>If throttle 429 or repeated capacity errors"]
    S9 --> S12["Pause Growth<br/>If p95 latency rising"]

    S10 --> S13["Continue Processing"]
    S11 --> S13
    S12 --> S13

    S13 --> S0

What this control loop guarantees
	•	🔻 Fast ramp down on throttling or saturation
	•	🔺 Slow ramp up only when stable
	•	🟡 Latency-aware pause to avoid pushing to failure
	•	🔁 Works continuously without hardcoding Bedrock limits

⸻
