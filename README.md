# High-Throughput Fleet Tracking System (CQRS Architecture)

A highly scalable, distributed backend system designed to ingest, process, and query real-time IoT telemetry data (GPS coordinates) from tens of thousands of active vehicles. Built entirely in Go, this project implements the **CQRS (Command Query Responsibility Segregation)** pattern to achieve massive throughput with zero data loss.

## 🚀 Performance Metrics (Load Tested)
Running locally on a single machine via Docker Desktop:
* **Peak Distributed Throughput (Write Path):** `~435 Requests / Second` (60s load test across 3 ingestion replicas and 10 Kafka partitions)
* **Average Latency:** `~355ms` response time under sustained 200-worker bombardment (halved from initial implementations through Kafka batching).
* **Concurrency Limitations:** Tested with 200 concurrent workers. Shows Nginx/OS connection limits under sustained local Docker Desktop execution on Windows (some connection drops observed), but architecture maintains stability.
* **Data Durability:** Kafka buffers the persistent database from traffic spikes, ensuring zero data loss for all accepted payloads.

## 🏗️ System Architecture

The architecture separates the high-volume write path (Command) from the low-latency read path (Query).

```mermaid
graph TD
    %% Define Styles
    classDef client fill:#2D3748,stroke:#4A5568,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef gateway fill:#ED8936,stroke:#DD6B20,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef api fill:#4299E1,stroke:#3182CE,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef queue fill:#48BB78,stroke:#38A169,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef cache fill:#E53E3E,stroke:#C53030,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef db fill:#319795,stroke:#2C7A7B,stroke-width:2px,color:#fff,rx:5px,ry:5px;
    classDef worker fill:#805AD5,stroke:#6B46C1,stroke-width:2px,color:#fff,rx:5px,ry:5px;

    %% Nodes
    IoT["🚗 IoT Devices / Load Tester"]:::client
    Client["💻 Web/Mobile Clients"]:::client

    NGINX["🔀 Nginx Layer 7 Load Balancer"]:::gateway

    IngestAPI["⚡ Ingestion API (3x Replicas)"]:::api
    QueryAPI["🔍 Query API"]:::api

    Kafka[("📨 Redpanda / Kafka - 10 Partitions")]:::queue
    Redis[("⚡ Redis In-Memory Cache")]:::cache
    TimescaleDB[("🗄️ TimescaleDB Columnar DB")]:::db

    Worker["⚙️ Stream Processor (3x Replicas)"]:::worker

    %% Command Path (Writes)
    IoT -- "POST /api/v1/telemetry" --> NGINX
    NGINX -- "Round Robin" --> IngestAPI
    IngestAPI -- "Produce Payload" --> Kafka
    Kafka -- "Consume Batch" --> Worker
    Worker -- "O(1) Update Latest State" --> Redis
    Worker -- "Batch Insert History" --> TimescaleDB

    %% Query Path (Reads)
    Client -- "GET /api/v1/assets/:id/location" --> NGINX
    NGINX -- "Route to Query Service" --> QueryAPI
    QueryAPI -- "Fetch Real-Time State" --> Redis
    
    Client -- "GET /api/v1/assets/:id/route" --> NGINX
    QueryAPI -- "Fetch Historical Data" --> TimescaleDB
```

## 🗄️ Database Schema & TimescaleDB Optimization
Instead of a traditional relational table, this system leverages **TimescaleDB Hypertables** to manage massive volumes of time-series telemetry data efficiently.

* **Hypertables & Automatic Partitioning:** The `location_history` table is not a standard Postgres table; it is a hypertable automatically partitioned into 1-hour chunks based on the timestamp.
* **Columnar Compression:** Older chunks are automatically compressed and grouped by `asset_id`. This drastically reduces storage costs and massively speeds up historical route queries (`/api/v1/assets/:id/route`).
* **Schema:**
  * `assets` table: Stores static metadata (`id`, `driver_name`).
  * `location_history` table: Stores billions of compressed data points (`time`, `asset_id`, `latitude`, `longitude`).

## 🛠️ Technology Stack
* **Language:** Go (Golang)
* **API Gateway / Load Balancer:** Nginx
* **Message Broker:** Redpanda (Kafka-compatible) for asynchronous ingestion buffering.
* **In-Memory Cache:** Redis for `O(1)` real-time location lookups.
* **Database:** TimescaleDB (PostgreSQL) with columnar compression and continuous aggregates for time-series data.
* **Infrastructure:** Docker & Docker Compose.

## 📂 Codebase Structure
* `api-gateway/` - Nginx configurations for reverse-proxying and load-balancing.
* `cmd/ingestion-api/` - Fast HTTP server that validates payloads and pushes them directly to Kafka.
* `cmd/stream-processor/` - Background worker that safely drains the Kafka queue, updates Redis, and batch-inserts into TimescaleDB.
* `cmd/query-api/` - Handles client read requests and exposes a full system observability dashboard.
* `cmd/loadtest/` - High-concurrency load testing script to stress test the architecture.
* `internal/` - Shared business logic, domain models, and infrastructure adapters.

## 🚀 Quick Start

1. **Configure Environment Variables:**
Create a `.env` file in the root directory (this file is gitignored to protect secrets).
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=fleet
DB_CONN=postgres://postgres:your_secure_password@timescaledb:5432/fleet?sslmode=disable
```

2. **Spin up the cluster:**
```bash
docker compose up -d --build
```

3. **Run the High-Concurrency Load Tester:**
Stress test the architecture with 200 concurrent background workers firing payloads for 10 seconds.
```powershell
.\run-loadtest.ps1
```

4. **View the Observability Dashboard:**
The load tester automatically scrapes the `Query API` at the end of the run to generate a massive, structured JSON report detailing the health, queue offsets, memory usage, and throughput of all system components. Open `loadtest_results.log` to see the performance metrics!

## 💡 System Design Highlights
* **Idempotency:** The database utilizes `UNIQUE (asset_id, time)` constraints and `ON CONFLICT DO NOTHING` statements to prevent duplicate telemetry points during Kafka batch redeliveries.
* **Batch Processing:** The stream processors aggregate Kafka messages into 50ms time-windows (or up to 1,000 payloads) before issuing a single bulk SQL `INSERT`, allowing the consumers to process the queue synchronously under load.
* **High Availability & Horizontal Scaling:** The ingestion and stream processing layers are decoupled and horizontally scaled (3 replicas each) across 10 Kafka partitions.
* **Backpressure Management:** Kafka acts as a buffer. The ingestion API does not block or time out under database load.
* **Storage Efficiency:** TimescaleDB utilizes chunk compression to reduce the storage footprint of time-series data.
