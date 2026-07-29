# High-Throughput Fleet Tracking System (CQRS Architecture)

A highly scalable, distributed backend system designed to ingest, process, and query real-time IoT telemetry data (GPS coordinates) from tens of thousands of active vehicles. Built entirely in Go, this project implements the **CQRS (Command Query Responsibility Segregation)** pattern to achieve massive throughput with zero data loss.

## 🚀 Performance Metrics (Load Tested)
Running locally on a single machine via Docker Desktop, this architecture effortlessly handles:
* **Throughput:** `2,500+ Requests / Second`
* **Latency:** `~60 ms` average response time
* **Zero Data Loss:** Kafka buffering completely shields the persistent database from heavy traffic spikes.

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
    IoT[🚗 IoT Devices / Load Tester]:::client
    Client[💻 Web/Mobile Clients]:::client

    NGINX[🔀 Nginx Layer 7 Load Balancer]:::gateway

    IngestAPI[⚡ Ingestion API - Go]:::api
    QueryAPI[🔍 Query API - Go]:::api

    Kafka[(📨 Redpanda / Kafka)]:::queue
    Redis[(⚡ Redis In-Memory Cache)]:::cache
    TimescaleDB[(🗄️ TimescaleDB Columnar DB)]:::db

    Worker[⚙️ Stream Processor - Go]:::worker

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
* **High Availability:** The API layer is stateless and easily horizontally scalable behind Nginx.
* **Backpressure Management:** If the database experiences heavy load, Kafka acts as a shock-absorber. The ingestion API will never block or time out, and the stream processor will pull messages at a sustainable rate.
* **Storage Efficiency:** TimescaleDB utilizes chunk compression, allowing billions of GPS pings to be stored at a fraction of standard relational database sizes.
