Kasparro Backend & ETL Systems Challenge
This repository contains a resilient, self-healing Extract, Transform, Load (ETL) pipeline and an associated REST API system. The system is designed to ingest, normalize, and serve data from multiple, dynamic sources while ensuring high observability and fault tolerance.

The project is structured to meet advanced requirements, including handling rate limiting, automated schema drift, and providing transactional recovery from mid-run failures.

1. System Architecture
The entire system is containerized and managed via Docker Compose. The architecture is modular, separating the data ingestion logic (scheduler) from the data serving layer (api).

Architecture Diagram
The diagram below illustrates the flow of data from external sources through the ETL process, into the MongoDB database, and finally out through the API and into the Prometheus monitoring service.

<details>
<summary>Click to Expand Architecture Diagram (HTML)</summary>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kasparro Architecture</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            flex-direction: column;
            background-color: #f4f4f9;
        }
        .container {
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 20px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        }
        .component {
            background-color: #0077c2; /* Deep Blue for main components */
            color: white;
            padding: 10px 20px;
            margin: 10px;
            border-radius: 5px;
            text-align: center;
            width: 180px;
            box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.2);
            font-weight: bold;
            transition: transform 0.2s;
        }
        .component:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 10px rgba(0, 0, 0, 0.15);
        }
        .data-source {
            background-color: #4CAF50; /* Green for data sources */
            width: 140px;
        }
        .db {
            background-color: #FF9800; /* Orange for Database */
        }
        .monitoring {
            background-color: #9C27B0; /* Purple for Monitoring */
            width: 180px;
        }
        .flow-line {
            height: 30px;
            border-left: 2px dashed #666;
            margin: 0 10px;
            position: relative;
        }
        .arrow::after {
            content: '▼';
            position: absolute;
            bottom: -8px;
            left: -6px;
            color: #666;
            font-size: 14px;
        }
        .horizontal-flow {
            display: flex;
            align-items: center;
            margin: 10px 0;
            gap: 10px;
        }
        .horizontal-arrow {
            width: 40px;
            height: 0;
            border-top: 2px dashed #666;
            margin: 0 5px;
            position: relative;
        }
        .horizontal-arrow::after {
            content: '▶';
            position: absolute;
            right: -8px;
            top: -6px;
            color: #666;
            font-size: 14px;
        }
        .label {
            font-size: 0.85em;
            color: #555;
            margin-top: -10px;
            margin-bottom: 5px;
            font-style: italic;
        }
        h1 {
            color: #333;
        }
    </style>
</head>
<body>
    <h1>Kasparro ETL & API System Architecture</h1>
    <div class="container">
        <!-- Top Row: Data Sources -->
        <div class="horizontal-flow">
            <div class="data-source component">Source A (API)</div>
            <div class="data-source component">Source B (CSV)</div>
            <div class="data-source component">Source C (API/RSS)</div>
        </div>
        <div class="label">Multi-Source Data Ingestion</div>

        <!-- Flow to Scheduler -->
        <div class="flow-line arrow"></div>
        <div class="label">ETL Run Trigger (Hourly)</div>

        <!-- ETL/Scheduler Component -->
        <div class="component">SCHEDULER SERVICE</div>
        <div class="label">Rate Limiting / Schema Mapping / Failure Recovery</div>

        <!-- Flow to DB -->
        <div class="flow-line arrow"></div>
        <div class="label">Store Raw + Normalized Data</div>

        <!-- Database Component -->
        <div class="db component">MONGODB (etl_db)</div>
        <div class="label">Data Storage / Checkpoints / ETL Metadata</div>

        <!-- Flow to API -->
        <div class="flow-line arrow"></div>
        <div class="label">Serves Data & Metrics</div>

        <!-- API Component -->
        <div class="component">API SERVICE</div>
        <div class="label">/data, /stats, /metrics, /health, /refresh (Express/FastAPI)</div>

        <!-- Horizontal Flow for Monitoring -->
        <div class="horizontal-flow" style="margin-left: auto; margin-right: auto; width: fit-content; align-items: flex-start;">
            <div style="flex-direction: column; display: flex; align-items: center;">
                <div class="horizontal-arrow"></div>
                <div class="label">Scrapes /metrics</div>
            </div>
            <div class="monitoring component">PROMETHEUS</div>
        </div>

    </div>
</body>
</html>

</details>

2. Running the System Locally
The system relies on Docker Compose to manage its services.

Prerequisites
Docker and Docker Compose (v2.x)

2.1. Docker Compose Configuration
The following docker-compose.yml file defines the four main services:

version: "3.8"

services:
  mongodb:
    image: mongo:6
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5

  api:
    build: .
    ports:
      - "3000:3000"
    environment:
      - MONGODB_URI=mongodb://mongodb:27017/etl_db
      - PORT=3000
      - NODE_ENV=production
    depends_on:
      mongodb:
        condition: service_healthy
    volumes:
      - ./Service/Historical_Data.csv:/app/Service/Historical_Data.csv # Source B data

  scheduler:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["node", "Schedular/etlScheduler.js"] # Hourly ETL entry point
    environment:
      - MONGODB_URI=mongodb://mongodb:27017/etl_db
      - NODE_ENV=production
      - ALERT_EMAIL_USER=${ALERT_EMAIL_USER:-}
      - ALERT_EMAIL_PASS=${ALERT_EMAIL_PASS:-}
    depends_on:
      mongodb:
        condition: service_healthy
      api:
        condition: service_started

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    depends_on:
      - api

volumes:
  mongodb_data:

2.2. Quick Start
Prepare CSV Data: Ensure the Historical_Data.csv file for Source B is present in the ./Service directory.

Start the Stack:

docker compose up --build -d

Monitor Status: Check the status of the containers. The api and scheduler will wait for mongodb to become healthy.

docker compose ps

Access Services:

API: http://localhost:3000

Prometheus: http://localhost:9090

3. API Endpoints
The api service exposes several REST endpoints for data retrieval, status checks, and operational control. Every successful API response includes traceability headers (request_id, run_id, api_latency_ms).

Method

Endpoint

Description

Query Parameters

GET

/data

Fetch Normalized Data. Returns the latest unified data with filtering, pagination, and sorting capabilities.

page, limit, symbol, sort_by

GET

/stats

Operational Statistics. Provides high-level metrics: record counts per source, average ETL latency, last successful run ID, and system error rates.

None

POST

/refresh

Manual ETL Trigger. Forces an immediate execution of the ETL pipeline. (Requires JWT Authentication).

None

GET

/metrics

Observability Endpoint. Exposes metrics in Prometheus format (latency histograms, error counters, etc.).

None

GET

/health

System Health Check. Reports the connectivity status of the API service, MongoDB, and the scheduler service.

None

4. Key Design Decisions
4.1. Recovery Flow: Checkpointing & Idempotency
To ensure Constraint 3: Failure Recovery is met, the ETL pipeline employs two mechanisms:



Checkpointing (Resume): The scheduler logs metadata (e.g., last successfully processed batch ID/timestamp) to a dedicated etl_runs collection in MongoDB before processing a batch. If the ETL fails, the next run reads the last successful checkpoint and resumes cleanly from that point, avoiding data reprocessing.



Idempotency (Safety): When loading data into the final normalized collection, a unique, deterministic hash key (idempotency_key) is calculated for each record (e.g., using source name, symbol, and original timestamp). This key is used to perform an UPSERT (Update or Insert) operation, preventing duplicate records if the same data chunk is accidentally run twice.



4.2. Schema Drift Handling
For Constraint 2: Schema Drift, the ETL process includes a dynamic mapping step:

It maintains a configuration of expected fields.

New source fields are compared against the known schema using fuzzy string matching (e.g., Levenshtein distance).

If the confidence of the match is below 0.8, the field is skipped, and a warning is logged in the etl_runs summary.




4.3. Rate Limiting
To adhere to the rate limits for Source A (10/min) and Source C (3/min), the ETL client implements an adaptive backoff strategy. It tracks the time window and request count. If a limit is detected, it pauses execution and uses an exponential backoff algorithm before retrying, ensuring service reliability and compliance.