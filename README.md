Multi-Source ETL Pipeline
A cloud-native ETL pipeline that ingests cryptocurrency data from multiple sources and stores it in MongoDB Atlas.
Technology Stack
LayerTechnologyPurposeRuntimeNode.jsJavaScript runtime environmentAPI FrameworkExpress.jsREST API serverDatabaseMongoDB AtlasCloud-hosted NoSQL databaseValidationJoiSchema validationSchedulingnode-cronAutomated ETL runsMetricsPrometheusMonitoring and metricsContainerizationDockerApplication containerizationOrchestrationDocker ComposeMulti-container orchestrationHTTP ClientAxiosAPI requestsCSV Parsingcsv-parserCSV file processing
Data Flow
mermaidsequenceDiagram
    participant S as Data Sources
    participant E as ETL Pipeline
    participant V as Validator
    participant M as MongoDB Atlas
    participant A as REST API
    
    S->>E: Raw Data
    E->>V: Validate Schema
    V->>E: Validation Result
    E->>E: Transform & Normalize
    E->>M: Check Existing Data
    M->>E: Return Status
    E->>M: Upsert Records
    M->>E: Confirmation
    E->>M: Update Checkpoint
    A->>M: Query Data
    M->>A: Return Results
Quick Start
Prerequisites

Docker Desktop installed
Docker Compose installed
MongoDB Atlas account (free tier)

Setup & Run

Clone the repository

bash   git clone https://github.com/yourusername/etl-services.git
   cd etl-services

Configure environment

bash   cp .env.example .env
   # Edit .env and add your MongoDB Atlas URI

Start all services

bash   docker-compose up -d

Verify services are running

bash   docker-compose ps
Access Services
ServiceURLDescriptionREST APIhttp://localhost:3000Main API endpointsAPI Docshttp://localhost:3000/api-docsSwagger documentationMetricshttp://localhost:3000/metricsPrometheus metricsPrometheushttp://localhost:9090Metrics dashboard
Docker Compose Services
yamlServices:
  - etl-api:        REST API server (Port 3000)
  - etl-scheduler:  Automated ETL runs (cron-based)
  - prometheus:     Metrics monitoring (Port 9090)
Key Features

✅ Multi-Source Ingestion: CoinPaprika API, CoinGecko API, CSV files
✅ Schema Validation: Automatic validation and type conversion
✅ Incremental Loading: Skip already-processed data
✅ Idempotent Operations: Duplicate handling with unique indexes
✅ Checkpointing: Resume from last successful point
✅ Monitoring: Prometheus metrics and logging
✅ Cloud Storage: MongoDB Atlas integration

Management Commands
bash# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Restart specific service
docker-compose restart etl-api

# Rebuild and restart
docker-compose up -d --build

# Trigger manual ETL run
curl -X POST http://localhost:3000/api/etl/run
Environment Variables
bash# MongoDB
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/etl_db

# API Configuration
PORT=3000
NODE_ENV=production

# ETL Settings
BATCH_SIZE=100
MAX_RETRIES=3
RETRY_DELAY=1000
Project Structure
etl-services/
├── api/              # REST API endpoints
├── etl/              # ETL pipeline logic
├── Service/          # Data source connectors
├── Schemas/          # Data validation schemas
├── DB/               # Database connections
├── Schedular/        # Cron job configuration
├── Config/           # Application configuration
├── Utility/          # Helper functions
├── docker-compose.yml
├── Dockerfile
└── .env
Monitoring
View real-time metrics at http://localhost:3000/metrics:

Pipeline execution count
Processing duration
API request latency
Error rates by source
Total records processed

Troubleshooting
Services won't start:
bashdocker-compose logs
MongoDB connection failed:

Verify MongoDB Atlas URI in .env
Check IP whitelist in MongoDB Atlas
Ensure database user has read/write permissions

API not responding:
bashdocker-compose restart etl-api
curl http://localhost:3000/health