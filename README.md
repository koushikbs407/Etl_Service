# Kasparro ETL System

![CI Status](https://github.com/koushikbs407/Etl_Service/workflows/CI%20Pipeline/badge.svg)

A resilient Market Data ETL and API service built with Node.js, Express, and MongoDB, fully containerized with Docker. This project ingests data from multiple sources (public APIs and CSV files), normalizes it, and serves it via a RESTful API while handling failures gracefully.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Node.js 16+ (for local development)
- Git

### Installation
```bash
git clone https://github.com/koushikbs407/Etl_Service.git
cd etl-services
cp .env.example .env
docker-compose up -d --build
```

### Trigger ETL
```bash
curl -X POST http://localhost:8080/refresh
```

## 🧪 CI/CD & Testing

### Local Testing
```bash
npm install
npm run lint          # ESLint code quality
npm run typecheck     # Type validation
npm test              # Unit tests
npm run smoke-test    # End-to-end smoke test
```

### CI Pipeline
The repository includes GitHub Actions CI that runs on every PR:
- ✅ **Lint**: Code quality checks with ESLint
- ✅ **TypeCheck**: Type validation
- ✅ **Unit Tests**: Jest test suite
- ✅ **Smoke Test**: End-to-end integration test

### Smoke Test Coverage
The smoke test verifies complete ETL flow:
1. Seeds tiny CSV test data
2. Triggers `POST /refresh`
3. Validates `/metrics` contains ETL metrics
4. Confirms `/runs` shows ETL execution
5. Verifies end-to-end data processing

## 📊 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | System health check |
| `/metrics` | GET | Prometheus metrics |
| `/stats` | GET | ETL statistics |
| `/data` | GET | Normalized data with filters |
| `/runs` | GET | ETL run history |
| `/refresh` | POST | Trigger ETL manually |

## 🏗️ Architecture

The system implements:
- **Incremental Loads**: Watermark-based processing
- **Rate Limiting**: Adaptive throttling with exponential backoff
- **Fault Tolerance**: Checkpoint/resume functionality
- **Monitoring**: Prometheus metrics exposition
- **Schema Drift**: Automatic field mapping detection

## 🔧 Troubleshooting

### 429 / Throttle Loops
Check token-bucket config in `Config/config.js`

### Resume Not Working
Ensure `run_id + batch_no + offset` are persisted before each commit

### No Metrics
Verify prom-client registration and `/metrics` route

## 📈 Monitoring

Access monitoring endpoints:
- **Metrics**: http://localhost:8080/metrics
- **Health**: http://localhost:8080/health
- **API Docs**: http://localhost:8080/api-docs