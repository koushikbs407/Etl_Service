# Kasparro ETL System



A resilient Market Data ETL and API service built with Node.js, Express, and MongoDB, fully containerized with Docker. This project demonstrates **adaptive rate limiting**, **transactional resume**, **automated schema drift mapping**, **Prometheus metrics**, and **incremental loads**.

## ✅ Advanced Proof Checklist

This demonstrates adaptive rate limiting, transactional resume, automated schema drift mapping, Prometheus metrics, and incremental loads. All proofs are reproducible via make commands and simple curls.

### 0) Quick Start
```bash
# 1) Configure env
cp .env.example .env

# 2) Build & run services (API + DB)
make up   # or: docker-compose up --build -d

# 3) Health check
curl -s localhost:8080/health | jq
```

### 1) Adaptive Rate Limiting (Per-Source Token Bucket)
**Goal**: Show different quotas (e.g., A: 10 req/min, C: 3 req/min) + throttle metrics.

```bash
# Trigger a run that hits both API sources
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq

# Watch logs (throttle events should appear)
docker logs -f etl-services-api-1
```

**Verify (metrics)**:
```bash
curl -s localhost:8080/metrics | grep -E 'throttle|quota|etl_latency_seconds|etl_rows_processed_total'
```

**Accept if**: Logs show throttle events for stricter sources. `/metrics` shows `throttle_events_total{source=...}` and latency histogram.

### 2) Transactional Resume (Persisted Checkpoints)
**Goal**: Crash mid-run, then resume without duplicates.

```bash
# Run (induce failure)
make fail   # kills the app mid-batch
# Bring service back
make up
```

**Verify**:
```bash
# Resume on next refresh (should skip completed batches)
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq

# Inspect runs
curl -s localhost:8080/runs | jq '.[:3]'
RUN_ID=<paste-one>
curl -s localhost:8080/runs/$RUN_ID | jq
```

**Accept if**: `runs/:id` shows `failed_batches` and resumed batch. No duplicates when querying normalized collections.

### 3) Automated Schema Drift Mapping (Fuzzy)
**Goal**: Rename a column & change a type between runs; auto-map when confidence ≥ 0.8.

```bash
make seed-drift  # renames column, flips type
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq
```

**Verify**:
```bash
# Check run metadata for applied_mappings with confidence
curl -s localhost:8080/runs | jq '.[:1]'
# Check logs for "low_confidence" warnings
docker logs etl-services-api-1 | grep -i confidence
```

**Accept if**: `applied_mappings[{from,to,confidence}]` recorded with confidence ≥ 0.8. Low-confidence fields skipped and warned.

### 4) Prometheus Metrics Exposition
**Goal**: Expose operational counters/histograms.

```bash
curl -s localhost:8080/metrics | sed -n '1,50p'
# Expect:
# etl_rows_processed_total{source="A"} 1234
# etl_errors_total{source="C",type="data"} 2
# throttle_events_total{source="C"} 7
# etl_latency_seconds_bucket{stage="extract",le="..."} ...
```

**Accept if**: `/metrics` responds with Prometheus format and includes rows, errors, throttle, latency.

### 5) Incremental Loads (Watermark / Upsert)
**Goal**: Re-run ETL and confirm no re-ingestion of already processed data.

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq
```

**Verify**:
```bash
# Stats should show stable totals or explicit "skipped" count
curl -s localhost:8080/stats | jq
```

**Accept if**: Second run shows skipped/duplicate=0 inserts (upsert or watermark). `/stats` or `/runs/:id` reports incremental behavior.

### 6) API Surface Proof
```bash
# Data with filters + pagination
curl -s 'localhost:8080/data?symbol=BTC&limit=5&sort=timestamp:desc' | jq

# Stats + Health
curl -s localhost:8080/stats | jq
curl -s localhost:8080/health | jq

# Runs history
curl -s localhost:8080/runs | jq '.[:5]'
```

**Accept if**: Every response includes `request_id`, `run_id` (where applicable), and `api_latency_ms`.

### 7) Local Smoke Test
**Goal**: Ensure repo ships with guardrails.

```bash
# Run local smoke test that:
# 1) seeds tiny CSV
# 2) POST /refresh
# 3) asserts /metrics and /runs not empty
npm run smoke-test
```

**Accept if**: Smoke test passes; proves end-to-end flow locally.

## 🎯 Proving Advanced Requirements

### Live Demo: Adaptive Rate Limiting in Action

```bash
# Set up environment
export TOKEN="demo-token-123"

# 1. Trigger ETL to hit rate limits
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq

# 2. Check throttle metrics immediately
curl -s localhost:8080/metrics | grep throttle_events_total
# Expected output:
# throttle_events_total{source="coinpaprika"} 3
# throttle_events_total{source="coingecko"} 7

# 3. View rate limit configuration
curl -s localhost:8080/metrics | grep quota_requests_per_minute
# Expected output:
# quota_requests_per_minute{source="coinpaprika"} 10
# quota_requests_per_minute{source="coingecko"} 3

# 4. Watch live throttling in logs
docker logs -f etl-services-api-1 | grep -i throttle
```

### Live Demo: Crash Recovery & Resume

```bash
# 1. Start ETL process
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq '.run_id'
# Note the run_id: "abc-123-def"

# 2. Induce crash mid-processing
make fail  # Kills container during batch processing

# 3. Check failed run status
curl -s localhost:8080/runs | jq '.[0] | {run_id, status, failed_batches}'
# Expected output:
# {
#   "run_id": "abc-123-def",
#   "status": "failed",
#   "failed_batches": [{"batchNum": 3, "error": "Simulated crash"}]
# }

# 4. Restart service
make up

# 5. Resume processing
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq

# 6. Verify resume worked
curl -s localhost:8080/runs | jq '.[0] | {run_id, status, resume_info}'
# Expected output:
# {
#   "run_id": "xyz-456-ghi",
#   "status": "success", 
#   "resume_info": {"coinpaprika": {"resumedFromBatch": 3}}
# }
```

### Live Demo: Schema Drift Detection

```bash
# 1. Create schema drift (rename columns)
make seed-drift
# This creates: symbol,coin_name,price_dollars,vol_24h,market_capitalization,change_24h,ts
# Instead of:   symbol,name,price_usd,volume_24h,market_cap,percent_change_24h,timestamp

# 2. Trigger ETL with drifted schema
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq '.run_id'

# 3. Check applied mappings
curl -s localhost:8080/runs | jq '.[0].applied_mappings'
# Expected output:
# [
#   {"from": "coin_name", "to": "name", "confidence": 0.85},
#   {"from": "price_dollars", "to": "price_usd", "confidence": 0.92},
#   {"from": "ts", "to": "timestamp", "confidence": 0.78}
# ]

# 4. Check confidence warnings in logs
docker logs etl-services-api-1 | grep -i "low_confidence"
# Expected: Warning for "ts" field (confidence < 0.8)
```

### Live Demo: Incremental Processing

```bash
# 1. First ETL run
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq '.pre_run_counts'
# Output: {"raw": 0, "normalized": 0}

# 2. Check results
curl -s localhost:8080/stats | jq '.counts'
# Output: {"raw": 266, "normalized": 266}

# 3. Second ETL run (should skip existing data)
curl -s -X POST -H "Authorization: Bearer $TOKEN" localhost:8080/refresh | jq '.pre_run_counts'
# Output: {"raw": 266, "normalized": 266}

# 4. Verify incremental behavior
curl -s localhost:8080/stats | jq '.incremental'
# Expected output:
# {
#   "last_run_new_records": 0,
#   "last_run_skipped": 266,
#   "total_duplicate_prevention": 532
# }
```

### Live Demo: Real-Time Metrics

```bash
# Monitor ETL metrics in real-time
watch -n 2 'curl -s localhost:8080/metrics | grep -E "etl_rows_processed_total|etl_latency_seconds_count|throttle_events_total"'

# Expected live output:
# etl_rows_processed_total{source="coinpaprika"} 1234
# etl_rows_processed_total{source="coingecko"} 567  
# etl_latency_seconds_count{stage="extract"} 15
# throttle_events_total{source="coingecko"} 7
```

## 🧪 Local Testing

### Testing Commands
```bash
npm install
npm run lint          # ESLint code quality
npm run typecheck     # Type validation
npm test              # Unit tests
npm run smoke-test    # End-to-end smoke test
```

### Make Commands
```bash
make up              # Start all services
make down            # Stop all services
make logs            # View API logs
make test            # Run unit tests
make smoke-test      # Run smoke test
make fail            # Induce failure for testing resume
make seed-drift      # Create schema drift test data
make clean           # Clean up containers and volumes
```

## 📊 API Endpoints

| Endpoint | Method | Description | Features |
|----------|--------|-------------|----------|
| `/health` | GET | System health check | Component status, DB connectivity |
| `/metrics` | GET | Prometheus metrics | ETL counters, histograms, gauges |
| `/stats` | GET | ETL statistics | Incremental behavior, error rates |
| `/data` | GET | Normalized crypto data | Filtering, pagination, sorting |
| `/runs` | GET | ETL run history | Resume info, failed batches |
| `/runs/:id` | GET | Specific run details | Applied mappings, schema drift |
| `/refresh` | POST | Trigger ETL manually | Async processing, pre-run counts |
| `/api-docs` | GET | Swagger documentation | Interactive API explorer |

**All endpoints include**: `request_id`, `api_latency_ms`, and `run_id` (where applicable)

## 🏗️ Architecture

The system implements advanced ETL patterns:
- **🔄 Adaptive Rate Limiting**: Per-source token bucket with different quotas
- **💾 Transactional Resume**: Persisted checkpoints for crash recovery
- **🤖 Schema Drift Detection**: Automated fuzzy field mapping with confidence scoring
- **📈 Prometheus Metrics**: Comprehensive operational monitoring
- **⬆️ Incremental Loads**: Watermark-based processing to prevent duplicates
- **🚪 API Surface**: RESTful endpoints with consistent response format
- **🧪 Local Testing**: Comprehensive test suite with smoke tests

## 🏆 Advanced Features

### Rate Limiting
- **Per-source quotas**: Different limits for each data source
- **Token bucket algorithm**: Smooth rate limiting with burst capacity
- **Exponential backoff**: Adaptive retry with increasing delays
- **Metrics tracking**: Monitor throttle events and latency

### Fault Tolerance
- **Checkpoint persistence**: Resume from exact failure point
- **Batch-level recovery**: Skip completed batches on resume
- **Transactional safety**: No data loss during crashes
- **Failed batch tracking**: Detailed failure reporting

### Schema Evolution
- **Fuzzy matching**: Automatic field mapping with confidence scores
- **Version tracking**: Schema version bumping per run
- **Drift warnings**: Low-confidence mappings logged as warnings
- **Applied mappings**: Full audit trail of schema changes

### Observability
- **Request tracing**: Unique request IDs for all API calls
- **Latency tracking**: API response time monitoring
- **ETL metrics**: Rows processed, errors, and performance data
- **Health checks**: Component status monitoring

## 🔧 Troubleshooting

### 429 / Throttle Loops
Check token-bucket config in `Config/config.js`

### Resume Not Working
Ensure `run_id + batch_no + offset` are persisted before each commit

### No Metrics
Verify prom-client registration and `/metrics` route

### Schema Drift Issues
Check confidence scores in run metadata; low confidence fields are skipped with warnings

### Incremental Load Problems
Verify watermark timestamps and unique constraints on `{symbol, timestamp, source}`

## 📈 Monitoring

Access monitoring endpoints:
- **Metrics**: http://localhost:8080/metrics (Prometheus format)
- **Health**: http://localhost:8080/health (System status)
- **Stats**: http://localhost:8080/stats (ETL statistics)
- **Runs**: http://localhost:8080/runs (ETL run history)
- **Data**: http://localhost:8080/data (Normalized crypto data)
- **API Docs**: http://localhost:8080/api-docs (Swagger UI)