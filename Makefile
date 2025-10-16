.PHONY: up down refresh test eval fail

# Start all services
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# Trigger ETL refresh
refresh:
	@echo "Triggering ETL refresh..."
	@curl -X POST http://localhost:3000/refresh \
		-H "Authorization: Bearer $(shell node -e "console.log(require('jsonwebtoken').sign({}, process.env.REFRESH_JWT_SECRET || 'dev-secret'))")"

# Simulate failure during ETL
fail:
	@echo "Inducing ETL failure..."
	@docker-compose exec -e FAULT_INJECTION=true api node -e "process.exit(1)" || true
	@echo "Restarting service..."
	@docker-compose restart api

# Run tests
test:
	docker-compose exec api npm test

# Show service status and latest ETL metrics
eval:
	@echo "\nService Status:"
	@curl -s http://localhost:3000/health | jq
	@echo "\nLatest ETL Stats:"
	@curl -s http://localhost:3000/stats | jq
	@echo "\nRecent ETL Runs:"
	@curl -s http://localhost:3000/runs | jq '.runs[:3]'

# Test schema drift detection
seed-drift:
	@echo "Simulating schema drift..."
	@node test-schema-drift.js

# Default target
all: up