.PHONY: up down refresh test eval

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

# Run tests
test:
	docker-compose exec api npm test

# Show service status and latest ETL metrics
eval:
	@echo "\nService Status:"
	@curl -s http://localhost:3000/health | jq
	@echo "\nLatest ETL Stats:"
	@curl -s http://localhost:3000/stats | jq

# Default target
all: up