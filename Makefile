.PHONY: up down logs test smoke-test fail seed-drift clean

# Quick start commands
up:
	docker-compose up --build -d

down:
	docker-compose down

logs:
	docker-compose logs -f api

# Testing commands
test:
	npm test

smoke-test:
	npm run smoke-test

# Fault injection for testing resume functionality
fail:
	@echo "Triggering ETL run..."
	curl -s -X POST -H "Authorization: Bearer demo-token-123" localhost:8080/refresh > /dev/null &
	@echo "Waiting 3 seconds for batch processing to start..."
	sleep 3
	@echo "Killing API process mid-batch..."
	docker-compose exec api pkill -9 node || true
	@echo "Restarting service..."
	docker-compose restart api
	@echo "Waiting for service to be ready..."
	sleep 5

# Schema drift testing - rename fields and flip types
seed-drift:
	@echo "Creating schema drift: usd_price → price_in_usd, timestamp str → int"
	@echo "symbol,coin_name,price_in_usd,vol_24h,market_capitalization,change_24h,timestamp_unix" > Service/Historical_Data_Drift.csv
	@echo "BTC,Bitcoin,50000.50,1000000000,950000000000,2.5,1704067200" >> Service/Historical_Data_Drift.csv
	@echo "ETH,Ethereum,3000.25,500000000,360000000000,1.8,1704067260" >> Service/Historical_Data_Drift.csv
	@echo "ADA,Cardano,0.45,200000000,15000000000,-0.8,1704067320" >> Service/Historical_Data_Drift.csv

# Resume testing after failure
resume:
	@echo "Resuming ETL after failure..."
	curl -s -X POST -H "Authorization: Bearer demo-token-123" localhost:8080/refresh | jq '.run_id'
	@echo "Checking resume status..."
	sleep 2
	curl -s localhost:8080/runs | jq '.[0] | {run_id, status, resume_from}'

# Cleanup
clean:
	docker-compose down -v
	docker system prune -f