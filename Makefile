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
	docker-compose exec api pkill -f "node api/server.js" || true
	docker-compose restart api

# Schema drift testing
seed-drift:
	@echo "Creating schema drift test data..."
	@echo "symbol,coin_name,price_dollars,vol_24h,market_capitalization,change_24h,ts" > Service/Historical_Data_Drift.csv
	@echo "BTC,Bitcoin,50000,1000000000,950000000000,2.5,1704067200" >> Service/Historical_Data_Drift.csv
	@echo "ETH,Ethereum,3000,500000000,360000000000,1.8,1704067200" >> Service/Historical_Data_Drift.csv

# Cleanup
clean:
	docker-compose down -v
	docker system prune -f