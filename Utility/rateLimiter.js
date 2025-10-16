// rateLimiter.js
const axios = require('axios');
const promClient = require('prom-client');
const config = require('../Config/config');

// Lightweight in-memory TTL cache to avoid external dependency
const cache = new Map(); // key -> { value, expiresAt }
const STD_TTL_MS = 60 * 1000; // 60 seconds

// Get the registry from the server
const register = promClient.register;

const cacheGet = (key) => {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    cache.delete(key);
    return null;
  }
  return entry.value;
};

const cacheSet = (key, value, ttlMs = STD_TTL_MS) => {
  cache.set(key, { value, expiresAt: Date.now() + ttlMs });
};

// Rate limiter state per source (from config)
const rateLimiters = {};
Object.keys(config.rateLimits).forEach(source => {
  const sourceConfig = config.rateLimits[source];
  rateLimiters[source] = {
    limit: sourceConfig.requestsPerMinute,
    burstCapacity: sourceConfig.burstCapacity,
    interval: 60000, // 1 minute
    tokens: sourceConfig.burstCapacity, // Start with full burst
    lastRefill: Date.now(),
    retryBackoffMs: sourceConfig.retryBackoffMs
  };
});

// Prometheus metrics
const throttleEventsTotal = new promClient.Counter({
  name: 'throttle_events_total',
  help: 'Total throttle events by source',
  labelNames: ['source']
});

const retryLatencySeconds = new promClient.Histogram({
  name: 'retry_latency_seconds',
  help: 'Retry latency histogram by source',
  labelNames: ['source'],
  buckets: [0.1, 0.5, 1, 2, 5, 10]
});

const tokensRemaining = new promClient.Gauge({
  name: 'tokens_remaining',
  help: 'Remaining tokens in bucket by source',
  labelNames: ['source']
});

const quotaRequestsPerMinute = new promClient.Gauge({
  name: 'quota_requests_per_minute',
  help: 'Configured quota per source',
  labelNames: ['source']
});

// Register metrics
register.registerMetric(throttleEventsTotal);
register.registerMetric(retryLatencySeconds);
register.registerMetric(tokensRemaining);
register.registerMetric(quotaRequestsPerMinute);

// Initialize quota metrics
Object.keys(config.rateLimits).forEach(source => {
  quotaRequestsPerMinute.labels(source).set(config.rateLimits[source].requestsPerMinute);
});

async function rateLimitedRequest(source, url) {
  const limiter = rateLimiters[source];
  if (!limiter) {
    throw new Error(`Unknown source: ${source}`);
  }

  // Token bucket refill
  const now = Date.now();
  const timeSinceRefill = now - limiter.lastRefill;
  const tokensToAdd = Math.floor((timeSinceRefill / limiter.interval) * limiter.limit);
  
  if (tokensToAdd > 0) {
    limiter.tokens = Math.min(limiter.burstCapacity, limiter.tokens + tokensToAdd);
    limiter.lastRefill = now;
    console.log(`[INFO] Token bucket refill for ${source}: ${limiter.tokens}/${limiter.burstCapacity}`);
  }

  // Update tokens remaining metric
  tokensRemaining.labels(source).set(limiter.tokens);

  // Check if tokens available
  if (limiter.tokens <= 0) {
    throttleEventsTotal.labels(source).inc();
    
    const cachedData = cacheGet(source);
    if (cachedData) {
      console.log(`[WARN] Throttling ${source} - no tokens (${limiter.limit}/min quota), using cached data`);
      return cachedData;
    }
    
    // Wait with exponential backoff
    const retryStart = Date.now();
    console.log(`[WARN] Throttling ${source} - waiting ${limiter.retryBackoffMs}ms`);
    await new Promise(res => setTimeout(res, limiter.retryBackoffMs));
    
    const retryLatency = (Date.now() - retryStart) / 1000;
    retryLatencySeconds.labels(source).observe(retryLatency);
    
    // Recursive retry after wait
    return rateLimitedRequest(source, url);
  }

  // Consume token and make request
  limiter.tokens -= 1;
  tokensRemaining.labels(source).set(limiter.tokens);
  
  try {
    const response = await axios.get(url);
    cacheSet(source, response.data);
    return response.data;
  } catch (error) {
    console.error(`❌ ${source} request failed:`, error.message);
    return [];
  }
}

module.exports = { rateLimitedRequest, rateLimiters };
