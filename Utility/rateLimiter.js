// rateLimiter.js
const axios = require('axios');
// Lightweight in-memory TTL cache to avoid external dependency
const cache = new Map(); // key -> { value, expiresAt }
const STD_TTL_MS = 60 * 1000; // 60 seconds

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

// Rate limiter state per source
const rateLimiters = {
  coinpaprika: { limit: 10, interval: 60000, requests: 0, lastReset: Date.now() },
  coingecko: { limit: 3, interval: 60000, requests: 0, lastReset: Date.now() }
};

// Throttling metrics (per source)
const throttlingMetrics = {
  coinpaprika: { throttled: 0, totalRequestLatencyMs: 0, totalRetryWaitMs: 0 },
  coingecko: { throttled: 0, totalRequestLatencyMs: 0, totalRetryWaitMs: 0 }
};

async function rateLimitedRequest(source, url) {
  const limiter = rateLimiters[source];

  // Reset request count if interval passed
  if (Date.now() - limiter.lastReset > limiter.interval) {
    limiter.requests = 0;
    limiter.lastReset = Date.now();
  }

  // If limit reached, return cached data or wait
  if (limiter.requests >= limiter.limit) {
    throttlingMetrics[source].throttled += 1;

    const cachedData = cacheGet(source);
    if (cachedData) {
      console.log(`⚠️ Rate limit exceeded for ${source}, using cached data`);
      return cachedData;
    } else {
      // Adaptive backoff: simple linear backoff based on overage
      const windowRemainingMs = Math.max(0, limiter.interval - (Date.now() - limiter.lastReset));
      const retryDelay = Math.min(2000, Math.max(500, Math.floor(windowRemainingMs * 0.1)));
      console.log(`⚠️ Rate limit exceeded for ${source}, waiting ${retryDelay}ms`);
      const waitStart = Date.now();
      await new Promise(res => setTimeout(res, retryDelay));
      throttlingMetrics[source].totalRetryWaitMs += Date.now() - waitStart;
    }
  }

  limiter.requests += 1;
  const startTime = Date.now();
  try {
    const response = await axios.get(url);
    const latency = Date.now() - startTime;
    throttlingMetrics[source].totalRequestLatencyMs += latency;

  // Cache response
  cacheSet(source, response.data);

    return response.data;
  } catch (error) {
    console.error(`❌ ${source} request failed:`, error.message);
    return [];
  }
}

module.exports = { rateLimitedRequest, throttlingMetrics };
