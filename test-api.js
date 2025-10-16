const express = require('express');
const { rateLimitedRequest, throttlingMetrics } = require('./Utility/rateLimiter');
const promClient = require('prom-client');

// Create Express app
const app = express();
app.use(express.json());

// Prometheus metrics registry and instruments
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });



// Rate limiting metrics
const throttleEventsTotal = new promClient.Counter({
  name: 'throttle_events_total',
  help: 'Total number of throttling events',
  labelNames: ['source']
});
register.registerMetric(throttleEventsTotal);

const quotaRequestsPerMinute = new promClient.Gauge({
  name: 'quota_requests_per_minute',
  help: 'Configured request quota per source',
  labelNames: ['source']
});
register.registerMetric(quotaRequestsPerMinute);

// Set initial quota values
quotaRequestsPerMinute.labels('coinpaprika').set(10);
quotaRequestsPerMinute.labels('coingecko').set(3);

// API endpoint to test rate limiting for a specific source
app.get('/test-rate-limit/:source', async (req, res) => {
  const source = req.params.source;
  
  if (source !== 'coinpaprika' && source !== 'coingecko') {
    return res.status(400).json({ error: 'Invalid source. Use "coinpaprika" or "coingecko"' });
  }
  
  try {
    const url = 'https://example.com/';
    const result = await rateLimitedRequest(source, url);
    
    // Return the current throttling metrics for this source
    res.json({
      source,
      result: 'Request processed',
      throttling_metrics: throttlingMetrics[source]
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Metrics endpoint
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Status endpoint
app.get('/status', (req, res) => {
  res.json({
    status: 'ok',
    throttling_metrics: throttlingMetrics
  });
});

// Refresh endpoint that hits both API sources
app.post('/refresh', async (req, res) => {
  const authHeader = req.headers.authorization;
  
  // Simple token validation (optional)
  if (authHeader && authHeader.startsWith('Bearer ')) {
    const token = authHeader.split(' ')[1];
    // In a real app, you would validate the token here
  }
  
  try {
    const results = {
      timestamp: new Date().toISOString(),
      sources: {}
    };
    
    // Make requests to both sources
    const url = 'https://example.com/';
    
    // Request to Source A (coinpaprika)
    try {
      await rateLimitedRequest('coinpaprika', url);
      results.sources.coinpaprika = {
        status: 'success',
        metrics: throttlingMetrics.coinpaprika
      };
    } catch (error) {
      results.sources.coinpaprika = {
        status: 'error',
        error: error.message,
        metrics: throttlingMetrics.coinpaprika
      };
    }
    
    // Request to Source C (coingecko)
    try {
      await rateLimitedRequest('coingecko', url);
      results.sources.coingecko = {
        status: 'success',
        metrics: throttlingMetrics.coingecko
      };
    } catch (error) {
      results.sources.coingecko = {
        status: 'error',
        error: error.message,
        metrics: throttlingMetrics.coingecko
      };
    }
    
    res.json(results);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Start server
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Test API server running on http://localhost:${PORT}`);
  console.log(`Test rate limiting: http://localhost:${PORT}/test-rate-limit/coinpaprika`);
  console.log(`Test rate limiting: http://localhost:${PORT}/test-rate-limit/coingecko`);
  console.log(`View metrics: http://localhost:${PORT}/metrics`);
  console.log(`View status: http://localhost:${PORT}/status`);
});