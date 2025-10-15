const { rateLimitedRequest, throttlingMetrics } = require('./rateLimiter');

const runTest = async () => {
  console.log('🚀 Starting rate limiter test...\n');

  const url = 'https://example.com/';
  const source = 'coinpaprika';

  // Fire several requests in quick succession to trigger throttling/cache behavior
  for (let i = 0; i < 15; i++) {
    try {
      const data = await rateLimitedRequest(source, url);
      console.log(`Call ${i + 1}: received ${Array.isArray(data) ? data.length : typeof data}`);
    } catch (err) {
      console.error('Request error:', err.message);
    }
  }

  console.log('\nThrottling metrics:', throttlingMetrics);
};

runTest().catch(err => console.error(err));
