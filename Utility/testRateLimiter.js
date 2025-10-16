const { rateLimitedRequest, throttlingMetrics } = require('./rateLimiter');

const testSource = async (source, requestCount) => {
  console.log(`\n🔍 Testing rate limiting for source: ${source}...\n`);
  
  const url = 'https://example.com/';
  
  // Fire several requests in quick succession to trigger throttling/cache behavior
  for (let i = 0; i < requestCount; i++) {
    try {
      const data = await rateLimitedRequest(source, url);
      console.log(`${source} - Call ${i + 1}: received ${Array.isArray(data) ? data.length : typeof data}`);
    } catch (err) {
      console.error(`${source} - Request error:`, err.message);
    }
  }
  
  console.log(`\n${source} throttling metrics:`, throttlingMetrics[source]);
};

const runTest = async () => {
  console.log('🚀 Starting comprehensive rate limiter test...\n');
  
  // Test Source A (coinpaprika) - 10 req/min quota
  await testSource('coinpaprika', 12);
  
  // Test Source C (coingecko) - 3 req/min quota
  await testSource('coingecko', 5);
  
  console.log('\n📊 Final throttling metrics for all sources:', throttlingMetrics);
};

runTest().catch(err => console.error(err));
