#!/usr/bin/env node

/**
 * Manual Rate Limiting Test Script
 * Run this locally to test rate limiting without CI overhead
 */

const axios = require('axios');

const BASE_URL = 'http://localhost:3000';
const TOKEN = 'test-token-123';

async function testRateLimiting() {
  console.log('🧪 Manual Rate Limiting Test\n');
  
  const results = [];
  
  console.log('Making 10 rapid requests to trigger rate limiting...\n');
  
  for (let i = 1; i <= 10; i++) {
    const startTime = Date.now();
    
    try {
      const response = await axios.post(`${BASE_URL}/refresh`, {}, {
        headers: { 'Authorization': `Bearer ${TOKEN}` },
        timeout: 10000
      });
      
      const latency = Date.now() - startTime;
      const data = response.data;
      
      console.log(`Request ${i}:`);
      console.log(`  ⏱️  Latency: ${latency}ms`);
      
      if (data.sources) {
        Object.entries(data.sources).forEach(([source, info]) => {
          console.log(`  📊 ${source}:`);
          console.log(`     Status: ${info.status}`);
          if (info.metrics) {
            console.log(`     Throttled: ${info.metrics.throttled || 0}`);
            console.log(`     Total Latency: ${info.metrics.totalRequestLatencyMs || 0}ms`);
            console.log(`     Retry Wait: ${info.metrics.totalRetryWaitMs || 0}ms`);
          }
        });
      }
      
      results.push({
        request: i,
        latency,
        success: true,
        sources: data.sources
      });
      
    } catch (error) {
      console.log(`Request ${i}: ❌ Error - ${error.message}`);
      results.push({
        request: i,
        latency: Date.now() - startTime,
        success: false,
        error: error.message
      });
    }
    
    console.log('');
    
    // Small delay between requests
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  
  // Summary
  console.log('📋 Summary:');
  const successful = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;
  const avgLatency = results.reduce((sum, r) => sum + r.latency, 0) / results.length;
  
  console.log(`✅ Successful requests: ${successful}`);
  console.log(`❌ Failed requests: ${failed}`);
  console.log(`⏱️  Average latency: ${avgLatency.toFixed(0)}ms`);
  
  // Check for throttling evidence
  let totalThrottled = 0;
  results.forEach(result => {
    if (result.sources) {
      Object.values(result.sources).forEach(source => {
        if (source.metrics && source.metrics.throttled) {
          totalThrottled += source.metrics.throttled;
        }
      });
    }
  });
  
  console.log(`🚦 Total throttling events: ${totalThrottled}`);
  
  if (totalThrottled > 0) {
    console.log('\n✅ Rate limiting is working correctly!');
  } else {
    console.log('\n⚠️  No throttling detected - rate limiting may not be working');
  }
}

// Check if server is running
async function checkServer() {
  try {
    await axios.get(`${BASE_URL}/health`, { timeout: 3000 });
    return true;
  } catch (error) {
    return false;
  }
}

async function main() {
  const serverRunning = await checkServer();
  
  if (!serverRunning) {
    console.log('❌ Server not running at', BASE_URL);
    console.log('Start the server first: npm start');
    process.exit(1);
  }
  
  await testRateLimiting();
}

if (require.main === module) {
  main().catch(error => {
    console.error('Test failed:', error.message);
    process.exit(1);
  });
}