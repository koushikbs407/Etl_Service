#!/usr/bin/env node

/**
 * Simple checkpoint resume test
 */

const axios = require('axios');

const BASE_URL = 'http://localhost:3000';

async function makeRequest(endpoint, method = 'GET') {
  try {
    const config = {
      method,
      url: `${BASE_URL}${endpoint}`,
      timeout: 10000
    };
    
    if (method === 'POST' && endpoint === '/refresh') {
      // Simple token for testing
      config.headers = { 'Authorization': 'Bearer test-token' };
    }
    
    const response = await axios(config);
    return response.data;
  } catch (error) {
    if (error.response) {
      console.log(`Error ${error.response.status}:`, error.response.data);
      return error.response.data;
    }
    throw error;
  }
}

async function testCheckpointResume() {
  console.log('🧪 Simple Checkpoint Resume Test\n');
  
  try {
    // Check server health
    console.log('1. Checking server health...');
    const health = await makeRequest('/health');
    console.log('Health:', health.components || 'OK');
    
    // Trigger ETL
    console.log('\n2. Triggering ETL run...');
    const refreshResponse = await makeRequest('/refresh', 'POST');
    console.log('ETL Response:', refreshResponse.message || refreshResponse);
    
    // Wait for completion
    console.log('\n3. Waiting for ETL completion...');
    await new Promise(resolve => setTimeout(resolve, 8000));
    
    // Check runs
    console.log('\n4. Checking ETL runs...');
    const runs = await makeRequest('/runs?limit=3');
    
    if (runs && runs.runs && runs.runs.length > 0) {
      console.log(`Found ${runs.runs.length} runs:`);
      runs.runs.forEach((run, i) => {
        console.log(`  ${i+1}. ${run.run_id} - ${run.status} (${run.rows_processed} rows, ${run.errors} errors)`);
        if (run.failed_batches && run.failed_batches.length > 0) {
          console.log(`     Failed batches: ${run.failed_batches.length}`);
        }
        if (run.resume_info && Object.keys(run.resume_info).length > 0) {
          console.log(`     Resume info: ${JSON.stringify(run.resume_info)}`);
        }
      });
    } else {
      console.log('No runs found or runs endpoint not available');
    }
    
    // Check data counts
    console.log('\n5. Checking data counts...');
    const stats = await makeRequest('/stats');
    if (stats && stats.counts) {
      console.log(`Raw records: ${stats.counts.raw}`);
      console.log(`Normalized records: ${stats.counts.normalized}`);
    } else {
      console.log('Stats not available');
    }
    
    // Sample data
    console.log('\n6. Checking sample data...');
    const data = await makeRequest('/data?limit=5');
    if (data && data.data && data.data.length > 0) {
      console.log(`Sample records (${data.data.length}):`);
      data.data.slice(0, 2).forEach(record => {
        console.log(`  ${record.symbol} - ${record.source} - $${record.price_usd}`);
      });
    } else {
      console.log('No data available yet');
    }
    
    console.log('\n✅ Test completed');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    process.exit(1);
  }
}

if (require.main === module) {
  testCheckpointResume();
}