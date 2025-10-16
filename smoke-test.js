const http = require('http');
const fs = require('fs');
const path = require('path');

// Create test CSV data
const testCsvData = `symbol,name,price_usd,volume_24h,market_cap,percent_change_24h,timestamp
BTC,Bitcoin,50000,1000000000,950000000000,2.5,2024-01-01T00:00:00Z
ETH,Ethereum,3000,500000000,360000000000,1.8,2024-01-01T00:00:00Z
ADA,Cardano,0.5,100000000,16000000000,-0.5,2024-01-01T00:00:00Z`;

function makeRequest(method, path, data = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: process.env.PORT || 8080,
      path: path,
      method: method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    const req = http.request(options, (res) => {
      let responseData = '';
      res.on('data', (chunk) => {
        responseData += chunk;
      });
      res.on('end', () => {
        try {
          resolve({
            status: res.statusCode,
            data: JSON.parse(responseData)
          });
        } catch (e) {
          resolve({
            status: res.statusCode,
            data: responseData
          });
        }
      });
    });

    req.on('error', (e) => {
      reject(e);
    });

    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

async function smokeTest() {
  console.log('🚀 Starting Smoke Test...\n');
  
  try {
    // Step 1: Seed tiny CSV
    console.log('1. Seeding test CSV data...');
    const csvPath = path.join(__dirname, 'Service', 'test_data.csv');
    fs.writeFileSync(csvPath, testCsvData);
    console.log('✅ Test CSV created');

    // Wait for server to be ready
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Step 2: POST /refresh
    console.log('\n2. Triggering ETL refresh...');
    const refreshResponse = await makeRequest('POST', '/refresh');
    
    if (refreshResponse.status !== 202) {
      throw new Error(`Refresh failed with status ${refreshResponse.status}`);
    }
    console.log('✅ ETL refresh triggered successfully');
    console.log(`   Run ID: ${refreshResponse.data.run_id}`);

    // Wait for ETL to complete
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Step 3: Assert /metrics not empty
    console.log('\n3. Checking /metrics endpoint...');
    const metricsResponse = await makeRequest('GET', '/metrics');
    
    if (metricsResponse.status !== 200) {
      throw new Error(`Metrics endpoint failed with status ${metricsResponse.status}`);
    }
    
    const metricsData = metricsResponse.data;
    if (!metricsData || metricsData.length < 50) {
      throw new Error('Metrics endpoint returned empty or insufficient data');
    }
    
    // Check for etl_rows_processed_total > 0
    const rowsProcessedMatch = metricsData.match(/etl_rows_processed_total\{[^}]*\}\s+(\d+)/);
    if (!rowsProcessedMatch) {
      throw new Error('etl_rows_processed_total metric not found in /metrics');
    }
    
    const rowsProcessed = parseInt(rowsProcessedMatch[1]);
    if (rowsProcessed <= 0) {
      throw new Error(`etl_rows_processed_total is ${rowsProcessed}, expected > 0`);
    }
    
    console.log(`✅ etl_rows_processed_total = ${rowsProcessed} (> 0)`);
    
    // Check for other required metrics
    const requiredMetrics = ['etl_latency_seconds', 'throttle_events_total'];
    for (const metric of requiredMetrics) {
      if (!metricsData.includes(metric)) {
        throw new Error(`Required metric '${metric}' not found in /metrics`);
      }
    }
    console.log('✅ All required metrics present');

    // Step 4: Assert /runs not empty
    console.log('\n4. Checking /runs endpoint...');
    const runsResponse = await makeRequest('GET', '/runs');
    
    if (runsResponse.status !== 200) {
      throw new Error(`Runs endpoint failed with status ${runsResponse.status}`);
    }
    
    if (!runsResponse.data.runs || runsResponse.data.runs.length === 0) {
      throw new Error('/runs list is empty - no ETL runs found');
    }
    
    const latestRun = runsResponse.data.runs[0];
    if (!latestRun.run_id) {
      throw new Error('Latest run missing run_id');
    }
    
    console.log('✅ /runs list is non-empty');
    console.log(`   Found ${runsResponse.data.runs.length} ETL runs`);
    console.log(`   Latest run: ${latestRun.run_id} (${latestRun.status})`);
    
    // Validate run has processed some data
    if (latestRun.stats && latestRun.stats.extracted === 0) {
      throw new Error('Latest run processed 0 records');
    }

    // Cleanup
    try {
      fs.unlinkSync(csvPath);
      console.log('✅ Test CSV cleaned up');
    } catch (e) {
      // Ignore cleanup errors
    }

    console.log('\n🎉 Smoke Test PASSED - End-to-end flow verified!');
    process.exit(0);

  } catch (error) {
    console.error('\n❌ Smoke Test FAILED:', error.message);
    process.exit(1);
  }
}

smokeTest();