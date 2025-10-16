const http = require('http');

function makeRequest(method, path) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 8080,
      path: path,
      method: method
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve({ status: res.statusCode, data });
      });
    });

    req.on('error', (e) => {
      reject(e);
    });

    req.end();
  });
}

async function listEndpoints() {
  console.log('Available API Endpoints:\n');
  
  const endpoints = [
    { method: 'GET', path: '/health', desc: 'System health check' },
    { method: 'GET', path: '/metrics', desc: 'Prometheus metrics' },
    { method: 'GET', path: '/stats', desc: 'ETL statistics and incremental behavior' },
    { method: 'GET', path: '/data', desc: 'Normalized crypto data with filtering' },
    { method: 'GET', path: '/runs', desc: 'ETL run history' },
    { method: 'POST', path: '/refresh', desc: 'Trigger ETL refresh (requires Bearer token)' },
    { method: 'POST', path: '/etl/refresh', desc: 'Alternative ETL trigger endpoint' },
    { method: 'GET', path: '/api-docs', desc: 'Swagger API documentation' }
  ];

  for (const endpoint of endpoints) {
    try {
      const result = await makeRequest(endpoint.method, endpoint.path);
      console.log(`✅ ${endpoint.method} ${endpoint.path} - ${endpoint.desc}`);
      console.log(`   Status: ${result.status}`);
    } catch (error) {
      console.log(`❌ ${endpoint.method} ${endpoint.path} - ${endpoint.desc}`);
      console.log(`   Error: ${error.message}`);
    }
  }

  console.log('\n📊 Key Endpoints for Incremental Load Testing:');
  console.log('- POST /refresh - Triggers ETL with pre-run count tracking');
  console.log('- GET /stats - Shows incremental metrics (last_run_skipped, etc.)');
  console.log('- GET /runs - Shows detailed run history with duplicate counts');
  console.log('- GET /data - Access to normalized data for verification');
}

listEndpoints();