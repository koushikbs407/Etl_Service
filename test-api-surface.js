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
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          resolve(data);
        }
      });
    });

    req.on('error', (e) => {
      reject(e);
    });

    req.end();
  });
}

function checkRequiredFields(response, endpoint) {
  const required = ['request_id', 'api_latency_ms'];
  const missing = [];
  
  required.forEach(field => {
    if (!(field in response)) {
      missing.push(field);
    }
  });
  
  // run_id is required where applicable (not for /health)
  if (endpoint !== '/health' && !('run_id' in response)) {
    missing.push('run_id');
  }
  
  return missing;
}

async function testApiSurface() {
  console.log('API Surface Proof Testing...\n');
  
  const tests = [
    { name: 'Data with filters + pagination', path: '/data?symbol=BTC&limit=5&sort_by=timestamp&sort_dir=desc' },
    { name: 'Stats', path: '/stats' },
    { name: 'Health', path: '/health' },
    { name: 'Runs history', path: '/runs?limit=5' }
  ];
  
  for (const test of tests) {
    try {
      console.log(`${test.name}:`);
      const response = await makeRequest('GET', test.path);
      
      const missing = checkRequiredFields(response, test.path);
      
      if (missing.length === 0) {
        console.log('✅ All required fields present');
        console.log(`   request_id: ${response.request_id}`);
        if (response.run_id) console.log(`   run_id: ${response.run_id}`);
        console.log(`   api_latency_ms: ${response.api_latency_ms}`);
      } else {
        console.log(`❌ Missing fields: ${missing.join(', ')}`);
      }
      
      console.log(JSON.stringify(response, null, 2));
      console.log('\n' + '='.repeat(50) + '\n');
      
    } catch (error) {
      console.log(`❌ Error: ${error.message}\n`);
    }
  }
}

testApiSurface();