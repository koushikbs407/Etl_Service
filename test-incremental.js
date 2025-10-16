const http = require('http');

function makeRequest(method, path, headers = {}) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 8080,
      path: path,
      method: method,
      headers: headers
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

async function testIncrementalLoads() {
  console.log('Testing Incremental Loads...\n');
  
  const token = 'test-token-123';
  const headers = { 'Authorization': `Bearer ${token}` };
  
  try {
    // First refresh
    console.log('1. First refresh call:');
    const result1 = await makeRequest('POST', '/refresh', headers);
    console.log(JSON.stringify(result1, null, 2));
    
    // Wait a moment
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Second refresh
    console.log('\n2. Second refresh call:');
    const result2 = await makeRequest('POST', '/refresh', headers);
    console.log(JSON.stringify(result2, null, 2));
    
    // Wait a moment
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Check stats
    console.log('\n3. Stats after both runs:');
    const stats = await makeRequest('GET', '/stats');
    console.log(JSON.stringify(stats, null, 2));
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

testIncrementalLoads();