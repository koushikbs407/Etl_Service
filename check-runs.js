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

async function checkRuns() {
  try {
    console.log('Checking ETL runs for incremental behavior...\n');
    
    // Get recent runs
    const runs = await makeRequest('GET', '/runs?limit=5');
    console.log('Recent ETL Runs:');
    console.log(JSON.stringify(runs, null, 2));
    
    if (runs.runs && runs.runs.length > 0) {
      const latestRunId = runs.runs[0].run_id;
      console.log(`\nDetailed info for latest run: ${latestRunId}`);
      
      const runDetail = await makeRequest('GET', `/runs/${latestRunId}`);
      console.log(JSON.stringify(runDetail, null, 2));
    }
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

checkRuns();