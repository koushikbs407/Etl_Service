const http = require('http');

const options = {
  hostname: 'localhost',
  port: 8080,
  path: '/metrics',
  method: 'GET'
};

const req = http.request(options, (res) => {
  let data = '';
  res.on('data', (chunk) => {
    data += chunk;
  });
  res.on('end', () => {
    const lines = data.split('\n');
    const etlLines = lines.filter(line => 
      line.includes('etl_') || line.includes('throttle_')
    );
    console.log('ETL Metrics found:');
    etlLines.forEach(line => console.log(line));
  });
});

req.on('error', (e) => {
  console.error(`Problem with request: ${e.message}`);
});

req.end();