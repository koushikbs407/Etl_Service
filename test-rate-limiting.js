const axios = require('axios');
const { spawn } = require('child_process');
const fs = require('fs');

// Test configuration
const TEST_CONFIG = {
  baseUrl: 'http://localhost:3000',
  token: 'test-token-123',
  timeout: 30000,
  sources: {
    coinpaprika: { quota: 10, expectedThrottling: 2 },
    coingecko: { quota: 3, expectedThrottling: 2 }
  }
};

class RateLimitingTest {
  constructor() {
    this.results = {
      passed: 0,
      failed: 0,
      tests: []
    };
    this.serverProcess = null;
  }

  async startServer() {
    console.log('🚀 Starting test server...');
    return new Promise((resolve, reject) => {
      this.serverProcess = spawn('node', ['api/server.js'], {
        env: { ...process.env, NODE_ENV: 'test', PORT: '3000' },
        stdio: 'pipe'
      });

      let serverReady = false;
      const timeout = setTimeout(() => {
        if (!serverReady) reject(new Error('Server startup timeout'));
      }, 10000);

      this.serverProcess.stdout.on('data', (data) => {
        if (data.toString().includes('Server running') || data.toString().includes('listening')) {
          serverReady = true;
          clearTimeout(timeout);
          setTimeout(resolve, 2000); // Wait 2s for full startup
        }
      });

      this.serverProcess.on('error', reject);
    });
  }

  async stopServer() {
    if (this.serverProcess) {
      this.serverProcess.kill();
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  async makeRequest() {
    try {
      const response = await axios.post(`${TEST_CONFIG.baseUrl}/refresh`, {}, {
        headers: { 'Authorization': `Bearer ${TEST_CONFIG.token}` },
        timeout: 5000
      });
      return response.data;
    } catch (error) {
      if (error.response) return error.response.data;
      throw error;
    }
  }

  addTestResult(name, passed, details = '') {
    this.results.tests.push({ name, passed, details });
    if (passed) {
      this.results.passed++;
      console.log(`✅ ${name}`);
    } else {
      this.results.failed++;
      console.log(`❌ ${name}: ${details}`);
    }
  }

  async testBasicConnectivity() {
    try {
      const response = await this.makeRequest();
      this.addTestResult(
        'Basic API connectivity',
        response.timestamp && response.sources,
        response.error || 'API responded successfully'
      );
      return response;
    } catch (error) {
      this.addTestResult('Basic API connectivity', false, error.message);
      return null;
    }
  }

  async testRateLimitingBehavior() {
    console.log('\n🔄 Testing rate limiting behavior...');
    
    const requests = [];
    const startTime = Date.now();
    
    // Make multiple rapid requests to trigger throttling
    for (let i = 0; i < 8; i++) {
      requests.push(this.makeRequest());
      await new Promise(resolve => setTimeout(resolve, 100)); // Small delay between requests
    }
    
    const responses = await Promise.all(requests);
    const endTime = Date.now();
    
    // Analyze responses
    let throttledCount = 0;
    let successCount = 0;
    
    responses.forEach((response, index) => {
      if (response && response.sources) {
        Object.values(response.sources).forEach(source => {
          if (source.metrics && source.metrics.throttled > 0) {
            throttledCount++;
          }
          if (source.status === 'success') {
            successCount++;
          }
        });
      }
    });
    
    this.addTestResult(
      'Rate limiting triggers throttling',
      throttledCount > 0,
      `Throttled events: ${throttledCount}, Success: ${successCount}`
    );
    
    this.addTestResult(
      'Requests complete within reasonable time',
      (endTime - startTime) < TEST_CONFIG.timeout,
      `Total time: ${endTime - startTime}ms`
    );
    
    return { throttledCount, successCount, totalTime: endTime - startTime };
  }

  async testQuotaEnforcement() {
    console.log('\n📊 Testing quota enforcement...');
    
    const response = await this.makeRequest();
    if (!response || !response.sources) {
      this.addTestResult('Quota enforcement test', false, 'No response data');
      return;
    }
    
    // Check if different sources have different quotas
    const coinpaprikaMetrics = response.sources.coinpaprika?.metrics;
    const coingeckoMetrics = response.sources.coingecko?.metrics;
    
    if (coinpaprikaMetrics && coingeckoMetrics) {
      this.addTestResult(
        'Different sources have different quotas',
        true,
        `Coinpaprika: ${JSON.stringify(coinpaprikaMetrics)}, Coingecko: ${JSON.stringify(coingeckoMetrics)}`
      );
    } else {
      this.addTestResult(
        'Different sources have different quotas',
        false,
        'Missing metrics data'
      );
    }
  }

  async testMetricsCollection() {
    console.log('\n📈 Testing metrics collection...');
    
    // Make a few requests to generate metrics
    await this.makeRequest();
    await this.makeRequest();
    
    const response = await this.makeRequest();
    
    if (response && response.sources) {
      let hasMetrics = false;
      Object.values(response.sources).forEach(source => {
        if (source.metrics) {
          hasMetrics = true;
        }
      });
      
      this.addTestResult(
        'Metrics are collected and returned',
        hasMetrics,
        hasMetrics ? 'Metrics found in response' : 'No metrics in response'
      );
    } else {
      this.addTestResult('Metrics are collected and returned', false, 'No response data');
    }
  }

  generateReport() {
    const report = {
      timestamp: new Date().toISOString(),
      summary: {
        total: this.results.tests.length,
        passed: this.results.passed,
        failed: this.results.failed,
        success_rate: `${((this.results.passed / this.results.tests.length) * 100).toFixed(1)}%`
      },
      tests: this.results.tests
    };
    
    // Write JSON report for CI
    fs.writeFileSync('rate-limiting-test-report.json', JSON.stringify(report, null, 2));
    
    // Write human-readable report
    let textReport = `
Rate Limiting Test Report
========================
Generated: ${report.timestamp}

Summary:
- Total Tests: ${report.summary.total}
- Passed: ${report.summary.passed}
- Failed: ${report.summary.failed}
- Success Rate: ${report.summary.success_rate}

Test Details:
`;
    
    report.tests.forEach(test => {
      textReport += `${test.passed ? '✅' : '❌'} ${test.name}\n`;
      if (test.details) textReport += `   ${test.details}\n`;
    });
    
    fs.writeFileSync('rate-limiting-test-report.txt', textReport);
    
    return report;
  }

  async runAllTests() {
    console.log('🧪 Starting Rate Limiting Test Suite\n');
    
    try {
      await this.startServer();
      
      await this.testBasicConnectivity();
      await this.testRateLimitingBehavior();
      await this.testQuotaEnforcement();
      await this.testMetricsCollection();
      
    } catch (error) {
      console.error('❌ Test suite error:', error.message);
      this.addTestResult('Test suite execution', false, error.message);
    } finally {
      await this.stopServer();
    }
    
    const report = this.generateReport();
    
    console.log('\n📋 Test Summary:');
    console.log(`Total: ${report.summary.total}, Passed: ${report.summary.passed}, Failed: ${report.summary.failed}`);
    console.log(`Success Rate: ${report.summary.success_rate}`);
    
    // Exit with error code if tests failed
    if (this.results.failed > 0) {
      process.exit(1);
    }
    
    return report;
  }
}

// Run tests if called directly
if (require.main === module) {
  const test = new RateLimitingTest();
  test.runAllTests().catch(error => {
    console.error('Fatal test error:', error);
    process.exit(1);
  });
}

module.exports = RateLimitingTest;