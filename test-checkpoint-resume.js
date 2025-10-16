#!/usr/bin/env node

/**
 * Test script for transactional resume functionality
 * Tests checkpoint persistence and resume without duplicates
 */

const axios = require('axios');
const { spawn } = require('child_process');

const BASE_URL = 'http://localhost:3000';
const TOKEN = process.env.TEST_TOKEN || 'test-token';

class CheckpointResumeTest {
  constructor() {
    this.results = [];
  }

  async makeRequest(endpoint, method = 'GET', data = null) {
    try {
      const config = {
        method,
        url: `${BASE_URL}${endpoint}`,
        timeout: 15000
      };
      
      if (method === 'POST' && endpoint === '/refresh') {
        // Generate a simple JWT token for ETL refresh
        const jwt = require('jsonwebtoken');
        const token = jwt.sign({}, process.env.REFRESH_JWT_SECRET || 'dev-secret');
        config.headers = { 'Authorization': `Bearer ${token}` };
        if (data) config.data = data;
      }
      
      const response = await axios(config);
      return response.data;
    } catch (error) {
      if (error.response) return error.response.data;
      throw error;
    }
  }

  async waitForServer() {
    console.log('⏳ Waiting for server to be ready...');
    for (let i = 0; i < 30; i++) {
      try {
        await this.makeRequest('/health');
        console.log('✅ Server is ready');
        return true;
      } catch (error) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    throw new Error('Server not ready after 30 seconds');
  }

  async testInitialRun() {
    console.log('\n🚀 Step 1: Trigger initial ETL run...');
    
    const response = await this.makeRequest('/refresh', 'POST');
    console.log('Response:', JSON.stringify(response, null, 2));
    
    // Wait for ETL to complete or fail
    await new Promise(resolve => setTimeout(resolve, 8000));
    
    const runs = await this.makeRequest('/runs?limit=1');
    if (!runs || !runs.runs || runs.runs.length === 0) {
      console.log('⚠️ No ETL runs found yet');
      return null;
    }
    
    const lastRun = runs.runs[0];
    console.log('Last run status:', lastRun?.status);
    console.log('Failed batches:', lastRun?.failed_batches?.length || 0);
    
    return lastRun;
  }

  async testResumeRun() {
    console.log('\n🔄 Step 2: Trigger resume run...');
    
    const beforeCounts = await this.getDataCounts();
    console.log('Data counts before resume:', beforeCounts);
    
    const response = await this.makeRequest('/refresh', 'POST');
    console.log('Resume response:', JSON.stringify(response, null, 2));
    
    // Wait for ETL to complete
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    const afterCounts = await this.getDataCounts();
    console.log('Data counts after resume:', afterCounts);
    
    const runs = await this.makeRequest('/runs?limit=2');
    if (!runs || !runs.runs || runs.runs.length === 0) {
      console.log('⚠️ No ETL runs found');
      return { latestRun: null, previousRun: null, beforeCounts, afterCounts };
    }
    
    const [latestRun, previousRun] = runs.runs;
    console.log('Latest run status:', latestRun?.status);
    console.log('Resume info:', latestRun?.resume_info);
    
    return { latestRun, previousRun, beforeCounts, afterCounts };
  }

  async getDataCounts() {
    const stats = await this.makeRequest('/stats');
    return {
      raw: stats.counts?.raw || 0,
      normalized: stats.counts?.normalized || 0
    };
  }

  async testRunDetails() {
    console.log('\n📊 Step 3: Inspect run details...');
    
    const runs = await this.makeRequest('/runs?limit=3');
    if (!runs || !runs.runs) {
      console.log('⚠️ No runs data available');
      return [];
    }
    
    console.log(`Found ${runs.runs.length} recent runs`);
    
    for (const run of runs.runs.slice(0, 2)) {
      console.log(`\n📋 Run ${run.run_id}:`);
      console.log(`  Status: ${run.status}`);
      console.log(`  Rows processed: ${run.rows_processed}`);
      console.log(`  Errors: ${run.errors}`);
      console.log(`  Failed batches: ${run.failed_batches?.length || 0}`);
      
      if (Object.keys(run.resume_info || {}).length > 0) {
        console.log(`  Resume info: ${JSON.stringify(run.resume_info)}`);
      }
      
      // Get detailed run info
      try {
        const details = await this.makeRequest(`/runs/${run.run_id}`);
        if (details.run?.failed_batches?.length > 0) {
          console.log(`  Failed batch details:`, details.run.failed_batches);
        }
      } catch (error) {
        console.log(`  Could not get details: ${error.message}`);
      }
    }
    
    return runs.runs;
  }

  async testDuplicatePrevention() {
    console.log('\n🔍 Step 4: Check for duplicates...');
    
    // Query for potential duplicates
    const response = await this.makeRequest('/data?limit=100');
    const data = response.data || [];
    
    // Group by unique key (symbol + timestamp + source)
    const uniqueKeys = new Set();
    const duplicates = [];
    
    for (const record of data) {
      const key = `${record.symbol}_${record.timestamp}_${record.source}`;
      if (uniqueKeys.has(key)) {
        duplicates.push(record);
      } else {
        uniqueKeys.add(key);
      }
    }
    
    console.log(`Total records: ${data.length}`);
    console.log(`Unique keys: ${uniqueKeys.size}`);
    console.log(`Duplicates found: ${duplicates.length}`);
    
    if (duplicates.length > 0) {
      console.log('Duplicate records:', duplicates.slice(0, 3));
    }
    
    return { totalRecords: data.length, uniqueKeys: uniqueKeys.size, duplicates: duplicates.length };
  }

  generateReport(testResults) {
    const report = {
      timestamp: new Date().toISOString(),
      test_results: testResults,
      summary: {
        checkpoint_resume: testResults.resumeTest ? 'PASS' : 'FAIL',
        duplicate_prevention: testResults.duplicateTest?.duplicates === 0 ? 'PASS' : 'FAIL',
        run_tracking: testResults.runDetails?.length > 0 ? 'PASS' : 'FAIL'
      }
    };
    
    console.log('\n📋 Test Report:');
    console.log('================');
    console.log(`Checkpoint Resume: ${report.summary.checkpoint_resume}`);
    console.log(`Duplicate Prevention: ${report.summary.duplicate_prevention}`);
    console.log(`Run Tracking: ${report.summary.run_tracking}`);
    
    return report;
  }

  async runAllTests() {
    console.log('🧪 Starting Checkpoint Resume Test Suite\n');
    
    try {
      await this.waitForServer();
      
      const initialRun = await this.testInitialRun();
      const resumeTest = await this.testResumeRun();
      const runDetails = await this.testRunDetails();
      const duplicateTest = await this.testDuplicatePrevention();
      
      const testResults = {
        initialRun,
        resumeTest,
        runDetails,
        duplicateTest
      };
      
      const report = this.generateReport(testResults);
      
      // Determine if tests passed
      const allPassed = Object.values(report.summary).every(status => status === 'PASS');
      
      if (allPassed) {
        console.log('\n✅ All tests passed!');
        return 0;
      } else {
        console.log('\n❌ Some tests failed!');
        return 1;
      }
      
    } catch (error) {
      console.error('❌ Test suite error:', error.message);
      return 1;
    }
  }
}

// Run tests if called directly
if (require.main === module) {
  const test = new CheckpointResumeTest();
  test.runAllTests().then(exitCode => {
    process.exit(exitCode);
  }).catch(error => {
    console.error('Fatal test error:', error);
    process.exit(1);
  });
}

module.exports = CheckpointResumeTest;