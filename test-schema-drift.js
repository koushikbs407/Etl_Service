#!/usr/bin/env node

/**
 * Test script for schema drift detection
 */

const { schemaDriftDetector } = require('./Utility/schemaDrift');

// Simulate original data
const originalData = [
  {
    symbol: 'BTC',
    name: 'Bitcoin',
    price_usd: 45000,
    volume_24h: 1000000,
    timestamp: '2024-01-01T00:00:00Z'
  }
];

// Simulate drifted data (renamed column + type change)
const driftedData = [
  {
    ticker: 'BTC',  // renamed from 'symbol' 
    name: 'Bitcoin',
    usd_price: '45000',  // renamed from 'price_usd', type changed from number to string
    volume_24h: 1000000,
    created_at: 1704067200  // renamed from 'timestamp', type changed to number
  }
];

console.log('🧪 Testing Schema Drift Detection\n');

// Test 1: Initial schema
console.log('1. Processing original schema...');
const result1 = schemaDriftDetector.detectDrift('test_source', originalData);
console.log('Result:', result1);

// Test 2: Drifted schema
console.log('\n2. Processing drifted schema...');
const result2 = schemaDriftDetector.detectDrift('test_source', driftedData);
console.log('Result:', result2);

// Test 3: Check confidence levels
console.log('\n3. Confidence Analysis:');
result2.applied_mappings.forEach(mapping => {
  console.log(`✅ Applied: ${mapping.from} -> ${mapping.to} (confidence: ${mapping.confidence})`);
});

result2.warnings.forEach(warning => {
  console.log(`⚠️  Skipped: ${warning.from} -> ${warning.to} (confidence: ${warning.confidence})`);
});

console.log(`\n📊 Schema version: ${result2.schema_version}`);
console.log('\n✅ Schema drift test completed');