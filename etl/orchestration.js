// Use central DB connector for consistent connection handling
const { connectMongoDB, closeMongoDB, getDb } = require('../DB/mongo');
const fetchCoinPaprikaData = require('../Service/coinpaprika');
const fetchCSVData = require('../Service/csvSource');
const fetchCoinGeckoData = require('../Service/coingecko');
const { generateAndStoreETLSummary } = require('../Service/etlReportService');
const { logETLRun } = require('../Service/etlRunLogger');
const { saveCheckpoint, getCheckpoint, clearCheckpoints } = require('../Schedular/etlCheckpointService');
const EtlSummary = require('../Schems/EtlSummary');
const { throttlingMetrics } = require('../Utility/rateLimiter');
const { schemaDriftDetector } = require('../Utility/schemaDrift');
const unifiedSchema = require('../Schems/unifiedSchema');
const promClient = require('prom-client');
const crypto = require('crypto');

const config = require('../Config/config');

const BATCH_SIZE = 5; // Adjustable batch size
const FAULT_INJECTION = process.env.FAULT_INJECTION === 'true';

// Connect to MongoDB using Mongoose
// Delegate connection management to DB/mongo.js
// connectMongoDB and closeMongoDB are provided by that module.

// Get watermark (last processed timestamp) for incremental loads
async function getWatermark(source) {
  const mongoDb = getDb();
  if (!mongoDb) return null;
  
  const lastRecord = await mongoDb.collection('normalized_crypto_data')
    .findOne({ source }, { sort: { timestamp: -1 } });
  
  return lastRecord ? lastRecord.timestamp : null;
}

// Process a single source with checkpoint and batch processing using Mongoose
async function processSourceWithCheckpoint(source, data, runId) {
  const batchId = `${runId}_${source}`; // unique batchId per run and source
  let lastIndex = await getCheckpoint(batchId, source); // fetch last checkpoint
  const failedIds = [];
  const failedBatches = [];
  let validationErrors = 0;
  let resumedFromBatch = null;
  let skippedCount = 0;
  const mongoDb = getDb();
  
  // Get watermark for incremental processing
  const watermark = await getWatermark(source);
  console.log(`📊 Watermark for ${source}: ${watermark}`);

  console.log(`📦 Processing ${source}: starting from index ${lastIndex} (${data.length - lastIndex} remaining)`);
  
  if (lastIndex > 0) {
    resumedFromBatch = Math.floor(lastIndex / BATCH_SIZE);
    console.log(`🔄 Resuming ${source} from batch ${resumedFromBatch}`);
  }

  for (let i = lastIndex; i < data.length; i += BATCH_SIZE) {
    const batchNum = Math.floor(i / BATCH_SIZE);
    const batch = data.slice(i, i + BATCH_SIZE);
    
    console.log(`📦 Processing ${source} batch ${batchNum} (${batch.length} records)`);

    try {
      // Fault injection: simulate crash after processing 60% of data
      if (FAULT_INJECTION && i >= Math.floor(data.length * 0.6)) {
        console.log(`💥 FAULT_INJECTION: Simulating crash at ${source} batch ${batchNum}`);
        throw new Error(`Simulated crash during ${source} processing`);
      }

      for (const record of batch) {
        try {
          // Validate against unified schema
          const { error, value } = unifiedSchema.validate(record);
          if (error) {
            validationErrors += 1;
            continue;
          }
          
          // Skip records older than watermark (incremental load)
          if (watermark && value.timestamp <= watermark) {
            skippedCount++;
            continue;
          }

          // Idempotent upserts with unique key constraint
          if (mongoDb) {
            const keyFilter = { 
              symbol: value.symbol, 
              timestamp: value.timestamp, 
              source: value.source 
            };

            // RAW collection with unique constraint
            const rawResult = await mongoDb.collection('raw_crypto_data').updateOne(
              keyFilter,
              {
                $set: {
                  symbol: value.symbol,
                  name: value.name,
                  price_usd: value.price_usd,
                  volume_24h: value.volume_24h,
                  market_cap: value.market_cap,
                  percent_change_24h: value.percent_change_24h,
                  timestamp: value.timestamp,
                  source: value.source,
                  raw_data: value.raw_data,
                  run_id: runId
                },
                $setOnInsert: { created_at: new Date() }
              },
              { upsert: true }
            );
            
            // Track new vs existing records
            if (rawResult.upsertedId) {
              // New record inserted
            } else if (rawResult.matchedCount > 0) {
              // Existing record updated (should be rare with watermark)
            }

            // NORMALIZED collection with unique constraint
            await mongoDb.collection('normalized_crypto_data').updateOne(
              keyFilter,
              {
                $set: {
                  symbol: value.symbol,
                  name: value.name,
                  price_usd: value.price_usd,
                  volume_24h: value.volume_24h,
                  market_cap: value.market_cap,
                  percent_change_24h: value.percent_change_24h,
                  timestamp: value.timestamp,
                  source: value.source,
                  run_id: runId
                },
                $setOnInsert: { created_at: new Date() }
              },
              { upsert: true }
            );
          }

        } catch (err) {
          console.error(`❌ Failed to insert ${record.symbol}:`, err.message);
          failedIds.push(record.symbol);
        }
      }

      // Save checkpoint after successful batch
      await saveCheckpoint(batchId, i + batch.length, source, runId);
      console.log(`✅ Completed ${source} batch ${batchNum}`);

    } catch (err) {
      console.error(`❌ Batch ${batchNum} failed for ${source}:`, err.message);
      failedBatches.push({ batchNum, error: err.message, recordCount: batch.length });
      
      // Don't save checkpoint on batch failure - will retry from this point
      throw err; // Re-throw to stop processing
    }
  }

  return { 
    totalProcessed: data.length - lastIndex - skippedCount, 
    totalFetched: data.length - lastIndex,
    skippedByWatermark: skippedCount,
    failedIds, 
    validationErrors,
    failedBatches,
    resumedFromBatch
  };
}

// ETL pipeline main function
const runETLPipeline = async () => {
  const startTime = new Date();
  const runId = crypto.randomUUID();
  let totalRowsProcessed = 0;
  let totalErrors = 0;
  let totalValidationErrors = 0;
  let skippedFields = [];
  let allFailedBatches = [];
  let resumeInfo = {};
  
  console.log(`🚀 Starting ETL Pipeline - Run ID: ${runId}`);
  
  // Get Prometheus metrics
  let etlLatencyMetric, etlRowsProcessedMetric;
  try {
    etlLatencyMetric = promClient.register.getSingleMetric('etl_latency_seconds');
    etlRowsProcessedMetric = promClient.register.getSingleMetric('etl_rows_processed_total');
  } catch (err) {
    // Ignore if metrics not registered yet
  }

  try {
    await connectMongoDB();
    
    // Create unique indexes to prevent duplicates
    const mongoDb = getDb();
    if (mongoDb) {
      try {
        await mongoDb.collection('raw_crypto_data').createIndex(
          { symbol: 1, timestamp: 1, source: 1 }, 
          { unique: true, background: true }
        );
        await mongoDb.collection('normalized_crypto_data').createIndex(
          { symbol: 1, timestamp: 1, source: 1 }, 
          { unique: true, background: true }
        );
      } catch (err) {
        // Indexes may already exist
      }
    }

    // Fetch data from all sources
    let [coinpaprikaData, csvData, coingeckoData] = await Promise.all([
      fetchCoinPaprikaData(),
      fetchCSVData(),
      fetchCoinGeckoData()
    ]);

    const allData = [...coinpaprikaData, ...csvData, ...coingeckoData];
    console.log(`📊 Fetched ${allData.length} total records (coinpaprika: ${coinpaprikaData.length}, csv: ${csvData.length}, coingecko: ${coingeckoData.length})`);

    // Process each source with batch + checkpoint
    const results = [];
    const sources = [
      { name: 'coinpaprika', data: coinpaprikaData },
      { name: 'csv', data: csvData },
      { name: 'coingecko', data: coingeckoData }
    ];

    for (const { name, data } of sources) {
      try {
        // Detect schema drift
        const driftResult = schemaDriftDetector.detectDrift(name, data);
        console.log(`🔍 Schema drift analysis for ${name}:`, driftResult);
        
        const result = await processSourceWithCheckpoint(name, data, runId);
        results.push({ 
          source: name, 
          ...result,
          applied_mappings: driftResult.applied_mappings,
          schema_warnings: driftResult.warnings,
          schema_version: driftResult.schema_version
        });
        
        if (result.resumedFromBatch !== null) {
          resumeInfo[name] = { resumedFromBatch: result.resumedFromBatch };
        }
        
        if (result.failedBatches.length > 0) {
          allFailedBatches.push(...result.failedBatches.map(b => ({ source: name, ...b })));
        }
        
      } catch (error) {
        console.error(`❌ Source ${name} failed:`, error.message);
        results.push({ 
          source: name, 
          totalProcessed: 0, 
          failedIds: [], 
          validationErrors: 0,
          failedBatches: [{ batchNum: 'unknown', error: error.message, recordCount: data.length }],
          resumedFromBatch: null
        });
        allFailedBatches.push({ source: name, batchNum: 'unknown', error: error.message, recordCount: data.length });
        
        // Continue processing other sources even if one fails
      }
    }

    // Collect metrics
    const failedIdsAll = results.flatMap(r => r.failedIds);
    totalValidationErrors = results.reduce((sum, r) => sum + (r.validationErrors || 0), 0);
    totalRowsProcessed = results.reduce((sum, r) => sum + (r.totalProcessed || 0), 0);
    totalErrors = failedIdsAll.length + totalValidationErrors;
    skippedFields = ['volume_24h']; // example

    // Generate summary
    const avgRetryLatency = {
      coinpaprika: throttlingMetrics.coinpaprika.throttled > 0
        ? Math.round(throttlingMetrics.coinpaprika.totalRetryWaitMs / throttlingMetrics.coinpaprika.throttled)
        : 0,
      coingecko: throttlingMetrics.coingecko.throttled > 0
        ? Math.round(throttlingMetrics.coingecko.totalRetryWaitMs / throttlingMetrics.coingecko.throttled)
        : 0,
    };

    const isPartialSuccess = allFailedBatches.length > 0;
    const status = isPartialSuccess ? 'partial_success' : 'success';

    const summary = {
      run_id: runId,
      totalFetched: allData.length,
      validRecords: totalRowsProcessed,
      invalidRecords: totalErrors,
      failedIds: failedIdsAll,
      validationErrors: totalValidationErrors,
      failed_batches: allFailedBatches,
      resume_info: resumeInfo,
      sources: {
        coinpaprika: coinpaprikaData.length,
        csv: csvData.length,
        coingecko: coingeckoData.length
      },
      throttling: {
        coinpaprika: {
          throttled: throttlingMetrics.coinpaprika.throttled,
          avgRetryLatencyMs: avgRetryLatency.coinpaprika,
        },
        coingecko: {
          throttled: throttlingMetrics.coingecko.throttled,
          avgRetryLatencyMs: avgRetryLatency.coingecko,
        }
      },
      timestamp: new Date()
    };

    await generateAndStoreETLSummary(summary);

    const totalLatency = new Date() - startTime;
    
    // Update Prometheus metrics
    if (etlLatencyMetric) {
      etlLatencyMetric.observe(totalLatency / 1000);
    }
    
    if (etlRowsProcessedMetric) {
      etlRowsProcessedMetric.inc(totalRowsProcessed);
    }

    // Collect schema drift info
    const allAppliedMappings = results.flatMap(r => r.applied_mappings || []);
    const allSchemaWarnings = results.flatMap(r => r.schema_warnings || []);
    const maxSchemaVersion = Math.max(...results.map(r => r.schema_version || 1));

    // Log ETL run
    await logETLRun({
      run_id: runId,
      startTime,
      status,
      rowsProcessed: totalRowsProcessed,
      errors: totalErrors,
      failed_batches: allFailedBatches,
      resume_info: resumeInfo,
      applied_mappings: allAppliedMappings,
      schema_warnings: allSchemaWarnings,
      schema_version: maxSchemaVersion,
      skippedFields,
      totalLatency,
      skipped_by_watermark: results.reduce((sum, r) => sum + (r.skippedByWatermark || 0), 0)
    });

    // Clear checkpoints on successful completion
    if (!isPartialSuccess) {
      await clearCheckpoints(runId);
      console.log(`✅ ETL Pipeline completed successfully - cleared checkpoints`);
    } else {
      console.log(`⚠️ ETL Pipeline completed with failures - checkpoints preserved for resume`);
    }

    console.log(`📋 ETL Pipeline Summary:`, summary);
    return summary;

  } catch (error) {
    const totalLatency = new Date() - startTime;

    // Log failed ETL run
    await logETLRun({
      run_id: runId,
      startTime,
      status: 'failed',
      rowsProcessed: totalRowsProcessed,
      errors: totalErrors + 1,
      failed_batches: allFailedBatches,
      resume_info: resumeInfo,
      skippedFields,
      totalLatency,
      error_message: error.message
    });

    console.error(`❌ ETL Pipeline Error:`, error.message);
    throw error;

  } finally {
    // Keep connection open for scheduled runs
  }
};

// Run ETL if this file is executed directly
if (require.main === module) {
  runETLPipeline()
    .then(() => console.log(' ETL Completed'))
    .catch(err => console.error(' ETL Failed:', err));
}

module.exports = { runETLPipeline, processSourceWithCheckpoint };

// Handle graceful shutdowns: close mongoose when process exits
process.on('SIGINT', async () => {
  console.log('\n SIGINT received — shutting down gracefully...');
  await closeMongoDB();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n SIGTERM received — shutting down gracefully...');
  await closeMongoDB();
  process.exit(0);
});
