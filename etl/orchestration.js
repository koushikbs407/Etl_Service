// Use central DB connector for consistent connection handling
const { connectMongoDB, closeMongoDB, getDb } = require('../DB/mongo');
const fetchCoinPaprikaData = require('../Service/coinpaprika');
const fetchCSVData = require('../Service/csvSource');
const fetchCoinGeckoData = require('../Service/coingecko');
const { generateAndStoreETLSummary } = require('../Service/etlReportService');
const { logETLRun } = require('../Service/etlRunLogger');
const { saveCheckpoint, getCheckpoint } = require('../Schedular/etlCheckpointService');
const EtlSummary = require('../Schems/EtlSummary');
const { throttlingMetrics } = require('../Utility/rateLimiter');
const unifiedSchema = require('../Schems/unifiedSchema');
const promClient = require('prom-client');

const config = require('../Config/config');

const BATCH_SIZE = 5; // Adjustable batch size

// Connect to MongoDB using Mongoose
// Delegate connection management to DB/mongo.js
// connectMongoDB and closeMongoDB are provided by that module.

// Process a single source with checkpoint and batch processing using Mongoose
async function processSourceWithCheckpoint(source, data) {
  const batchId = source; // simple batchId for each source
  let lastIndex = await getCheckpoint(batchId, source); // fetch last checkpoint
  const failedIds = [];
  let validationErrors = 0;
  const mongoDb = getDb();

  for (let i = lastIndex; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);

    for (const record of batch) {
      try {
        // Validate against unified schema (Pydantic equivalent)
        const { error, value } = unifiedSchema.validate(record);
        if (error) {
          validationErrors += 1;
          continue;
        }

        // Artificial failure simulation after 60% processed (optional)
        if (i >= Math.floor(data.length * 0.6)) {
          throw new Error(`Artificial failure for ${record.symbol}`);
        }

        // Idempotent upserts for both RAW and NORMALIZED collections using native driver
        if (mongoDb) {
          const keyFilter = { symbol: value.symbol, timestamp: value.timestamp, source: value.source };

          // RAW collection
          await mongoDb.collection('raw_crypto_data').updateOne(
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
              },
              $setOnInsert: { created_at: new Date() }
            },
            { upsert: true }
          );

          // NORMALIZED collection
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
              },
              $setOnInsert: { created_at: new Date() }
            },
            { upsert: true }
          );
        } else {
          // Fallback: keep previous Mongoose upsert into summary collection
          await EtlSummary.findOneAndUpdate(
            { symbol: value.symbol, timestamp: value.timestamp },
            value,
            { upsert: true, new: true }
          );
        }

      } catch (err) {
        console.error(` Failed to insert ${record.symbol}:`, err.message);
        failedIds.push(record.symbol);
      }
    }

    // Save checkpoint after batch
    await saveCheckpoint(batchId, i + batch.length - 1, source);
  }

  return { totalProcessed: data.length, failedIds, validationErrors };
}

// ETL pipeline main function
const runETLPipeline = async () => {
  const startTime = new Date();
  let totalRowsProcessed = 0;
  let totalErrors = 0;
  let totalValidationErrors = 0;
  let skippedFields = [];
  
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

    // Fetch data from all sources
    let [coinpaprikaData, csvData, coingeckoData] = await Promise.all([
      fetchCoinPaprikaData(),
      fetchCSVData(),
      fetchCoinGeckoData()
    ]);

    // For testing: slice data
    //coinpaprikaData = coinpaprikaData.slice(0, 5);
    //csvData = csvData.slice(0, 5);
    //coingeckoData = coingeckoData.slice(0, 3);

    const allData = [...coinpaprikaData, ...csvData, ...coingeckoData];

    // Process each source with batch + checkpoint
    const results = [];
    results.push(await processSourceWithCheckpoint('coinpaprika', coinpaprikaData));
    results.push(await processSourceWithCheckpoint('csv', csvData));
    results.push(await processSourceWithCheckpoint('coingecko', coingeckoData));

    // Collect metrics
    const failedIdsAll = results.flatMap(r => r.failedIds);
    totalValidationErrors = results.reduce((sum, r) => sum + (r.validationErrors || 0), 0);
    totalRowsProcessed = allData.length - failedIdsAll.length;
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

    const summary = {
      totalFetched: allData.length,
      validRecords: totalRowsProcessed,
      invalidRecords: totalErrors,
      failedIds: failedIdsAll,
      validationErrors: totalValidationErrors,
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
      etlLatencyMetric.observe(totalLatency / 1000); // Convert ms to seconds
    }
    
    if (etlRowsProcessedMetric) {
      etlRowsProcessedMetric.inc(totalRowsProcessed);
    }

    // Log successful ETL run
    await logETLRun({
      startTime,
      status: 'success',
      rowsProcessed: totalRowsProcessed,
      errors: totalErrors,
      skippedFields,
      totalLatency
    });

    console.log(' ETL Pipeline Summary:', summary);
    return summary;

  } catch (error) {
    const totalLatency = new Date() - startTime;

    // Log failed ETL run
    await logETLRun({
      startTime,
      status: 'failed',
      rowsProcessed: totalRowsProcessed,
      errors: totalErrors + 1,
      skippedFields,
      totalLatency
    });

    console.error(' ETL Pipeline Error:', error.message);
    throw error;

  } finally {
    // Do not close mongoose here so scheduled runs keep using the same connection.
    // clean shutdown will close the connection via process handlers below.
  }
};

// Run ETL if this file is executed directly
if (require.main === module) {
  runETLPipeline()
    .then(() => console.log(' ETL Completed'))
    .catch(err => console.error(' ETL Failed:', err));
}

module.exports = { runETLPipeline };

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
