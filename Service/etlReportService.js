const { getDb } = require('../DB/mongo'); // use accessor to obtain DB after connection

/**
 * Generate ETL summary and store in DB.
 * Supports two call signatures for backward compatibility:
 *  - generateAndStoreETLSummary(allData, sourcesCount)
 *  - generateAndStoreETLSummary(summaryObject)
 */
const generateAndStoreETLSummary = async (arg1, arg2 = {}) => {
  let summary;

  // If caller passed a prebuilt summary object, use it (preferred)
  if (arg1 && typeof arg1 === 'object' && !Array.isArray(arg1) && 'totalFetched' in arg1) {
    summary = { ...arg1 };
    summary.database = summary.database || 'mongodb_atlas';
    summary.timestamp = summary.timestamp || new Date();
  } else {
    // Backward-compatible: arg1 is allData array, arg2 is sourcesCount
    const allData = Array.isArray(arg1) ? arg1 : [];
    const sourcesCount = arg2 || {};

    const validRecordsCount = allData.filter(d => d.valid).length;
    const invalidRecordsCount = allData.filter(d => !d.valid).length;

    summary = {
      totalFetched: allData.length,
      validRecords: validRecordsCount,
      invalidRecords: invalidRecordsCount,
      sources: {
        coinpaprika: sourcesCount.coinpaprika || 0,
        csv: sourcesCount.csv || 0,
        coingecko: sourcesCount.coingecko || 0
      },
      timestamp: new Date(),
      database: 'mongodb_atlas'
    };
  }

  // Store summary in MongoDB
  const mongoDb = getDb();
  if (!mongoDb) throw new Error('MongoDB is not initialized. Call connectMongoDB() first.');
  await mongoDb.collection('etl_summaries').insertOne(summary);

  console.log('\n📈 ETL Pipeline Summary:');
  console.log(JSON.stringify(summary, null, 2));

  return summary;
};

module.exports = { generateAndStoreETLSummary };
