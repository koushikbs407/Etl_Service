const unifiedSchema = require('../Schems/unifiedSchema');
const { getDb } = require('../DB/mongo');

const validateAndStoreData = async (data) => {
  const validRecords = [];
  const invalidRecords = [];

  for (const record of data) {
    const { error, value } = unifiedSchema.validate(record);
    if (error) {
      invalidRecords.push({ record, error: error.details[0].message });
    } else {
      validRecords.push(value);
    }
  }

  for (const record of validRecords) {
    const now = new Date();
    const rawDocument = { ...record, created_at: now };
    const normalizedDocument = {
      symbol: record.symbol,
      name: record.name,
      price_usd: record.price_usd,
      volume_24h: record.volume_24h,
      market_cap: record.market_cap,
      percent_change_24h: record.percent_change_24h,
      timestamp: record.timestamp,
      source: record.source,
      created_at: now
    };

    try {
      const mongoDb = getDb();
      if (!mongoDb) throw new Error('MongoDB is not initialized. Call connectMongoDB() first.');
      await mongoDb.collection('raw_crypto_data').insertOne(rawDocument);
      await mongoDb.collection('normalized_crypto_data').insertOne(normalizedDocument);
    } catch (error) {
      if (error.code !== 11000) console.error(error);
    }
  }

  return { validRecords, invalidRecords };
};

module.exports = { validateAndStoreData };
