const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const Joi = require('joi');
const csv = require('csv-parser');
const moment = require('moment');
require('dotenv').config();
const { mapCsvRowToUnifiedSchema } = require('../Utility/schemaDrift');
const config = require('../Config/config');

// Configuration


// Unified Schema Validation
const unifiedSchema = Joi.object({
  symbol: Joi.string().required(),
  name: Joi.string().required(),
  price_usd: Joi.number().positive().required(),
  volume_24h: Joi.number().min(0).required(),
  market_cap: Joi.number().min(0).allow(null),
  percent_change_24h: Joi.number().allow(null),
  timestamp: Joi.date().iso().required(),
  source: Joi.string().valid('coinpaprika', 'coingecko', 'csv').required(),
  raw_data: Joi.object().required()
});

// Type conversion utilities
const convertToNumber = (value) => {
  if (!value) return 0;
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const cleaned = value.replace(/[,$\s]/g, ''); // remove commas, $ sign, whitespace
    const parsed = parseFloat(cleaned);
    return isNaN(parsed) ? 0 : parsed;
  }
  return 0;
};


const convertToDate = (value) => {
  if (value instanceof Date) return value;
  if (typeof value === 'string') {
    const parsed = moment(value);
    return parsed.isValid() ? parsed.toDate() : new Date();
  }
  return new Date();
};

// MongoDB connection
let mongoClient;
let mongoDb;

const connectMongoDB = async () => {
  try {
    console.log(' Connecting to MongoDB Atlas...');
    console.log(' URI:', config.mongodb.uri.replace(/\/\/.*@/, '//***:***@')); // Hide credentials in log
    
    // MongoDB Atlas connection
    mongoClient = new MongoClient(config.mongodb.uri, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });
    
    await mongoClient.connect();
    mongoDb = mongoClient.db(config.mongodb.database);
    console.log(' Connected to MongoDB Atlas');

    // Create collections and indexes
    await createCollections();
  } catch (error) {
    console.error(' MongoDB Atlas connection error:', error);
    throw error;
  }
};

const createCollections = async () => {
  try {
    // Create raw_crypto_data collection with validation
    await mongoDb.createCollection('raw_crypto_data', {
      validator: {
        $jsonSchema: {
          bsonType: 'object',
          required: ['symbol', 'name', 'price_usd', 'volume_24h', 'timestamp', 'source', 'raw_data'],
          properties: {
            symbol: { bsonType: 'string', minLength: 1, maxLength: 20 },
            name: { bsonType: 'string', minLength: 1, maxLength: 100 },
            price_usd: { bsonType: 'number', minimum: 0 },
            volume_24h: { bsonType: 'number', minimum: 0 },
            market_cap: { bsonType: ['number', 'null'], minimum: 0 },
            percent_change_24h: { bsonType: ['number', 'null'] },
            timestamp: { bsonType: 'date' },
            source: { 
              bsonType: 'string', 
              enum: ['coinpaprika', 'coingecko', 'csv'] 
            },
            raw_data: { bsonType: 'object' },
            created_at: { bsonType: 'date' }
          }
        }
      }
    });

    // Create normalized_crypto_data collection with validation
    await mongoDb.createCollection('normalized_crypto_data', {
      validator: {
        $jsonSchema: {
          bsonType: 'object',
          required: ['symbol', 'name', 'price_usd', 'volume_24h', 'timestamp', 'source'],
          properties: {
            symbol: { bsonType: 'string', minLength: 1, maxLength: 20 },
            name: { bsonType: 'string', minLength: 1, maxLength: 100 },
            price_usd: { bsonType: 'number', minimum: 0 },
            volume_24h: { bsonType: 'number', minimum: 0 },
            market_cap: { bsonType: ['number', 'null'], minimum: 0 },
            percent_change_24h: { bsonType: ['number', 'null'] },
            timestamp: { bsonType: 'date' },
            source: { 
              bsonType: 'string', 
              enum: ['coinpaprika', 'coingecko', 'csv'] 
            },
            created_at: { bsonType: 'date' }
          }
        }
      }
    });

    // Create etl_summaries collection
    await mongoDb.createCollection('etl_summaries');

    // Create indexes for performance
    await mongoDb.collection('raw_crypto_data').createIndex({ symbol: 1, timestamp: 1, source: 1 }, { unique: true });
    await mongoDb.collection('raw_crypto_data').createIndex({ timestamp: -1 });
    await mongoDb.collection('raw_crypto_data').createIndex({ source: 1 });

    await mongoDb.collection('normalized_crypto_data').createIndex({ symbol: 1, timestamp: 1, source: 1 }, { unique: true });
    await mongoDb.collection('normalized_crypto_data').createIndex({ timestamp: -1 });
    await mongoDb.collection('normalized_crypto_data').createIndex({ source: 1 });

    await mongoDb.collection('etl_summaries').createIndex({ timestamp: -1 });

    console.log(' MongoDB collections and indexes created successfully');
  } catch (error) {
    console.error('Error creating MongoDB collections:', error);
    throw error;
  }
};

// Data Source A: CoinPaprika API
const fetchCoinPaprikaData = async () => {
  try {
    console.log(' Fetching data from CoinPaprika API...');
    const response = await axios.get(config.apis.coinpaprika);
    
    return response.data.slice(0, 10).map(coin => {
      const normalizedData = {
        symbol: coin.symbol?.toUpperCase() || 'UNKNOWN',
        name: coin.name || 'Unknown',
        price_usd: convertToNumber(coin.quotes?.USD?.price),
        volume_24h: convertToNumber(coin.quotes?.USD?.volume_24h),
        market_cap: convertToNumber(coin.quotes?.USD?.market_cap),
        percent_change_24h: convertToNumber(coin.quotes?.USD?.percent_change_24h),
        timestamp: convertToDate(coin.last_updated),
        source: 'coinpaprika',
        raw_data: coin
      };
      
      return normalizedData;
    });
  } catch (error) {
    console.error(' Error fetching CoinPaprika data:', error.message);
    return [];
  }
};

// Data Source B: CSV File
const fetchCSVData = async () => {
  return new Promise((resolve, reject) => {
    const results = [];
    
    const csvPath = path.join(__dirname, 'Historical_Data.csv');

    // Create read stream and attach explicit error handler to avoid unhandled 'error' events
    const readStream = fs.createReadStream(csvPath);

    readStream
      .on('error', (err) => {
        // Provide clearer path and context in the error message
        console.error('\u274c Error opening CSV file at', csvPath, err);
        reject(err);
      })
      .pipe(csv())
      .on('data', (data) => {
        // Use schema drift mapping to map CSV columns to unified schema
        const { mappedRow, mappingLog } = mapCsvRowToUnifiedSchema(data);

        // Log the mapping for debugging
        if (mappingLog.length > 0) {
          console.log(' CSV mapping:', mappingLog);
        }

       
        const normalizedData = {
          symbol: (mappedRow.symbol || 'UNKNOWN').trim().toUpperCase(),
          name: (mappedRow.name || mappedRow.symbol || 'Unknown').trim(),
          price_usd: convertToNumber(mappedRow.price_usd),
          volume_24h: convertToNumber(mappedRow.volume_24h),
          market_cap: convertToNumber(mappedRow.market_cap),
          percent_change_24h: convertToNumber(mappedRow.percent_change_24h),
          timestamp: convertToDate(mappedRow.timestamp),
          source: 'csv',
          raw_data: data
    };


        //console.log(normalizedData);
        
        results.push(normalizedData);
      })
      .on('end', () => {
        console.log(` Loaded ${results.length} records from CSV`);
        resolve(results);
      })
      .on('error', (error) => {
        console.error('\u274c Error parsing CSV file:', error);
        reject(error);
      });
  });
};

// Data Source C: CoinGecko API
const fetchCoinGeckoData = async () => {
  try {
    console.log(' Fetching data from CoinGecko API...');
    const response = await axios.get(`${config.apis.coingecko}?vs_currency=usd&order=market_cap_desc&per_page=20&page=1`);
    
    return response.data.map(coin => {
      const normalizedData = {
        symbol: coin.symbol?.toUpperCase() || 'UNKNOWN',
        name: coin.name || 'Unknown',
        price_usd: convertToNumber(coin.current_price),
        volume_24h: convertToNumber(coin.total_volume),
        market_cap: convertToNumber(coin.market_cap),
        percent_change_24h: convertToNumber(coin.price_change_percentage_24h),
        timestamp: convertToDate(coin.last_updated),
        source: 'coingecko',
        raw_data: coin
      };
      
      return normalizedData;
    });
  } catch (error) {
    console.error(' Error fetching CoinGecko data:', error.message);
    return [];
  }
};

// Incremental loading - check if data already exists
const isDataAlreadyProcessed = async (symbol, timestamp, source) => {
  try {
    const count = await mongoDb.collection('raw_crypto_data').countDocuments({
      symbol: symbol,
      timestamp: timestamp,
      source: source
    });
    return count > 0;
  } catch (error) {
    console.error('Error checking if data already processed:', error);
    return false;
  }
};

// Validate and store data
const validateAndStoreData = async (data) => {
  const validRecords = [];
  const invalidRecords = [];

  for (const record of data) {
    const { error, value } = unifiedSchema.validate(record);
    
    if (error) {
      console.warn(`  Validation error for ${record.symbol}:`, error.details[0].message);
      invalidRecords.push({ record, error: error.details[0].message });
    } else {
      validRecords.push(value);
    }
  }

  console.log(` Valid records: ${validRecords.length}, Invalid records: ${invalidRecords.length}`);
  
  // Store valid records
  for (const record of validRecords) {
    try {
      // Check if already processed (incremental loading)
      const alreadyProcessed = await isDataAlreadyProcessed(
        record.symbol, 
        record.timestamp, 
        record.source
      );

      if (alreadyProcessed) {
        console.log(` Skipping already processed record: ${record.symbol} from ${record.source}`);
        continue;
      }

      const now = new Date();

      // Prepare raw data document
      const rawDocument = {
        ...record,
        raw_data: record.raw_data,
        created_at: now
      };

      // Prepare normalized data document
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

      // Print before storing raw data
      console.log(` About to store RAW data for ${record.symbol} from ${record.source}:`, {
        symbol: record.symbol,
        price_usd: record.price_usd,
        volume_24h: record.volume_24h,
        timestamp: record.timestamp,
        source: record.source
      });

      // Store raw data
      await mongoDb.collection('raw_crypto_data').insertOne(rawDocument);

      // Print before storing normalized data
      console.log(` About to store NORMALIZED data for ${record.symbol} from ${record.source}:`, {
        symbol: record.symbol,
        name: record.name,
        price_usd: record.price_usd,
        volume_24h: record.volume_24h,
        market_cap: record.market_cap,
        percent_change_24h: record.percent_change_24h,
        timestamp: record.timestamp,
        source: record.source
      });

      // Store normalized data
      await mongoDb.collection('normalized_crypto_data').insertOne(normalizedDocument);

      console.log(` Successfully stored: ${record.symbol} from ${record.source}`);

    } catch (error) {
      if (error.code === 11000) {
        // Duplicate key error - record already exists
        console.log(`  Duplicate record skipped: ${record.symbol} from ${record.source}`);
      } else {
        console.error(` Error storing record for ${record.symbol}:`, error.message);
      }
    }
  }

  return { validRecords, invalidRecords };
};

// Main ETL Orchestrator
const runETLPipeline = async () => {
  try {
    console.log(' Starting ETL Pipeline with MongoDB Atlas...\n');
    
    // Connect to MongoDB Atlas
    await connectMongoDB();

    // Fetch data from all sources
    console.log(' Fetching data from all sources...');
    let [coinpaprikaData, csvData, coingeckoData] = await Promise.all([
      fetchCoinPaprikaData(),
      fetchCSVData(),
      fetchCoinGeckoData()
    ]);
    coinpaprikaData = coinpaprikaData.slice(0, 5); // Only 5 from Source A
    csvData = csvData.slice(0, 5);                 // Only 5 from CSV
    coingeckoData = coingeckoData.slice(0, 3);     // Only 3 from Source C

    // Combine all data
    const allData = [...coinpaprikaData, ...csvData, ...coingeckoData];
    console.log(` Total records fetched: ${allData.length}`);

    // Validate and store data
    const { validRecords, invalidRecords } = await validateAndStoreData(allData);

    // Generate summary
    const summary = {
      totalFetched: allData.length,
      validRecords: validRecords.length,
      invalidRecords: invalidRecords.length,
      sources: {
        coinpaprika: coinpaprikaData.length,
        csv: csvData.length,
        coingecko: coingeckoData.length
      },
      timestamp: new Date(),
      database: 'mongodb_atlas'
    };

    console.log('\n ETL Pipeline Summary:');
    console.log(JSON.stringify(summary, null, 2));

    // Store summary in MongoDB
    await mongoDb.collection('etl_summaries').insertOne(summary);

    return summary;

  } catch (error) {
    console.error(' ETL Pipeline Error:', error);
    throw error;
  } finally {
    // Close MongoDB connection
    if (mongoClient) {
      await mongoClient.close();
      console.log(' MongoDB Atlas connection closed');
    }
  }
};

// Run the ETL pipeline
if (require.main === module) {
  runETLPipeline()
    .then(summary => {
      console.log('\n ETL Pipeline completed successfully!');
      process.exit(0);
    })
    .catch(error => {
      console.error('\n ETL Pipeline failed:', error);
      process.exit(1);
    });
}

module.exports = {
  runETLPipeline,
  fetchCoinPaprikaData,
  fetchCSVData,
  fetchCoinGeckoData,
  validateAndStoreData,
  connectMongoDB
};