const { MongoClient } = require('mongodb');
require('dotenv').config();

// MongoDB Atlas configuration
const mongodbConfig = {
  uri: process.env.MONGODB_ATLAS_URI || 'mongodb+srv://username:password@cluster.mongodb.net/',
  database: process.env.MONGODB_DB || 'crypto_etl'
};

async function setupMongoDBAtlas() {
  const client = new MongoClient(mongodbConfig.uri, {
    maxPoolSize: 10,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
  });
  
  try {
    await client.connect();
    console.log('✅ Connected to MongoDB Atlas');

    const db = client.db(mongodbConfig.database);

    // Create raw_crypto_data collection with validation
    await db.createCollection('raw_crypto_data', {
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
    await db.createCollection('normalized_crypto_data', {
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
    await db.createCollection('etl_summaries');

    // Create indexes for performance
    console.log('📊 Creating indexes...');
    
    await db.collection('raw_crypto_data').createIndex({ symbol: 1, timestamp: 1, source: 1 }, { unique: true });
    await db.collection('raw_crypto_data').createIndex({ timestamp: -1 });
    await db.collection('raw_crypto_data').createIndex({ source: 1 });

    await db.collection('normalized_crypto_data').createIndex({ symbol: 1, timestamp: 1, source: 1 }, { unique: true });
    await db.collection('normalized_crypto_data').createIndex({ timestamp: -1 });
    await db.collection('normalized_crypto_data').createIndex({ source: 1 });

    await db.collection('etl_summaries').createIndex({ timestamp: -1 });

    console.log('✅ MongoDB Atlas collections and indexes created successfully');

    // Test the setup
    console.log('🧪 Testing collections...');
    const collections = await db.listCollections().toArray();
    console.log(`📁 Created collections: ${collections.map(c => c.name).join(', ')}`);

  } catch (error) {
    console.error('❌ MongoDB Atlas setup error:', error.message);
    throw error;
  } finally {
    await client.close();
  }
}

async function main() {
  console.log('🚀 Setting up ETL Pipeline with MongoDB Atlas...\n');

  try {
    await setupMongoDBAtlas();
    
    console.log('\n🎉 MongoDB Atlas setup completed successfully!');
    console.log('\n📋 Next steps:');
    console.log('1. Run: npm install');
    console.log('2. Set your MONGODB_ATLAS_URI in .env file');
    console.log('3. Run: npm start');
    console.log('\n💡 MongoDB Atlas URI format:');
    console.log('MONGODB_ATLAS_URI=mongodb+srv://username:password@cluster.mongodb.net/');
    
  } catch (error) {
    console.error('\n💥 Setup failed:', error.message);
    console.log('\n🔧 Troubleshooting:');
    console.log('1. Verify your MongoDB Atlas connection string');
    console.log('2. Check if your IP is whitelisted in MongoDB Atlas');
    console.log('3. Ensure your database user has read/write permissions');
    console.log('4. Check your internet connection');
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { setupMongoDBAtlas };