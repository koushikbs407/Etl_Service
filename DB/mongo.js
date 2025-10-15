const { MongoClient } = require('mongodb');
const config = require('../Config/config.js');

let mongoClient;
let mongoDb;

const connectMongoDB = async () => {
  try {
    mongoClient = new MongoClient(config.mongodb.uri, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 2000,
      socketTimeoutMS: 45000
    });
    await mongoClient.connect();
    mongoDb = mongoClient.db(config.mongodb.database);
    console.log('✅ Connected to MongoDB Atlas');

    // If mongoose is available, connect it too so mongoose models work immediately
    try {
      // lazy-require mongoose so the package is optional
      // eslint-disable-next-line global-require
      const mongoose = require('mongoose');
      // Avoid double-connecting if mongoose already connected
      if (mongoose.connection.readyState === 0) {
        await mongoose.connect(config.mongodb.uri, { dbName: config.mongodb.database });
        console.log('✅ Mongoose connected');
      }
    } catch (e) {
      // mongoose not installed or failed to connect — skip but log
      console.warn('⚠️ Mongoose not initialized (optional):', e.message);
    }
    return mongoDb;
  } catch (error) {
    console.error('❌ MongoDB connection error:', error);
    throw error;
  }
};

const closeMongoDB = async () => {
  if (mongoClient) await mongoClient.close();
};

// Accessor to get the current mongoDb instance after connectMongoDB() has been called
const getDb = () => mongoDb;

module.exports = { connectMongoDB, closeMongoDB, getDb };
