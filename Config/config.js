const config = {
  mongodb: {
    uri: process.env.MONGODB_URI || 'mongodb://localhost:27017/crypto_etl',
    database: process.env.MONGODB_DB || 'crypto_etl'
  },
  apis: {
    coinpaprika: process.env.COINPAPRIKA_API_URL || 'https://api.coinpaprika.com/v1/tickers',
    coingecko: process.env.COINGECKO_API_URL || 'https://api.coingecko.com/api/v3/coins/markets'
  },
  rateLimits: {
    coinpaprika: {
      requestsPerMinute: 10,
      burstCapacity: 15,
      retryBackoffMs: 2000
    },
    coingecko: {
      requestsPerMinute: 3,
      burstCapacity: 5,
      retryBackoffMs: 5000
    }
  },
  batchSize: parseInt(process.env.BATCH_SIZE) || 100,
  maxRetries: parseInt(process.env.MAX_RETRIES) || 3,
  retryDelay: parseInt(process.env.RETRY_DELAY) || 1000
};

module.exports = config;