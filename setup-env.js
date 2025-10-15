const fs = require('fs');
const path = require('path');

console.log('🔧 MongoDB Atlas ETL Pipeline Setup\n');

console.log('📋 To run this ETL pipeline, you need to:');
console.log('1. Create a MongoDB Atlas account at https://mongodb.com/atlas');
console.log('2. Create a free cluster (M0 tier)');
console.log('3. Set up database access and network access');
console.log('4. Get your connection string\n');

console.log('📝 Create a .env file in the project root with:');
console.log('MONGODB_ATLAS_URI=mongodb+srv://username:password@cluster.mongodb.net/');
console.log('MONGODB_DB=crypto_etl\n');

console.log('🚀 Then run:');
console.log('npm run setup  # Setup collections');
console.log('npm start      # Run ETL pipeline\n');

// Check if .env file exists
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  console.log('✅ .env file found!');
  const envContent = fs.readFileSync(envPath, 'utf8');
  if (envContent.includes('mongodb+srv://')) {
    console.log('✅ MongoDB Atlas URI detected in .env file');
  } else {
    console.log('⚠️  Please update MONGODB_ATLAS_URI in .env file');
  }
} else {
  console.log('❌ .env file not found');
  console.log('📝 Creating sample .env file...');
  
  const sampleEnv = `# MongoDB Atlas Configuration
# Replace with your actual MongoDB Atlas connection string
MONGODB_ATLAS_URI=mongodb+srv://username:password@cluster.mongodb.net/
MONGODB_DB=crypto_etl

# API Configuration (optional - defaults provided)
COINPAPRIKA_API_URL=https://api.coinpaprika.com/v1/tickers
COINGECKO_API_URL=https://api.coingecko.com/api/v3/coins/markets

# ETL Configuration (optional - defaults provided)
BATCH_SIZE=100
MAX_RETRIES=3
RETRY_DELAY=1000`;

  fs.writeFileSync(envPath, sampleEnv);
  console.log('✅ Sample .env file created!');
  console.log('📝 Please update MONGODB_ATLAS_URI with your actual connection string');
}

