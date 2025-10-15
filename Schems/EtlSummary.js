const mongoose = require('mongoose');

const etlSummarySchema = new mongoose.Schema({
  symbol: String,
  price_usd: Number,
  volume_24h: Number,
  source: String,
  timestamp: Date,
}, { timestamps: true });

module.exports = mongoose.model('EtlSummary', etlSummarySchema);
