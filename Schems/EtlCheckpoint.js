const mongoose = require('mongoose');

const etlCheckpointSchema = new mongoose.Schema({
  batchId: { type: String, required: true },
  source: { type: String, required: true },
  lastProcessedIndex: { type: Number, default: 0 },
  updatedAt: { type: Date, default: Date.now },
}, { timestamps: true });

module.exports = mongoose.model('EtlCheckpoint', etlCheckpointSchema);
