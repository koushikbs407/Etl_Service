const mongoose = require('mongoose');

const etlCheckpointSchema = new mongoose.Schema({
  batchId: { type: String, required: true },
  source: { type: String, required: true },
  runId: { type: String, required: true },
  lastProcessedIndex: { type: Number, default: 0 },
  updatedAt: { type: Date, default: Date.now },
}, { timestamps: true });

// Compound index for efficient queries
etlCheckpointSchema.index({ batchId: 1, source: 1 }, { unique: true });
etlCheckpointSchema.index({ runId: 1 });

module.exports = mongoose.model('EtlCheckpoint', etlCheckpointSchema);
