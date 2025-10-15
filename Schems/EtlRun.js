const mongoose = require('mongoose');

const etlRunSchema = new mongoose.Schema({
  run_id: { type: String, required: true },
  start_time: { type: Date, required: true },
  end_time: { type: Date, required: true },
  status: { type: String, enum: ['success', 'failed'], required: true },
  rows_processed: { type: Number, default: 0 },
  errors: { type: Number, default: 0 },
  skipped_fields: { type: [String], default: [] },
  total_latency_ms: { type: Number, default: 0 },
}, { timestamps: true });

module.exports = mongoose.model('EtlRun', etlRunSchema);
