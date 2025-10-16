const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../DB/mongo');

// Optional mongoose model fallback (only used if native DB isn't available)
let EtlRunModel;
try {
  EtlRunModel = require('../Schems/EtlRun');
} catch (e) {
  EtlRunModel = null;
}

async function logETLRun({
  run_id,
  startTime,
  status,
  rowsProcessed,
  errors,
  skippedFields,
  totalLatency,
  applied_mappings,
  schema_warnings,
  schema_version,
  failed_batches,
  resume_info,
  error_message
}) {
  const runId = run_id || uuidv4();
  const endTime = new Date();

  const record = {
    run_id: runId,
    start_time: startTime,
    end_time: endTime,
    status,
    rows_processed: rowsProcessed,
    errors,
    skipped_fields: skippedFields || [],
    total_latency_ms: totalLatency,
    applied_mappings: applied_mappings || [],
    schema_warnings: schema_warnings || [],
    schema_version: schema_version || 1,
    failed_batches: failed_batches || [],
    resume_info: resume_info || {},
    error_message: error_message || null
  };

  const mongoDb = getDb();
  if (mongoDb) {
    // Use native driver to insert immediately on the connected client
    try {
  // Insert into the collection that matches the Mongoose model name 'EtlRun'
  await mongoDb.collection('etlruns').insertOne(record);
  console.log(` ETL Run Logged (native -> etlruns): ${runId}`);
      return;
    } catch (err) {
      console.warn(' Failed to write etl run via native driver:', err.message);
      // fallthrough to mongoose fallback
    }
  }

  if (EtlRunModel) {
    // Fallback to mongoose model if available
    await EtlRunModel.create(record);
    console.log(` ETL Run Logged (mongoose): ${runId}`);
    return;
  }

  // If neither is available, just log to console
  console.log(' ETL Run (no DB):', JSON.stringify(record));
}

module.exports = { logETLRun };
