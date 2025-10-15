// Service/etlCheckpointService.js
const Checkpoint = require('../Schems/EtlCheckpoint'); // new collection for checkpoints
const crypto = require('crypto');

async function saveCheckpoint(batchId, lastProcessedIndex, source) {
  return Checkpoint.updateOne(
    { batchId, source },
    { $set: { lastProcessedIndex, updatedAt: new Date() } },
    { upsert: true }
  );
}

async function getCheckpoint(batchId, source) {
  const checkpoint = await Checkpoint.findOne({ batchId, source });
  return checkpoint ? checkpoint.lastProcessedIndex : 0;
}

module.exports = { saveCheckpoint, getCheckpoint };
