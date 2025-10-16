// Service/etlCheckpointService.js
const Checkpoint = require('../Schems/EtlCheckpoint');
const crypto = require('crypto');

async function saveCheckpoint(batchId, lastProcessedIndex, source, runId) {
  console.log(`💾 Saving checkpoint: ${source} at index ${lastProcessedIndex}`);
  return Checkpoint.updateOne(
    { batchId, source },
    { 
      $set: { 
        lastProcessedIndex, 
        runId,
        updatedAt: new Date() 
      } 
    },
    { upsert: true }
  );
}

async function getCheckpoint(batchId, source) {
  const checkpoint = await Checkpoint.findOne({ batchId, source });
  const index = checkpoint ? checkpoint.lastProcessedIndex : 0;
  if (checkpoint) {
    console.log(`💾 Found checkpoint: ${source} at index ${index} (run: ${checkpoint.runId})`);
  }
  return index;
}

async function clearCheckpoints(runId) {
  console.log(`🧽 Clearing checkpoints for run: ${runId}`);
  return Checkpoint.deleteMany({ runId });
}

async function getAllCheckpoints(runId) {
  return Checkpoint.find({ runId }).sort({ source: 1 });
}

module.exports = { saveCheckpoint, getCheckpoint, clearCheckpoints, getAllCheckpoints };
