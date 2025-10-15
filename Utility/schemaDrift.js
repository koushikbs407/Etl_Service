const levenshtein = require('fast-levenshtein');

// Minimal unified fields expected by the pipeline
const unifiedFields = ['symbol', 'name', 'price_usd', 'volume_24h', 'market_cap', 'percent_change_24h', 'timestamp', 'source'];

// Known column aliases - exact matches for common variations
const columnAliases = {
  'time': 'timestamp',
  'ticker': 'symbol',
  'usd_price': 'price_usd',
  'tx_volume': 'volume_24h'
};

// Confidence threshold for fuzzy mapping (0-1)
const CONFIDENCE_THRESHOLD = 0.8;

// Lightweight converter for common field types
const convertValueType = (field, value) => {
  if (value === undefined || value === null || value === '') return null;

  switch (field) {
    case 'price_usd':
    case 'market_cap':
    case 'volume_24h':
    case 'percent_change_24h':
      if (typeof value === 'string') value = value.replace(/[$,\s]/g, '');
      const num = parseFloat(value);
      return isNaN(num) ? null : num;
    case 'timestamp':
      return value;
    default:
      return typeof value === 'string' ? value.trim() : value;
  }
};

const mapCsvRowToUnifiedSchema = (row) => {
  const mapped = {};
  const rawMappingLog = [];

  const normalizeColName = col => col.toLowerCase().replace(/[\s_]/g, '');

  // First try exact matches from aliases
  for (const [csvCol, targetField] of Object.entries(columnAliases)) {
    if (row[csvCol] !== undefined) {
      mapped[targetField] = convertValueType(targetField, row[csvCol]);
      rawMappingLog.push({ csvCol, mappedTo: targetField, confidence: '1.00' });
      delete row[csvCol];
    }
  }

  for (const csvCol in row) {
    let value = row[csvCol];
    let bestMatch = null;
    let bestScore = 0;

    const effectiveCol = columnAliases[csvCol] || csvCol;

    for (const targetField of unifiedFields) {
      const distance = levenshtein.get(normalizeColName(effectiveCol), normalizeColName(targetField));
      const score = 1 - distance / Math.max(effectiveCol.length, targetField.length || 1);

      if (score > bestScore) {
        bestScore = score;
        bestMatch = targetField;
      }
    }

    if (bestScore >= CONFIDENCE_THRESHOLD) {
      mapped[bestMatch] = convertValueType(bestMatch, value);
      rawMappingLog.push({ csvCol, mappedTo: bestMatch, confidence: bestScore.toFixed(2) });
    } else if (bestScore >= 0.5) {
      console.warn(`⚠️ Column "${csvCol}" mapping below threshold (candidate: "${bestMatch}", score: ${bestScore.toFixed(2)}) — skipped.`);
    } else {
      console.warn(`⚠️ Column "${csvCol}" could not be mapped confidently (score: ${bestScore.toFixed(2)}), skipping.`);
    }
  }

  // Detect unknown columns
  const unknownCols = Object.keys(row).filter(col => {
    const normCol = normalizeColName(col);
    return !unifiedFields.some(f => normalizeColName(f) === normCol) &&
           !Object.keys(columnAliases).includes(col);
  });
  if (unknownCols.length > 0) console.log('⚠️ New/unknown columns detected:', unknownCols);

  return { mappedRow: mapped, mappingLog: rawMappingLog };
};

module.exports = { mapCsvRowToUnifiedSchema };
