const levenshtein = require('fast-levenshtein');

// Minimal unified fields expected by the pipeline
const unifiedFields = ['symbol', 'name', 'price_usd', 'volume_24h', 'market_cap', 'percent_change_24h', 'timestamp', 'source'];

// Known column aliases - exact matches for common variations
const columnAliases = {
  'time': 'timestamp',
  'ticker': 'symbol',
  'usd_price': 'price_usd',
  'tx_volume': 'volume_24h',
  'created_at': 'timestamp',
  'price_timestamp': 'timestamp'
};

// Confidence threshold for fuzzy mapping (0-1)
const CONFIDENCE_THRESHOLD = 0.8;

// Schema drift detector
class SchemaDriftDetector {
  constructor() {
    this.knownSchemas = new Map();
    this.schemaVersion = 1;
  }

  detectDrift(source, currentData) {
    const currentSchema = this.extractSchema(currentData);
    const previousSchema = this.knownSchemas.get(source);
    
    const result = {
      applied_mappings: [],
      warnings: [],
      schema_version: this.schemaVersion
    };

    if (!previousSchema) {
      console.log(`📋 New schema detected for ${source}`);
      this.knownSchemas.set(source, currentSchema);
      return result;
    }

    const hasChanges = !this.schemasEqual(currentSchema, previousSchema);
    if (hasChanges) {
      this.schemaVersion++;
      result.schema_version = this.schemaVersion;
      console.log(`🔄 Schema drift detected for ${source}, version: ${this.schemaVersion}`);
      
      const mappings = this.findMappings(previousSchema, currentSchema);
      result.applied_mappings = mappings.filter(m => m.confidence >= CONFIDENCE_THRESHOLD);
      result.warnings = mappings.filter(m => m.confidence < CONFIDENCE_THRESHOLD);
      
      // Log results
      result.applied_mappings.forEach(m => {
        console.log(`🔄 Auto-mapped: ${m.from} -> ${m.to} (confidence: ${m.confidence})`);
      });
      
      result.warnings.forEach(w => {
        console.log(`⚠️ Low confidence mapping skipped: ${w.from} -> ${w.to} (confidence: ${w.confidence})`);
      });
      
      this.knownSchemas.set(source, currentSchema);
    }

    return result;
  }

  findMappings(oldSchema, newSchema) {
    const mappings = [];
    const oldFields = Object.keys(oldSchema);
    const newFields = Object.keys(newSchema);
    const missingFields = oldFields.filter(f => !newFields.includes(f));
    const addedFields = newFields.filter(f => !oldFields.includes(f));

    for (const missing of missingFields) {
      let bestMatch = null;
      let bestConfidence = 0;

      for (const added of addedFields) {
        const confidence = this.calculateSimilarity(missing, added);
        if (confidence > bestConfidence) {
          bestConfidence = confidence;
          bestMatch = added;
        }
      }

      if (bestMatch) {
        mappings.push({
          from: missing,
          to: bestMatch,
          confidence: parseFloat(bestConfidence.toFixed(3))
        });
      }
    }

    return mappings;
  }

  calculateSimilarity(field1, field2) {
    // Check exact aliases first
    if (columnAliases[field1] === field2 || columnAliases[field2] === field1) {
      return 1.0;
    }
    
    // Normalize field names for comparison
    const normalize = (str) => str.toLowerCase().replace(/[_-]/g, '');
    const norm1 = normalize(field1);
    const norm2 = normalize(field2);
    
    // Check if one contains the other
    if (norm1.includes(norm2) || norm2.includes(norm1)) {
      return 0.9;
    }
    
    // Levenshtein distance calculation
    const maxLen = Math.max(norm1.length, norm2.length);
    if (maxLen === 0) return 1.0;
    const distance = levenshtein.get(norm1, norm2);
    return 1 - (distance / maxLen);
  }

  extractSchema(data) {
    if (!data || data.length === 0) return {};
    const schema = {};
    const sample = data[0];
    for (const [key, value] of Object.entries(sample)) {
      schema[key] = typeof value;
    }
    return schema;
  }

  schemasEqual(schema1, schema2) {
    const keys1 = Object.keys(schema1).sort();
    const keys2 = Object.keys(schema2).sort();
    return JSON.stringify(keys1) === JSON.stringify(keys2) &&
           keys1.every(k => schema1[k] === schema2[k]);
  }
}

const schemaDriftDetector = new SchemaDriftDetector();

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

module.exports = { mapCsvRowToUnifiedSchema, schemaDriftDetector, SchemaDriftDetector };
