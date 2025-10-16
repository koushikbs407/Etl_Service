const promClient = require('prom-client');

// Outlier detection metrics
const outlierDetectedTotal = new promClient.Counter({
  name: 'outlier_detected_total',
  help: 'Total outliers detected by field and type',
  labelNames: ['field', 'type', 'symbol']
});

// Register metric
const register = promClient.register;
register.registerMetric(outlierDetectedTotal);

// Simple outlier detection using z-score and percentage jump
class OutlierDetector {
  constructor() {
    this.historicalData = new Map(); // symbol -> field -> values[]
    this.zScoreThreshold = 2.5; // Standard deviations
    this.percentageJumpThreshold = 50; // Percentage change
  }

  detectOutliers(records) {
    const outliers = [];
    const numericFields = ['price_usd', 'volume_24h', 'market_cap', 'percent_change_24h'];

    for (const record of records) {
      const symbol = record.symbol;
      if (!symbol) continue;

      for (const field of numericFields) {
        const value = parseFloat(record[field]);
        if (isNaN(value) || value <= 0) continue;

        const outlier = this.checkFieldOutlier(symbol, field, value);
        if (outlier) {
          outliers.push({
            symbol,
            field,
            value,
            type: outlier.type,
            threshold: outlier.threshold,
            actual: outlier.actual
          });
          
          // Update Prometheus metric
          outlierDetectedTotal.labels(field, outlier.type, symbol).inc();
          
          console.log(`🚨 OUTLIER: ${symbol} ${field}=${value} (${outlier.type}: ${outlier.actual.toFixed(2)} > ${outlier.threshold})`);
        }

        // Store value for future comparisons
        this.storeValue(symbol, field, value);
      }
    }

    return outliers;
  }

  checkFieldOutlier(symbol, field, value) {
    const key = `${symbol}:${field}`;
    const history = this.historicalData.get(key) || [];
    
    if (history.length < 3) return null; // Need minimum history

    // Z-score detection
    const mean = history.reduce((sum, v) => sum + v, 0) / history.length;
    const variance = history.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / history.length;
    const stdDev = Math.sqrt(variance);
    
    if (stdDev > 0) {
      const zScore = Math.abs((value - mean) / stdDev);
      if (zScore > this.zScoreThreshold) {
        return { type: 'z_score', threshold: this.zScoreThreshold, actual: zScore };
      }
    }

    // Percentage jump detection (compared to last value)
    const lastValue = history[history.length - 1];
    if (lastValue > 0) {
      const percentChange = Math.abs((value - lastValue) / lastValue) * 100;
      if (percentChange > this.percentageJumpThreshold) {
        return { type: 'percentage_jump', threshold: this.percentageJumpThreshold, actual: percentChange };
      }
    }

    return null;
  }

  storeValue(symbol, field, value) {
    const key = `${symbol}:${field}`;
    const history = this.historicalData.get(key) || [];
    
    history.push(value);
    
    // Keep only last 20 values for efficiency
    if (history.length > 20) {
      history.shift();
    }
    
    this.historicalData.set(key, history);
  }

  getOutlierStats() {
    const stats = {};
    for (const [key, values] of this.historicalData.entries()) {
      const [symbol, field] = key.split(':');
      if (!stats[symbol]) stats[symbol] = {};
      stats[symbol][field] = {
        count: values.length,
        latest: values[values.length - 1],
        mean: values.reduce((sum, v) => sum + v, 0) / values.length
      };
    }
    return stats;
  }
}

const outlierDetector = new OutlierDetector();

module.exports = { outlierDetector, OutlierDetector };