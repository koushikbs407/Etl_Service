const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { convertToNumber, convertToDate } = require('../Utility/converters');
const { mapCsvRowToUnifiedSchema } = require('../Utility/schemaDrift');

const fetchCSVData = async () => {
  return new Promise((resolve, reject) => {
    const results = [];
    const csvPath = path.join(__dirname, 'Historical_Data.csv');

    const readStream = fs.createReadStream(csvPath);

    readStream
      .on('error', (err) => {
        console.error('❌ Error opening CSV file at', csvPath, err);
        reject(err);
      })
      .pipe(csv())
      .on('data', (data) => {
        try {
          const { mappedRow, mappingLog } = mapCsvRowToUnifiedSchema(data);

          if (mappingLog.length > 0) {
            console.log('🔄 CSV mapping:', mappingLog);
          }

          const normalizedData = {
            symbol: (mappedRow.symbol || 'UNKNOWN').toString().trim().toUpperCase(),
            name: (mappedRow.name || mappedRow.symbol || 'Unknown').toString().trim(),
            price_usd: convertToNumber(mappedRow.price_usd),
            volume_24h: convertToNumber(mappedRow.volume_24h),
            market_cap: convertToNumber(mappedRow.market_cap),
            percent_change_24h: convertToNumber(mappedRow.percent_change_24h),
            timestamp: convertToDate(mappedRow.timestamp), // Safely handle invalid/missing dates
            source: 'csv',
            raw_data: data
          };

          results.push(normalizedData);

        } catch (err) {
          console.error('⚠️ Failed to process CSV row:', data, err.message);
        }
      })
      .on('end', () => {
        console.log(`📄 Loaded ${results.length} records from CSV`);
        resolve(results);
      })
      .on('error', (error) => {
        console.error('❌ Error parsing CSV file:', error);
        reject(error);
      });
  });
};

module.exports = fetchCSVData;
