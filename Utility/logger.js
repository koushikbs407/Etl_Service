// logger.js
const { createLogger, format, transports } = require('winston');

const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp(),
    format.json()  // Structured JSON log format
  ),
  transports: [
    new transports.Console(),
    new transports.File({ filename: 'etl.log' })
  ]
});

module.exports = logger;
