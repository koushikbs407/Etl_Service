const Joi = require('joi');

const unifiedSchema = Joi.object({
  symbol: Joi.string().required(),
  name: Joi.string().required(),
  price_usd: Joi.number().positive().required(),
  volume_24h: Joi.number().min(0).required(),
  market_cap: Joi.number().min(0).allow(null),
  percent_change_24h: Joi.number().allow(null),
  timestamp: Joi.date().iso().required(),
  source: Joi.string().valid('coinpaprika', 'coingecko', 'csv').required(),
  raw_data: Joi.object().required()
});

module.exports = unifiedSchema;
