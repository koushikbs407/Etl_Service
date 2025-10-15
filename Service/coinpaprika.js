const axios = require('axios');
const { convertToNumber, convertToDate } = require('../Utility/converters');
const config = require('../Config/config');
const {rateLimitedRequest} = require("../Utility/rateLimiter");

const fetchCoinPaprikaData = async () => {
  const data = await rateLimitedRequest('coinpaprika', config.apis.coinpaprika);

  return data.slice(0, 10).map(coin => ({
    symbol: coin.symbol?.toUpperCase() || 'UNKNOWN',
    name: coin.name || 'Unknown',
    price_usd: convertToNumber(coin.quotes?.USD?.price),
    volume_24h: convertToNumber(coin.quotes?.USD?.volume_24h),
    market_cap: convertToNumber(coin.quotes?.USD?.market_cap),
    percent_change_24h: convertToNumber(coin.quotes?.USD?.percent_change_24h),
    timestamp: convertToDate(coin.last_updated),
    source: 'coinpaprika',
    raw_data: coin
  }));
};

module.exports = fetchCoinPaprikaData;
