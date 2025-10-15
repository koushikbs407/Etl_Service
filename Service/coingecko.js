const axios = require('axios');
const { convertToNumber, convertToDate } = require('../Utility/converters');
const config = require('../Config/config');
const {rateLimitedRequest} = require("../Utility/rateLimiter");

const fetchCoinGeckoData = async () => {
  const url = `${config.apis.coingecko}?vs_currency=usd&order=market_cap_desc&per_page=20&page=1`;
  const data = await rateLimitedRequest('coingecko', url);

  return data.map(coin => ({
    symbol: coin.symbol?.toUpperCase() || 'UNKNOWN',
    name: coin.name || 'Unknown',
    price_usd: convertToNumber(coin.current_price),
    volume_24h: convertToNumber(coin.total_volume),
    market_cap: convertToNumber(coin.market_cap),
    percent_change_24h: convertToNumber(coin.price_change_percentage_24h),
    timestamp: convertToDate(coin.last_updated),
    source: 'coingecko',
    raw_data: coin
  }));
};

module.exports = fetchCoinGeckoData;
