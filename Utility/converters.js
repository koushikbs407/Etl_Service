const moment = require('moment');

// Convert various numeric string formats to number. Returns 0 for falsy or unparseable values.
const convertToNumber = (value) => {
	if (value === undefined || value === null || value === '') return 0;
	if (typeof value === 'number') return value;
	if (typeof value === 'string') {
		// remove commas, dollar signs, spaces
		const cleaned = value.replace(/[,$\s]/g, '');
		const parsed = parseFloat(cleaned);
		return isNaN(parsed) ? 0 : parsed;
	}
	return 0;
};

// Convert common timestamp inputs to Date. If invalid, return current Date.
const convertToDate = (value) => {
	if (value instanceof Date) return value;
	if (typeof value === 'string' || typeof value === 'number') {
		const parsed = moment(value);
		return parsed.isValid() ? parsed.toDate() : new Date();
	}
	return new Date();
};

module.exports = { convertToNumber, convertToDate };
