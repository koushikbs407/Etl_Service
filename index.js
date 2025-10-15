
const { runETLPipeline } = require('./etl/orchestration');


runETLPipeline()
  .then(() => console.log('🎉 ETL Completed'))
  .catch(err => console.error('💥 ETL Failed:', err));
  

