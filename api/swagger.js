const swaggerJsdoc = require('swagger-jsdoc');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'ETL Services API',
      version: '1.0.0',
      description: 'API documentation for ETL Services including data retrieval, stats, and ETL operations',
      contact: {
        name: 'API Support',
        url: 'https://github.com/yourusername/etl-services'
      },
    },
    servers: [
      {
        url: 'http://localhost:3000',
        description: 'Development server',
      },
    ],
    components: {
      securitySchemes: {
        bearerAuth: {
          type: 'http',
          scheme: 'bearer',
          bearerFormat: 'JWT'
        }
      }
    }
  },
  apis: ['./api/server.js'], // Path to the API docs
};

module.exports = swaggerJsdoc(options);