const request = require('supertest');
const { app } = require('./server');

describe('API Endpoints', () => {
  test('GET /health should return 200', async () => {
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('request_id');
    expect(response.body).toHaveProperty('api_latency_ms');
  });

  test('GET /metrics should return Prometheus format', async () => {
    const response = await request(app).get('/metrics');
    expect(response.status).toBe(200);
    expect(response.text).toContain('etl_rows_processed_total');
  });

  test('GET /stats should return statistics', async () => {
    const response = await request(app).get('/stats');
    expect(response.status).toBe(200);
    expect(response.body).toHaveProperty('request_id');
    expect(response.body).toHaveProperty('counts');
  });

  test('POST /refresh should trigger ETL', async () => {
    const response = await request(app).post('/refresh');
    expect(response.status).toBe(202);
    expect(response.body).toHaveProperty('message', 'ETL refresh triggered');
  });
});