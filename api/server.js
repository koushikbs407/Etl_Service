const express = require('express');
const crypto = require('crypto');
const promClient = require('prom-client');
const swaggerUi = require('swagger-ui-express');
const swaggerSpec = require('./swagger');
require('dotenv').config();

const { connectMongoDB, getDb } = require('../DB/mongo');
const { runETLPipeline } = require('../etl/orchestration');

const app = express();
app.use(express.json());

// Swagger UI setup
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));

// Prometheus metrics registry and instruments
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const httpRequestDurationMs = new promClient.Histogram({
  name: 'api_http_request_duration_ms',
  help: 'API HTTP request latency in ms',
  buckets: [50, 100, 200, 500, 1000, 2000, 5000],
  labelNames: ['route', 'method', 'status']
});
register.registerMetric(httpRequestDurationMs);

const httpRequestErrors = new promClient.Counter({
  name: 'api_http_request_errors_total',
  help: 'Total number of API request errors',
  labelNames: ['route', 'method', 'status']
});
register.registerMetric(httpRequestErrors);

// Middleware: request_id and timing
app.use((req, res, next) => {
  const start = Date.now();
  const requestId = crypto.randomUUID();
  res.locals.requestId = requestId;

  res.on('finish', () => {
    const duration = Date.now() - start;
    const labels = { route: req.path, method: req.method, status: String(res.statusCode) };
    httpRequestDurationMs.labels(labels.route, labels.method, labels.status).observe(duration);
    if (res.statusCode >= 400) {
      httpRequestErrors.labels(labels.route, labels.method, labels.status).inc();
    }
  });

  res.setHeader('X-Request-Id', requestId);
  next();
});

// Helper: short run health summary
async function getLastRunSummary() {
  const db = getDb();
  if (!db) return { run_id: null, status: 'unknown', errors: 0, rowsProcessed: 0, latencyMs: 0 };
  const lastRun = await db.collection('etlruns').find({}).sort({ end_time: -1 }).limit(1).toArray();
  if (!lastRun.length) return { run_id: null, status: 'unknown', errors: 0, rowsProcessed: 0, latencyMs: 0 };
  const r = lastRun[0];
  return {
    run_id: r.run_id,
    status: r.status,
    errors: r.errors || 0,
    rowsProcessed: r.rows_processed || 0,
    latencyMs: r.total_latency_ms || 0
  };
}

/**
 * @swagger
 * /data:
 *   get:
 *     summary: Get latest normalized crypto data
 *     description: Retrieve normalized crypto data with filtering, pagination, and sorting options
 *     parameters:
 *       - in: query
 *         name: symbol
 *         schema:
 *           type: string
 *         description: Cryptocurrency symbol (e.g., BTC)
 *       - in: query
 *         name: source
 *         schema:
 *           type: string
 *         description: Data source name
 *       - in: query
 *         name: start
 *         schema:
 *           type: string
 *           format: date-time
 *         description: Start date for filtering
 *       - in: query
 *         name: end
 *         schema:
 *           type: string
 *           format: date-time
 *         description: End date for filtering
 *       - in: query
 *         name: sort_by
 *         schema:
 *           type: string
 *           default: timestamp
 *         description: Field to sort by
 *       - in: query
 *         name: sort_dir
 *         schema:
 *           type: string
 *           enum: [asc, desc]
 *           default: desc
 *         description: Sort direction
 *       - in: query
 *         name: page
 *         schema:
 *           type: integer
 *           default: 1
 *         description: Page number
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 50
 *           maximum: 500
 *         description: Items per page
 *     responses:
 *       200:
 *         description: Successful response with data and pagination
 *       503:
 *         description: Database connection error
 *       500:
 *         description: Server error
 */
app.get('/data', async (req, res) => {
  const t0 = Date.now();
  try {
    const db = getDb();
    if (!db) return res.status(503).json({ error: 'DB not connected' });

    const {
      symbol,
      source,
      start,
      end,
      sort_by = 'timestamp',
      sort_dir = 'desc',
      page = '1',
      limit = '50'
    } = req.query;

    const filter = {};
    if (symbol) filter.symbol = symbol.toString().toUpperCase();
    if (source) filter.source = source.toString();
    if (start || end) {
      filter.timestamp = {};
      if (start) filter.timestamp.$gte = new Date(start);
      if (end) filter.timestamp.$lte = new Date(end);
    }

    const sort = { [sort_by]: sort_dir.toLowerCase() === 'asc' ? 1 : -1 };
    const pageNum = Math.max(1, parseInt(page, 10) || 1);
    const limitNum = Math.min(500, Math.max(1, parseInt(limit, 10) || 50));
    const skip = (pageNum - 1) * limitNum;

    const [items, total, health] = await Promise.all([
      db.collection('normalized_crypto_data').find(filter).sort(sort).skip(skip).limit(limitNum).toArray(),
      db.collection('normalized_crypto_data').countDocuments(filter),
      getLastRunSummary()
    ]);

    const apiLatency = Date.now() - t0;
    return res.json({
      request_id: res.locals.requestId,
      run_id: health.run_id,
      api_latency_ms: apiLatency,
      health: { status: health.status, errors: health.errors },
      data: items,
      pagination: { page: pageNum, limit: limitNum, total }
    });
  } catch (err) {
    return res.status(500).json({ error: err.message, request_id: res.locals.requestId });
  }
});

/**
 * @swagger
 * /stats:
 *   get:
 *     summary: Get ETL statistics
 *     description: Retrieve counts, latency averages, last run ID, and error rates
 *     responses:
 *       200:
 *         description: Statistics retrieved successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 request_id:
 *                   type: string
 *                 run_id:
 *                   type: string
 *                 api_latency_ms:
 *                   type: number
 *                 health:
 *                   type: object
 *                   properties:
 *                     status:
 *                       type: string
 *                     errors:
 *                       type: number
 *                 counts:
 *                   type: object
 *                   properties:
 *                     raw:
 *                       type: number
 *                     normalized:
 *                       type: number
 *                 latency_avg_ms:
 *                   type: number
 *                 error_rate:
 *                   type: number
 *       503:
 *         description: Database connection error
 *       500:
 *         description: Server error
 */
app.get('/stats', async (req, res) => {
  const t0 = Date.now();
  try {
    const db = getDb();
    if (!db) return res.status(503).json({ error: 'DB not connected' });

    const [rawCount, normCount, runs] = await Promise.all([
      db.collection('raw_crypto_data').countDocuments({}),
      db.collection('normalized_crypto_data').countDocuments({}),
      db.collection('etlruns').find({}).sort({ end_time: -1 }).limit(20).toArray()
    ]);

    const latencyAvg = runs.length ? Math.round(runs.reduce((a, r) => a + (r.total_latency_ms || 0), 0) / runs.length) : 0;
    const errorRate = runs.length ? Number((runs.reduce((a, r) => a + (r.errors || 0), 0) / runs.reduce((a, r) => a + (r.rows_processed || 0), 0 || 1)).toFixed(4)) : 0;
    const lastRun = runs[0];

    const apiLatency = Date.now() - t0;
    return res.json({
      request_id: res.locals.requestId,
      run_id: lastRun ? lastRun.run_id : null,
      api_latency_ms: apiLatency,
      health: { status: lastRun ? lastRun.status : 'unknown', errors: lastRun ? lastRun.errors : 0 },
      counts: { raw: rawCount, normalized: normCount },
      latency_avg_ms: latencyAvg,
      error_rate: errorRate
    });
  } catch (err) {
    return res.status(500).json({ error: err.message, request_id: res.locals.requestId });
  }
});

// Simple JWT validation (expects HS256 token matching REFRESH_JWT_SECRET)
function verifyJwt(token) {
  try {
    const [headerB64, payloadB64, sig] = token.split('.');
    if (!headerB64 || !payloadB64 || !sig) return null;
    const secret = process.env.REFRESH_JWT_SECRET || '';
    const data = `${headerB64}.${payloadB64}`;
    const expected = crypto.createHmac('sha256', secret).update(data).digest('base64url');
    if (expected !== sig) return null;
    const payloadJson = Buffer.from(payloadB64, 'base64url').toString('utf8');
    const payload = JSON.parse(payloadJson);
    if (payload.exp && Date.now() / 1000 > payload.exp) return null;
    return payload;
  } catch (e) {
    return null;
  }
}

/**
 * @swagger
 * /refresh:
 *   post:
 *     summary: Trigger manual ETL refresh
 *     description: Manually trigger an ETL pipeline run. Requires JWT authentication.
 *     security:
 *       - bearerAuth: []
 *     responses:
 *       202:
 *         description: ETL refresh triggered successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 request_id:
 *                   type: string
 *                 run_id:
 *                   type: string
 *                 api_latency_ms:
 *                   type: number
 *                 health:
 *                   type: object
 *                   properties:
 *                     status:
 *                       type: string
 *                     errors:
 *                       type: number
 *                 message:
 *                   type: string
 *       401:
 *         description: Unauthorized - Invalid or missing JWT token
 *       500:
 *         description: Server error
 */
app.post('/refresh', async (req, res) => {
  const t0 = Date.now();
  try {
    const auth = req.headers.authorization || '';
    const token = auth.startsWith('Bearer ') ? auth.substring(7) : '';
    const payload = verifyJwt(token);
    if (!payload) return res.status(401).json({ error: 'Unauthorized', request_id: res.locals.requestId });

    // Kick off ETL asynchronously
    setImmediate(async () => {
      try {
        await runETLPipeline();
      } catch (e) {
        // errors are already logged by the pipeline
      }
    });

    const health = await getLastRunSummary();
    const apiLatency = Date.now() - t0;
    return res.status(202).json({
      request_id: res.locals.requestId,
      run_id: health.run_id,
      api_latency_ms: apiLatency,
      health: { status: health.status, errors: health.errors },
      message: 'ETL refresh triggered'
    });
  } catch (err) {
    return res.status(500).json({ error: err.message, request_id: res.locals.requestId });
  }
});

/**
 * @swagger
 * /metrics:
 *   get:
 *     summary: Get Prometheus metrics
 *     description: Retrieve Prometheus-formatted metrics for monitoring
 *     responses:
 *       200:
 *         description: Metrics retrieved successfully
 *         content:
 *           text/plain:
 *             schema:
 *               type: string
 *       500:
 *         description: Server error
 */
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  } catch (err) {
    res.status(500).end(err.message);
  }
});

/**
 * @swagger
 * /health:
 *   get:
 *     summary: Get system health status
 *     description: Check the health status of API, database, and scheduler components
 *     responses:
 *       200:
 *         description: System health check successful
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 request_id:
 *                   type: string
 *                 run_id:
 *                   type: string
 *                 api_latency_ms:
 *                   type: number
 *                 health:
 *                   type: object
 *                   properties:
 *                     status:
 *                       type: string
 *                     errors:
 *                       type: number
 *                 components:
 *                   type: object
 *                   properties:
 *                     api:
 *                       type: string
 *                     db_connected:
 *                       type: boolean
 *                     db_ping:
 *                       type: boolean
 *                     scheduler:
 *                       type: string
 *       500:
 *         description: Server error
 */
app.get('/health', async (req, res) => {
  const t0 = Date.now();
  try {
    const db = getDb();
    const dbOk = !!db;
    let pingOk = false;
    if (db) {
      try {
        await db.command({ ping: 1 });
        pingOk = true;
      } catch (e) {
        pingOk = false;
      }
    }

    // We do not have a direct scheduler handle here; report unknown
    const schedulerStatus = 'unknown';
    const health = await getLastRunSummary();
    const apiLatency = Date.now() - t0;

    return res.json({
      request_id: res.locals.requestId,
      run_id: health.run_id,
      api_latency_ms: apiLatency,
      health: { status: health.status, errors: health.errors },
      components: {
        api: 'ok',
        db_connected: dbOk,
        db_ping: pingOk,
        scheduler: schedulerStatus
      }
    });
  } catch (err) {
    return res.status(500).json({ error: err.message, request_id: res.locals.requestId });
  }
});

async function start() {
  await connectMongoDB();
  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(` API server listening on port ${port}`);
  });
}

if (require.main === module) {
  start().catch(err => {
    console.error('Failed to start API:', err);
    process.exit(1);
  });
}

module.exports = { app, start };


