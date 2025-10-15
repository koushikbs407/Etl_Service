// Scheduler/etlScheduler.js
const cron = require('node-cron');
const { runETLPipeline } = require('../etl/orchestration'); // ETL pipeline
const crypto = require('crypto');
const nodemailer = require('nodemailer'); // ✅ Nodemailer

let isRunning = false;
let failureCount = 0;

// 🔔 Email Alert Setup
const transporter = nodemailer.createTransport({
  service: 'gmail', // or any SMTP service
  auth: {
    user: process.env.ALERT_EMAIL_USER, 
    pass: process.env.ALERT_EMAIL_PASS, 
  },
});

// Send alert via email
async function sendAlert(message) {
  try {
    await transporter.sendMail({
      from: process.env.ALERT_EMAIL_USER,
      to: process.env.ALERT_EMAIL_RECIPIENT || process.env.ALERT_EMAIL_USER,
      subject: '⚠️ ETL Pipeline Failure Alert',
      text: message,
    });
    console.log('📨 Alert email sent successfully.');
  } catch (err) {
    console.error('❌ Failed to send alert email:', err.message);
  }
}

// 🧠 Executes ETL safely & logs run details with checkpoint awareness
async function executeETLJob() {
  if (isRunning) {
    console.log('⚠️ Previous ETL still running — skipping this run.');
    return;
  }

  const startTime = new Date();
  isRunning = true;
  console.log(`🕒 Starting scheduled ETL job at ${startTime.toISOString()}...`);

  let rowsProcessed = 0;
  let errors = 0;
  let skippedFields = [];

  try {
    // Run ETL pipeline (with checkpoint resume inside)
    const etlResult = await runETLPipeline();

    if (etlResult) {
      rowsProcessed = etlResult.validRecords || 0;
      errors = etlResult.invalidRecords || 0;
      skippedFields = etlResult.skippedFields || [];
    }

    const endTime = new Date();
    const totalLatencyMs = endTime - startTime;

    // Orchestration already logs ETL run details (via `logETLRun`).
    // We avoid writing again with Mongoose here because the ETL runner
    // closes the DB/mongoose connection before returning.
    failureCount = 0;
    console.log(`✅ ETL job completed successfully at ${endTime.toISOString()}.`);

  } catch (error) {
    const endTime = new Date();
    const totalLatencyMs = endTime - startTime;

    // Orchestration already logs failed ETL runs; here we only track failures
    // for alerting and local diagnostics.
    failureCount += 1;
    console.error(`❌ ETL job failed (${failureCount} consecutive failures):`, error.message);

    if (failureCount >= 3) {
      await sendAlert(
        `⚠️ ETL Pipeline failed 3 times consecutively!\nTime: ${new Date().toISOString()}\nCheck logs immediately.`
      );
      failureCount = 0;
    }
  } finally {
    isRunning = false;
  }
}

// 🕐 Schedule ETL every 3 minutes (for testing)
cron.schedule('*/3 * * * *', async () => {
  await executeETLJob();
});

console.log('⏰ ETL Scheduler initialized — will run every 3 minutes.');

// 🔹 Optional: Run immediately on startup
executeETLJob();
