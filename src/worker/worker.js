// src/worker/worker.js
require('dotenv').config();
const { v4: uuidv4 } = require('uuid');
const { pool } = require('../shared/db');
const { writeCsvAtomically, OUTPUT_DIR } = require('../shared/csv');
const {
  amqp,
  RABBITMQ_URL,
  REPORT_QUEUE,
  REPORT_RETRY_QUEUE,
  setupQueues,
} = require('../shared/rabbitmq');

const WORKER_ID = uuidv4().slice(0, 8);
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '3', 10);
const PROCESSING_DELAY_MS = parseInt(process.env.PROCESSING_DELAY_MS || '8000', 10);

const log = (reportId, attempt, action, extra = {}) => {
  console.log(
    JSON.stringify({
      ts: new Date().toISOString(),
      worker: WORKER_ID,
      reportId,
      attempt,
      action,
      ...extra,
    })
  );
};

async function fetchSales(startDate, endDate) {
  const { rows } = await pool.query(
    `SELECT sale_id, sale_date, customer_name, product_name,
            quantity, unit_price, total_amount
     FROM sales
     WHERE sale_date >= $1 AND sale_date <= $2
     ORDER BY sale_date ASC, sale_id ASC`,
    [startDate, endDate]
  );
  return rows;
}

function simulateDelay() {
  const ms = PROCESSING_DELAY_MS + Math.floor(Math.random() * 2000);
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processReport(channel, msg) {
  const content = JSON.parse(msg.content.toString());
  const { reportId, startDate, endDate } = content;

  const { rows: currentRows } = await pool.query(
    `SELECT status, attempts FROM reports WHERE id = $1`,
    [reportId]
  );
  if (currentRows.length === 0) {
    log(reportId, null, 'skip_missing_report');
    channel.ack(msg);
    return;
  }

  const current = currentRows[0];

  if (current.status === 'completed' || current.status === 'failed') {
    log(reportId, current.attempts, 'skip_duplicate', { status: current.status });
    channel.ack(msg);
    return;
  }

  const claim = await pool.query(
    `UPDATE reports
     SET status = 'processing', attempts = attempts + 1, updated_at = NOW()
     WHERE id = $1 AND status IN ('queued', 'retrying')
     RETURNING attempts`,
    [reportId]
  );

  if (claim.rows.length === 0) {
    log(reportId, null, 'skip_cannot_claim');
    channel.ack(msg);
    return;
  }

  const attempt = claim.rows[0].attempts;
  log(reportId, attempt, 'processing_started');

  try {
    if (process.env.FAIL_TRANSIENT_ONCE === '1' && attempt === 1) {
      throw new Error('ECONNREFUSED simulated transient failure');
    }
    if (process.env.FAIL_PERMANENT === '1') {
      throw new Error('Simulated permanent failure');
    }

    await simulateDelay();
    const sales = await fetchSales(startDate, endDate);
    const { filePath, rowCount } = await writeCsvAtomically(reportId, sales);

    await pool.query(
      `UPDATE reports
       SET status = 'completed', exported_rows = $2, file_path = $3,
           error_message = NULL, updated_at = NOW()
       WHERE id = $1 AND status = 'processing'`,
      [reportId, rowCount, filePath]
    );

    log(reportId, attempt, 'completed', { rows: rowCount, file: filePath });
    channel.ack(msg);
  } catch (err) {
    log(reportId, attempt, 'processing_error', {
      error: err.message,
      category: classifyError(err),
    });

    if (attempt < MAX_ATTEMPTS) {
      await pool.query(
        `UPDATE reports SET status = 'retrying', error_message = $2, updated_at = NOW()
         WHERE id = $1`,
        [reportId, `Transient error: ${err.message}`]
      );
      channel.sendToQueue(
        REPORT_RETRY_QUEUE,
        Buffer.from(JSON.stringify(content)),
        { persistent: true }
      );
      channel.ack(msg);
    } else {
      await pool.query(
        `UPDATE reports SET status = 'failed', error_message = $2, updated_at = NOW()
         WHERE id = $1`,
        [reportId, `Permanent failure after ${attempt} attempts: ${err.message}`]
      );
      channel.nack(msg, false, false);
      log(reportId, attempt, 'failed_permanent');
    }
  }
}

function classifyError(err) {
  const msg = (err.message || '').toLowerCase();
  if (msg.includes('econnrefused') || msg.includes('timeout') || msg.includes('connection')) {
    return 'transient';
  }
  if (msg.includes('syntax') || msg.includes('constraint')) {
    return 'permanent';
  }
  return 'unknown';
}

async function start() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const channel = await conn.createConfirmChannel();
  await setupQueues(channel);
  await channel.prefetch(1);

  console.log(`[worker ${WORKER_ID}] Esperando mensajes en "${REPORT_QUEUE}"...`);

  channel.consume(
    REPORT_QUEUE,
    async (msg) => {
      if (!msg) return;
      try {
        await processReport(channel, msg);
      } catch (err) {
        console.error(`[worker ${WORKER_ID}] Error inesperado:`, err);
        channel.nack(msg, false, false);
      }
    },
    { noAck: false }
  );

  conn.on('close', () => {
    console.error(`[worker ${WORKER_ID}] Conexión RabbitMQ cerrada. Reintentando en 5s...`);
    setTimeout(start, 5000);
  });
}

start().catch((err) => {
  console.error('[worker] No se pudo iniciar:', err);
  process.exit(1);
});