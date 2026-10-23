// src/shared/rabbitmq.js
const amqp = require('amqplib');
require('dotenv').config();

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const REPORT_QUEUE = process.env.REPORT_QUEUE || 'report_processing';
const REPORT_DLQ = process.env.REPORT_DLQ || 'report_dead_letter';
const REPORT_RETRY_QUEUE = process.env.REPORT_RETRY_QUEUE || 'report_retry';
const RETRY_DELAY_MS = parseInt(process.env.RETRY_DELAY_MS || '5000', 10);

/**
 * Declara colas durables y el DLX/DLQ.
 * Si la cola principal falla, el mensaje se enruta al DLQ.
 */
async function setupQueues(channel) {
  // Dead Letter Exchange para mensajes que agotan reintentos
  await channel.assertExchange('report.dlx', 'direct', { durable: true });

  // Cola de inspección (DLQ) para fallos permanentes
  await channel.assertQueue(REPORT_DLQ, { durable: true });
  await channel.bindQueue(REPORT_DLQ, 'report.dlx', 'failed');

  // Cola de reintentos con TTL (mensajes esperan aquí antes de volver a la principal)
  await channel.assertQueue(REPORT_RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_DELAY_MS,
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': REPORT_QUEUE, // vuelve a la cola principal
    },
  });

  // Cola principal de procesamiento (durable, persistent messages)
  await channel.assertQueue(REPORT_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': 'report.dlx',
      'x-dead-letter-routing-key': 'failed',
    },
  });
}

/**
 * Publica un mensaje persistente con publisher confirms.
 * Devuelve true si el broker confirmó; false en caso contrario.
 */
async function publishWithConfirm(channel, queue, payload) {
  return new Promise((resolve) => {
    const msg = Buffer.from(JSON.stringify(payload));
    channel.publish('', queue, msg, { persistent: true }, (err) => {
      if (err) {
        console.error('[RabbitMQ] Publish NOT confirmed', err.message);
        resolve(false);
      } else {
        resolve(true);
      }
    });
  });
}

module.exports = {
  amqp,
  RABBITMQ_URL,
  REPORT_QUEUE,
  REPORT_DLQ,
  REPORT_RETRY_QUEUE,
  setupQueues,
  publishWithConfirm,
};