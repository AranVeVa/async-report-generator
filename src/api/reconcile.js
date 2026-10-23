// src/api/reconcile.js
require('dotenv').config();
const { pool } = require('../shared/db');
const {
    amqp,
    RABBITMQ_URL,
    REPORT_QUEUE,
    setupQueues,
    publishWithConfirm,
} = require('../shared/rabbitmq');

/**
 * Reconciliación: reintenta publicar reportes que quedaron en "pending"
 * porque RabbitMQ no estaba disponible durante el POST.
 * Uso: npm run reconcile
 */
async function reconcile() {
    const { rows } = await pool.query(`
    SELECT r.* FROM reports r
    JOIN pending_publications pp ON pp.report_id = r.id
    WHERE r.status = 'pending'
  `);

    if (rows.length === 0) {
        console.log('[reconcile] No hay reportes pendientes de publicación.');
        return;
    }

    const conn = await amqp.connect(RABBITMQ_URL);
    const ch = await conn.createConfirmChannel();
    await setupQueues(ch);

    for (const rep of rows) {
        const ok = await publishWithConfirm(ch, REPORT_QUEUE, {
            reportId: rep.id,
            reportType: rep.report_type,
            startDate: rep.start_date.toISOString().slice(0, 10),
            endDate: rep.end_date.toISOString().slice(0, 10),
        });

        if (ok) {
            await pool.query(`UPDATE reports SET status = 'queued' WHERE id = $1`, [
                rep.id,
            ]);
            await pool.query(`DELETE FROM pending_publications WHERE report_id = $1`, [
                rep.id,
            ]);
            console.log(`[reconcile] Reporte ${rep.id} publicado y marcado como queued.`);
        } else {
            console.warn(`[reconcile] Falló la publicación de ${rep.id}.`);
        }
    }

    await ch.close();
    await conn.close();
    await pool.end();
}

reconcile().catch((err) => {
    console.error('[reconcile] Error:', err);
    process.exit(1);
});