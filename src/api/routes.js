// src/api/routes.js
const express = require('express');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const { pool } = require('../shared/db');
const {
    amqp,
    RABBITMQ_URL,
    REPORT_QUEUE,
    setupQueues,
    publishWithConfirm,
} = require('../shared/rabbitmq');

const router = express.Router();
const OUTPUT_DIR = process.env.OUTPUT_DIR || './output';

let rabbitChannel = null;

/** Conexión perezosa a RabbitMQ (con confirm channel). */
async function getChannel() {
    if (rabbitChannel) return rabbitChannel;
    const conn = await amqp.connect(RABBITMQ_URL);
    const ch = await conn.createConfirmChannel(); // habilita publisher confirms
    await setupQueues(ch);
    rabbitChannel = ch;
    conn.on('close', () => {
        rabbitChannel = null;
    });
    return rabbitChannel;
}

/** Valida el payload de creación de reporte. Devuelve string de error o null. */
function validateReportInput(body) {
    const { report_type, start_date, end_date } = body || {};
    if (report_type !== 'sales') {
        return 'Unsupported report_type. Only "sales" is supported.';
    }
    if (!start_date || !end_date) {
        return 'start_date and end_date are required (YYYY-MM-DD).';
    }
    const start = new Date(start_date + 'T00:00:00Z');
    const end = new Date(end_date + 'T00:00:00Z');
    if (isNaN(start.getTime()) || isNaN(end.getTime())) {
        return 'Invalid date format. Use YYYY-MM-DD.';
    }
    if (start > end) {
        return 'start_date must be before or equal to end_date.';
    }
    return null;
}

// POST /api/reports
router.post('/reports', async (req, res) => {
    const validationError = validateReportInput(req.body);
    if (validationError) {
        return res.status(400).json({ error: validationError });
    }

    const { report_type, start_date, end_date } = req.body;
    const idempotencyKey = req.header('Idempotency-Key') || null;

    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // 1) Idempotencia HTTP: misma clave + mismos parámetros => mismo reporte
        if (idempotencyKey) {
            const existing = await client.query(
                `SELECT * FROM reports WHERE idempotency_key = $1`,
                [idempotencyKey]
            );
            if (existing.rows.length > 0) {
                const rep = existing.rows[0];
                const sameParams =
                    rep.report_type === report_type &&
                    rep.start_date.toISOString().slice(0, 10) === start_date &&
                    rep.end_date.toISOString().slice(0, 10) === end_date;

                if (!sameParams) {
                    await client.query('ROLLBACK');
                    return res.status(409).json({
                        error: 'Idempotency-Key reused with different parameters.',
                    });
                }

                await client.query('COMMIT');
                return res.status(200).json(toStatusPayload(rep));
            }
        }

        // 2) Crear reporte en estado "pending"
        const reportId = uuidv4();
        await client.query(
            `INSERT INTO reports (id, report_type, start_date, end_date, status, idempotency_key)
       VALUES ($1, $2, $3, $4, 'pending', $5)`,
            [reportId, report_type, start_date, end_date, idempotencyKey]
        );

        // 3) Registrar en pending_publications para reconciliación si falla la publicación
        await client.query(
            `INSERT INTO pending_publications (report_id) VALUES ($1)`,
            [reportId]
        );

        await client.query('COMMIT');

        // 4) Publicar en RabbitMQ con publisher confirms
        let published = false;
        try {
            const channel = await getChannel();
            published = await publishWithConfirm(channel, REPORT_QUEUE, {
                reportId,
                reportType: report_type,
                startDate: start_date,
                endDate: end_date,
            });
        } catch (err) {
            console.error('[API] Broker unavailable during publish:', err.message);
        }

        // 5) Si se publicó, actualizar a "queued" y limpiar pending_publications
        if (published) {
            await pool.query(
                `UPDATE reports SET status = 'queued' WHERE id = $1`,
                [reportId]
            );
            await pool.query(`DELETE FROM pending_publications WHERE report_id = $1`, [
                reportId,
            ]);
        } else {
            // El reporte queda "pending" y pending_publications conserva el registro.
            // El comando `npm run reconcile` reintentará la publicación.
            console.warn(
                `[API] Report ${reportId} left as pending; run "npm run reconcile".`
            );
        }

        const { rows } = await pool.query(`SELECT * FROM reports WHERE id = $1`, [
            reportId,
        ]);
        return res.status(202).json(toStatusPayload(rows[0]));
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('[API] POST /reports error:', err);
        return res.status(500).json({ error: 'Internal server error.' });
    } finally {
        client.release();
    }
});

// GET /api/reports/:id
router.get('/reports/:id', async (req, res) => {
    const { rows } = await pool.query(`SELECT * FROM reports WHERE id = $1`, [
        req.params.id,
    ]);
    if (rows.length === 0) {
        return res.status(404).json({ error: 'Report not found.' });
    }
    return res.json(toStatusPayload(rows[0]));
});

// GET /api/reports/:id/download
router.get('/reports/:id/download', async (req, res) => {
    const { rows } = await pool.query(`SELECT * FROM reports WHERE id = $1`, [
        req.params.id,
    ]);
    if (rows.length === 0) {
        return res.status(404).json({ error: 'Report not found.' });
    }

    const rep = rows[0];
    if (rep.status !== 'completed' || !rep.file_path) {
        return res.status(409).json({ error: 'Report is not ready for download.' });
    }

    // Seguridad: nunca aceptar rutas del cliente; usar la ruta persistida
    const absolute = path.resolve(rep.file_path);
    const outputRoot = path.resolve(OUTPUT_DIR);
    if (!absolute.startsWith(outputRoot + path.sep)) {
        return res.status(500).json({ error: 'Invalid stored file path.' });
    }
    if (!fs.existsSync(absolute)) {
        return res.status(404).json({ error: 'File not found on disk.' });
    }

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader(
        'Content-Disposition',
        `attachment; filename="report-${rep.id}.csv"`
    );
    return res.sendFile(absolute);
});

/** Convierte una fila de reports en el payload público de estado. */
function toStatusPayload(rep) {
    const ready = rep.status === 'completed';
    return {
        report_id: rep.id,
        status: rep.status,
        created_at: rep.created_at,
        updated_at: rep.updated_at,
        attempts: rep.attempts,
        exported_rows: rep.exported_rows,
        download_url: ready ? `/api/reports/${rep.id}/download` : null,
        error: rep.error_message || null,
    };
}

module.exports = router;