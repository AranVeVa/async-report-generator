// src/shared/csv.js
const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = process.env.OUTPUT_DIR || './output';

/**
 * Escapa un valor CSV: si contiene coma, comilla o salto de línea, lo rodea
 * de comillas y duplica las comillas internas (RFC 4180).
 */
function escapeCsvValue(value) {
  const str = String(value ?? '');
  if (/[",\n\r]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function toCsvRow(values) {
  return values.map(escapeCsvValue).join(',');
}

/**
 * Genera el archivo CSV de forma atómica:
 * escribe primero a un archivo temporal y solo lo renombra al final.
 * Así nunca se publica un archivo incompleto.
 */
async function writeCsvAtomically(reportId, rows) {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const finalPath = path.join(OUTPUT_DIR, `${reportId}.csv`);
  const tempPath = path.join(OUTPUT_DIR, `.${reportId}.tmp`);

  const header = toCsvRow([
    'sale_id',
    'sale_date',
    'customer_name',
    'product_name',
    'quantity',
    'unit_price',
    'total_amount',
  ]);

  const lines = [header];
  for (const r of rows) {
    lines.push(
      toCsvRow([
        r.sale_id,
        r.sale_date,
        r.customer_name,
        r.product_name,
        r.quantity,
        r.unit_price,
        r.total_amount,
      ])
    );
  }

  fs.writeFileSync(tempPath, lines.join('\n') + '\n', 'utf8');
  fs.renameSync(tempPath, finalPath); // operación atómica

  return { filePath: finalPath, rowCount: rows.length };
}

module.exports = { writeCsvAtomically, toCsvRow, escapeCsvValue, OUTPUT_DIR };