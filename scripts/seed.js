// scripts/seed.js
require('dotenv').config();
const { pool } = require('../src/shared/db');

const CUSTOMERS = ['Acme Corp', 'Globex', 'Initech', 'Umbrella', 'Wayne Enterprises', 'Stark Industries', 'Wonka Industries', 'Hooli', 'Pied Piper', 'Soylent Corp'];
const PRODUCTS = ['Widget A', 'Widget B', 'Gadget X', 'Gadget Y', 'Service Pro', 'License Plus', 'Add-on Pack', 'Premium Support', 'Starter Kit', 'Enterprise Suite'];

function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function randomFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

async function seed() {
  const client = await pool.connect();
  try {
    const { rows } = await client.query('SELECT COUNT(*) AS c FROM sales');
    if (parseInt(rows[0].c, 10) >= 10000) {
      console.log('[seed] La tabla sales ya contiene >=10,000 registros. Omitiendo.');
      return;
    }

    await client.query('BEGIN');
    console.log('[seed] Generando 10,000 ventas sintéticas...');

    const start = new Date('2024-01-01');
    const end = new Date('2025-12-31');

    for (let i = 0; i < 10000; i++) {
      const saleDate = randomDate(start, end);
      const quantity = Math.floor(Math.random() * 10) + 1;
      const unitPrice = (Math.random() * 200 + 5).toFixed(2);
      const total = (quantity * parseFloat(unitPrice)).toFixed(2);

      await client.query(
        `INSERT INTO sales (sale_date, customer_name, product_name, quantity, unit_price, total_amount)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          saleDate.toISOString().split('T')[0],
          randomFrom(CUSTOMERS),
          randomFrom(PRODUCTS),
          quantity,
          unitPrice,
          total,
        ]
      );
    }

    await client.query('COMMIT');
    console.log('[seed] Listo: 10,000 ventas insertadas.');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[seed] Error:', err);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

seed();