// src/api/server.js
require('dotenv').config();
const express = require('express');
const path = require('path');
const routes = require('./routes');

const app = express();
app.use(express.json());
app.use('/files', express.static(path.join(__dirname, '../../output'))); // solo lectura de CSV finales
app.use('/api', routes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`[API] Escuchando en http://localhost:${PORT}`);
});