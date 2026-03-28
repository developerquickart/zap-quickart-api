// app.js
require('dotenv').config();
const express = require('express');
const path = require('path');
const bodyParser = require('body-parser');
const cors = require('cors');
const app = express();
const PORT = process.env.PORT || 3001;
console.log(`Port debug: process.env.PORT is ${process.env.PORT}`);
const logToFile = require("./utils/logger");
const knex = require('./db');

// --- STARTUP DIAGNOSTICS ---
console.log('--- STARTING NODE PROCESS ---');
console.log('Environment:', process.env.NODE_ENV);
console.log('Memory Usage on Start:', JSON.stringify(process.memoryUsage()));

// Database connection check: complete before accepting traffic so the pool is warmed.
// Use a longer timeout (20s) so first connection to Supabase/cloud DB can complete.
const DB_CHECK_TIMEOUT_MS = 20000;
const dbCheckTimeout = new Promise((_, reject) =>
  setTimeout(() => reject(new Error(`Database connection check timed out after ${DB_CHECK_TIMEOUT_MS / 1000}s`)), DB_CHECK_TIMEOUT_MS)
);

const dbCheckPromise = Promise.race([knex.raw('SELECT 1'), dbCheckTimeout])
  .then(() => {
    console.log('✅ Database connected successfully');
    app.locals.db_status = 'Connected';
  })
  .catch(err => {
    console.error('❌ Database connection check failed or timed out:', err.message);
    logToFile(`CRITICAL: DB Connection Check Failed/Timed Out - ${err.message}`);
    app.locals.db_status = 'Error/Timeout';
  });
// ---------------------------

// Global error handlers for better diagnostics on Railway
process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
  logToFile(`CRITICAL: Uncaught Exception - ${err.message}\n${err.stack}`);
  setTimeout(() => process.exit(1), 500);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('UNHANDLED REJECTION at:', promise, 'reason:', reason);
  logToFile(`CRITICAL: Unhandled Rejection - ${reason.message || reason}\n${reason.stack || ''}`);
});

// app.use(cors());
// Middleware to parse JSON bodies
app.use(bodyParser.json());
// Middleware to parse URL-encoded bodies
app.use(bodyParser.urlencoded({ extended: true }));

// Serve invoice PDFs from the public_html/invoice directory
app.use('/invoice', express.static(path.join(__dirname, '..', 'public_html', 'invoice')));

// Request Logger Middleware
app.use((req, res, next) => {
  const start = Date.now();
  next();
  res.on('finish', () => {
    const duration = Date.now() - start;
    if (req.url === '/') {
      // Health check specific logging
      console.log(`[HEALTH CHECK] ${req.method} ${req.url} ${res.statusCode} - ${duration}ms - DB: ${app.locals.db_status}`);
    } else {
      console.log(`[${new Date().toISOString()}] ${req.method} ${req.url} ${res.statusCode} - ${duration}ms`);
    }
  });
});

// Help Railway capture shutdown event
process.on('SIGTERM', () => {
  console.log('RECEIVED SIGTERM - Container is being stopped');
  logToFile('INFO: Process received SIGTERM');
  // Flush logs and exit
  setTimeout(() => process.exit(0), 500);
});

process.on('SIGINT', () => {
  console.log('RECEIVED SIGINT - Manual interruption');
  process.exit(0);
});

// Periodic Heartbeat to monitor event loop health and keep process alive
let lastHeartbeat = Date.now();
const heartbeat = setInterval(() => {
  const now = Date.now();
  const lag = now - lastHeartbeat - 10000;
  const memory = process.memoryUsage();
  // Only log heartbeat if lag is significant to reduce log noise, or every 1 min
  if (lag > 100 || (now % 60000 < 10000)) {
    console.log(`[HEARTBEAT] ${new Date().toISOString()} - RSS: ${Math.round(memory.rss / 1024 / 1024)}MB, Lag: ${lag}ms`);
  }

  if (lag > 500) {
    console.warn(`[LAG DETECTED] Event loop delayed by ${lag}ms at ${new Date().toISOString()}`);
    logToFile(`WARNING: High event loop lag detected - ${lag}ms`);
  }

  lastHeartbeat = now;
}, 10000);

const usersRouter = require('./routes/index');

// Root route for Railway health check
app.get('/', (req, res) => {
  // Return info even if DB is down to keep container alive
  res.status(200).json({
    status: app.locals.db_status === 'Connected' ? 'OK' : 'Degraded',
    service: 'OneAPI-Node',
    db_connected: app.locals.db_status,
    timestamp: new Date().toISOString()
  });
});

app.use('/', usersRouter);

// Start server only after DB check settles so the pool is warmed (or we know DB is down).
dbCheckPromise.then(() => {
  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀🚀🚀 SERVER STARTED SUCCESSFULLY 🚀🚀🚀`);
    console.log(`Port debug: process.env.PORT is ${process.env.PORT}`);
    console.log(`Listening on: http://0.0.0.0:${PORT}`);
    if (app.locals.db_status !== 'Connected') {
      console.warn('⚠️ Server running in DEGRADED mode (DB not connected). API requests may fail.');
    }
  });

  server.on('error', (err) => {
    console.error('SERVER ERROR:', err);
    logToFile(`CRITICAL: Server Error - ${err.message}`);
  });
});
