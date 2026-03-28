require('dotenv').config();

module.exports = {
  client: 'pg',
  connection: {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    ssl: { rejectUnauthorized: false } // Required for Supabase
  },
  migrations: {
    tableName: 'knex_migrations',
    directory: './migrations',
  },
  pool: {
    min: 2,
    max: 30,
    acquireTimeoutMillis: 15000
  },
};
