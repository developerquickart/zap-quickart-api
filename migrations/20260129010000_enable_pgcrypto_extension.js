/**
 * Migration: Enable pgcrypto extension for UUID generation
 * Purpose: Required for gen_random_uuid() function used in verify_details API
 * Database: PostgreSQL
 */

exports.up = function (knex) {
  return knex.schema.raw(`
    -- Enable pgcrypto extension for UUID generation
    -- This extension provides gen_random_uuid() function used in verify_details API
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
  `);
};

exports.down = function (knex) {
  return knex.schema.raw(`
    -- Note: Extensions are typically not dropped in production
    -- Uncomment only if you're sure you want to remove it
    -- DROP EXTENSION IF EXISTS "pgcrypto";
  `);
};
