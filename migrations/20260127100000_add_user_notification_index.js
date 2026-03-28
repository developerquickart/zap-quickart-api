/**
 * Migration: Add index on user_notification(user_id, created_at DESC)
 * Purpose: Optimize notificationlist API query for <100ms latency
 * Table: public.user_notification (PostgreSQL)
 */

exports.up = function (knex) {
  return knex.schema.raw(`
    CREATE INDEX IF NOT EXISTS idx_user_notification_user_id_created_at
    ON user_notification (user_id, created_at DESC);
  `);
};

exports.down = function (knex) {
  return knex.schema.raw(`
    DROP INDEX IF EXISTS idx_user_notification_user_id_created_at;
  `);
};
