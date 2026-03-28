/**
 * Migration: Add indexes for feature_categories API optimization
 * Purpose: Optimize feature_category API query for <100ms latency
 * Tables: public.feature_categories, public.product (PostgreSQL)
 */

exports.up = function (knex) {
  return knex.schema.raw(`
    -- Index on product.fcat_id for faster join lookups (only non-null values)
    CREATE INDEX IF NOT EXISTS idx_product_fcat_id 
    ON product(fcat_id) 
    WHERE fcat_id IS NOT NULL AND fcat_id != '';

    -- Composite index on feature_categories for filtering (status and is_deleted)
    CREATE INDEX IF NOT EXISTS idx_feature_categories_status_deleted 
    ON feature_categories(status, is_deleted) 
    WHERE status = 1 AND is_deleted = 0;

    -- Index on feature_categories.order for faster sorting
    CREATE INDEX IF NOT EXISTS idx_feature_categories_order 
    ON feature_categories("order" ASC) 
    WHERE is_deleted = 0 AND status = 1;
  `);
};

exports.down = function (knex) {
  return knex.schema.raw(`
    DROP INDEX IF EXISTS idx_product_fcat_id;
    DROP INDEX IF EXISTS idx_feature_categories_status_deleted;
    DROP INDEX IF EXISTS idx_feature_categories_order;
  `);
};
