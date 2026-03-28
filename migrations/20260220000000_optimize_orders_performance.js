/**
 * Migration: Optimize indexes for orders table
 * Purpose: Improve performance of orderwiselist (MAX+GROUP BY) and user-based queries
 */

exports.up = function (knex) {
    return knex.schema.alterTable('orders', (table) => {
        // Composite index for user-based order listing with sorting
        // This helps: WHERE user_id = ? ... ORDER BY order_id DESC
        table.index(['user_id', 'order_id'], 'idx_orders_user_id_order_id_desc');

        // Index for payment_method and order_status since they are filtered in orderwiselist
        table.index(['payment_method', 'order_status'], 'idx_orders_payment_status_filter');
    });
};

exports.down = function (knex) {
    return knex.schema.alterTable('orders', (table) => {
        table.dropIndex(['user_id', 'order_id'], 'idx_orders_user_id_order_id_desc');
        table.dropIndex(['payment_method', 'order_status'], 'idx_orders_payment_status_filter');
    });
};
