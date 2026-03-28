const knex = require('./db');

async function checkHealth() {
    console.log('--- Database Health Check ---');
    try {
        // 1. Check Row Counts
        const orderCount = await knex('orders').count('* as count').first();
        const subCount = await knex('subscription_order').count('* as count').first();
        const storeOrderCount = await knex('store_orders').count('* as count').first();

        console.log(`Orders count: ${orderCount.count}`);
        console.log(`Subscription orders count: ${subCount.count}`);
        console.log(`Store orders count: ${storeOrderCount.count}`);

        // 2. Check Indexes in Postgres
        console.log('\n--- Existing Indexes on "orders" ---');
        const indexes = await knex.raw(`
      SELECT
          t.relname as table_name,
          i.relname as index_name,
          a.attname as column_name
      FROM
          pg_class t,
          pg_class i,
          pg_index ix,
          pg_attribute a
      WHERE
          t.oid = ix.indrelid
          AND i.oid = ix.indexrelid
          AND a.attrelid = t.oid
          AND a.attnum = ANY(ix.indkey)
          AND t.relkind = 'r'
          AND t.relname = 'orders'
      ORDER BY
          t.relname,
          i.relname;
    `);
        console.table(indexes.rows);

        // 3. Performance Test: orderwiselist logic (simplified)
        console.log('\n--- Performance Test: orderwiselist (Simplified) ---');
        const startTime = Date.now();

        // Pick a random user with orders to test
        const randomUser = await knex('orders').select('user_id').whereNotNull('user_id').limit(1).first();
        if (randomUser) {
            console.log(`Testing with user_id: ${randomUser.user_id}`);
            await knex("orders")
                .select(
                    knex.raw("MAX(orders.pastorecentrder) as pastorecentrder"),
                    knex.raw("MAX(orders.cart_id) as cart_id"),
                    "orders.group_id",
                    knex.raw("MAX(orders.is_subscription) as is_subscription")
                )
                .where("orders.user_id", randomUser.user_id)
                .whereNotNull("orders.payment_method")
                .groupBy("orders.group_id")
                .orderByRaw("MAX(orders.order_id) DESC")
                .limit(8);

            console.log(`Query Time: ${Date.now() - startTime}ms`);
        } else {
            console.log('No users found in orders table.');
        }

    } catch (err) {
        console.error('Error during health check:', err);
    } finally {
        await knex.destroy();
    }
}

checkHealth();
