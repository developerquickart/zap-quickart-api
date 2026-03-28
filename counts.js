const knex = require('./db');
async function run() {
    const counts = await Promise.all([
        knex('orders').count('* as count').first(),
        knex('subscription_order').count('* as count').first(),
        knex('store_orders').count('* as count').first()
    ]);
    console.log('Orders:', counts[0].count);
    console.log('Subscriptions:', counts[1].count);
    console.log('Store Orders:', counts[2].count);
    await knex.destroy();
}
run();
