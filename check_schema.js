const knex = require('./db');

async function checkSchema() {
    try {
        const columns = await knex('store_orders').columnInfo();
        console.log('--- store_orders Schema ---');
        console.log(JSON.stringify(columns.store_approval, null, 2));
        console.log(JSON.stringify(columns.price, null, 2));

        const ordersCols = await knex('orders').columnInfo();
        console.log('--- orders Schema ---');
        console.log(JSON.stringify(ordersCols.user_id, null, 2));
        console.log(JSON.stringify(ordersCols.total_price, null, 2));

    } catch (error) {
        console.error('Schema Error:', error);
    } finally {
        process.exit();
    }
}

checkSchema();
