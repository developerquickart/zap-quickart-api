const knex = require('./db');

async function checkProductSchema() {
    try {
        const pv = await knex('product_varient').columnInfo();
        console.log('--- product_varient Columns ---');
        console.log(Object.keys(pv));

        const sp = await knex('store_products').columnInfo();
        console.log('--- store_products Columns ---');
        console.log(Object.keys(sp));

        const p = await knex('product').columnInfo();
        console.log('--- product Columns ---');
        console.log(Object.keys(p));

    } catch (error) {
        console.error('Schema Error:', error);
    } finally {
        process.exit();
    }
}

checkProductSchema();
