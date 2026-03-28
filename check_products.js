const knex = require('./db');

async function checkStoreProducts() {
    try {
        const varientIds = [1709, 2122, 2616, 3481];
        console.log(`Checking store_products for varients: ${varientIds}`);

        const products = await knex('store_products')
            .whereIn('varient_id', varientIds)
            .where('store_id', 7);

        console.log('--- store_products data ---');
        console.log(JSON.stringify(products, null, 2));

    } catch (error) {
        console.error('Debug Error:', error);
    } finally {
        process.exit();
    }
}

checkStoreProducts();
