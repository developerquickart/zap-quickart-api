const knex = require('./db');

async function debugQuery() {
    const store_id = 7; // using a real store_id from previous conversation history or common sense
    const banner_id = 117; // verified banner_id
    const banner_type = 'store';
    const baseurl = 'https://quickart.b-cdn.net/';

    try {
        console.log('--- Step 1: Banner Query ---');
        const banner = await knex('store_banner')
            .select('banner_id', 'banner_name', knex.raw("CONCAT(?::text, banner_image) as banner_image", [baseurl]), 'parent_cat_id', 'cat_id', 'varient_id')
            .where('banner_id', banner_id)
            .first();
        console.log('✅ Banner Query Result:', banner ? `Found (${banner.banner_name})` : 'Not Found');

        console.log('--- Step 2: Main Query with Joins and GROUP BY ---');
        const topsellingsQuery = knex('store_products')
            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
            .join('product', 'product_varient.product_id', '=', 'product.product_id')
            .leftJoin('tbl_country', knex.raw('tbl_country.id::text'), '=', 'product.country_id')
            .select(
                knex.raw('MAX(store_products.stock) as stock'),
                'product.product_id',
                'product.product_name',
                knex.raw('100-((MAX(store_products.price)*100)/NULLIF(MAX(store_products.mrp), 0)) as discountper')
            )
            .groupBy(
                'product.product_id',
                'product.product_name'
            )
            .limit(1);

        console.log('Executing Main Query...');
        const result = await topsellingsQuery;
        console.log('✅ Main Query OK, rows:', result.length);
        if (result.length > 0) console.log('Sample data:', result[0]);

    } catch (error) {
        console.error('❌ Error during debug:', error);
    } finally {
        process.exit(0);
    }
}

debugQuery();
