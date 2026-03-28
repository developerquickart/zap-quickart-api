const knex = require('./db');

async function debugOrders() {
    try {
        const userId = '1000013';
        console.log(`Debugging orders for userId: ${userId}`);

        const orders = await knex('orders')
            .where('user_id', userId)
            .where('order_type', 'trail')
            .orderBy('order_id', 'desc')
            .limit(3);

        if (!orders || orders.length === 0) {
            console.log('No trail orders found for this user.');
        } else {
            console.log(`Found ${orders.length} orders.`);
            orders.forEach(o => {
                console.log(`Order ID: ${o.order_id}, Cart ID: ${o.cart_id}, Total Price: ${o.total_price}, Discount: ${o.trail_discount}`);
            });

            const cartIds = orders.map(o => o.cart_id);
            const storeOrders = await knex('store_orders')
                .whereIn('order_cart_id', cartIds);

            console.log(`Found ${storeOrders.length} related store_orders.`);
            storeOrders.forEach(so => {
                console.log(`Store Order ID: ${so.store_order_id}, Cart ID: ${so.order_cart_id}, Price (stored): ${so.price}, MRP: ${so.total_mrp}, Varient: ${so.varient_id}`);
            });
        }

    } catch (error) {
        console.error('Debug Error:', error);
    } finally {
        process.exit();
    }
}

debugOrders();
