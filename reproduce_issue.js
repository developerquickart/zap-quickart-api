const orderModel = require('./models/orderModel');
const knex = require('./db');

async function reproduceError() {
    try {
        console.log('Testing orderwiselist with empty user_id...');
        const testData = {
            store_id: 7,
            user_id: "",
            device_id: ""
        };
        const result = await orderModel.orderwiselist(testData);
        console.log('✅ Result:', result);
    } catch (error) {
        console.error('❌ Caught expected error:', error.message);
        if (error.message.includes('invalid input syntax for type bigint')) {
            console.log('🎯 Successfully reproduced the bigint error!');
        } else {
            console.log('Unexpected error:', error);
        }
    } finally {
        process.exit(0);
    }
}

reproduceError();
