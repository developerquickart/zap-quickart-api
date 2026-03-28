const knex = require('./db');
const searchModel = require('./models/searchModel');

async function debugCall() {
    console.log("Starting isolated debug call...");
    const appDetails = {
        bannerid: 4,
        store_id: 7,
        user_id: "1",
        device_id: "test_device"
    };

    try {
        console.log("Calling getSearchbypopup...");
        const res = await searchModel.getSearchbypopup(appDetails);
        console.log("Result length:", res.length);
        if (res.length > 0) {
            console.log("Products in first banner:", res[0].product_details.length);
        }
    } catch (e) {
        console.error("Caught error:");
        console.error(e);
    } finally {
        process.exit(0);
    }
}

debugCall();
