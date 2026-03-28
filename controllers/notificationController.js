const notificationModel = require('../models/notificationModel');
const checkoutModel = require('../models/checkoutModel');
const trailpackModel = require('../models/trailpackModel');
const knex = require('../db'); // Import your Knex instance
const axios = require('axios');
const crypto = require('crypto');

const notificationlist = async (req, res) => {
    try {
        const appDetatils = req.body;
        console.log('=== NotificationList Request ===');
        console.log('Request body:', JSON.stringify(appDetatils, null, 2));

        const startTime = Date.now();
        const sneaky = await notificationModel.notificationlist(appDetatils);
        const dbTime = Date.now() - startTime;
        console.log(`📊 Database query completed in ${dbTime}ms`);

        var data = {
            "status": "1",
            "message": "Notification List",
            "data": sneaky,
        };

        const totalTime = Date.now() - startTime;
        console.log(`✅ Response sent - Total time: ${totalTime}ms`);

        res.status(200).json(data);
    } catch (error) {
        console.error('=== NotificationList Error ===');
        console.error('Error:', error);
        console.error('Error message:', error.message);
        console.error('Error stack:', error.stack);
        res.status(500).json({ status: 0, message: 'Not found' });
    }
}

const seosource = async (req, res) => {

    try {
        const appDetatils = req.body;
        const sneaky = await notificationModel.seosource(appDetatils);
        var data = {
            "status": "1",
            "message": "Success",
        };
        res.status(200).json(data);
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'Not found' });
    }

}


const paymentnotification = async (req, res) => {
    const ipnLogId = `IPN-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    try {
        console.log(`[${ipnLogId}] ========== PAYMENT NOTIFICATION (IPN) RECEIVED ==========`);
        console.log(`[${ipnLogId}] Method: ${req.method}, Content-Type: ${req.headers['content-type'] || 'not set'}`);

        const appDetatils = req.body;
        console.log(`[${ipnLogId}] Raw body type: ${typeof appDetatils}, keys: ${appDetatils ? Object.keys(appDetatils).join(', ') : 'null/undefined'}`);
        console.log(`[${ipnLogId}] Full IPN payload:`, JSON.stringify(appDetatils, null, 2));

        if (!appDetatils || (typeof appDetatils === 'object' && Object.keys(appDetatils).length === 0)) {
            console.log(`[${ipnLogId}] ERROR: Empty or missing request body - TotalPay may be sending wrong format`);
            return res.status(400).json({ status: 0, message: 'Empty IPN body' });
        }

        console.log(`[${ipnLogId}] Key fields - order_number: ${appDetatils.order_number}, ordertype: ${appDetatils.ordertype}, id: ${appDetatils.id}, card: ${appDetatils.card ? 'PRESENT' : 'MISSING'}`);
        if (appDetatils.card) {
            console.log(`[${ipnLogId}] Card value (masked): ${String(appDetatils.card).substring(0, 4)}****${String(appDetatils.card).slice(-4)}`);
        }

        const sneaky = await notificationModel.InsertPaymentNotification(appDetatils, ipnLogId);
        var data = {
            "status": "1",
            "message": "Success",
        };
        console.log(`[${ipnLogId}] IPN processed successfully, responding 200`);
        res.status(200).json(data);
    } catch (error) {
        console.error(`[${ipnLogId}] IPN ERROR at`, error.stack || error.message);
        res.status(500).json({ status: 0, message: 'Not found' });
    }
}

const successold = async (req, res) => {

    try {
        const appDetatils = req.body;
        const sneaky = await notificationModel.successData(appDetatils);
        const getnotification = await notificationModel.getNotification(req.query);
        // Merge appDetails and getNotification into a single JSON object
        const mergedData = { ...req.query, ...getnotification };
        if (mergedData.ordertype == 'quick') {
            checkout = await checkoutModel.getQuickordercheckout(mergedData);
        } else if (mergedData.ordertype == 'subscription') {
            checkout = await checkoutModel.getSubordercheckout(mergedData);
        }

        //  const getOrderlist = await checkoutModel.getQuickordercheckout(appData);
        //let query = url.parse(req.url, true).query;
        // const { payment_id, trans_id, order_id, hash } = req.query;

        // You can add logic here to process the payment response
        // For demonstration purposes, we'll just return the parsed query parameters
        // const abcd =   res.json({
        //                 payment_id,
        //                 trans_id,
        //                 order_id,
        //                 hash,
        //               });





        // var data = {
        //     "status": "1",
        //     "message":"Success List",
        //     //"data":req.query,
        //     "data":mergedData.ordertype,
        //     "notilist" :checkout
        //     };
        // res.status(200).json(data);
        // return "Waiting for payment confirmation";
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message });
    }

}

const failure = async (req, res) => {

    try {
        var data;
        trackAppsFlyerEvent();

        res.status(200).json(data);
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'Not found' });
    }

}

const trackAppsFlyerEvent = async () => {
    const app_id = "com.quickart.customer";
    const dev_key = "UcP5dBePhwjBa7aXRTbLD8";

    const payload = {
        appsflyer_id: "1762508611215-1044090",
        eventName: "order_failed",
        eventValue: JSON.stringify({
            data: data
        }),
        customer_user_id: user_id,
        eventTime: new Date().toISOString().slice(0, 19).replace('T', ' ')
    };

    try {
        const response = await axios.post(
            `https://api2.appsflyer.com/inappevent/${app_id}`,
            payload,
            {
                headers: {
                    "Content-Type": "application/json",
                    "authentication": dev_key
                }
            }
        );

        console.log("AppsFlyer Event Response:", response.data);
        return response.data;

    } catch (error) {
        console.error("AppsFlyer Tracking Error:", error.response?.data || error.message);
        return null;
    }
};



const successfirst = async (req, res) => {
    try {
        // Robust detection: Check query and body for various potential parameter names
        const orderId = req.query.order_id || req.body.order_id || req.query.order_number || req.body.order_number || req.query.group_id || req.body.group_id;

        console.log(`[SuccessFirst] Callback received. Detected orderId: ${orderId}`);
        if (!orderId) {
            console.error(`[SuccessFirst] MISSING orderId in query (${JSON.stringify(req.query)}) or body (${JSON.stringify(req.body)})`);
            // We continue polling for req.query anyway to match existing logic if findByGroupId can handle empty obj
        }

        console.log(`[SuccessFirst] Started polling for group_id: ${orderId}`);

        let maxRetries = 15; // Increased to 15 attempts (30s) to give IPN more time
        let delay = 2000;
        let paymentDetails;
        let checkout;

        // Polling to check for the group_id in payment_notification_details
        for (let i = 0; i < maxRetries; i++) {
            paymentDetails = await notificationModel.findByGroupId(req.query);
            if (paymentDetails) {
                console.log(`[SuccessFirst] Found payment info for ${orderId} on attempt ${i + 1}`);
                break;
            } else {
                if (i % 5 === 0) console.log(`[SuccessFirst] Still waiting for IPN for ${orderId} (attempt ${i + 1})...`);
                await new Promise((resolve) => setTimeout(resolve, delay));
            }
        }

        if (!paymentDetails) {
            console.error(`[SuccessFirst] Polling TIMEOUT for ${orderId}. IPN never arrived. Falling back to manual status check.`);

            // Fallback: Check status directly from TotalPay if IPN failed
            const trans_id = req.query.trans_id || req.body.trans_id;
            const payment_id = req.query.payment_id || req.body.payment_id;

            if (trans_id && payment_id) {
                try {
                    const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
                    const merchantpassword = process.env.TOTALPAY_PASSWORD;
                    const statusHashData = `${payment_id}${merchantpassword}`;
                    const statusHash = crypto.createHash('sha1').update(crypto.createHash('md5').update(statusHashData.toUpperCase()).digest('hex')).digest('hex');

                    console.log(`[SuccessFirst-Fallback] Querying TotalPay status for ${orderId} (trans_id: ${trans_id})`);
                    const statusResponse = await axios.post('https://checkout.totalpay.global/api/v1/payment/status', {
                        merchant_key: merchantKey,
                        payment_id: payment_id,
                        hash: statusHash
                    });

                    const statusData = statusResponse.data;
                    const successStatuses = ['success', 'approved', 'settled', 'captured'];
                    if (statusData && successStatuses.includes(statusData.status?.toLowerCase())) {
                        console.log(`[SuccessFirst-Fallback] Payment CONFIRMED by Status API for ${orderId}. Manually retrieving custom_data.`);
                        // Try to get custom_data from our own logs since IPN is missing
                        const customDataFromRequest = await notificationModel.findByOrderIdFromRequests(orderId);
                        if (customDataFromRequest) {
                            paymentDetails = { custom_data: customDataFromRequest };
                            console.log(`[SuccessFirst-Fallback] Metadata recovered. Proceeding with order insertion.`);
                        } else {
                            console.error(`[SuccessFirst-Fallback] Payment is success but custom_data not found in requests log for ${orderId}.`);
                        }
                    } else {
                        console.error(`[SuccessFirst-Fallback] Status API returned: ${JSON.stringify(statusData)}`);
                    }
                } catch (fallbackError) {
                    const errorBody = fallbackError.response ? JSON.stringify(fallbackError.response.data) : "No response body";
                    console.error(`[SuccessFirst-Fallback] Error during fallback check for ${orderId}:`, fallbackError.message, "Response Body:", errorBody);
                }
            }
        }

        if (!paymentDetails) {
            console.error(`[SuccessFirst] Polling TIMEOUT for ${orderId}. IPN never arrived and Fallback failed.`);
            return res.status(404).json({ status: 0, message: "Payment confirmation not received from gateway yet. Please check your order history in a moment." });
        }

        // Process order
        // Process order using already retrieved paymentDetails
        let getnotification;
        if (paymentDetails.custom_data) {
            // Fallback case: Metadata recovered from logs
            console.log(`[SuccessFirst] Using metadata RECOVERED from request logs for ${orderId}`);
            getnotification = paymentDetails.custom_data;
        } else if (paymentDetails.json_data) {
            // Polling case: Row found in payment_notification_details
            console.log(`[SuccessFirst] Using metadata from IPN notification for ${orderId}`);
            const dataString = paymentDetails.json_data;
            const data = (typeof dataString === 'string') ? JSON.parse(dataString) : dataString;
            getnotification = data ? data.custom_data : null;
        }

        if (!getnotification) {
            console.error(`[SuccessFirst] Could not parse custom_data for ${orderId}. paymentDetails keys: ${Object.keys(paymentDetails).join(", ")}`);
            return res.status(500).json({ status: 0, message: "Error processing payment metadata." });
        }

        // Merge sources: req.body (POST), req.query (GET fallback), and getnotification (from DB)
        const mergedData = { ...req.query, ...req.body, ...getnotification };
        console.log(`[SuccessFirst] Processing order for ${orderId}. Type: ${mergedData.ordertype}`);

        if (mergedData.ordertype === 'quick') {
            checkout = await checkoutModel.getQuickordercheckout(mergedData);
        } else if (mergedData.ordertype === 'subscription') {
            checkout = await checkoutModel.getSubordercheckout(mergedData);
        } else if (mergedData.ordertype === 'trail') {
            checkout = await trailpackModel.gettrailcheckout(mergedData);
        } else {
            console.error(`[SuccessFirst] Unknown ordertype: ${mergedData.ordertype} for ${orderId}`);
            return res.status(400).json({ status: 0, message: "Invalid order type." });
        }

        console.log(`[SuccessFirst] Checkout result for ${orderId}: ${checkout}`);
        if (checkout === 'success') {
            res.redirect("https://quickart2.democheck.in/nodejsapp/api/success");
        } else {
            res.status(500).json({ status: 0, message: "Checkout failed after payment.", detail: checkout });
        }

    } catch (error) {
        console.error(`[SuccessFirst] CRITICAL ERROR for ${req.query.order_id}:`, error);
        res.status(500).json({ status: 0, message: error.message });
    }
};

// The success function
const success = async (req, res) => {
    var data;
    res.status(200).json(data);
};

module.exports = {
    notificationlist,
    paymentnotification,
    success,
    failure,
    seosource,
    successfirst
};
