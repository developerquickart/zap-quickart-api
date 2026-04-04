const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const crypto = require('crypto');


const SaveCardDetails = async (appDetails) => {
  const { user_id, platform, successroutename, cancelroutename, addedFrom, tab } = appDetails;
  // Generate random letters
  const chars1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let val1 = "";
  for (let i = 0; i < 6; i++) {
    val1 += chars1.charAt(Math.floor(Math.random() * chars1.length));
  }

  // Generate random digits
  const chars3 = "0123456789";
  let val3 = "";
  for (let i = 0; i < 2; i++) {
    val3 += chars3.charAt(Math.floor(Math.random() * chars3.length));
  }

  // Generate a random substring from md5 hash of current time
  const cr1 = crypto.createHash('md5').update(String(Date.now())).digest('hex').substr(Math.floor(Math.random() * 25), 2);
  const pay_amounts = 1;
  // Combine all parts to form cart_id
  const randomNumber = val1 + val3 + cr1;
  const number = randomNumber;
  const description = 'Order description';
  const amount = (pay_amounts).toFixed(2); // Ensure two decimal places
  const currency = 'AED';

  // TotalPay Credentials from Environment
  const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
  const merchantpassword = process.env.TOTALPAY_PASSWORD;

  // TotalPay Credentials for Live
  // const merchantKey = '7f066f26-36b4-11ee-8433-eecb8191d36e';
  // const merchantpassword = '96bb03851c3553fd132339acc06ce060';

  const hashData = `${number}${amount}${currency}${description}${merchantpassword}`;
  const hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');

  const userdetails = await knex('users')
    .where('id', user_id)
    .select('name', 'email')
    .first();

  // Customer Information
  const customerInfo = { name: userdetails.name, email: userdetails.email, birth_date: '1970-02-17' }; // Only include email as per comments

  const address = await knex('address')
    .where('address.user_id', user_id)
    .select('address.receiver_phone', 'address.house_no', 'address.state')
    .first();

  // Billing Information
  const billingAddress = {
    country: 'AE',
    state: 'Dubai',
    city: 'Dubai',
    address: 'dubai',
    house_number: 305,
    phone: "123456789",
    district: 'Dubai'
  };

  //return billingAddress;
  const ordertype = 'savecard';

  const orderJson = { number, description, amount, currency };
  const custom_data = { ordertype, user_id }

  if (appDetails.platform == "web") {
    success_url = `https://quickartweb-production.up.railway.app/${appDetails.successroutename}?addedFrom=${appDetails.addedFrom}&tab=${appDetails.tab}`;
    cancel_url = `https://quickartweb-production.up.railway.app/${appDetails.cancelroutename}?addedFrom=${appDetails.addedFrom}&tab=${appDetails.tab}`;
  } else {

    success_url = 'https://supaapioriginal-production.up.railway.app/testnodejsapp/api/savesuccess'
    cancel_url = 'https://supaapioriginal-production.up.railway.app/testnodejsapp/api/savefailure'
  }

  const server_callback_url = 'https://supaapioriginal-production.up.railway.app/testnodejsapp/api/paymentnotification/';
  const mainJson = {
    merchant_key: merchantKey,
    operation: 'purchase',
    methods: ['card'],
    success_url: success_url,
    cancel_url: cancel_url,
    server_callback_url,
    hash,
    order: orderJson,
    customer: customerInfo,
    billing_address: billingAddress,
    custom_data: custom_data,
    // req_token:true,
    recurring_init: true,
    addedFrom: addedFrom
  };

  const jsonData = JSON.stringify(mainJson);

  const checkoutUrl = 'https://checkout.totalpay.global/api/v1/session';

  try {
    // Save request to payment_order_request_details (like other payment flows do)
    const maxPayId = await knex('payment_order_request_details').max('id as maxId').first();
    const nextPayId = (maxPayId?.maxId ? parseInt(maxPayId.maxId, 10) : 0) + 1;

    await knex('payment_order_request_details').insert({
      id: nextPayId,
      json_data: jsonData,
      group_id: number,
      order_type: 'savecard',
      added_on: new Date(),
      datetime: new Date()
    });

    console.log(`[SAVECARD] Session created - group_id: ${number}, user_id: ${user_id}, server_callback_url included for IPN`);
    console.log(`[SAVECARD] Request saved to payment_order_request_details with group_id: ${number}, user_id: ${user_id}`);

    const fetch = (await import('node-fetch')).default;

    const response = await fetch(checkoutUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: jsonData,
    });

    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error sending payment data:', error);
    throw error; // Re-throw for handling in the calling code
  }
};

const InsertCard = async (appDetails) => {
  const group_id = appDetails.order_id;
  const redirect_payment_id = appDetails.payment_id; // from success redirect URL
  const saveLogId = `SAVECARD-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  console.log(`[${saveLogId}] ========== INSERT CARD (savesuccess) START ==========`);
  console.log(`[${saveLogId}] group_id: ${group_id}, payment_id: ${redirect_payment_id}, full query:`, JSON.stringify(appDetails));
  console.log(`[SAVECARD] InsertCard called for group_id: ${group_id}, payment_id: ${redirect_payment_id}`);

  // TotalPay Credentials from Environment
  const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
  const merchantpassword = process.env.TOTALPAY_PASSWORD;

  let data = null;

  // Step 1: Try to find the IPN notification (may have arrived already)
  let notifybys;
  let maxRetries = 5;
  let delay = 2000;

  for (let i = 0; i < maxRetries; i++) {
    console.log(`[${saveLogId}] IPN poll attempt ${i + 1}/${maxRetries} for group_id: ${group_id}`);
    notifybys = await knex('payment_notification_details')
      .where('group_id', group_id)
      .orderBy('id', 'DESC')
      .first();

    if (notifybys) {
      console.log(`[${saveLogId}] IPN FOUND on attempt ${i + 1} - data source: payment_notification_details`);
      console.log(`[SAVECARD] Found IPN notification for group_id: ${group_id} on attempt ${i + 1}`);
      break;
    }

    console.log(`[${saveLogId}] IPN not found, waiting ${delay}ms before retry...`);
    await new Promise(resolve => setTimeout(resolve, delay));
  }

  if (notifybys) {
    // Use data from IPN
    const dataString = notifybys.json_data;
    data = (typeof dataString === 'string') ? JSON.parse(dataString) : dataString;
    console.log(`[${saveLogId}] Using IPN data - card: ${data.card ? 'PRESENT' : 'MISSING'}, id: ${data.id || 'MISSING'}`);

  } else {
    // Step 2: IPN not found — fetch from TotalPay status API directly
    console.log(`[${saveLogId}] IPN NOT FOUND after ${maxRetries} attempts - TotalPay may not be sending IPN to paymentnotification. Falling back to status API.`);
    console.log(`[SAVECARD] IPN not found for group_id: ${group_id}. Fetching from TotalPay status API using payment_id: ${redirect_payment_id}`);

    if (!redirect_payment_id) {
      throw new Error(`Payment notification not found for group_id: ${group_id} and no payment_id available to fetch status.`);
    }

    const statusHashData = `${redirect_payment_id}${merchantpassword}`;
    const statusHash = crypto.createHash('sha1').update(crypto.createHash('md5').update(statusHashData.toUpperCase()).digest('hex')).digest('hex');
    const statusJson = {
      merchant_key: merchantKey,
      payment_id: redirect_payment_id,
      hash: statusHash,
    };

    try {
      const fetch = (await import('node-fetch')).default;
      const statusResponse = await fetch('https://checkout.totalpay.global/api/v1/payment/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(statusJson),
      });

      if (!statusResponse.ok) {
        throw new Error(`TotalPay status API error! HTTP Status: ${statusResponse.status}`);
      }

      data = await statusResponse.json();
      console.log(`[${saveLogId}] Status API FULL response (for card field inspection):`, JSON.stringify(data));
      console.log(`[${saveLogId}] Status API - status: ${data?.status}, card: ${data?.card ? 'PRESENT' : 'MISSING'}, id: ${data?.id || 'MISSING'}`);
      console.log(`[SAVECARD] TotalPay status API response:`, JSON.stringify(data, null, 2));

      if (!data || data.status === 'fail' || data.status === 'error') {
        throw new Error(`TotalPay status API returned error: ${JSON.stringify(data)}`);
      }
    } catch (fetchError) {
      console.error(`[SAVECARD] Failed to fetch from TotalPay status API:`, fetchError);
      throw new Error(`Payment notification not found for group_id: ${group_id} and TotalPay status API failed: ${fetchError.message}`);
    }
  }

  // Step 3: Determine user_id — from custom_data or from payment_order_request_details
  let user_id = data.custom_data?.user_id;
  if (!user_id) {
    // Look up the original request to get user_id
    const requestRecord = await knex('payment_order_request_details')
      .where('group_id', group_id)
      .first();
    if (requestRecord) {
      const reqData = (typeof requestRecord.json_data === 'string') ? JSON.parse(requestRecord.json_data) : requestRecord.json_data;
      user_id = reqData.custom_data?.user_id;
    }
  }

  if (!user_id) {
    throw new Error(`Cannot determine user_id for group_id: ${group_id}`);
  }

  // Step 4: Save card details
  const maxIdRow = await knex('tbl_user_bank_details').max('id as max_id').first();
  const nextId = (maxIdRow && maxIdRow.max_id != null) ? Number(maxIdRow.max_id) + 1 : 1;

  // Handle missing card details from TotalPay status API
  // Try multiple possible field paths (TotalPay may use different structure)
  let card_no = data.card
    || data.masked_card
    || data.card_number
    || data.transactions?.[0]?.card
    || data.transactions?.[0]?.masked_card
    || data.payment_method_details?.card
    || '';
  let card_expired_date = data.card_expiration_date
    || data.card_expiry
    || data.transactions?.[0]?.card_expiration_date
    || '';

  // If we have brand + card_last_four, build masked display (e.g. "VISA **** 1234")
  if (!card_no && appDetails.brand && (data.card_last_four || data.transactions?.[0]?.card_last_four)) {
    const last4 = String(data.card_last_four || data.transactions?.[0]?.card_last_four).slice(-4);
    card_no = `${String(appDetails.brand).toUpperCase()} **** ${last4}`;
    console.log(`[${saveLogId}] Built card from brand + card_last_four: ${card_no}`);
  }

  console.log(`[${saveLogId}] Card determination - card_no: ${card_no ? 'PRESENT' : 'MISSING'}, appDetails.brand: ${appDetails.brand || 'MISSING'}`);

  if (!card_no && appDetails.brand) {
    // Construct placeholder: "VISA **** 1234"
    console.log(`[${saveLogId}] USING PLACEHOLDER - no real card from IPN or status API, building from brand + token`);
    // Use the last 4 chars of the recurring_token to generate a unique 4-digit suffix
    // This allows users to distinguish between multiple cards
    let last4 = '0000';
    const token = data.recurring_token;
    if (token) {
      // Create a simple numeric hash from the token to get 4 digits
      const cleanToken = token.replace(/-/g, '');
      const lastSegment = cleanToken.substring(cleanToken.length - 4);
      const decimal = parseInt(lastSegment, 16); // Convert hex to decimal
      if (!isNaN(decimal)) {
        last4 = String(decimal % 10000).padStart(4, '0');
      }
    }

    const brand = appDetails.brand.toUpperCase();
    card_no = `${brand} **** ${last4}`;
    if (!card_expired_date) card_expired_date = '00/00';
    console.log(`[${saveLogId}] Placeholder built: ${card_no}`);
    console.log(`[SAVECARD] Using placeholder card details for brand: ${brand}, suffix: ${last4} (derived from token)`);
  } else if (card_no) {
    console.log(`[${saveLogId}] USING REAL CARD from IPN/status API`);
  }

  const recurring_init_trans_id = data.recurring_init_trans_id || data.trans_id || redirect_payment_id || '';
  console.log(`[${saveLogId}] Inserting - user_id: ${user_id}, card_no: ${card_no}, recurring_init_trans_id: ${recurring_init_trans_id}`);
  console.log(`[SAVECARD] Saving card for user_id: ${user_id}, card: ${card_no}, recurring_token: ${data.recurring_token ? 'present' : 'missing'}`);

  await knex('tbl_user_bank_details')
    .insert({
      id: nextId,
      user_id: user_id,
      si_sub_ref_no: data.recurring_token || '',
      recurring_init_trans_id: data.recurring_init_trans_id || data.trans_id || redirect_payment_id || '',
      hash: data.hash || appDetails.hash || '',
      holder_name: data.customer?.name || data.customer_name || '',
      email_id: data.customer?.email || data.customer_email || '',
      card_no: card_no,
      card_expired_date: card_expired_date,
      bank_type: "totalpay",
    });

  console.log(`[${saveLogId}] ========== INSERT CARD END - card_id: ${nextId}, card_no: ${card_no} ==========`);
  console.log(`[SAVECARD] Card saved successfully for user_id: ${user_id}, card_id: ${nextId}`);

  // Step 5: Void the 1 AED charge
  const void_payment_id = data.payment_id || data.id || redirect_payment_id;
  const voidHashData = `${void_payment_id}${merchantpassword}`;
  const voidHash = crypto.createHash('sha1').update(crypto.createHash('md5').update(voidHashData.toUpperCase()).digest('hex')).digest('hex');
  const voidJson = {
    merchant_key: merchantKey,
    payment_id: void_payment_id,
    hash: voidHash,
  };

  const voidJsonData = JSON.stringify(voidJson);
  const voidUrl = 'https://checkout.totalpay.global/api/v1/payment/void';

  try {
    const fetch = (await import('node-fetch')).default;

    const response = await fetch(voidUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: voidJsonData,
    });

    if (!response.ok) {
      console.error(`[SAVECARD] Void request failed with HTTP status: ${response.status}`);
    } else {
      console.log(`[SAVECARD] Void request successful for payment_id: ${void_payment_id}`);
    }

    return data;
  } catch (error) {
    console.error('[SAVECARD] Error voiding payment:', error);
    // Don't throw — card is already saved, void failure is non-critical
    return data;
  }

};

const DeductionRecurringPayment = async (appDetails) => {
  // TotalPay Credentials from Environment
  const recurring_init_trans_id = "3bb7dbc8-5e0f-11ef-9ae0-0e182cc57e01";
  const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
  const merchantpassword = process.env.TOTALPAY_PASSWORD;

  const recurring_token = "3bc316f0-5e0f-11ef-a2dd-0e182cc57e01";
  const pay_amounts = 1;
  const orderNumber = "order-1234";
  const orderDescription = "Payment Deduction";
  const amount = (pay_amounts).toFixed(2); // Ensure two decimal places

  const hashData = `${recurring_init_trans_id}${recurring_token}${orderNumber}${amount}${orderDescription}${merchantpassword}`;
  const hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');
  const mainJson = {
    merchant_key: merchantKey,
    recurring_init_trans_id: recurring_init_trans_id,
    recurring_token: recurring_token,
    hash,
    order: {
      number: orderNumber,
      amount: amount,
      description: orderDescription
    }
  };

  const jsonData = JSON.stringify(mainJson);
  const checkoutUrl = 'https://checkout.totalpay.global/api/v1/payment/recurring';

  try {
    const fetch = (await import('node-fetch')).default;

    const response = await fetch(checkoutUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: jsonData,
    });
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error sending payment data:', error);
    throw error; // Re-throw for handling in the calling code
  }

};

const DeleteCardDetails = async (appDetails) => {
  const { user_id, bank_id } = appDetails;
  const bankDetails = await knex('tbl_user_bank_details')
    .where('id', bank_id)
    .select('si_sub_ref_no')
    .first();

  // Step 1: Check if there are any pending orders or related orders using the bank_id
  const relatedOrders = await knex('orders')
    .join('subscription_order', 'subscription_order.cart_id', 'orders.cart_id')
    .where('orders.user_id', user_id)
    .where('orders.si_sub_ref_no', bankDetails.si_sub_ref_no)
    .where('subscription_order.order_status', 'Pending') // Assuming these statuses mean the order is still active
    .where('orders.payment_type', 'payperdelivery')
    .where('subscription_order.si_payment_flag', 'no')
    .count('subscription_order.id as count')
    .first();

  // Step 2: If there are pending orders, throw an error
  if (relatedOrders.count > 0) {
    throw new Error('Card cannot be deleted as it is associated with pending orders');
  }
  await knex('tbl_user_bank_details')
    .where('user_id', user_id)
    .where('id', bank_id)
    .update({ 'is_delete': 1 });
};

module.exports = {
  SaveCardDetails,
  InsertCard,
  DeductionRecurringPayment,
  DeleteCardDetails,
};
