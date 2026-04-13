const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const crypto = require('crypto');
const { format } = require('date-fns');
const moment = require('moment');
require('moment-timezone');
const uaeTime = moment.tz(new Date(), "Asia/Dubai");
const logToFile = require("../utils/logger");

const trailPaymentData = async (appDetails) => {
  const currencyval = 'AED';
  const user_id = (appDetails.user_id != "null" && appDetails.user_id) ? appDetails.user_id : appDetails.device_id;
  const userIdStr = String(user_id);
  const userIdInt = parseInt(user_id, 10);
  const address_id = appDetails.address_id;
  let discount_amount = appDetails.discount_amount;
  const totalwalletamt = appDetails.totalwalletamt || 0;
  const payment_method = appDetails.payment_method;
  const store_id = 7;
  const bank_id = 0;
  const si_sub_ref_no = (appDetails.payment_method == 'applepay') ? '' : (appDetails.si_sub_ref_no || '');
  const wallet = appDetails.wallet;
  const payment_gateway = appDetails.payment_gateway;
  const payment_id = appDetails.payment_id;
  const coupon_id = appDetails.coupon_id;
  const coupon_code = appDetails.coupon_code;
  const delivery_date = appDetails.delivery_date;
  const time_slot = appDetails.time_slot;
  const del_partner_tip = appDetails.del_partner_tip || 0;
  const del_partner_instruction = appDetails.del_partner_instruction;
  const order_instruction = appDetails.order_instruction;
  const device_id = appDetails.device_id;
  const is_subscription = appDetails.is_subscription;
  const payment_status = appDetails.payment_status;
  const payment_type = appDetails.payment_type;
  const platform = appDetails.platform;
  const browser = appDetails.browser;

  // Date/time slot cut-off blocks removed as requested.

  // Parallel: storeItemList + trailDetails (PostgreSQL type-safe joins)
  const [storeItemList, trailDetails] = await Promise.all([
    knex('store_orders')
      .select('store_products.stock', 'store_orders.*')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .where('store_orders.store_approval', userIdStr)
      .where('store_orders.order_cart_id', 'incart')
      .whereNull('subscription_flag'),
    knex('tbl_trail_cart')
      .join('tbl_trail_pack_deatils', 'tbl_trail_cart.trail_id', '=', 'tbl_trail_pack_deatils.trail_id')
      .join('tbl_trail_pack_basic', knex.raw('tbl_trail_cart.trail_id = tbl_trail_pack_basic.id::text'))
      .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
      .where('tbl_trail_cart.user_id', !isNaN(userIdInt) ? userIdInt : user_id)
      .where('store_products.stock', '>', 0)
      .where('tbl_trail_cart.qty', '>', 0)
      .select('tbl_trail_pack_deatils.varient_id', 'tbl_trail_cart.trail_id', 'tbl_trail_cart.qty', 'tbl_trail_pack_basic.qty_limit', 'tbl_trail_pack_basic.discount_percentage')
  ]);

  for (const storeItem of storeItemList || []) {
    if (storeItem.stock === 0) {
      throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
    }
  }

  if (!trailDetails || trailDetails.length === 0) {
    throw new Error('Cart is empty');
  }

  const varientIdsInt = [...new Set(trailDetails.map(d => parseInt(d.varient_id, 10)).filter(n => !isNaN(n)))];
  const trailIdsInt = [...new Set(trailDetails.map(d => parseInt(d.trail_id, 10)).filter(n => !isNaN(n)))];
  const nowDate = new Date().toISOString().split('T')[0];

  // Parallel batch fetch: products, deals, trail discounts (single round-trip each)
  const [productsRows, dealsRows, trailDiscountsRows] = await Promise.all([
    varientIdsInt.length > 0
      ? knex('store_products')
        .join('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', 'product.product_id')
        .whereIn('store_products.varient_id', varientIdsInt)
        .andWhere('store_products.store_id', store_id)
        .select('store_products.varient_id', 'store_products.mrp')
      : [],
    varientIdsInt.length > 0
      ? knex('deal_product')
        .whereIn('varient_id', varientIdsInt)
        .andWhere('store_id', store_id)
        .andWhere('valid_from', '<=', nowDate)
        .andWhere('valid_to', '>', nowDate)
        .select('varient_id', 'deal_price')
      : [],
    trailIdsInt.length > 0
      ? knex('tbl_trail_pack_basic')
        .whereIn('id', trailIdsInt)
        .select('id', 'discount_percentage')
      : []
  ]);

  const productMap = (productsRows || []).reduce((acc, r) => { acc[r.varient_id] = r; return acc; }, {});
  const dealMap = (dealsRows || []).reduce((acc, r) => { acc[r.varient_id] = r; return acc; }, {});
  const discountMap = (trailDiscountsRows || []).reduce((acc, r) => { acc[String(r.id)] = r.discount_percentage; return acc; }, {});

  let totalPrice = 0;
  let trailpackamt = 0;

  for (const ProductList of trailDetails) {
    const qtyLimit = parseInt(ProductList.qty_limit, 10) || 0;
    if (ProductList.qty > qtyLimit) {
      throw new Error('No more stock available.');
    }

    const varientIdInt = parseInt(ProductList.varient_id, 10);
    const product = productMap[varientIdInt];
    if (!product) {
      throw new Error(`Product not found for varient ${ProductList.varient_id}`);
    }

    const deal = dealMap[varientIdInt];
    const price = deal ? parseFloat(deal.deal_price) : parseFloat(product.mrp || 0);
    const price1 = price * ProductList.qty;
    totalPrice += price1;

    const trailpackdiscountval = discountMap[ProductList.trail_id] ?? 0;
    trailpackamt += (parseFloat(price1) * parseFloat(trailpackdiscountval)) / 100;
  }

  const price2 = totalPrice;

  // Parallel: delivery settings + user + address
  const [deliveryFlag, deliveryChargeInfo, userdetails, address] = await Promise.all([
    knex('app_settings').where('store_id', store_id).select('cod_charges').first(),
    knex('freedeliverycart').where('store_id', store_id).first(),
    knex('users').where('id', !isNaN(userIdInt) ? userIdInt : user_id).select('name', 'email').first(),
    knex('address').where('address.address_id', parseInt(address_id, 10)).select('address.receiver_phone', 'address.house_no', 'address.state').first()
  ]);

  if (!userdetails) {
    throw new Error('User not found');
  }
  if (!address) {
    throw new Error('Address not found');
  }

  const codCharges = (payment_method === 'COD' || payment_method === 'cod') ? (deliveryFlag?.cod_charges ?? 0) : 0;
  let charge = 0;
  if (deliveryChargeInfo) {
    charge = (parseFloat(deliveryChargeInfo.min_cart_value || 0) <= price2) ? 0 : parseFloat(deliveryChargeInfo.delivery_charge || 0);
  }

  let couponDiscountAmount = 0;
  if (coupon_code) {
    const coupon = await knex('coupon').where('coupon_code', coupon_code).first();
    if (!coupon) {
      throw new Error('Invalid coupon code');
    }
    const am = coupon.amount;
    const type = coupon.type;
    const p = 1 * price2;
    if (type == '%' || type == 'Percentage' || type == 'percentage') {
      couponDiscountAmount = (p * am) / 100;
    } else {
      couponDiscountAmount = p - am;
    }
  }
  discount_amount = couponDiscountAmount;

  const rem_price = parseFloat(price2) + parseFloat(charge) + parseFloat(codCharges) + parseFloat(del_partner_tip) - parseFloat(trailpackamt);
  const fprice = parseFloat(rem_price).toFixed(2);
  const finalrem_price = parseFloat(fprice) - parseFloat(totalwalletamt);
  const pay_amount = (discount_amount != 0) ? (finalrem_price - discount_amount).toFixed(2) : finalrem_price.toFixed(2);

  const chars1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let val1 = "";
  for (let i = 0; i < 6; i++) {
    val1 += chars1.charAt(Math.floor(Math.random() * chars1.length));
  }
  const chars3 = "0123456789";
  let val3 = "";
  for (let i = 0; i < 2; i++) {
    val3 += chars3.charAt(Math.floor(Math.random() * chars3.length));
  }
  const cr1 = crypto.createHash('md5').update(String(Date.now())).digest('hex').substr(Math.floor(Math.random() * 25), 2);
  const number = val1 + val3 + cr1;
  const description = 'Order description';
  const amount = pay_amount;
  const currency = currencyval;

  const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
  const merchantpassword = process.env.TOTALPAY_PASSWORD;

  const hashData = `${number}${amount}${currency}${description}${merchantpassword}`;
  const hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');

  const customerInfo = { name: userdetails.name, email: userdetails.email };
  const billingAddress = {
    country: 'AE',
    state: 'Dubai',
    city: 'Dubai',
    address: "Dubai",
    phone: "1234567890"
  };

  const group_id = number;
  const ordertype = 'trail';
  const success_url = (appDetails.platform == "web")
    ? `https://quickartweb-production.up.railway.app/success?screen=daily`
    : `https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/successfirst?order_id=${number}`;
  const cancel_url = (appDetails.platform == "web")
    ? `https://quickartweb-production.up.railway.app/failure`
    : 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/failure';

  const custom_data = {
    ordertype, payment_status, group_id, user_id: userIdStr, address_id, bank_id, si_sub_ref_no, store_id,
    payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount,
    delivery_date, time_slot, del_partner_tip, del_partner_instruction, order_instruction, device_id,
    totalwalletamt, is_subscription, payment_type, platform, browser, storeItemList, trailDetails
  };

  const orderJson = { number, description, amount, currency };
  const mainJson = {
    merchant_key: merchantKey,
    operation: 'purchase',
    methods: payment_method == 'applepay' ? ['applepay'] : ['card'],
    success_url,
    cancel_url,
    server_callback_url: 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/paymentnotification/',
    hash,
    order: orderJson,
    customer: customerInfo,
    billing_address: billingAddress,
    custom_data
  };

  const jsonData = JSON.stringify(mainJson);
  const now = new Date();
  const [maxPayId, maxSiLogId] = await Promise.all([
    knex('payment_order_request_details').max('id as maxId').first(),
    knex('tbl_si_deduction_log').max('id as maxId').first()
  ]);
  const nextId = (maxPayId?.maxId ? parseInt(maxPayId.maxId, 10) : 0) + 1;
  const nextSiLogId = (maxSiLogId?.maxId ? parseInt(maxSiLogId.maxId, 10) : 0) + 1;

  await knex('payment_order_request_details').insert({
    id: nextId,
    json_data: jsonData,
    group_id,
    order_type: ordertype,
    added_on: now,
    datetime: now
  });

  const checkoutUrl = 'https://checkout.totalpay.global/api/v1/session';

  try {
    const fetch = (await import('node-fetch')).default;
    const response = await fetch(checkoutUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: jsonData,
    });

    const data = await response.json();
    logToFile("https://checkout.totalpay.global/api/v1/session response body (trail): " + JSON.stringify(data));

    if (!response.ok) {
      logToFile(`paymentModel fun trailPaymentData Error: Status ${response.status} - Body: ${JSON.stringify(data)}`);
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    await knex('tbl_si_deduction_log').insert({
      id: nextSiLogId,
      si_sub_ref_no: "",
      user_id: userIdStr,
      amount: String(parseFloat(amount).toFixed(2)),
      payment_method: 'PayNow',
      payment_status: '0',
      card_id: group_id,
      created_date: new Date(),
    });

    return data;
  } catch (error) {
    console.error('Error sending payment data:', error);
    throw error;
  }
};

const preparePaymentData = async (appDetails) => {
  console.log(`[PAYMENT-PREPARE] Incoming request from ${appDetails.platform}: user_id=${appDetails.user_id}, address_id=${appDetails.address_id}`);
  logToFile("preparePaymentData STARTED - appDetails Keys: " + Object.keys(appDetails).join(", "));
  const currencyval = 'AED';
  const user_id = (appDetails.user_id != "null" && appDetails.user_id) ? appDetails.user_id : appDetails.device_id;
  const userIdStr = String(user_id);
  const address_id = appDetails.address_id;
  let discount_amount = appDetails.discount_amount;
  const totalwalletamt = appDetails.totalwalletamt;
  const totalrefwalletamt = appDetails.totalrefwalletamt;
  const payment_method = appDetails.payment_method;
  const store_id = 7;
  const bank_id = 0;
  const si_sub_ref_no = appDetails.si_sub_ref_no;
  const wallet = appDetails.wallet;
  const payment_gateway = appDetails.payment_gateway;
  const payment_id = appDetails.payment_id;
  const coupon_id = appDetails.coupon_id;
  const coupon_code = appDetails.coupon_code;
  const delivery_date = appDetails.delivery_date;
  const time_slot = appDetails.time_slot;
  const del_partner_tip = appDetails.del_partner_tip || 0;
  const del_partner_instruction = appDetails.del_partner_instruction;
  const order_instruction = appDetails.order_instruction;
  const device_id = appDetails.device_id;
  const is_subscription = appDetails.is_subscription;
  const payment_status = appDetails.payment_status;
  const payment_type = "paynow";
  const platform = appDetails.platform;
  const browser = appDetails.browser;
  const exp_eta = appDetails.exp_eta;
  const parsedExpEta = Number(exp_eta);

  if (!Number.isInteger(parsedExpEta)) {
    throw new Error('exp_eta is required and must be an integer');
  }

  // Get today's date and current time in Dubai timezone
  const dubaiTime = moment.tz("Asia/Dubai");
  const todayDubai = dubaiTime.format("YYYY-MM-DD");
  const isAfter6PM = dubaiTime.hour() > 17;
  //   const isAfter12PM = dubaiTime.hour() >= 14;
  const isAfter12PM = dubaiTime.isSameOrAfter(
    moment.tz("Asia/Dubai").hour(11).minute(0).second(0)
  );

  //   if (timeSlot === undefined || timeSlot === null || timeSlot === '' || timeSlot === 'undefined' || deliveryDate === undefined || deliveryDate === null || deliveryDate === '' || deliveryDate === 'undefined') {
  //         logToFile("time slot is blank or undefined");
  //         throw new Error("Your selected date is in the past.Please select a different date & timeslot.");
  //     }
  //     // if (!time_slot || time_slot === undefined || time_slot === null || time_slot === '' || time_slot === 'undefined') {
  //     // //   || deliveryDate === undefined || deliveryDate === null || deliveryDate === '' || deliveryDate === 'undefined'
  //     //     logToFile("time slot is blank or undefined");
  //     //     throw new Error("This timeslot is not available for selected products delivery, kindly select different delivery date.");
  //     // }

  //   // Condition 1: Check if any order has a sub_delivery_date of today
  //   if (delivery_date === todayDubai && isAfter12PM) {
  //       logToFile("Check if any order has a sub_delivery_date of today");
  //   throw new Error("Unable to place order for selected date time. Please select different date and time.");
  //   }

  //   // Condition 2: If it's after 6 PM in Dubai, prevent placing orders for tomorrow with "06:00 am - 10:00 am" time slot
  //   if (isAfter6PM) {
  //   const tomorrowDubai = dubaiTime.add(1, 'day').format("YYYY-MM-DD"); // Get tomorrow's date in Dubai time
  //   if (delivery_date == tomorrowDubai && time_slot == "06:00 am - 10:00 am") {
  //       logToFile("If it's after 6 PM in Dubai, prevent placing orders for tomorrow with 06:00 am - 10:00 am time slot");
  //   throw new Error(`Unable to place order for selected date time. Please select different date and time.`);
  //   }
  //   }

  //   // Condition 3: Check if any order has a sub_delivery_date of today
  //   if (delivery_date === todayDubai && (time_slot == "06:00 am - 10:00 am" || time_slot == "02:00 pm - 05:00 pm" || time_slot == "02:00 pm - 04:00 pm")) {
  //       logToFile("Unable to place order for selected date time. Please select different date and time.");
  //   throw new Error("Unable to place order for selected date time. Please select different date and time.");
  //   }

  //stock 0 Or not check 
  //   const storeItemList = await knex('store_orders')
  //   .select('store_products.stock','store_orders.*')
  //   .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
  //   .where('store_orders.store_approval', user_id)
  //   .where('store_orders.order_cart_id', 'incart')
  //   .whereNull('subscription_flag');
  // // Iterate through the list of items and check the stock
  //   for (const storeItem of storeItemList) {
  //     if (storeItem.stock === 0) {
  //     throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
  //     }
  //   }

  const storeItemList = await knex('store_orders')
    .select('store_products.stock', 'product.hide as product_hide', 'product.is_delete as product_delete', `store_orders.store_order_id`, `store_orders.product_name`, `store_orders.varient_image`, `store_orders.quantity`, `store_orders.unit`,
      `store_orders.varient_id`, `store_orders.qty`, `store_orders.price`, `store_orders.total_mrp`, `store_orders.order_cart_id`, `store_orders.order_date`,
      `store_orders.repeat_orders`, `store_orders.store_approval`, `store_orders.store_id`, `store_orders.tx_per`, `store_orders.price_without_tax`,
      `store_orders.tx_price`, `store_orders.tx_name`, `store_orders.type`, `store_orders.repeated_order_cart`, `store_orders.incart_noti`, `store_orders.buying_price`, `store_orders.base_mrp`, `store_orders.partner_id`,
      `store_orders.device_id`, `store_orders.subscription_flag`, `store_orders.sub_time_slot`, `store_orders.sub_total_delivery`, `store_orders.percentage`, `store_orders.sub_delivery_date`,
      `store_orders.is_selected`, `store_orders.trail_id`, `store_orders.order_type`, `store_orders.isautorenew`, `store_orders.autorenewid`,
      `store_orders.discount_percentage_trail`, `store_orders.platform`, `store_orders.is_offer_product`, `store_orders.product_feature_id`)
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .where('store_orders.store_approval', userIdStr)
    .where('store_orders.order_cart_id', 'incart')
    .whereNull('store_orders.subscription_flag');
  // Iterate through the list of items and check the stock
  for (const storeItem of storeItemList) {
    if (storeItem.stock === 0 || storeItem.product_hide == 1 || storeItem.product_delete == 1) {
      logToFile("One or more items in your cart are out of stock. Unable to proceed with the order.");
      throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
    }

    if (!storeItem.sub_time_slot || storeItem.sub_time_slot === undefined || storeItem.sub_time_slot === null || storeItem.sub_time_slot === '' || storeItem.sub_time_slot === 'undefined'
      || storeItem.sub_delivery_date === undefined || storeItem.sub_delivery_date === null || storeItem.sub_delivery_date === '' || storeItem.sub_delivery_date === 'undefined') {
      logToFile("time slot is blank or undefined");
      throw new Error("This timeslot is not available for selected products delivery, kindly select different delivery date.");
    }

    // Date/time slot cut-off blocks removed as requested.

  }




  const [sumResult, deliveryFlag, deliveryChargeInfo] = await Promise.all([
    knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .where('store_products.store_id', store_id)
      .where('store_orders.store_approval', userIdStr)
      .where('store_orders.order_cart_id', 'incart')
      .whereNull('store_orders.subscription_flag')
      .select(knex.raw('SUM(store_orders.price) as sum'))
      .first(),
    knex('app_settings').where('store_id', store_id).select('cod_charges').first(),
    knex('freedeliverycart').where('store_id', store_id).first()
  ]);

  if (!sumResult || sumResult.sum == null) {
    throw new Error('Cart is empty');
  }

  const price2 = parseFloat(sumResult.sum).toFixed(2);
  const codCharges = (payment_method === 'COD' || payment_method === 'cod') ? (deliveryFlag?.cod_charges ?? 0) : 0;
  let charge = 0;
  if (deliveryChargeInfo) {
    charge = (parseFloat(deliveryChargeInfo.min_cart_value || 0) <= price2) ? 0 : parseFloat(deliveryChargeInfo.delivery_charge || 0);
  }

  const rem_price = parseFloat(price2) + parseFloat(charge) + parseFloat(codCharges) + parseFloat(del_partner_tip);
  const fprice = parseFloat(rem_price).toFixed(2);
  const finalrem_price = parseFloat(fprice) - parseFloat(totalwalletamt || 0) - parseFloat(totalrefwalletamt || 0);

  // Same logic as checkout_quickorder: coupon only on non-discounted products.
  // In checkoutModel the check is: (store_orders.price / store_orders.qty) >= store_products.mrp
  // (i.e., items whose unit price is below MRP are treated as already-discounted and excluded)
  if (coupon_code) {
    const CouponEligible = await knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .where('store_products.store_id', store_id)
      .where('store_orders.order_cart_id', 'incart')
      .where('store_orders.store_approval', userIdStr)
      .whereNull('store_orders.subscription_flag')
      .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
      .sum({ total_price: 'store_orders.price' })
      .first();

    const couponRow = await knex('coupon').where('coupon_code', coupon_code).first();
    if (!couponRow) {
      throw new Error('Invalid coupon code');
    }
    const am = couponRow.amount;
    const type = couponRow.type;
    const eligiblePrice = parseFloat(CouponEligible?.total_price ?? 0) || 0;
    if (eligiblePrice > 0) {
      if (type === '%' || type === 'Percentage' || type === 'percentage') {
        discount_amount = (eligiblePrice * parseFloat(am)) / 100;
      } else {
        discount_amount = Math.min(eligiblePrice, parseFloat(am));
      }
    } else {
      discount_amount = 0;
    }
  } else {
    discount_amount = 0;
  }

  const pay_amount = (discount_amount != 0) ? (finalrem_price - discount_amount).toFixed(2) : finalrem_price.toFixed(2);

  const minval = 10000;
  const maxval = 99999;

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

  // Combine all parts to form cart_id
  const randomNumber = val1 + val3 + cr1;
  const number = randomNumber;
  const description = 'Order description';
  const amount = pay_amount; // Ensure two decimal places
  const currency = currencyval;

  // TotalPay Credentials from Environment
  const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
  const merchantpassword = process.env.TOTALPAY_PASSWORD;

  const hashData = `${number}${amount}${currency}${description}${merchantpassword}`;
  let hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');

  const [userdetails, address, recurringData] = await Promise.all([
    knex('users').where('id', user_id).select('name', 'email').first(),
    knex('address').where('address.address_id', parseInt(address_id, 10)).select('address.receiver_phone', 'address.house_no', 'address.state').first(),
    si_sub_ref_no ? knex('tbl_user_bank_details').where('si_sub_ref_no', si_sub_ref_no).select('recurring_init_trans_id').first() : Promise.resolve(null)
  ]);

  if (!userdetails) {
    throw new Error('User not found');
  }
  if (!address) {
    throw new Error('Address not found');
  }

  const customerInfo = { name: userdetails.name, email: userdetails.email };

  // Billing Information
  const billingAddress = {
    country: 'AE',
    state: 'Dubai',
    city: 'Dubai',
    address: "Dubai",
    phone: "1234567890"
  };

  //return billingAddress;
  const group_id = number;
  const ordertype = 'quick';


  const orderJson = { number, description, amount, currency };
  const custom_data = { ordertype, payment_status, group_id, user_id, address_id, bank_id, si_sub_ref_no, store_id, payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount, delivery_date, time_slot, del_partner_tip, del_partner_instruction, order_instruction, device_id, totalwalletamt, totalrefwalletamt, is_subscription, payment_type, platform, browser, exp_eta: parsedExpEta, storeItemList }

  if (appDetails.platform == "web") {
    var success_url = `https://quickartweb-production.up.railway.app/${appDetails.successroutename}?screen=daily`;
    var cancel_url = `https://quickartweb-production.up.railway.app/${appDetails.cancelroutename}`;
  } else {
    var success_url = `https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/successfirst?order_id=${number}`
    var cancel_url = 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/failure'
  }

  let mainJson = {};
  let checkoutUrl = 'https://checkout.totalpay.global/api/v1/session';

  // Determine Payment Mode (Recurring vs ApplePay vs New Card)
  if (si_sub_ref_no && recurringData && recurringData.recurring_init_trans_id) {
    // --- SAVED CARD (RECURRING) ---
    const recurring_init_trans_id = recurringData.recurring_init_trans_id;
    // Helper: Recurring hash format: recurring_init_trans_id + recurring_token + order_number + amount + description + password
    const hashData = `${recurring_init_trans_id}${si_sub_ref_no}${number}${amount}${description}${merchantpassword}`;
    hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');

    mainJson = {
      merchant_key: merchantKey,
      recurring_init_trans_id: recurring_init_trans_id,
      recurring_token: si_sub_ref_no,
      hash,
      order: orderJson
    };
    checkoutUrl = 'https://checkout.totalpay.global/api/v1/payment/recurring';
    logToFile(`[PAYMENT] Using Recurring API for saved card: ${si_sub_ref_no}`);

  } else if (payment_method == 'applepay') {
    // --- APPLE PAY ---
    // Re-calculate hash for standard payment just in case, though it uses const hashData from above
    mainJson = {
      merchant_key: merchantKey,
      operation: 'purchase',
      methods: ['applepay'],
      success_url: success_url,
      cancel_url: cancel_url,
      server_callback_url: 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/paymentnotification/',
      hash,
      order: orderJson,
      customer: customerInfo,
      billing_address: billingAddress,
      custom_data: custom_data
    };
  } else {
    // --- NEW CARD (SESSION) ---
    mainJson = {
      merchant_key: merchantKey,
      operation: 'purchase',
      methods: ['card'],
      success_url: success_url,
      cancel_url: cancel_url,
      server_callback_url: 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/paymentnotification/',
      hash,
      order: orderJson,
      customer: customerInfo,
      billing_address: billingAddress,
      custom_data: custom_data
    };
  }

  const jsonData = JSON.stringify(mainJson);

  const now = new Date();
  const [maxPayId, maxSiLogId] = await Promise.all([
    knex('payment_order_request_details').max('id as maxId').first(),
    knex('tbl_si_deduction_log').max('id as maxId').first()
  ]);
  const nextPayId = (maxPayId?.maxId ? parseInt(maxPayId.maxId, 10) : 0) + 1;
  const nextSiLogId = (maxSiLogId?.maxId ? parseInt(maxSiLogId.maxId, 10) : 0) + 1;

  await knex('payment_order_request_details').insert({
    id: nextPayId,
    json_data: jsonData,
    group_id,
    order_type: ordertype,
    added_on: now,
    datetime: now
  });

  try {
    const fetch = (await import('node-fetch')).default;
    const response = await fetch(checkoutUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: jsonData,
    });

    const data = await response.json();
    logToFile("https://checkout.totalpay.global/api/v1/session response body (prepare): " + JSON.stringify(data));

    if (!response.ok) {
      logToFile(`paymentModel fun preparePaymentData Error: Status ${response.status} - Body: ${JSON.stringify(data)}`);
      throw new Error(`Payment gateway error ! HTTP error! Status: ${response.status}`);
    }

    await knex('tbl_si_deduction_log').insert({
      id: nextSiLogId,
      si_sub_ref_no: "",
      user_id: userIdStr,
      amount: String(parseFloat(amount).toFixed(2)),
      payment_method: 'PayNow',
      payment_status: '0',
      card_id: group_id,
      created_date: now
    });

    return data;
  } catch (error) {
    console.error('Error sending payment data:', error);
    logToFile(`paymentModel fun preparePaymentData error: ${error instanceof Error ? error.stack || error.message : JSON.stringify(error)}`);
    throw error;
  }
};

const subPaymentData = async (appDetails) => {
  logToFile("subPaymentData STARTED");
  const currencyval = 'AED';
  const user_id = (appDetails.user_id != "null" && appDetails.user_id) ? appDetails.user_id : appDetails.device_id;
  const userIdStr = String(user_id);

  const ordersDetails = await knex('store_orders')
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .select(`store_orders.store_order_id`, `store_orders.product_name`, `store_orders.varient_image`, `store_orders.quantity`, `store_orders.unit`,
      `store_orders.varient_id`, `store_orders.qty`, `store_orders.price`, `store_orders.total_mrp`, `store_orders.order_cart_id`, `store_orders.order_date`,
      `store_orders.repeat_orders`, `store_orders.store_approval`, `store_orders.store_id`, `store_orders.tx_per`, `store_orders.price_without_tax`,
      `store_orders.tx_price`, `store_orders.tx_name`, `store_orders.type`, `store_orders.repeated_order_cart`, `store_orders.incart_noti`, `store_orders.buying_price`, `store_orders.base_mrp`, `store_orders.partner_id`,
      `store_orders.device_id`, `store_orders.subscription_flag`, `store_orders.sub_time_slot`, `store_orders.sub_total_delivery`, `store_orders.percentage`, `store_orders.sub_delivery_date`,
      `store_orders.is_selected`, `store_orders.trail_id`, `store_orders.order_type`, `store_orders.isautorenew`, `store_orders.autorenewid`,
      `store_orders.discount_percentage_trail`, `store_orders.platform`, `store_orders.is_offer_product`, `store_orders.product_feature_id`)
    .where('store_orders.store_approval', userIdStr)
    .where('store_orders.subscription_flag', 1)
    .where('store_orders.order_cart_id', 'incart');

  // Get today's date and current time in Dubai timezone
  const dubaiTime = moment.tz("Asia/Dubai");
  const todayDubai = dubaiTime.format("YYYY-MM-DD");
  const isAfter6PM = dubaiTime.hour() > 17;

  // Condition 1: Check if any order has a sub_delivery_date of today
  ordersDetails.forEach(order => {
    if (order.sub_delivery_date === todayDubai) {
      logToFile("Unable to place order for selected date time. Please select different date and time.");
      throw new Error("Unable to place order for selected date time. Please select different date and time.");
    }
  });


  // Condition 2: If it's after 6 PM in Dubai, prevent placing orders for tomorrow with "06:00 am - 10:00 am" time slot
  if (isAfter6PM) {
    const tomorrowDubai = dubaiTime.add(1, 'day').format("YYYY-MM-DD"); // Get tomorrow's date in Dubai time
    ordersDetails.forEach(order => {
      if (order.sub_delivery_date == tomorrowDubai && order.sub_time_slot == "06:00 am - 10:00 am") {
        logToFile("If it's after 6 PM in Dubai, prevent placing orders for tomorrow with 06:00 am - 10:00 am time slot");
        throw new Error(`Unable to place order for selected date time. Please select different date and time.`);
      }
    });
  }

  const storeItems = await knex('store_orders')
    .select('store_products.stock', 'product.hide as product_hide', 'product.is_delete as product_delete', `store_orders.store_order_id`, `store_orders.product_name`, `store_orders.varient_image`, `store_orders.quantity`, `store_orders.unit`,
      `store_orders.varient_id`, `store_orders.qty`, `store_orders.price`, `store_orders.total_mrp`, `store_orders.order_cart_id`, `store_orders.order_date`,
      `store_orders.repeat_orders`, `store_orders.store_approval`, `store_orders.store_id`, `store_orders.tx_per`, `store_orders.price_without_tax`,
      `store_orders.tx_price`, `store_orders.tx_name`, `store_orders.type`, `store_orders.repeated_order_cart`, `store_orders.incart_noti`, `store_orders.buying_price`, `store_orders.base_mrp`, `store_orders.partner_id`,
      `store_orders.device_id`, `store_orders.subscription_flag`, `store_orders.sub_time_slot`, `store_orders.sub_total_delivery`, `store_orders.percentage`, `store_orders.sub_delivery_date`,
      `store_orders.is_selected`, `store_orders.trail_id`, `store_orders.order_type`, `store_orders.isautorenew`, `store_orders.autorenewid`,
      `store_orders.discount_percentage_trail`, `store_orders.platform`, `store_orders.is_offer_product`, `store_orders.product_feature_id`)
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .where('store_orders.store_approval', userIdStr)
    .where('store_orders.order_cart_id', 'incart')
    .where('store_orders.subscription_flag', 1);

  for (const storeItem of storeItems) {
    if (storeItem.stock === 0 || storeItem.product_hide == 1 || storeItem.product_delete == 1) {
      logToFile("One or more items in your cart are out of stock. Unable to proceed with the order.");
      throw new Error(`One or more items in your cart are out of stock. Unable to proceed with the order.`);
    }
  }

  const is_subscription = appDetails.is_subscription;
  const address_id = appDetails.address_id;
  const bank_id = 0;
  const si_sub_ref_no = appDetails.si_sub_ref_no;
  const store_id = 7;
  const payment_method = appDetails.payment_method;
  const wallet = appDetails.wallet;
  const payment_id = appDetails.payment_id;
  const payment_gateway = appDetails.payment_gateway;
  const coupon_id = appDetails.coupon_id;
  const coupon_code = appDetails.coupon_code;
  let discount_amount = appDetails.discount_amount;
  const del_partner_instruction = appDetails.del_partner_instruction;
  const order_instruction = appDetails.order_instruction;
  const device_id = appDetails.device_id;
  const payment_status = appDetails.payment_status;
  const payment_type = 'paynow';
  const platform = appDetails.platform;
  const browser = appDetails.browser;
  const totalwalletamt = appDetails.totalwalletamt;
  const totalrefwalletamt = appDetails.totalrefwalletamt;

  const [sumResult, deliveryChargeInfo] = await Promise.all([
    knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .where('store_products.store_id', store_id)
      .where('store_orders.store_approval', userIdStr)
      .where('store_orders.order_cart_id', 'incart')
      .where('store_orders.subscription_flag', 1)
      .select(knex.raw('SUM(store_orders.price) as sum'))
      .first(),
    knex('freedeliverycart').where('store_id', store_id).first()
  ]);

  if (!sumResult || sumResult.sum == null) {
    throw new Error('Cart is empty');
  }

  const price2 = parseFloat(sumResult.sum).toFixed(2);
  let charge = 0;
  if (deliveryChargeInfo) {
    charge = (parseFloat(deliveryChargeInfo.min_cart_value || 0) <= price2) ? 0 : parseFloat(deliveryChargeInfo.delivery_charge || 0);
  }

  const rem_price = parseFloat(price2) + charge;
  const reserve_amount = (parseFloat(price2) * 50) / 100;

  const ph = await knex('users')
    .select('name', 'user_phone', 'wallet', 'wallet_balance', 'referral_balance')
    .where('id', user_id)
    .first();
  let phwallet = 0;
  if (ph) {
    phwallet = ph.wallet_balance;
  } else {
    phwallet = 0;
  }

  let finalreserve_amount = 0;
  if (phwallet >= reserve_amount) {
    finalreserve_amount = reserve_amount;
  } else {
    finalreserve_amount = phwallet;
  }

  let rem_price1 = 0;
  if (wallet == 'yes') {
    rem_price1 = (rem_price - finalreserve_amount - totalrefwalletamt)
  } else {
    rem_price1 = rem_price;
  }
  const fprice = parseFloat(rem_price1).toFixed(2);
  const finalrem_price = parseFloat(fprice);

  if (coupon_code) {
    const couponRow = await knex('coupon').where('coupon_code', coupon_code).first();
    if (couponRow) {
      const am = couponRow.amount;
      const type = couponRow.type;
      const p = 1 * parseFloat(price2);
      if (type === '%' || type === 'Percentage' || type === 'percentage') {
        discount_amount = (p * am) / 100;
      } else {
        discount_amount = p - am;
      }
    } else {
      discount_amount = 0;
    }
  } else {
    discount_amount = 0;
  }

  const pay_amount = parseFloat(finalrem_price).toFixed(2);
  const minval = 10000;
  const maxval = 99999;

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

  // Combine all parts to form cart_id
  const randomNumber = val1 + val3 + cr1;
  const number = randomNumber;
  const description = 'Order description';
  const amount = pay_amount; // Ensure two decimal places
  const currency = currencyval;

  // TotalPay Credentials from Environment
  const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
  const merchantpassword = process.env.TOTALPAY_PASSWORD;

  const hashData = `${number}${amount}${currency}${description}${merchantpassword}`;
  const hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');

  const [userdetails, address, recurringData] = await Promise.all([
    knex('users').where('id', user_id).select('name', 'email').first(),
    knex('address').where('address.address_id', parseInt(address_id, 10)).select('address.receiver_phone', 'address.house_no', 'address.state').first(),
    si_sub_ref_no ? knex('tbl_user_bank_details').where('si_sub_ref_no', si_sub_ref_no).select('recurring_init_trans_id').first() : Promise.resolve(null)
  ]);

  if (!userdetails) {
    throw new Error('User not found');
  }

  const sanitizeName = (name) => (name || '').replace(/[-.'"\/\\_]/g, '');
  const customerInfo = { name: sanitizeName(userdetails.name), email: userdetails.email };
  const billingAddress = {
    country: 'AE',
    state: 'Dubai',
    city: 'Dubai',
    address: address?.house_no || 'Dubai',
    phone: address?.receiver_phone || '1234567890'
  };

  const success_url = appDetails.platform === 'web'
    ? 'https://quickartweb-production.up.railway.app/success?screen=subscription'
    : `https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/successfirst?order_id=${number}`;
  const cancel_url = appDetails.platform === 'web'
    ? 'https://quickartweb-production.up.railway.app/failure'
    : 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/failure';

  const group_id = number;
  const ordertype = 'subscription';
  const storeItemList = ordersDetails;
  const orderJson = { number, description, amount, currency };
  const custom_data = { ordertype, payment_status, group_id, user_id, address_id, bank_id, si_sub_ref_no, store_id, payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount, del_partner_instruction, order_instruction, device_id, is_subscription, payment_type, totalwalletamt, totalrefwalletamt, platform, browser, storeItemList };

  const mainJson = (payment_method === 'applepay')
    ? { merchant_key: merchantKey, operation: 'purchase', methods: ['applepay'], success_url, cancel_url, server_callback_url: 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/paymentnotification/', hash, order: orderJson, customer: customerInfo, billing_address: billingAddress, custom_data }
    : { merchant_key: merchantKey, operation: 'purchase', methods: ['card'], success_url, cancel_url, server_callback_url: 'https://zap-quickart-api-production.up.railway.app/testnodejsapp/api/paymentnotification/', hash, order: orderJson, customer: customerInfo, billing_address: billingAddress, custom_data };

  logToFile("https://checkout.totalpay.global/api/v1/session " + JSON.stringify(mainJson));

  const jsonData = JSON.stringify(mainJson);
  const checkoutUrl = 'https://checkout.totalpay.global/api/v1/session';
  const now = new Date();

  const [maxPayId, maxSiLogId] = await Promise.all([
    knex('payment_order_request_details').max('id as maxId').first(),
    knex('tbl_si_deduction_log').max('id as maxId').first()
  ]);
  const nextPayId = (maxPayId?.maxId ? parseInt(maxPayId.maxId, 10) : 0) + 1;
  const nextSiLogId = (maxSiLogId?.maxId ? parseInt(maxSiLogId.maxId, 10) : 0) + 1;

  await knex('payment_order_request_details').insert({
    id: nextPayId,
    json_data: jsonData,
    group_id,
    order_type: ordertype,
    added_on: now,
    datetime: now
  });

  try {
    const fetch = (await import('node-fetch')).default;
    const response = await fetch(checkoutUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: jsonData,
    });

    const data = await response.json();
    logToFile("https://checkout.totalpay.global/api/v1/session response body: " + JSON.stringify(data));

    if (!response.ok) {
      logToFile(`paymentModel fun subPaymentData Error: Status ${response.status} - Body: ${JSON.stringify(data)}`);
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    await knex('tbl_si_deduction_log').insert({
      id: nextSiLogId,
      si_sub_ref_no: "",
      user_id: userIdStr,
      amount: String(parseFloat(amount).toFixed(2)),
      payment_method: 'PayNow',
      payment_status: '0',
      card_id: group_id,
      created_date: now
    });

    return data;
  } catch (error) {
    logToFile(`paymentModel fun subPaymentData error: ${error instanceof Error ? error.stack || error.message : JSON.stringify(error)}`);
    console.error('Error sending payment data:', error);
    throw error;
  }
};


module.exports = {
  preparePaymentData,
  subPaymentData,
  trailPaymentData
};
