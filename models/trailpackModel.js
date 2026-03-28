const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const { format } = require('date-fns');
const wordCount = require('word-count');
const crypto = require('crypto');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
const moment = require('moment');
require('moment-timezone');
const uaeTime = moment.tz(new Date(), "Asia/Dubai");
const axios = require('axios');
const apiKey = 'AIzaSyADPEHze6hgRTG83JXfEJ6owhtNTmJJWwg'; // Replace with your Geolocation API key


const gettrailcheckout = async (appDetails) => {
  const current = new Date();

  // Destructure other details from appDetails
  const {
    user_id: userId,
    address_id: addressId,
    delivery_date: deliveryDate,
    time_slot: timeSlot,
    store_id: storeId,
    payment_method: paymentMethod,
    payment_status: paymentStatus,
    payment_id: paymentId,
    payment_gateway: paymentGateway,
    si_sub_ref_no: siSubRefNo,
    bank_id: bankId,
    payment_type: paymentType,
    group_id: paymentOrderId,
    platform: platform,
    browser: browser,
    coupon_code: couponCodeInput,
    totalwalletamt: totalWalletAmtInput,
    wallet: walletInput
  } = appDetails;

  let delPartnerTip = appDetails.del_partner_tip || 0;
  let delPartnerInstruction = appDetails.del_partner_instruction;
  let order_instruction = appDetails.order_instruction;
  let couponId = 0;
  let couponCode = couponCodeInput || null;
  let totalWalletAmt = parseFloat(totalWalletAmtInput || 0);
  let wallet = walletInput || 'no';

  // Pre-fetch max IDs for consistent manual generation
  const [maxOrderIdRes, maxSubIdRes, maxStoreOrderIdRes, maxPaymentRequestIdRes, maxSiDeductionIdRes] = await Promise.all([
    knex('orders').max('order_id as maxOrderId').first(),
    knex('subscription_order').max('id as maxId').first(),
    knex('store_orders').max('store_order_id as maxStoreOrderId').first(),
    knex('payment_order_request_details').max('id as maxId').first(),
    knex('tbl_si_deduction_log').max('id as maxId').first()
  ]);
  let nextOrderIdCounter = (maxOrderIdRes?.maxOrderId ? parseInt(maxOrderIdRes.maxOrderId, 10) : 0) + 1;
  let nextSubIdCounter = (maxSubIdRes?.maxId ? parseInt(maxSubIdRes.maxId, 10) : 0) + 1;
  let nextStoreOrderIdCounter = (maxStoreOrderIdRes?.maxStoreOrderId ? parseInt(maxStoreOrderIdRes.maxStoreOrderId, 10) : 0) + 1;
  let nextPaymentRequestIdCounter = (maxPaymentRequestIdRes?.maxId ? parseInt(maxPaymentRequestIdRes.maxId, 10) : 0) + 1;
  let nextSiDeductionIdCounter = (maxSiDeductionIdRes?.maxId ? parseInt(maxSiDeductionIdRes.maxId, 10) : 0) + 1;



  const trailDetails = await knex('tbl_trail_cart')
    .join('tbl_trail_pack_deatils', 'tbl_trail_cart.trail_id', '=', 'tbl_trail_pack_deatils.trail_id')
    .join('tbl_trail_pack_basic', knex.raw('tbl_trail_cart.trail_id = tbl_trail_pack_basic.id::text'))
    .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
    .where('tbl_trail_cart.user_id', userId)
    .where('store_products.stock', '>', 0)
    .where('tbl_trail_cart.qty', '>', 0)
    .select('tbl_trail_pack_deatils.varient_id', 'tbl_trail_cart.trail_id', 'tbl_trail_cart.qty', 'tbl_trail_pack_basic.discount_percentage', 'tbl_trail_pack_basic.qty_limit');

  console.log("Trail Details Found:", trailDetails.length, JSON.stringify(trailDetails));


  //add data in store_orders
  if (trailDetails.length > 0) {
    for (let i = 0; i < trailDetails.length; i++) {
      const ProductList = trailDetails[i];

      const varientIdInt = parseInt(ProductList.varient_id, 10);
      const product = await knex('store_products')
        .join('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', 'product.product_id')
        .where('store_products.varient_id', varientIdInt)
        .andWhere('store_products.store_id', storeId)
        .first();

      // if (ProductList.qty > product.max_ord_qty) {
      // const p_name = `${product.product_name} (${product.quantity}${product.unit}) * ${ProductList.qty}`;
      // const message = `You have to order ${p_name} quantity between ${product.min_ord_qty} to ${product.max_ord_qty}.`;
      // return message;
      // }

      const qtyLimit = (ProductList.qty_limit != null) ? parseInt(ProductList.qty_limit, 10) : 0;
      if (qtyLimit > 0 && ProductList.qty > qtyLimit) {
        const message = 'No more stock available.';
        return message;
      }



      // Check for current deal
      const now = new Date();
      const deal = await knex('deal_product')
        .where('varient_id', varientIdInt)
        .andWhere('store_id', storeId)
        .andWhere('valid_from', '<=', now.toISOString().split('T')[0])
        .andWhere('valid_to', '>', now.toISOString().split('T')[0])
        .first(); // Retrieves the first matching deal

      let price;
      if (deal) {
        price = parseFloat(deal.deal_price);
      } else {
        price = parseFloat(product.mrp);
      }

      let price2 = price * ProductList.qty;
      let price5 = product.mrp * ProductList.qty;
      let created_at = new Date();

      // Check if the order already exists in the cart
      const existingOrder = await knex('store_orders')
        .where('store_approval', userId)
        .andWhere('varient_id', varientIdInt)
        .andWhere('order_cart_id', 'incart')
        .where('order_type', 'trail')
        .whereNull('subscription_flag')
        .first();

      const productVarient = await knex('product_varient')
        .where('varient_id', varientIdInt)
        .first();
      var product_id = productVarient.product_id;

      const productDeatils = await knex('product')
        .where('product_id', product_id)
        .first();
      var cat_id = productDeatils.cat_id;
      var percentage = productDeatils.percentage;
      var availability = productDeatils.availability;
      const categoriesSubDeatils = await knex('categories')
        .where('cat_id', cat_id)
        .first();
      var percentageSubCat = (categoriesSubDeatils && categoriesSubDeatils.discount_per != null)
        ? parseFloat(categoriesSubDeatils.discount_per) : 0;
      var parent = categoriesSubDeatils ? categoriesSubDeatils.parent : null;

      const categoriesParentDeatils = parent != null ? await knex('categories')
        .where('cat_id', parent)
        .first() : null;

      var disPrice = ((price2 * ProductList.discount_percentage) / 100);
      var finalPrice = (price2 - disPrice);
      var PriceNew = parseFloat(finalPrice.toFixed(2));
      console.log(`Debug Price Calc - Varient: ${varientIdInt}, MRP: ${product.mrp}, Price: ${price}, Qty: ${ProductList.qty}, Discount%: ${ProductList.discount_percentage}, disPrice: ${disPrice}, PriceNew: ${PriceNew}`);




      const orderData = {
        store_order_id: nextStoreOrderIdCounter++,
        store_id: storeId,
        varient_id: varientIdInt,
        qty: ProductList.qty,
        product_name: product.product_name,
        varient_image: product.product_image,
        quantity: product.quantity,
        unit: product.unit,
        store_approval: userId,
        total_mrp: parseFloat(price5),
        order_cart_id: 'incart',
        order_date: created_at,
        repeat_orders: 1,
        price: parseFloat(PriceNew),
        description: product.description,
        discount_percentage_trail: disPrice,
        tx_per: 0,
        price_without_tax: 0,
        tx_price: 0,
        tx_name: 'vat',
        type: product.type,
        trail_id: ProductList.trail_id,
        order_type: 'trail',
        repeated_order_cart: '',
        platform: (platform) ? platform : ''
      };

      if (!existingOrder) {
        // Insert new order if no existing order is found
        if (ProductList.qty != 0) {
          await knex('store_orders').insert(orderData);
        }
      } else {
        // Delete existing order and insert new one
        await knex('store_orders')
          .where('store_approval', userId)
          .where('varient_id', varientIdInt)
          .where('order_cart_id', 'incart')
          .whereNull('subscription_flag')
          .where('order_type', 'trail')
          .delete();
        if (ProductList.qty != 0) {
          await knex('store_orders').insert(orderData);
        }
      }
    }
  } else {
    throw new Error("cart is empty");
  }
  //end store order

  // Determine siStatus based on siSubRefNo
  const siStatus = siSubRefNo ? "yes" : "no";
  const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
  const hashSubstring = hash.substring(0, 2); // Adjust substring length as needed
  // Combine all parts to form group_id
  const groupId = (paymentOrderId) ? paymentOrderId : generateRandomLetters(6) + generateRandomDigits(2) + hashSubstring;
  const userIdStr = String(userId);
  const [ar, user] = await Promise.all([
    knex('address')
      .select('society', 'city', 'city_id', 'society_id', 'lat', 'lng', 'address_id', 'house_no', 'receiver_email', 'landmark')
      .where('user_id', userIdStr)
      .where('address_id', addressId)
      .first(),
    knex('users')
      .select('user_phone', 'wallet', 'country_code', 'name', 'email')
      .where('id', userId)
      .first()
  ]);

  if (!ar) {
    throw new Error('No address found for the provided user ID and address ID');
  }
  if (!user) {
    throw new Error('User not found');
  }


  // Get today's date and current time in Dubai timezone
  const dubaiTime = moment.tz("Asia/Dubai");
  const todayDubai = dubaiTime.format("YYYY-MM-DD");
  const isAfter6PM = dubaiTime.hour() > 17;
  const isAfter12PM = dubaiTime.hour() > 11;

  // Condition 1: Check if any order has a sub_delivery_date of today
  if (deliveryDate === todayDubai && isAfter12PM) {
    throw new Error("Unable to place order for selected date time. Please select different date and time.");
  }


  // Condition 2: If it's after 6 PM in Dubai, prevent placing orders for tomorrow with "06:00 am - 10:00 am" time slot
  if (isAfter6PM) {
    const tomorrowDubai = dubaiTime.add(1, 'day').format("YYYY-MM-DD"); // Get tomorrow's date in Dubai time
    if (deliveryDate == tomorrowDubai && timeSlot == "06:00 am - 10:00 am") {
      throw new Error(`Unable to place order for selected date time. Please select different date and time.`);
    }
  }

  // Condition 3: Check if any order has a sub_delivery_date of today
  if (deliveryDate === todayDubai && (timeSlot === "06:00 am - 10:00 am" || timeSlot === "02:00 pm - 05:00 pm" || timeSlot === "02:00 pm - 04:00 pm")) {
    throw new Error("Unable to place order for selected date time. Please select different date and time.");
  }

  const storeItemList = await knex('store_orders')
    .select('store_products.stock', 'store_orders.*')
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .where('store_orders.store_approval', userId)
    .where('store_orders.order_cart_id', 'incart')
    .where('store_orders.order_type', 'trail')
    .whereNull('store_orders.subscription_flag');
  // Iterate through the list of items and check the stock
  for (const storeItem of storeItemList) {
    if (storeItem.stock === 0) {
      throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
    }
  }


  let actualWallet = user.wallet;
  const walletEnabled = (wallet && wallet.toLowerCase() === 'yes') && parseFloat(totalWalletAmt || 0) > 0;
  let WalletBalanace = walletEnabled ? user.wallet - totalWalletAmt : user.wallet;
  let userPhone = user.country_code + user.user_phone;
  let userName = user.name;
  let userEmail = (user.email) ? user.email : ar.receiver_email;

  const [storeDetailsAmt, deliveryFlag] = await Promise.all([
    knex('store_orders')
      .where('store_approval', String(userId))
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('order_type', 'trail')
      .select(
        knex.raw('COALESCE(SUM(total_mrp::numeric), 0) as "Totalmrp"'),
        knex.raw('COALESCE(SUM(price::numeric), 0) as "Totalprice"'),
        knex.raw('COUNT(store_order_id) as count')
      )
      .first(),
    knex('app_settings')
      .where('store_id', storeId)
      .select('cod_charges')
      .first()
  ]);

  const codCharges = (paymentMethod && paymentMethod.toLowerCase() === 'cod' && deliveryFlag && deliveryFlag.cod_charges != null) ? deliveryFlag.cod_charges : 0;

  let couponPriceAmount = 0;
  if (couponCode != null) {
    const CouponDiscount = await knex('store_orders')
      .sum({ total_price: 'price' })
      .where('price', '=', knex.ref('total_mrp'))
      .where('order_cart_id', 'incart')
      .where('order_type', 'trail')
      .where('store_approval', userId)
      .where('subscription_flag', null)
      .first();
    const CouponDetails = await knex('coupon')
      .where('coupon_code', couponCode)
      .first();
    if (CouponDiscount && CouponDetails && CouponDiscount.total_price != null && CouponDetails.amount != null) {
      couponPriceAmount = (parseFloat(CouponDiscount.total_price) * parseFloat(CouponDetails.amount)) / 100;
    }
  }
  const storeTotalPrice = (storeDetailsAmt && storeDetailsAmt.Totalprice != null) ? parseFloat(storeDetailsAmt.Totalprice) : 0;
  let TotalpriceAmount = ((storeTotalPrice + parseFloat(delPartnerTip) + parseFloat(codCharges)) - (parseFloat(couponPriceAmount) + parseFloat(totalWalletAmt)));
  if (TotalpriceAmount < 0) TotalpriceAmount = 0;
  const WithWalletAmount = parseFloat(TotalpriceAmount) + parseFloat(totalWalletAmt);

  console.log("Checkout Calculation Debug:", {
    userId,
    storeTotalPrice,
    delPartnerTip,
    codCharges,
    couponCode,
    couponPriceAmount,
    totalWalletAmt,
    TotalpriceAmount,
    WithWalletAmount
  });

  const WalletDiscountAmount = ((((storeTotalPrice + parseFloat(delPartnerTip) + parseFloat(codCharges)) - couponPriceAmount) * 50) / 100).toFixed(2);
  const WalletStatus = (wallet && wallet.toLowerCase() === 'yes' && actualWallet >= WalletDiscountAmount) ? 'percentage' : 'fixed';


  if (siStatus == 'yes' || (paymentMethod && paymentMethod.toUpperCase() == 'COD')) {
    const number = groupId;
    const description = "";
    const amount = (TotalpriceAmount) ? TotalpriceAmount : "0";
    const orderJson = { number, description, amount };
    const ordertype = "trail";
    const payment_status = (paymentMethod && paymentMethod.toLowerCase() === 'cod') ? 'Pending' : 'success';
    const group_id = groupId;
    const user_id = userId;
    const bank_id = 0;
    const si_sub_ref_no = siSubRefNo;
    const store_id = storeId;
    const payment_method = paymentMethod;
    const payment_gateway = paymentGateway;
    const payment_id = paymentId;
    const coupon_id = "";
    const coupon_code = "";
    const discount_amount = 0;
    const delivery_date = deliveryDate;
    const time_slot = timeSlot;
    const del_partner_tip = delPartnerTip;
    const del_partner_instruction = delPartnerInstruction;
    const device_id = "";
    const totalwalletamt = totalWalletAmt;
    const is_subscription = null;
    const payment_type = paymentType;
    const address_id = addressId;
    const custom_data = { address_id, ordertype, payment_status, group_id, user_id, bank_id, si_sub_ref_no, store_id, payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount, delivery_date, time_slot, del_partner_tip, del_partner_instruction, order_instruction, device_id, totalwalletamt, is_subscription, payment_type, platform, browser, storeItemList, trailDetails };

    const mainJsonSaveRequest = {
      merchant_key: "",
      operation: 'purchase',
      methods: paymentMethod,
      success_url: "",
      cancel_url: "",
      hash: "",
      order: orderJson,
      customer: "",
      billing_address: "",
      custom_data: custom_data
    };

    const insert = await knex('payment_order_request_details').insert({
      id: nextPaymentRequestIdCounter++,
      json_data: mainJsonSaveRequest,
      group_id: group_id,
      order_type: ordertype,
      added_on: new Date()
    });
  }


  if (siStatus == 'yes' && paymentMethod != 'COD') {

    const BankDetails = await knex('tbl_user_bank_details')
      .where('si_sub_ref_no', siSubRefNo)
      .where('user_id', userId)
      .first();

    if (BankDetails) {
      // Success: BankDetails found   
      // TotalPay Credentials for Test/Live
      const recurring_init_trans_id = BankDetails.recurring_init_trans_id;
      // TotalPay Credentials from Environment
      const merchantKey = process.env.TOTALPAY_MERCHANT_KEY;
      const merchantpassword = process.env.TOTALPAY_PASSWORD;

      const recurring_token = BankDetails.si_sub_ref_no;
      const pay_amounts = TotalpriceAmount;
      const orderNumber = groupId;
      const orderDescription = "Payment Deduction";
      const amount = (pay_amounts).toFixed(2); // Ensure two decimal places

      if (parseFloat(amount) <= 0) {
        console.log("Skipping recurring payment call as amount is 0.");
        await knex('tbl_si_deduction_log').insert({
          id: nextSiDeductionIdCounter++,
          si_sub_ref_no: siSubRefNo,
          user_id: userIdStr,
          amount: parseFloat(TotalpriceAmount).toFixed(2),
          payment_method: 'SI',
          payment_status: 1, // Set to 1 as it's fully covered
          card_id: groupId,
          created_date: new Date()
        });
      } else {
        const hashData = `${recurring_init_trans_id}${recurring_token}${orderNumber}${amount}${orderDescription}${merchantpassword}`;
        const hash = crypto.createHash('sha1').update(crypto.createHash('md5').update(hashData.toUpperCase()).digest('hex')).digest('hex');
        const mainJson = {
          merchant_key: merchantKey,
          recurring_init_trans_id: recurring_init_trans_id,
          recurring_token: recurring_token,
          hash,
          order: {
            number: groupId,
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

          const data = await response.json();
          console.log("TotalPay Recurring Payment Response:", data);
          const status = (data && data.status) ? data.status.toLowerCase() : null;
          if (status === 'decline' || status === 'pending') {
            const errorMessage = status === 'decline'
              ? 'Card issue detected! Status: decline'
              : `HTTP error! Status: ${data.status}`;
            throw new Error(errorMessage);
          }

          if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
          }

          await knex('tbl_si_deduction_log').insert({
            id: nextSiDeductionIdCounter++,
            si_sub_ref_no: siSubRefNo,
            user_id: userIdStr,
            amount: parseFloat(TotalpriceAmount).toFixed(2),
            payment_method: 'SI',
            payment_status: 0,
            card_id: groupId,
            created_date: new Date()
          });
          //return await response.json();
        } catch (error) {
          console.error('Error sending payment data:', error);
          throw error; // Re-throw for handling in the calling code
        }
      }


    } else {
      // No BankDetails found
      throw new Error('No bank details found for the given criteria.');
    }
  }

  // Fetch orders
  const data_array = await knex('store_orders')
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .select('store_orders.*')
    .where('store_orders.store_approval', userId)
    .whereNull('subscription_flag')
    .where('store_orders.order_type', 'trail')
    .where('store_orders.order_cart_id', "incart");




  for (const productList of data_array) {
    const { varient_id: varientId, qty: orderQty, store_order_id: storeOrderId } = productList;

    // Generate cart_id
    const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
    const cartId = generateRandomLetters(4) + generateRandomDigits(2) + hash.substring(0, 2);

    const product = await knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .select('store_orders.*', 'product.cat_id', 'product_varient.buying_price', 'store_products.mrp')
      .where('store_orders.order_cart_id', 'incart')
      .where('store_orders.varient_id', varientId)
      .where('store_orders.store_id', storeId)
      .where('store_orders.store_approval', userId)
      .where('store_orders.order_type', 'trail')
      .whereNull('subscription_flag')
      .first();

    if (!product) {
      throw new Error('Product not found');
    }

    const { price, mrp, tx_per: taxPer, tx_price: taxPrice, min_ord_qty: minOrderQty, max_ord_qty: maxOrderQty, stock, product_name: productName, quantity, unit, varient_image: varientImage } = product;
    const totalMrp = mrp; // Adjust this if needed
    let price2 = price;

    const codorderamt = (parseFloat(codCharges) > 0 && storeTotalPrice > 0) ? (price2 * parseFloat(codCharges) / storeTotalPrice) : 0;
    const delPartnerTipAmt = (parseFloat(delPartnerTip) > 0 && storeTotalPrice > 0) ? (price2 * parseFloat(delPartnerTip) / storeTotalPrice) : 0;
    let couponPriceProduct = 0;
    const CouponDiscounts = await knex('store_orders')
      .sum({ total_price: 'price' })
      .where('price', '=', knex.ref('total_mrp'))
      .where('order_cart_id', 'incart')
      .where('store_orders.varient_id', varientId)
      .where('store_approval', userId)
      .where('order_type', 'trail')
      .whereNull('subscription_flag')
      .first();

    if (couponCode && (CouponDiscounts.total_price > 0)) {
      const CouponDetails = await knex('coupon')
        .where('coupon_code', couponCode)
        .first();
      couponPriceProduct = (parseFloat(price2) * parseFloat(CouponDetails.amount)) / 100;
    }

    const trailidval = productList.trail_id;
    const TrailDiscounts = await knex('tbl_trail_pack_basic')
      .where(knex.raw('id::text'), String(trailidval))
      .select('discount_percentage')
      .first();

    let trailpackdiscountval = TrailDiscounts ? TrailDiscounts.discount_percentage : 0;
    const trailpackamt = (parseFloat(price2) * parseFloat(trailpackdiscountval)) / 100;

    const totalPrice = ((parseFloat(price2) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt)) - parseFloat(couponPriceProduct));

    let paidByWallet = 0;
    if (wallet.toLowerCase() === 'yes') {
      if (WalletStatus == 'percentage') {
        paidByWallet = (parseFloat(totalPrice) * 50) / 100;
      } else {
        paidByWallet = (parseFloat(totalPrice) * parseFloat(totalWalletAmt)) / parseFloat(WithWalletAmount);
      }
    }

    const paymentStatus = (paymentMethod && paymentMethod.toLowerCase() === 'cod') ? 'Pending' : 'success';
    const remPrice = (paymentMethod && paymentMethod.toLowerCase() === 'cod') ? (totalPrice - paidByWallet) : 0;

    const timeslotval = (productList.sub_time_slot) ? productList.sub_time_slot : timeSlot;
    const deliverydateval = (productList.sub_delivery_date) ? productList.sub_delivery_date : deliveryDate;

    const nextOrderId = nextOrderIdCounter++;

    await knex('orders')
      .insert({
        order_id: nextOrderId,
        cart_id: cartId,
        total_price: totalPrice.toFixed(2),
        price_without_delivery: totalPrice.toFixed(2),
        total_products_mrp: totalPrice.toFixed(2),
        delivery_charge: 0,
        user_id: userIdStr,
        store_id: storeId,
        rem_price: (remPrice) ? parseFloat(remPrice).toFixed(2) : 0,
        order_date: current,
        delivery_date: deliverydateval,
        time_slot: timeslotval,
        address_id: addressId,
        avg_tax_per: 0,
        total_tax_price: 0,
        total_delivery: 1,
        del_partner_tip: (delPartnerTipAmt) ? parseFloat(delPartnerTipAmt).toFixed(2) : 0,
        del_partner_instruction: delPartnerInstruction,
        order_instruction: order_instruction,
        group_id: groupId,
        is_subscription: null,
        cod_charges: (codorderamt) ? parseFloat(codorderamt).toFixed(2) : 0,
        paid_by_wallet: (paidByWallet) ? parseFloat(paidByWallet).toFixed(2) : 0,
        payment_method: paymentMethod,
        coupon_id: (couponId) ? couponId : 0,
        coupon_code: couponCode,
        trail_discount: (productList.discount_percentage_trail) ? parseFloat(productList.discount_percentage_trail).toFixed(2) : 0,
        coupon_discount: (couponPriceProduct) ? parseFloat(couponPriceProduct).toFixed(2) : 0,
        payment_status: paymentStatus,
        payment_type: paymentType,
        trail_id: productList.trail_id,
        order_type: 'trail',
        pastorecentrder: 'new',
        platform: (platform) ? platform : '',
        browser: (browser) ? browser : '',
        order_status: 'Pending'
      });

    console.log(`Inserted order for cartId: ${cartId}, nextOrderId: ${nextOrderId}`);

    await knex('store_orders')
      .where('store_order_id', storeOrderId)
      .update({ 'order_cart_id': cartId });

    const currentSubId = nextSubIdCounter++;

    await knex('subscription_order')
      .insert({
        'id': currentSubId,
        store_order_id: storeOrderId,
        cart_id: cartId,
        user_id: userId,
        order_id: nextOrderId,
        store_id: storeId,
        delivery_date: deliverydateval,
        time_slot: timeslotval,
        created_date: current,
        order_status: 'Pending',
        si_payment_flag: (paymentMethod.toLowerCase() === 'cod') ? 'no' : 'yes',
        group_id: groupId,
        subscription_id: String(currentSubId),
        platform: (platform) ? platform : '',
        browser: (browser) ? browser : ''
      });

    //Paid by Wallet Save
    if (paidByWallet > 0) {
      const maxWIdResult = await knex('wallet_history').max('w_id as maxWId').first();
      const nextWId = (maxWIdResult?.maxWId != null ? parseInt(maxWIdResult.maxWId, 10) : 0) + 1;
      await knex('wallet_history').insert({
        w_id: nextWId,
        user_id: userId,
        amount: paidByWallet.toFixed(2),
        resource: 'order_placed_wallet',
        type: 'deduction',
        group_id: groupId,
        cart_id: cartId
      })
    }

    console.log(`Successfully processed productList entry for varientId: ${varientId}`);
  }

  //Wallet amount update for and insert wallet (only when wallet is used)
  if (walletEnabled) {
    await knex('users')
      .where('id', userId)
      .update({ 'wallet': WalletBalanace });
  }

  await knex('tbl_trail_cart')
    .where('user_id', userId)
    .delete();

  const orderMessage = "Thank you for your order! Your order " + groupId + " is now being processed & will be delivered right on schedule.  ";
  sendSMSOrder(userPhone, orderMessage);

  const totalCal = await knex('orders')
    .where('group_id', groupId)
    .sum('coupon_discount as coupon_discount')
    .sum('rem_price as rem_price')
    .sum('paid_by_wallet as paid_by_wallet')
    .sum({ cod_charges: knex.raw('cod_charges::numeric') })
    .sum('total_products_mrp as total_products_mrp');

  const storeOrders = await knex('store_orders')
    .select('store_orders.*', 'orders.group_id', 'orders.time_slot', 'orders.total_delivery')
    .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('orders.group_id', groupId);

  const totalCalRow = (Array.isArray(totalCal) && totalCal[0]) ? totalCal[0] : {};
  const finalAmount = (totalCalRow.total_products_mrp != null) ? parseFloat(totalCalRow.total_products_mrp).toFixed(2) : '0.00';
  const [logo, currency] = await Promise.all([
    knex('tbl_web_setting').first(),
    knex('currency').first()
  ]);
  const appName = logo ? logo.name : null;
  const currencySign = currency ? currency.currency_sign : null;

  const year = current.getFullYear();
  const month = String(current.getMonth() + 1).padStart(2, '0');
  const day = String(current.getDate()).padStart(2, '0');
  const formattedDate = `${year}-${month}-${day}`;

  const templateData = {
    baseurl: process.env.BASE_URL,
    group_id: groupId,
    user_name: userName,
    user_email: userEmail,
    paymentMethod: paymentMethod,
    delivery_date: deliveryDate,
    orderss_address: (ar.house_no || '') + ', ' + (ar.landmark || '') + ', ' + (ar.society || ''),
    store_orderss: storeOrders,
    coupon_discount: (totalCalRow.coupon_discount != null) ? Number(totalCalRow.coupon_discount).toFixed(2) : '0.00',
    paid_by_wallet: (totalCalRow.paid_by_wallet != null) ? Number(totalCalRow.paid_by_wallet).toFixed(2) : '0.00',
    cod_charges: (totalCalRow.cod_charges != null) ? Number(totalCalRow.cod_charges).toFixed(2) : '0.00',
    final_amount: finalAmount,
    app_name: appName,
    currency_sign: currencySign,
    order_type: 'trail',
    order_date: formattedDate,
  };
  const subject = 'Order Successfully Placed'
  // Trigger the email after order is placed
  //sendMail = await codorderplacedMail(userEmail,templateData,subject,groupId);

  return "success";
};

//Generate random letters, digits, and hash substring
const generateRandomLetters = (length) => {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
};

const generateRandomDigits = (length) => {
  const chars = "0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
};

const sendSMSOrder = async (phoneNumber, message) => {
  const user_name = process.env.SMARTVISION_LOGIN;
  const password = process.env.SMARTVISION_PASSWORD;
  const user_phone = phoneNumber; // Replace with the actual phone number
  // const Contacts = "971" + user_phone;
  const SenderId = "Quickart";
  const url = `https://rslr.connectbind.com:8443/bulksms/bulksms?username=${user_name}&password=${password}&type=0&dlr=1&destination=${user_phone}&source=${SenderId}&message=${encodeURIComponent(message)}`;
  axios.get(url)
    .then(response => {
      console.log('Response:', response.data);
    })
    .catch(error => {
      console.error('Error:', error);
    });
};

const showTrialpack = async (appDetatils) => {
  const { user_id, device_id } = appDetatils;

  // const cartItems = await knex('tbl_trail_cart')
  // .where('user_id', user_id);

  const orderlist = await knex('orders')
    .where('order_type', 'LIKE', 'trail')
    .where('user_id', user_id)
    .whereNotIn('order_status', ['Payment_failed', 'Cancelled'])
    .groupBy('trail_id')
    .pluck('trail_id');

  // PostgreSQL: WHERE trail_id IN () is invalid; only run delete when list is non-empty
  if (orderlist && orderlist.length > 0) {
    await knex('tbl_trail_cart')
      .whereIn('trail_id', orderlist)
      .where('user_id', user_id)
      .delete();
  }

  const today = new Date().toISOString().split('T')[0];
  const tbl_trail_pack_basic_list = await knex('tbl_trail_pack_basic')
    .where('tbl_trail_pack_basic.start_date', '<=', today)
    .where('tbl_trail_pack_basic.end_date', '>=', today)
    .where('tbl_trail_pack_basic.is_delete', 0)
    .pluck('id');

  // PostgreSQL: Casting integer IDs to text for comparison with trail_id (text)
  if (tbl_trail_pack_basic_list && tbl_trail_pack_basic_list.length > 0) {
    const stringIds = tbl_trail_pack_basic_list.map(id => String(id));
    await knex('tbl_trail_cart')
      .whereNotIn('trail_id', stringIds)
      .where('user_id', user_id)
      .delete();
  }

  const cartItems = await knex('tbl_trail_cart')
    .where('user_id', user_id)
    .select('trail_id');

  if (cartItems) {
    for (let i = 0; i < cartItems.length; i++) {

      const ProductList = cartItems[i];
      // Count distinct varient_ids that have stock in at least one store (store_products has one row per store per varient)
      const cartItemsval = await knex('tbl_trail_cart')
        .leftJoin('tbl_trail_pack_deatils', 'tbl_trail_cart.trail_id', '=', 'tbl_trail_pack_deatils.trail_id')
        .leftJoin('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
        .where('tbl_trail_cart.user_id', user_id)
        .where('tbl_trail_pack_deatils.trail_id', ProductList.trail_id)
        .where('store_products.stock', '>', 0)
        .distinct('tbl_trail_pack_deatils.varient_id');

      const tbl_trail_pack_deatils = await knex('tbl_trail_pack_deatils')
        .where('trail_id', ProductList.trail_id)
        .select('varient_id');

      if (cartItemsval.length !== tbl_trail_pack_deatils.length) {
        await knex('tbl_trail_cart')
          .where('trail_id', ProductList.trail_id)
          .where('user_id', user_id)
          .delete();
      }
    }
  }



  // const cartItems = await knex('store_orders')
  // .where('store_approval', user_id)
  // .where('order_cart_id', 'incart')
  // .whereNull('subscription_flag'); // Correctly checking for NULL values

  const sum = await knex('tbl_trail_cart')
    .join('tbl_trail_pack_deatils', 'tbl_trail_cart.trail_id', '=', 'tbl_trail_pack_deatils.trail_id')
    .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
    // .join('product_varient','store_products.varient_id','=','product_varient.varient_id')
    // .join('product','product_varient.product_id','=','product.product_id')
    .where('tbl_trail_cart.user_id', user_id)
    .where('store_products.stock', '>', 0)
    .where('tbl_trail_cart.qty', '>', 0)
    //.select(knex.raw('SUM(store_products.mrp) as Totalmrp'),knex.raw('SUM(store_products.price) as Totalprice'))
    .select(
      knex.raw('SUM(store_products.mrp * tbl_trail_cart.qty) as Totalmrp'),
      knex.raw('SUM(store_products.mrp * tbl_trail_cart.qty) as Totalprice')
    )
    .first();

  // PostgreSQL: SUM returns null when no rows; avoid null refs
  const totalSum = sum || { Totalmrp: 0, Totalprice: 0 };
  const Totalmrp = totalSum.Totalmrp != null ? parseFloat(totalSum.Totalmrp) : 0;
  const Totalprice = totalSum.Totalprice != null ? parseFloat(totalSum.Totalprice) : 0;

  const baseurl = process.env.BUNNY_NET_IMAGE;
  // PostgreSQL: trail_id is text, tbl_trail_pack_basic.id is integer
  const cartItems1 = await knex('tbl_trail_cart')
    .join('tbl_trail_pack_basic', knex.raw('tbl_trail_cart.trail_id = tbl_trail_pack_basic.id::text'))
    .select(
      'tbl_trail_cart.trail_id',
      'tbl_trail_pack_basic.title',
      knex.raw(`CONCAT('${baseurl}', tbl_trail_pack_basic.image) as product_image`),
      'tbl_trail_cart.qty as cart_qty',
      'tbl_trail_pack_basic.discount_percentage'
    )
    .where('tbl_trail_cart.user_id', user_id)
    .where('tbl_trail_cart.qty', '>', 0);



  // Parallel fetch for lower latency: wallet, reserve amounts, and city names in one round-trip batch
  const [walletResult, reserveAmounts, cityRow] = await Promise.all([
    user_id !== "null" && user_id
      ? knex('users').select('wallet').where('id', user_id).first()
      : Promise.resolve(null),
    knex('orders')
      .innerJoin('subscription_order', 'subscription_order.cart_id', '=', 'orders.cart_id')
      .select('orders.reserve_amount')
      .where('orders.is_subscription', 1)
      .where('orders.user_id', user_id)
      .groupBy('orders.order_id'),
    knex('city')
      .where('status', 1)
      .select(
        knex.raw("STRING_AGG(city_name, ',') as \"cityName\""),
        knex.raw("STRING_AGG(arabic_name, ',') as \"cityNameA\"")
      )
      .first()
  ]);

  let walletamt = (walletResult && walletResult.wallet != null) ? parseFloat(walletResult.wallet) : 0;
  let total_reserve_amt = 0;
  for (const amount of reserveAmounts || []) {
    total_reserve_amt += parseFloat(amount.reserve_amount || 0);
  }
  const total_wallet = walletamt - total_reserve_amt;

  // PostgreSQL: null-safe city lists (single query for both)
  const cityNameList = (cityRow && cityRow.cityName) ? cityRow.cityName.split(',') : [];
  const cityNameListA = (cityRow && cityRow.cityNameA) ? cityRow.cityNameA.split(',') : [];

  if (cartItems.length > 0) {
    // Process or return cart items

    // Fetch the latest order for the user
    const orderlist = await knex('orders')
      .where('user_id', user_id)
      .orderBy('order_id', 'DESC')
      .select('address_id', 'si_sub_ref_no')
      .first();
    let lastAddress = [];
    let users_acc_details;
    if (orderlist) {
      // Check if the address from the latest order exists
      const lastAdd = await knex('address')
        .select('address_id', 'type', 'house_no', 'landmark', 'lat', 'lng', knex.raw(`CONCAT('${baseurl}', doorimage) as doorimage`))
        .where('address_id', orderlist.address_id)
        .where('select_status', '!=', 2)
        .first(); // .first() to retrieve a single address

      // Step 3: Check if the address's city name exists in the city lists and validate using Geocoding API
      let updatedAddresses = [];
      if (lastAdd) {
        updatedAddresses = await Promise.all(
          [lastAdd].map(async (address) => {
            let cityExists =
              cityNameList.includes(address.city) || cityNameListA.includes(address.city);

            if (!cityExists) {
              const formattedAddress = await getFormattedAddress(address.lat, address.lng);
              if (formattedAddress) {
                // Dynamically check if the formatted address contains any city from the dynamic city lists
                cityExists = cityNameList.concat(cityNameListA).some(city =>
                  formattedAddress.includes(city)
                );
              }
            }
            return {
              ...address,
              cityExists // true or false based on both checks
            };
          })
        );
      }



      lastAddress = updatedAddresses;

      const orderlistSI = await knex('orders')
        .where('user_id', user_id)
        .whereNotNull('si_sub_ref_no')
        .orderBy('order_id', 'DESC')
        .select('address_id', 'si_sub_ref_no')
        .first();
      if (orderlistSI) {
        users_acc_details = await knex('tbl_user_bank_details')
          .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
          .where('user_id', user_id)
          .where('si_sub_ref_no', orderlistSI.si_sub_ref_no)
          .where('bank_type', 'totalpay')
          .where('is_delete', '!=', 1)
          .first();
        if (!users_acc_details) {
          users_acc_details = await knex('tbl_user_bank_details')
            .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
            .where('user_id', user_id)
            .where('bank_type', 'totalpay')
            .where('is_delete', '!=', 1)
            .first();
        }
      } else {
        users_acc_details = await knex('tbl_user_bank_details')
          .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
          .where('user_id', user_id)
          .where('bank_type', 'totalpay')
          .where('is_delete', '!=', 1)
          .first();
      }

    } else {
      lastAddress = [];
      users_acc_details = await knex('tbl_user_bank_details')
        .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
        .where('user_id', user_id)
        .where('bank_type', 'totalpay')
        .where('is_delete', '!=', 1)
        .first();
    }

    //Time slot 
    const dates = [];
    const currentDate = moment().tz('Asia/Dubai');
    const currentTime = new Date();

    // Generate the next 5 days' dates in Dubai time
    for (let i = 0; i <= 4; i++) {
      dates.push(currentDate.clone().add(i, 'days').format('YYYY-MM-DD'));
    }

    const dateList = dates;
    const today = currentDate.format('YYYY-MM-DD');
    const customizedProductData1 = [];

    // Get current time in HH:mm format
    const hours = String(currentTime.getHours()).padStart(2, '0');
    const minutes = String(currentTime.getMinutes()).padStart(2, '0');
    const currentTimeStr = `${hours}:${minutes}`;
    // Get the current date and time in Dubai time zone (UTC+4)
    const currentDateTime = moment().tz("Asia/Dubai");
    // Extract hours and minutes for current time in Dubai
    const currentTimes = currentDateTime.format("HH:mm");


    // Determine tomorrow's date
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const formattedTomorrow = tomorrow.toISOString().slice(0, 10);

    // Prefetch all timeslot variants in one round-trip for lower latency
    const timeSlotsBase = () => knex('tbl_time_slots').where('status', 0).select('time_slots').orderBy('seq', 'ASC');
    const [slotsId5, slotsId4_5, slotsId1_4_5] = await Promise.all([
      timeSlotsBase().clone().where('id', 5),
      timeSlotsBase().clone().whereIn('id', [4, 5]),
      timeSlotsBase().clone().whereIn('id', [1, 4, 5])
    ]);

    for (let m = 0; m < dateList.length; m++) {
      const selectedDate = dateList[m];
      let timeslots = [];
      if (today === selectedDate && currentTimes < "11:00") {
        timeslots = slotsId5;
      } else if (formattedTomorrow === selectedDate && currentTimes >= "17:00") {
        timeslots = slotsId4_5;
      } else {
        timeslots = slotsId1_4_5;
      }
      if (timeslots.length > 0) {
        customizedProductData1.push({ date: selectedDate, timeslots });
      }
    }


    for (const ProductList of cartItems1) {
      const { varient_id } = ProductList;

      // Handle wishlist, notify me, and cart quantity
      let isSubscriptions = 'false';

      if (user_id !== "null") {

        isSubscriptions = 'false';
      }

      const cartItemsprice = await knex('tbl_trail_cart')
        .join('tbl_trail_pack_deatils', 'tbl_trail_cart.trail_id', '=', 'tbl_trail_pack_deatils.trail_id')
        .join('tbl_trail_pack_basic', knex.raw('tbl_trail_cart.trail_id = tbl_trail_pack_basic.id::text'))
        .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
        .where('tbl_trail_cart.user_id', user_id)
        .where('tbl_trail_cart.trail_id', ProductList.trail_id)
        .where('store_products.stock', '>', 0)
        .where('tbl_trail_cart.qty', '>', 0)
        .sum({ ford_price: knex.raw('store_products.mrp * tbl_trail_cart.qty') });

      const fordPrice = cartItemsprice && cartItemsprice[0] && cartItemsprice[0].ford_price != null ? parseFloat(cartItemsprice[0].ford_price) : null;
      if (fordPrice != null) {
        const discountPercentage = ProductList.discount_percentage != null ? parseFloat(ProductList.discount_percentage) : 0;
        ProductList.discount_ord_price = fordPrice * (1 - discountPercentage / 100);
        ProductList.ord_price = fordPrice;
      } else {
        ProductList.discount_ord_price = 0;
        ProductList.ord_price = 0;
      }
    }




    // Remove empty objects and limit to 4 results
    const filteredTimeSlots = customizedProductData1.slice(0, 4);

    const trailpacklist = await knex('tbl_trail_cart')
      .join('tbl_trail_pack_basic', knex.raw('tbl_trail_cart.trail_id = tbl_trail_pack_basic.id::text'))
      .where('tbl_trail_cart.user_id', user_id)
      .sum({ traildiscount: knex.raw('tbl_trail_pack_basic.discount_percentage::numeric') });

    const traildiscountval = (trailpacklist && trailpacklist[0] && trailpacklist[0].traildiscount != null) ? parseFloat(trailpacklist[0].traildiscount) : 0;

    const trailpacklistData = await knex('tbl_trail_cart')
      .join('tbl_trail_pack_basic', knex.raw('tbl_trail_cart.trail_id = tbl_trail_pack_basic.id::text'))
      .where('tbl_trail_cart.user_id', user_id);

    let totalPaidAmount = 0;
    for (const trailpack of trailpacklistData || []) {
      const traildiscount = trailpack.discount_percentage != null ? parseFloat(trailpack.discount_percentage) : 0;
      const trailID = trailpack.trail_id;
      const result = await knex('tbl_trail_pack_deatils')
        .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
        .where('tbl_trail_pack_deatils.trail_id', trailID)
        .sum({ mrpamount: 'store_products.mrp' })
        .first();
      const mrpamount = (result && result.mrpamount != null) ? parseFloat(result.mrpamount) : 0;
      totalPaidAmount += mrpamount - (mrpamount * traildiscount / 100);
    }



    const freegiftlist = [];
    const mighthavemissed = [];
    const customizedProduct = {
      timeslotsdata: filteredTimeSlots,
      lastadd: lastAddress || [],
      lastcarddetails: users_acc_details || {},
      wallet_balance: total_wallet,
      discountonmrp: 0.00,
      total_price: Totalprice,
      discount_total_price: totalPaidAmount,
      total_mrp: Totalmrp,
      saving_price: (Totalmrp - Totalprice).toFixed(2),
      // total_items:sum.count,
      free_delivery: "0.00",
      total_tax: "0.00",
      avg_tax: "0.00",
      delivery_charge: "0.00",
      subscription_fee: "0.00",
      vat: "0.00",
      data: cartItems1,
      free_gift_list: freegiftlist,
      might_have_missed: mighthavemissed
    }
    return customizedProduct;

  } else {
    // Handle the case where no items are found
    return 2;
  }
};

//Function to fetch address details using Google Maps API
const getFormattedAddress = async (lat, lng) => {
  const url = `https://maps.googleapis.com/maps/api/geocode/json?latlng=${lat},${lng}&key=${apiKey}`;
  try {
    const response = await axios.get(url);
    const data = response.data;
    if (data.results.length > 0) {
      return data.results[0].formatted_address;
    }
    return null;
  } catch (error) {
    console.error('Error fetching the geolocation:', error);
    return null;
  }
};

const addtrailpack = async (appDetatils) => {
  const { trail_id, user_id, qty } = appDetatils;

  // Step 1: Get array of ids
  const trailIds = await knex('tbl_trail_pack_basic')
    .whereNot('status', 1)
    .pluck('id'); // returns an array of ids

  // Step 2: Delete from tbl_trail_cart where user_id matches and trail_id in array
  if (trailIds && trailIds.length > 0) {
    const stringTrailIds = trailIds.map(id => String(id));
    await knex('tbl_trail_cart')
      .where('user_id', user_id)
      .whereIn('trail_id', stringTrailIds)
      .delete();
  }

  const trail_pack_basic = await knex('tbl_trail_pack_basic')
    .where(knex.raw('id::text'), String(trail_id))
    .select('qty_limit', 'id')
    .first();

  if (!trail_pack_basic) {
    throw new Error('Trail pack not found');
  }

  const qtyLimit = parseInt(trail_pack_basic.qty_limit, 10) || 0;
  if (qty > qtyLimit) { // Check if the requested quantity exceeds the available stock
    throw new Error('No more stock available');
  }

  const tbl_trail_pack_deatils_list = await knex('tbl_trail_pack_deatils')
    .where('trail_id', String(trail_pack_basic.id))
    .pluck('varient_id');

  const storeproduct = await knex('store_products')
    .whereIn('varient_id', tbl_trail_pack_deatils_list)
    .where('stock', 0)
    .first();

  if (storeproduct) {
    //throw new Error('Stock is not available for some products in this trial pack');
    return 0;
  }

  const check = await knex('tbl_trail_cart')
    .where('trail_id', trail_id)
    .where('user_id', user_id)
    .first();
  if (check) {

    if (appDetatils.qty == 0) {

      await knex('tbl_trail_cart')
        .where('trail_id', trail_id)
        .where('user_id', user_id)
        .delete();

      return 'Remove from Trail Pack'

    } else {

      updatetrial = await knex('tbl_trail_cart')
        .where('trail_id', trail_id)
        .where('user_id', user_id)
        .update({ 'qty': appDetatils.qty });

      return 'Added to Trail Pack'

    }

  }
  else {
    const maxIdRow = await knex('tbl_trail_cart').max('id as max_id').first();
    const nextId = (maxIdRow && maxIdRow.max_id != null ? parseInt(maxIdRow.max_id, 10) : 0) + 1;
    const addtrial = await knex('tbl_trail_cart').insert({
      id: nextId,
      trail_id: trail_id,
      user_id: user_id,
      qty: qty
    })

    return 'Added to Trail Pack'

  }

};

const trailPackList = async (appDetatils) => {

  const { user_id } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  // Fetch all products in one go

  const orderlist = await knex('orders')
    .where('order_type', 'LIKE', 'trail')
    .where('user_id', user_id)
    .whereNotIn('order_status', ['Payment_failed', 'Cancelled'])
    .groupBy('trail_id')
    .pluck('trail_id')

  const storeproduct = await knex('tbl_trail_pack_deatils')
    .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = CAST(store_products.varient_id AS TEXT)'))
    .whereRaw('CAST(store_products.stock AS INTEGER) = ?', [0])
    .pluck('tbl_trail_pack_deatils.varient_id');

  const trialidlist = await knex('tbl_trail_pack_deatils')
    .whereIn('varient_id', storeproduct)
    .pluck('tbl_trail_pack_deatils.trail_id');

  const uniqueValues = trialidlist.filter((value, index, self) => self.indexOf(value) === index);
  //const today = new Date()
  const today = new Date().toISOString().split('T')[0];
  const trailDetails = await knex('tbl_trail_pack_basic')
    .select('id', 'title', knex.raw(`CONCAT('${baseurl}', image) as image`), 'tbl_trail_pack_basic.description', 'tbl_trail_pack_basic.qty_limit')
    .where('tbl_trail_pack_basic.status', 1)
    .where('tbl_trail_pack_basic.start_date', '<=', today)
    .where('tbl_trail_pack_basic.end_date', '>=', today)
    .where('tbl_trail_pack_basic.is_delete', 0)
    .whereNotIn(knex.raw('id::text'), uniqueValues)
    .whereNotIn(knex.raw('id::text'), orderlist)
    .orderBy('main_order', 'ASC');

  //return trailDetails;                 
  const customizedProductData = [];
  if (trailDetails.length > 0) {
    for (let i = 0; i < trailDetails.length; i++) {
      const trailList = trailDetails[i];
      const CartQtyList = await knex('tbl_trail_cart')
        .where('trail_id', trailList.id)
        .sum({ totalQty: 'qty' })
        .first();


      const cartQty = CartQtyList && CartQtyList.totalQty != null ? CartQtyList.totalQty : 0;

      const stock = trailList.qty_limit;

      const deliveryFlag = await knex('app_settings')
        .where('store_id', 7)
        .select('cod_charges')
        .first();

      const customizedProduct = {
        id: trailList.id,
        title: trailList.title,
        image: trailList.image,
        description: trailList.description,
        cartQty: cartQty,
        stock: stock,
        user_id: user_id,
        codcharges: deliveryFlag ? deliveryFlag.cod_charges : null
      };

      customizedProductData.push(customizedProduct);
    }
  }
  // Return empty array when no trail packs available (valid case), don't throw
  return customizedProductData;

};

const trailPackDetails = async (appDetails) => {
  const { trail_id, user_id } = appDetails;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const today = new Date().toISOString().split('T')[0];
  // Fetch basic trail pack details


  const orderlist = await knex('orders')
    .where('order_type', 'LIKE', 'trail')
    .where('user_id', user_id)
    .whereNotIn('order_status', ['Payment_failed', 'Cancelled'])
    .groupBy('trail_id')
    .pluck('trail_id')

  const basicDetails = await knex('tbl_trail_pack_basic')
    .select(
      'id',
      'title',
      knex.raw(`CONCAT('${baseurl}', image) as image`),
      'description',
      'qty_limit'
    )
    .where('status', 1)
    .where('tbl_trail_pack_basic.start_date', '<=', today)
    .andWhere('tbl_trail_pack_basic.end_date', '>=', today)
    .andWhere('is_delete', 0)
    .andWhere(knex.raw('id::text'), String(trail_id))
    .whereNotIn(knex.raw('id::text'), orderlist)
    .first();

  if (basicDetails) {
    // Fetch product details within the trail pack
    // const productDetails = await knex('tbl_trail_pack_deatils as tpd')
    //   .distinct('p.product_id', 'p.product_name')
    //   .select(
    //     knex.raw(`CONCAT('${baseurl}', p.product_image) as product_image`),
    //     'sp.mrp',
    //     'sp.price',
    //     'p.fcat_id',
    //     'tpb.discount_percentage',
    //     knex.raw('ROUND(sp.price * (1 - tpb.discount_percentage / 100), 2) as discounted_price')
    //   )
    //   .join('tbl_trail_pack_basic as tpb', 'tpd.trail_id', 'tpb.id')
    //   .join('product as p', 'p.product_id', 'tpd.product_id')
    //   .join('product_varient as pv', 'pv.product_id', 'p.product_id')
    //   .join('store_products as sp', 'pv.varient_id', 'sp.varient_id')
    //   .where('tpd.trail_id', trail_id);

    const productDetails = await knex('tbl_trail_pack_deatils')
      .distinct('product.product_id', 'product.product_name')
      .select(
        knex.raw(`CONCAT('${baseurl}', product.product_image) as product_image`),
        'store_products.mrp',
        'store_products.price',
        'tbl_trail_pack_basic.discount_percentage',
        'product.fcat_id',
        knex.raw('ROUND((store_products.mrp * (1 - tbl_trail_pack_basic.discount_percentage::numeric / 100))::numeric, 2) as discounted_price')
      )
      .join('tbl_trail_pack_basic', knex.raw('tbl_trail_pack_deatils.trail_id = tbl_trail_pack_basic.id::text'))
      .join('product_varient', knex.raw('tbl_trail_pack_deatils.varient_id = CAST(product_varient.varient_id AS TEXT)'))
      .join('product', 'product.product_id', 'product_varient.product_id')
      .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .where('tbl_trail_pack_deatils.trail_id', trail_id)
      .whereRaw('CAST(store_products.stock AS INTEGER) > ?', [0])
      .where('product.hide', 0)
      .where('product.is_delete', 0);


    const customizedProductData = [];

    for (let i = 0; i < productDetails.length; i++) {
      const ProductList = productDetails[i];
      if (ProductList.fcat_id != null) {
        fcatinput = ProductList.fcat_id;
        const resultArray = fcatinput.split(',').map(Number);
        const ftaglist = await knex('feature_categories')
          .whereIn('id', resultArray)
          .where('status', 1)
          .where('is_deleted', 0)
          .select('id', knex.raw(`CONCAT('${baseurl}', image) as image`))
        feature_tags = ftaglist;
      } else {
        feature_tags = [];
      }

      const discountedPriceVal = parseFloat(ProductList.discounted_price) || 0;
      const savingpriceVal = (parseFloat(ProductList.price) || 0) - discountedPriceVal;
      const customizedProduct = {
        stock: ProductList.stock,
        product_id: ProductList.product_id,
        product_name: ProductList.product_name,
        product_image: ProductList.product_image,
        price: ProductList.price,
        mrp: ProductList.mrp,
        discount_percentage: parseFloat(ProductList.discount_percentage) || 0,
        discounted_price: discountedPriceVal,
        savingprice: Number.isFinite(savingpriceVal) ? savingpriceVal : 0,
        feature_tags: feature_tags
        // Add or modify properties as needed
      };

      customizedProductData.push(customizedProduct);

    }

    // Calculate total discounted price for all products in the trail pack
    // const totalDiscountedPrice = await knex('tbl_trail_pack_deatils as tpd')
    //   .join('tbl_trail_pack_basic as tpb', 'tpd.trail_id', 'tpb.id')
    //   .join('product as p', 'p.product_id', 'tpd.product_id')
    //   .join('product_varient as pv', 'pv.product_id', 'p.product_id')
    //   .join('store_products as sp', 'pv.varient_id', 'sp.varient_id')
    //   .where('tpd.trail_id', trail_id)
    //   .sum({ total_discounted_price: knex.raw('ROUND(sp.price * (1 - tpb.discount_percentage / 100), 2)') })
    //   .first();

    const totalDiscountedPrice = await knex('tbl_trail_pack_deatils as tpd')
      .join('tbl_trail_pack_basic as tpb', knex.raw('tpd.trail_id = tpb.id::text'))
      .join('store_products as sp', knex.raw('tpd.varient_id = CAST(sp.varient_id AS TEXT)'))
      .where('tpd.trail_id', trail_id)
      .sum({ total_discounted_price: knex.raw('ROUND((sp.price * (1 - tpb.discount_percentage::numeric / 100))::numeric, 2)') })
      .sum({ total_price: knex.raw('sp.price') })
      .first();
    const CartQtyList = await knex('tbl_trail_cart')
      .where('trail_id', trail_id)
      .where('user_id', user_id)
      .sum({ totalQty: 'qty' })
      .first();
    const cartQty = CartQtyList && CartQtyList.totalQty != null ? CartQtyList.totalQty : 0;

    const stock = basicDetails.qty_limit;
    total_saving_price = totalDiscountedPrice.total_price - totalDiscountedPrice.total_discounted_price;
    return {
      id: basicDetails.id,
      title: basicDetails.title,
      image: basicDetails.image,
      description: basicDetails.description,
      total_discounted_price: totalDiscountedPrice.total_discounted_price,
      total_saving_price: total_saving_price,
      product_details: productDetails,
      product_details: customizedProductData,
      cartQty: cartQty,
      stock: stock
    };
  } else {
    throw new Error('Trail Pack Not found');
  }

};

module.exports = {
  trailPackList,
  trailPackDetails,
  addtrailpack,
  showTrialpack,
  gettrailcheckout
};
