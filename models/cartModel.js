const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const { format } = require('date-fns');
const wordCount = require('word-count');
const moment = require('moment');
require('moment-timezone');
const uaeTime = moment.tz(new Date(), "Asia/Dubai");
const axios = require('axios');
const apiKey = 'AIzaSyADPEHze6hgRTG83JXfEJ6owhtNTmJJWwg'; // Replace with your Geolocation API key
const logToFile = require("../utils/logger");
const { generateCartHashKey, generateSubCartHashKey, hSet, hDel, hGetAll } = require('../utils/redisClient');

const ONE_HOUR = 3600;

/** 
 * Cart Sync Helper
 * Updates a single item in Redis HASH, but triggers a full sync from DB if HASH is missing.
 * Now supports synchronous execution for immediate consistency.
 */
const syncCartItem = async (user_id, store_id, varient_id, qty, is_subscription) => {
  if (user_id === "null" || !user_id) return;

  const cartHashKey = is_subscription ? generateSubCartHashKey(user_id, store_id) : generateCartHashKey(user_id, store_id);

  try {
    // Check if HASH is initialized
    const existing = await hGetAll(cartHashKey);

    // If HASH is completely missing or not initialized marker is present
    if (Object.keys(existing).length === 0 || !existing._initialized) {
      const query = knex('store_orders')
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where('store_id', store_id)
        .select('varient_id', 'qty');

      if (is_subscription) {
        // PostgreSQL cast to ensure compatibility with both int and text columns
        query.whereRaw('subscription_flag::text = ?', ['1']);
      } else {
        query.whereNull('subscription_flag');
      }

      const cartFromDb = await query;

      // Use string '_initialized' to match redisDecorationHelper.js
      const data = { _initialized: '1' };
      cartFromDb.forEach(c => {
        data[c.varient_id.toString()] = c.qty.toString();
      });

      // Synchronize all fields
      await Promise.all(Object.entries(data).map(([field, val]) => hSet(cartHashKey, field, val, ONE_HOUR * 24)));
    } else {
      // Update or delete single item
      if (qty != 0) {
        await hSet(cartHashKey, varient_id.toString(), qty.toString(), ONE_HOUR * 24);
      } else {
        await hDel(cartHashKey, varient_id.toString());
      }
    }
  } catch (err) {
    console.error('Cart Sync Error:', err);
  }
};

/** Get next store_order_id using MAX+1 (for primary key on insert) */
const getNextStoreOrderId = async () => {
  const maxResult = await knex('store_orders').max('store_order_id as maxId').first();
  const maxId = maxResult?.maxId != null ? Number(maxResult.maxId) : 0;
  return maxId + 1;
};

/** Get next sub_id using MAX+1 (for primary key on subscribe_product insert) */
const getNextSubId = async () => {
  const maxResult = await knex('subscribe_product').max('sub_id as maxId').first();
  const maxId = maxResult?.maxId != null ? Number(maxResult.maxId) : 0;
  return maxId + 1;
};

const addtosubCart = async (appDetatils) => {
  const { user_id, qty, store_id, varient_id, is_subscription, percentage, device_id, repeat_orders, time_slot, sub_totaldelivery, start_delivery_date, isAutoRenew, platform, product_feature_id } = appDetatils;

  //Delete Quickart Items 
  await knex('store_orders').where('varient_id', varient_id)
    .where('order_cart_id', 'incart')
    .where('store_approval', user_id)
    .whereNull('subscription_flag')
    .delete();

  // Sync Redis: Remove from Daily Cart when moving to Subscription Cart (Awaited for immediate sync)
  const cartHashKey = generateCartHashKey(user_id, store_id);
  await hDel(cartHashKey, varient_id.toString());

  // Fetch product variant details
  const productItems = await knex('product_varient')
    .join('product', 'product.product_id', 'product_varient.product_id')
    .where('varient_id', varient_id)
    .where('product.availability', '!=', 'quick')
    .first();
  if (productItems) {
    if (user_id != "null") {

      if (is_subscription == 1) {
        let date_ob = new Date();
        const sub_id = await getNextSubId();
        const addsubscription = await knex('subscribe_product').insert({
          sub_id: sub_id,
          store_id: appDetatils.store_id,
          varient_id: appDetatils.varient_id,
          user_id: user_id,
          created_at: date_ob,
          updated_at: date_ob,
          percentage: 0
        })
      }
      // Query the user information
      const user = await knex('users')
        .select('user_phone', 'wallet')
        .where('id', user_id)
        .first();

      // Check if the user is found
      if (!user) {
        throw new Error("User not Found");
      }

      const product = await knex('store_products')
        .join('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', 'product.product_id')
        .where('store_products.varient_id', varient_id)
        .andWhere('store_products.store_id', store_id)
        .first(); // Retrieves the first matching record

      if (qty > product.max_ord_qty) {
        const p_name = `${product.product_name} (${product.quantity}${product.unit}) * ${qty}`;
        const message = `You have to order ${p_name} quantity between ${product.min_ord_qty} to ${product.max_ord_qty}.`;
        return message;
      }

      if (qty > product.stock) { // Check if the requested quantity exceeds the available stock
        //const message = 'No more stock available.';
        //return message;
        throw new Error("No more stock available");
      }

      // Check for current deal
      const now = new Date();
      const deal = await knex('deal_product')
        .where('varient_id', varient_id)
        .andWhere('store_id', store_id)
        .andWhere('valid_from', '<=', now.toISOString().split('T')[0])
        .andWhere('valid_to', '>', now.toISOString().split('T')[0])
        .first(); // Retrieves the first matching deal

      // Offer products must be valid only for today's date in UAE (Asia/Dubai)
      const dubaiToday = moment().tz('Asia/Dubai').format('YYYY-MM-DD');
      const productOfferDate = product.offer_date
        ? moment(product.offer_date).tz('Asia/Dubai').format('YYYY-MM-DD')
        : null;
      const isTodayOffer = product.is_offer_product == 1 && productOfferDate === dubaiToday;

      let price;
      if (deal) {
        price = parseFloat(deal.deal_price).toFixed(2);
      } else if (isTodayOffer) {
        price = parseFloat(product.offer_price).toFixed(2);
      } else {
        price = parseFloat(product.price).toFixed(2);
      }

      if (isTodayOffer) {
        product.mrp = product.offer_price;
      }

      let created_at = new Date();
      // Calculate repeat orders days
      let repeat_orders_days = repeat_orders ? wordCount(repeat_orders) : 1;
      const final_sub_totaldelivery = (sub_totaldelivery && sub_totaldelivery != 0) ? sub_totaldelivery : 1;
      // Calculate price5
      const price5 = product.mrp * qty * repeat_orders_days * final_sub_totaldelivery;
      // Calculate price2
      const price2 = price * qty * repeat_orders_days * final_sub_totaldelivery;

      // Check if the order already exists in the cart
      const existingOrder = await knex('store_orders')
        .where('store_approval', user_id)
        .where('varient_id', varient_id)
        .where('order_cart_id', 'incart')
        .where('subscription_flag', 1)
        .first();

      // Calculate discounts based on product, subcategory, or category
      const productVarient = await knex('product_varient').where('varient_id', varient_id).first();
      const product_id = productVarient.product_id;

      const productDetails = await knex('product').where('product_id', product_id).first();
      const cat_id = productDetails.cat_id;
      const percentages = productDetails.percentage;

      const subCategoryDetails = await knex('categories').where('cat_id', cat_id).first();
      const percentageSubCat = subCategoryDetails.discount_per;
      const parentCategoryId = subCategoryDetails.parent;

      const parentCategoryDetails = await knex('categories').where('cat_id', parentCategoryId).first();
      const percentageCat = parentCategoryDetails.discount_per;
      let percentageStore = 0;

      let PriceNew;
      if (percentages > 0) {
        PriceNew = (price5 - (price5 * percentages) / 100).toFixed(2);
        percentageStore = percentages;
      } else if (percentageSubCat > 0) {
        PriceNew = (price5 - (price5 * percentageSubCat) / 100).toFixed(2);
        percentageStore = percentageSubCat;
      } else if (percentageCat > 0) {
        PriceNew = (price5 - (price5 * percentageCat) / 100).toFixed(2);
        percentageStore = percentageCat;
      } else {
        PriceNew = price2;
      }

      if (repeat_orders_days == 1) {
        repeat_orders_days = "";
      }

      // Define order data
      const orderData = {
        store_id: store_id,
        varient_id: varient_id,
        qty: qty,
        product_name: product.product_name,
        varient_image: product.product_image,
        quantity: product.quantity,
        unit: product.unit,
        store_approval: user_id,
        total_mrp: parseFloat(price5),
        order_cart_id: 'incart',
        order_date: created_at,
        repeat_orders: repeat_orders,
        price: parseFloat(PriceNew),
        description: product.description,
        tx_per: 0,
        price_without_tax: 0,
        tx_price: 0,
        tx_name: 'vat',
        type: product.type,
        repeated_order_cart: '',
        subscription_flag: 1,
        percentage: percentageStore,
        sub_time_slot: time_slot,
        sub_total_delivery: final_sub_totaldelivery,
        sub_delivery_date: start_delivery_date,
        isautorenew: isAutoRenew,
        platform: (appDetatils.platform) ? appDetatils.platform : '',
        is_offer_product: isTodayOffer ? 1 : 0,
        product_feature_id: (product_feature_id) ? product_feature_id : null
      };

      if (!existingOrder) {
        // Insert new order if no existing order is found
        if (qty != 0) {
          orderData.store_order_id = await getNextStoreOrderId();
          await knex('store_orders').insert(orderData);
        }
      } else {
        // Delete existing order and insert new one
        await knex('store_orders')
          .where('store_approval', user_id)
          .where('varient_id', varient_id)
          .where('order_cart_id', 'incart')
          .where('subscription_flag', 1)
          .delete();
        if (qty != 0) {
          orderData.store_order_id = await getNextStoreOrderId();
          await knex('store_orders').insert(orderData);
        }
      }

      // Sync Redis: Update or Delete from Subscription Cart HASH (Awaited for immediate consistency)
      await syncCartItem(user_id, store_id, varient_id, qty, true);

      const sum = await knex('store_orders')
        .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .where('store_products.store_id', store_id)
        .where('store_orders.store_approval', user_id)
        .where('store_orders.order_cart_id', 'incart')
        .where('subscription_flag', 1)
        .select(knex.raw('SUM(store_orders.total_mrp) as totalmrp'), knex.raw('SUM(store_orders.price) as totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
        .first();

      if (sum.totalprice < 30) {
        const deletedSubOfferItems = await knex('store_orders')
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('is_offer_product', 1)
          .whereIn('varient_id', function () {
            this.select('pv.varient_id')
              .from('product_varient as pv')
              .join('product as p', 'p.product_id', 'pv.product_id')
              .where('p.is_offer_product', 1)
              .whereRaw("DATE(p.offer_date) = ?", [dubaiToday]);
          })
          .where('subscription_flag', 1)
          .returning(['store_id', 'varient_id'])
          .delete();

        // Sync Redis subcart HASH for all offer items deleted by threshold rule.
        if (deletedSubOfferItems && deletedSubOfferItems.length) {
          await Promise.all(
            deletedSubOfferItems.map((item) => {
              const deleteStoreId = item.store_id || store_id;
              const subCartHashKey = generateSubCartHashKey(user_id, deleteStoreId);
              return hDel(subCartHashKey, item.varient_id.toString());
            })
          );
        }
      }

      // Handle cases where the result might be null
      const customizedProduct = {
        saving_price: (sum.totalmrp || 0) - (sum.totalprice || 0),
        total_price: sum.totalprice || 0,
        total_items: Number(sum.count) || 0,
        isAutoRenew: isAutoRenew
      };
      return customizedProduct;

    }
    else {

      throw new Error("User ID is invalid.");
    }
  } else {
    throw new Error("No more product available.");
  }
}

const showsubCart = async (appDetatils) => {
  const { user_id, device_id } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const subFlagVal = 1;

  // Batch 1: Run cartItems, sum, checkcartitemstock, cartItems1, wallet, reserve, city in parallel
  const [cartItems, sum, checkcartitemstock, cartItems1, walletBalance, reserveAmounts, cityNamesResult, cityNamesAResult] = await Promise.all([
    knex('store_orders').where('store_approval', user_id).where('order_cart_id', 'incart').whereRaw('subscription_flag::text = ?', [String(subFlagVal)]),
    knex('store_orders').where('store_approval', user_id).where('order_cart_id', 'incart').whereRaw('subscription_flag::text = ?', [String(subFlagVal)])
      .select(knex.raw('SUM(total_mrp) as totalmrp'), knex.raw('SUM(price) as totalprice'), knex.raw('COUNT(store_order_id) as count')).first(),
    knex('store_orders')
      .join('store_products', function () {
        this.on('store_orders.varient_id', '=', 'store_products.varient_id')
          .andOn('store_orders.store_id', '=', 'store_products.store_id');
      })
      .where('store_orders.store_approval', user_id)
      .whereRaw('store_orders.subscription_flag::text = ?', [String(subFlagVal)])
      .where('store_orders.order_cart_id', 'incart')
      .where('store_products.stock', 0)
      .pluck('store_orders.store_order_id'),
    knex('store_orders')
      .join('store_products', function () {
        this.on('store_orders.varient_id', '=', 'store_products.varient_id')
          .andOn('store_orders.store_id', '=', 'store_products.store_id');
      })
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .select('store_orders.store_order_id', 'store_orders.isautorenew', 'product.percentage', 'product.cat_id', 'store_orders.sub_time_slot', 'store_orders.sub_total_delivery',
        'store_orders.sub_delivery_date', 'store_orders.repeated_order_cart', 'store_orders.product_name', knex.raw(`CONCAT('${baseurl}', store_orders.varient_image) as varient_image`),
        'store_orders.quantity', 'store_orders.unit', 'store_orders.total_mrp', 'store_products.price', 'store_products.mrp', 'store_orders.qty as cart_qty', 'store_orders.total_mrp',
        'store_orders.order_cart_id', 'store_orders.order_date', 'store_orders.store_approval', 'store_orders.store_id', 'store_orders.varient_id', 'product.product_id', 'store_products.stock',
        'store_orders.tx_per', 'store_orders.price_without_tax', 'store_orders.tx_price', 'store_orders.tx_name', knex.raw(`CONCAT('${baseurl}', product.product_image) as product_image`)
        , knex.raw(`CONCAT('${baseurl}', product.thumbnail) as thumbnail`), 'product.available_days', 'product_varient.description', 'product.type', 'store_orders.price as ord_price',
        'store_orders.sub_total_delivery', 'store_orders.repeat_orders as repeat_orders', 'store_orders.product_feature_id', 'product.hide', 'product.is_delete')
      .where('store_orders.store_approval', user_id)
      .whereRaw('store_orders.subscription_flag::text = ?', [String(subFlagVal)])
      .where('store_orders.order_cart_id', 'incart')
      .where('store_products.stock', '>', 0)
      .where('store_orders.qty', '>', 0),
    user_id !== "null" ? knex('users').select('wallet', 'wallet_balance', 'referral_balance').where('id', user_id).first() : Promise.resolve(null),
    knex('orders')
      .innerJoin('subscription_order', 'subscription_order.cart_id', '=', 'orders.cart_id')
      .select(knex.raw('MAX(orders.reserve_amount) as reserve_amount'))
      .where('orders.is_subscription', 1)
      .where('orders.user_id', user_id)
      .groupBy('orders.order_id'),
    knex('city').where('status', 1).select(knex.raw("STRING_AGG(city_name, ',') as \"cityName\"")).first(),
    knex('city').where('status', 1).select(knex.raw("STRING_AGG(arabic_name, ',') as \"cityName\"")).first()
  ]);

  if (checkcartitemstock && checkcartitemstock.length > 0) {
    await knex('store_orders').whereIn('store_order_id', checkcartitemstock).delete();
  }

  // Early exit: no subscription cart items
  if (!cartItems || cartItems.length === 0) {
    return 2;
  }

  let total_reserve_amt = 0;
  for (const amount of (reserveAmounts || [])) {
    total_reserve_amt += parseFloat(amount.reserve_amount || 0);
  }
  let walletamt = 0;
  let refwalletamt = 0;
  if (walletBalance) {
    walletamt = walletBalance.wallet_balance || 0;
    refwalletamt = walletBalance.referral_balance || 0;
  }
  const total_wallet = walletamt;





  const cityNameList = (cityNamesResult?.cityName || '').split(',').filter(Boolean);
  const cityNameListA = (cityNamesAResult?.cityName || '').split(',').filter(Boolean);

  if (cartItems1 && cartItems1.length > 0) {
    // Process or return cart items - fetch orderlist, orderlistSI, deliveryFlag, noticeList in parallel
    const baseurl = process.env.BUNNY_NET_IMAGE;
    const [orderlist, orderlistSI, deliveryFlag, noticeList] = await Promise.all([
      knex('orders').where('user_id', user_id).orderBy('order_id', 'DESC').select('address_id', 'si_sub_ref_no').first(),
      knex('orders').where('user_id', user_id).whereNotNull('si_sub_ref_no').orderBy('order_id', 'DESC').select('address_id', 'si_sub_ref_no').first(),
      knex('app_settings').where('store_id', 7).select('cod_charges', 'wallet_deduction_percentage').first(),
      knex('tbl_notice').select('message').where('type', '2').first()
    ]);
    let lastAddress = [];
    let users_acc_details;
    if (orderlist) {
      // Check if the address from the latest order exists
      const baseurl = process.env.BUNNY_NET_IMAGE;
      const lastAdd = await knex('address')
        .select('address_id', 'type', 'house_no', 'landmark', 'lat', 'lng', knex.raw(`CONCAT('${baseurl}', doorimage) as doorimage`))
        .where('address_id', orderlist.address_id)
        .where('select_status', '!=', 2)
        .first(); // .first() to retrieve a single address

      // Step 3: Check if the address's city name exists in the city lists and validate using Geocoding API (300ms timeout)
      let updatedAddresses = [];
      if (lastAdd) {
        updatedAddresses = await Promise.all(
          [lastAdd].map(async (address) => {
            let cityExists =
              cityNameList.includes(address.city) || cityNameListA.includes(address.city);

            if (!cityExists) {
              const formattedAddress = await getFormattedAddressWithTimeout(address.lat, address.lng);
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
      if (orderlistSI) {
        users_acc_details = await knex('tbl_user_bank_details')
          .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
          .where('user_id', user_id)
          .where('si_sub_ref_no', orderlistSI.si_sub_ref_no)
          .where('bank_type', 'totalpay')
          .whereNot('is_delete', 1)
          .first();
        if (!users_acc_details) {
          users_acc_details = await knex('tbl_user_bank_details')
            .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
            .where('user_id', user_id)
            .where('bank_type', 'totalpay')
            .whereNot('is_delete', 1)
            .first();
        }
      } else {
        users_acc_details = await knex('tbl_user_bank_details')
          .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
          .where('user_id', user_id)
          .where('bank_type', 'totalpay')
          .whereNot('is_delete', 1)
          .first();
      }

    } else {
      lastAddress = [];
      users_acc_details = await knex('tbl_user_bank_details')
        .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
        .where('user_id', user_id)
        .where('bank_type', 'totalpay')
        .whereNot('is_delete', 1)
        .first();
    }
    let totalSubscriptionPrice = 0;
    let finalPriceSubFinal = 0;
    let isAutoRenew = "no";
    const currentDate = new Date();

    // Batch prefetch for lower latency: deals, categories, feature values
    const varientStorePairs = [...new Set(cartItems1.map(i => `${i.varient_id}_${i.store_id}`))];
    const catIds = [...new Set(cartItems1.map(i => i.cat_id).filter(Boolean))];
    const featureIds = [...new Set(cartItems1.map(i => i.product_feature_id).filter(Boolean))];

    const pairs = varientStorePairs.map(p => p.split('_').map(x => parseInt(x, 10)));
    const dealPlaceholders = pairs.map(() => '(?, ?)').join(', ');
    const dealBindings = pairs.flat();

    const [dealsList, categoriesMap, featureMap] = await Promise.all([
      pairs.length ? knex('deal_product')
        .whereRaw(`(varient_id, store_id) IN (${dealPlaceholders})`, dealBindings)
        .where('valid_from', '<=', currentDate)
        .where('valid_to', '>', currentDate)
        .select('varient_id', 'store_id', 'deal_price') : Promise.resolve([]),
      (async () => {
        const map = {};
        if (catIds.length) {
          const cats = await knex('categories').whereIn('cat_id', catIds).select('cat_id', 'discount_per', 'parent');
          cats.forEach(c => { map[c.cat_id] = c; });
          const parents = [...new Set(cats.map(c => c.parent).filter(Boolean))];
          if (parents.length) {
            const parentCats = await knex('categories').whereIn('cat_id', parents).select('cat_id', 'discount_per');
            parentCats.forEach(c => { map[`p_${c.cat_id}`] = c; });
          }
        }
        return map;
      })(),
      featureIds.length ? knex('tbl_feature_value_master').whereIn('id', featureIds).select('id', 'feature_value')
        .then(rows => Object.fromEntries(rows.map(r => [r.id, r.feature_value]))) : Promise.resolve({})
    ]);

    const dealMap = Object.fromEntries((Array.isArray(dealsList) ? dealsList : []).map(d => [`${d.varient_id}_${d.store_id}`, d]));

    // Batch prefetch wishlist and notify_me for all cart varients (avoids N+1)
    const varientIds = [...new Set(cartItems1.map(i => i.varient_id))];
    const [wishlistVarients, notifyMeVarients] = user_id !== "null" ? await Promise.all([
      knex('wishlist').where('user_id', user_id).whereIn('varient_id', varientIds).pluck('varient_id'),
      knex('product_notify_me').where('user_id', user_id).whereIn('varient_id', varientIds).pluck('varient_id')
    ]) : [[], []];
    const wishlistSet = new Set(wishlistVarients || []);
    const notifyMeSet = new Set(notifyMeVarients || []);

    const priceUpdates = []; // collect for batch update
    const currentDateTime = moment().tz("Asia/Dubai");
    const currentTime = currentDateTime.format("HH:mm");
    const tomorrowDate = currentDateTime.clone().add(1, 'days').format("YYYY-MM-DD");

    for (const ProductList of cartItems1) {
      const { varient_id, store_id, cart_qty: qty, product_feature_id } = ProductList;
      let repeatOrders, repeatOrdersDays, length;

      const deal = dealMap[`${varient_id}_${store_id}`];
      let price = deal ? deal.deal_price : ProductList.price;
      let mrpprice = ProductList.mrp; // deal_product has no mrp column; use store_products.mrp
      let mrppriceTotal = mrpprice * qty;
      // return ProductList.repeat_orders;
      if (ProductList.repeat_orders != null) {

        repeatOrders = ProductList.repeat_orders;

        repeatOrdersDays = repeatOrders ? wordCount(repeatOrders) : 1;
      } else {

        repeatOrders = '';
        repeatOrdersDays = 1;
      }


      const subTotalDelivery = ProductList.sub_total_delivery || 1;
      const priceWithTax = price * qty * repeatOrdersDays * subTotalDelivery;
      let finalPrice = priceWithTax;

      // Handle category, sub-category, and product-level discounts (using prefetched data)
      const cat_id = ProductList.cat_id;
      const percentage = parseFloat(ProductList.percentage) || 0;

      const categoriesSubDetails = cat_id ? categoriesMap[cat_id] : null;
      const percentageSubCat = parseFloat(categoriesSubDetails?.discount_per) || 0;
      const parent = categoriesSubDetails?.parent;

      const categoriesParentDetails = parent != null ? categoriesMap[`p_${parent}`] : null;
      const percentageCat = parseFloat(categoriesParentDetails?.discount_per) || 0;

      let priceAfterDiscount;
      let appliedPercentage;

      if (percentage > 0) {
        priceAfterDiscount = ((mrppriceTotal - (mrppriceTotal * percentage) / 100) * repeatOrdersDays * subTotalDelivery).toFixed(2);
        appliedPercentage = percentage;
      } else if (percentageSubCat > 0) {
        priceAfterDiscount = ((mrppriceTotal - (mrppriceTotal * percentageSubCat) / 100) * repeatOrdersDays * subTotalDelivery).toFixed(2);
        appliedPercentage = percentageSubCat;
      } else if (percentageCat > 0) {
        priceAfterDiscount = ((mrppriceTotal - (mrppriceTotal * percentageCat) / 100) * repeatOrdersDays * subTotalDelivery).toFixed(2);
        appliedPercentage = percentageCat;
      } else {
        priceAfterDiscount = finalPrice.toFixed(2);
        appliedPercentage = 0;
      }

      priceUpdates.push({ varient_id, price: priceAfterDiscount, store_order_id: ProductList.store_order_id });
      ProductList.isFavourite = wishlistSet.has(varient_id) ? 'true' : 'false';
      ProductList.notifyMe = notifyMeSet.has(varient_id) ? 'true' : 'false';

      // Calculate discount percentage and savings
      const discountPercentage = ProductList.total_mrp
        ? 100 - ((ProductList.price * ProductList.cart_qty * 100) / ProductList.total_mrp)
        : 0;

      const savings = ProductList.mrp > ProductList.price
        ? ProductList.mrp - ProductList.price
        : 0;

      const sub_price = (ProductList.total_mrp * appliedPercentage) / 100;
      const finalSubPrice = (ProductList.total_mrp - sub_price).toFixed(2);

      if (ProductList.repeat_orders != null) {
        const items = ProductList.repeat_orders.split(',').map(item => item.trim());
        length = items.length;
      } else {
        length = 1;
      }

      const finalSubPriceNew = ((finalSubPrice / ProductList.sub_total_delivery) / length);

      finalPriceSubFinal += (finalSubPriceNew * length) * ProductList.sub_total_delivery;

      ProductList.subscription_price = sub_price
        ? (finalSubPriceNew / qty).toFixed(2)
        : parseFloat(price).toFixed(2);



      totalSubscriptionPrice += parseFloat(ProductList.subscription_price);

      // Update the product list details
      ProductList.isSubscription = "True";
      ProductList.percentage = ProductList.percentage;

      const selectedDate = ProductList.sub_delivery_date;
      if (tomorrowDate === selectedDate && currentTime >= "17:00") {
        ProductList.timeSlot = null;
        ProductList.no_of_week = null;
        ProductList.delivery_date = null;
        ProductList.sub_time_slot = null;
        ProductList.sub_total_delivery = null;
        ProductList.sub_delivery_date = null;
        //  ProductList.repeat_orders = null;
      } else {
        ProductList.timeSlot = ProductList.sub_time_slot;
        ProductList.no_of_week = ProductList.sub_total_delivery;
        ProductList.delivery_date = ProductList.sub_delivery_date;
      }

      // ProductList.selectedDate = selectedDate;
      // ProductList.tomorrowDate = tomorrowDate;
      // ProductList.currentTime = currentTime;
      ProductList.discountper = discountPercentage;
      ProductList.save_on = savings;

      if (ProductList.hide == 1 || ProductList.is_delete == 1) {
        ProductList.stock = 0;
      }

      if (ProductList.isautorenew == "yes") {
        isAutoRenew = "yes";
      }

      ProductList.product_feature_id = (product_feature_id) ? product_feature_id : null;
      ProductList.product_feature_value = product_feature_id ? (featureMap[product_feature_id] || null) : null;
    }

    // Batch update store_orders prices (parallel updates - much faster than sequential)
    if (priceUpdates.length > 0) {
      await Promise.all(priceUpdates.map(u =>
        knex('store_orders')
          .where('store_order_id', u.store_order_id)
          .where('store_approval', user_id)
          .whereRaw('subscription_flag::text = ?', ['1'])
          .where('order_cart_id', 'incart')
          .update({ price: parseFloat(u.price) })
      ));
    }

    const freegiftlist = [];
    const mighthavemissed = [];

    const totalPriceVal = Number(sum?.totalprice) || 0;
    const totalMrpVal = Number(sum?.totalmrp) || 0;
    const customizedProduct = {
      lastadd: lastAddress || [],
      lastcarddetails: users_acc_details || {},
      wallet_balance: total_wallet,
      referral_balance: refwalletamt,
      discountonmrp: "0.00",
      total_price: totalPriceVal,
      total_mrp: totalMrpVal,
      saving_price: (totalMrpVal - totalPriceVal).toFixed(2),
      total_items: Number(sum?.count) || 0,
      free_delivery: "0.00",
      total_tax: "0.00",
      avg_tax: "0.00",
      delivery_charge: "0.00",
      subscription_fee: "0.00",
      vat: "0.00",
      data: cartItems1,
      free_gift_list: freegiftlist,
      might_have_missed: mighthavemissed,
      isAutoRenew: isAutoRenew,
      wallet_deduction_percentage: deliveryFlag?.wallet_deduction_percentage ?? 0,
      noticeList: noticeList
    }
    return customizedProduct;

  } else {
    // Handle the case where no items are found
    return 2;
  }

}

const addtoCart = async (appDetatils) => {
  const { user_id, qty, store_id, varient_id, device_id, platform, product_feature_id } = appDetatils;
  const productItems = await knex('product_varient')
    .where('varient_id', varient_id)
    .first(); // Retrieves the first record

  const baseurl = process.env.BUNNY_NET_IMAGE;
  if (productItems) {

    // Query the user information
    const user = await knex('users')
      .select('user_phone', 'wallet')
      .where('id', user_id)
      .first();

    // Check if the user is found
    if (!user) {
      return 'User not Found';
    }

    // Extract the user phone
    const userPhone = user.user_phone;

    const product = await knex('store_products')
      .join('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', 'product.product_id')
      .where('store_products.varient_id', varient_id)
      .andWhere('store_products.store_id', store_id)
      .first(); // Retrieves the first matching record

    if (qty > product.max_ord_qty) {
      const p_name = `${product.product_name} (${product.quantity}${product.unit}) * ${qty}`;
      const message = `You have to order ${p_name} quantity between ${product.min_ord_qty} to ${product.max_ord_qty}.`;
      return message;
    }

    if (qty > product.stock) { // Check if the requested quantity exceeds the available stock
      //const message = 'No more stock available.';
      //return message;
      throw new Error("No more stock available");
    }

    // Check for current deal (based on current server date)
    const now = new Date();
    const deal = await knex('deal_product')
      .where('varient_id', varient_id)
      .andWhere('store_id', store_id)
      .andWhere('valid_from', '<=', now.toISOString().split('T')[0])
      .andWhere('valid_to', '>', now.toISOString().split('T')[0])
      .first(); // Retrieves the first matching deal
    // Offer products must be valid only for today's date in UAE (Asia/Dubai)
    const dubaiToday = moment().tz('Asia/Dubai').format('YYYY-MM-DD');
    const productOfferDate = product.offer_date
      ? moment(product.offer_date).tz('Asia/Dubai').format('YYYY-MM-DD')
      : null;
    const isTodayOffer = product.is_offer_product == 1 && productOfferDate === dubaiToday;

    let price;
    if (deal) {
      price = parseFloat(deal.deal_price);
    } else if (isTodayOffer) {
      price = product.offer_price;
    } else {
      price = parseFloat(product.price);
    }

    if (isTodayOffer) {
      product.mrp = product.offer_price;
    }

    let price2 = price * qty;
    let price5 = product.mrp * qty;
    let created_at = new Date();

    // Check if the order already exists in the cart
    const existingOrder = await knex('store_orders')
      .where('store_approval', user_id)
      .andWhere('varient_id', varient_id)
      .andWhere('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .first();

    // Check if the product/subcategory/category discount percentage
    const productVarient = await knex('product_varient')
      .where('varient_id', varient_id)
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
    var percentageSubCat = categoriesSubDeatils.discount_per;
    var parent = categoriesSubDeatils.parent;

    const categoriesParentDeatils = await knex('categories')
      .where('cat_id', parent)
      .first();
    var PriceNew = price2;
    /*
      if(availability == 'quick'){
      var percentageCat=categoriesParentDeatils.discount_per;
      if(percentage > 0)
      {
      PriceNew=((price5-((price5*percentage)/100))).toFixed(2);
      }
      else if(percentageSubCat > 0)
      {
      PriceNew=((price5-((price5*percentageSubCat)/100))).toFixed(2);
      }
      else if(percentageCat > 0)
      {
      PriceNew=((price5-((price5*percentageCat)/100))).toFixed(2);
      }else
      {
      PriceNew=price2;
      }
      }else
      {
      PriceNew=price2; 
      }
    */

    const dubaiTime = moment.tz("Asia/Dubai");
    const todayDubai = dubaiTime.format("YYYY-MM-DD HH:mm:ss");

    // Define order data
    const orderData = {
      store_id: store_id,
      varient_id: varient_id,
      qty: qty,
      product_name: product.product_name,
      varient_image: product.product_image,
      quantity: product.quantity,
      unit: product.unit,
      store_approval: user_id,
      total_mrp: parseFloat(price5),
      order_cart_id: 'incart',
      order_date: todayDubai,
      repeat_orders: 1,
      price: parseFloat(PriceNew),
      description: product.description,
      tx_per: 0,
      price_without_tax: 0,
      tx_price: 0,
      tx_name: 'vat',
      type: product.type,
      repeated_order_cart: '',
      platform: (platform) ? platform : null,
      is_offer_product: (product.is_offer_product == 1) ? 1 : 0,
      product_feature_id: (product_feature_id) ? product_feature_id : null
    };

    if (!existingOrder) {
      // Insert new order if no existing order is found
      if (qty != 0) {
        orderData.store_order_id = await getNextStoreOrderId();
        await knex('store_orders').insert(orderData);
      }
    } else {
      // Delete existing order and insert new one
      await knex('store_orders')
        .where('store_approval', user_id)
        .where('varient_id', varient_id)
        .where('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .delete();
      if (qty != 0) {
        orderData.store_order_id = await getNextStoreOrderId();
        await knex('store_orders').insert(orderData);
      }
    }

    // Sync Redis: Update or Delete from Daily Cart HASH (Awaited for immediate consistency)
    await syncCartItem(user_id, store_id, varient_id, qty, false);

    const sum1 = await knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .where('store_products.store_id', store_id)
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .select(knex.raw('SUM(store_orders.total_mrp) as totalmrp'), knex.raw('SUM(store_orders.price) as totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
      .first();

    if (sum1.totalprice < 30) {
      const deletedOfferItems = await knex('store_orders')
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where('is_offer_product', 1)
        .whereIn('varient_id', function () {
          this.select('pv.varient_id')
            .from('product_varient as pv')
            .join('product as p', 'p.product_id', 'pv.product_id')
            .where('p.is_offer_product', 1)
            .whereRaw("DATE(p.offer_date) = ?", [dubaiToday]);
        })
        .whereNull('subscription_flag')
        .returning(['store_id', 'varient_id'])
        .delete();

      // Sync Redis cart HASH for all offer items deleted by threshold rule.
      if (deletedOfferItems && deletedOfferItems.length) {
        await Promise.all(
          deletedOfferItems.map((item) => {
            const deleteStoreId = item.store_id || store_id;
            const cartHashKey = generateCartHashKey(user_id, deleteStoreId);
            return hDel(cartHashKey, item.varient_id.toString());
          })
        );
      }
    }

    const sum = await knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .where('store_products.store_id', store_id)
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .select(knex.raw('SUM(store_orders.total_mrp) as totalmrp'), knex.raw('SUM(store_orders.price) as totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
      .first();
    // Handle cases where the result might be null
    const customizedProduct = {
      saving_price: (sum.totalmrp || 0) - (sum.totalprice || 0),
      total_price: sum.totalprice || 0,
      total_items: Number(sum.count) || 0,
      product_name: product.product_name,
      price: parseFloat(PriceNew),
    };
    return customizedProduct;
  } else {
    return 'No more product available.';
  }

}

const showCart = async (appDetatils) => {
  const { user_id, device_id } = appDetatils;

  const cartItems = await knex('store_orders')
    .where('store_approval', user_id)
    .where('order_cart_id', 'incart')
    .whereNull('subscription_flag'); // Correctly checking for NULL values

  const sum = await knex('store_orders')
    // .join('store_products', 'store_orders.varient_id','=','store_products.varient_id')
    // .join('product_varient','store_products.varient_id','=','product_varient.varient_id')
    // .join('product','product_varient.product_id','=','product.product_id')
    .where('store_orders.store_approval', user_id)
    .where('store_orders.order_cart_id', 'incart')
    .whereNull('subscription_flag')
    .select(knex.raw('SUM(store_orders.total_mrp) as totalmrp'), knex.raw('SUM(store_orders.price) as totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
    .first();

  const baseurl = process.env.BUNNY_NET_IMAGE;
  const cartItems1 = await knex('store_orders')
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .select(
      'product.availability',
      'product.percentage',
      'store_orders.repeated_order_cart',
      'store_orders.product_name',
      knex.raw(`CONCAT('${baseurl}', store_orders.varient_image) as varient_image`),
      'store_orders.quantity',
      'store_orders.unit',
      'store_orders.total_mrp',
      // 'store_orders.price',
      knex.raw('ROUND((store_orders.price / store_orders.qty)::numeric, 2) as price'),
      'store_products.mrp',
      'store_products.price as store_price',
      'store_orders.qty as cart_qty',
      'store_orders.total_mrp',
      'store_orders.order_cart_id',
      'store_orders.order_date',
      'store_orders.store_approval',
      'store_orders.store_id',
      'store_orders.varient_id',
      'product.product_id',
      'store_products.stock',
      'store_orders.tx_per',
      'store_orders.price_without_tax',
      'store_orders.tx_price',
      'store_orders.tx_name', knex.raw(`CONCAT('${baseurl}', product.product_image) as product_image`), knex.raw(`CONCAT('${baseurl}', product.thumbnail) as thumbnail`), 'product.available_days', 'product_varient.description', 'product.type', 'store_orders.price as ord_price', 'store_orders.repeat_orders as repeat_orders')
    .groupBy('store_orders.varient_id')
    .where('store_orders.store_approval', user_id)
    .whereNull('store_orders.subscription_flag') // Corrected null condition
    .where('store_orders.order_cart_id', 'incart')
    .where('store_products.stock', '>', 0)
    .where('store_orders.qty', '>', 0);

  let walletamt = 0;
  // Check if user_id is valid and fetch wallet balance
  if (user_id !== "null") {
    const walletBalance = await knex('users')
      .select('wallet')
      .where('id', user_id)
      .first();
    if (walletBalance) {
      walletamt = walletBalance.wallet;
    } else {
      walletamt = 0;
    }
  } else {
    walletamt = 0;
  }

  // Calculate total reserve amount from subscription orders
  const reserveAmounts = await knex('orders')
    .innerJoin('subscription_order', 'subscription_order.cart_id', '=', 'orders.cart_id')
    .select('orders.reserve_amount')
    .where('orders.is_subscription', 1)
    .where('orders.user_id', user_id)
    .groupBy('orders.order_id');

  let total_reserve_amt = 0;
  for (const amount of reserveAmounts) {
    total_reserve_amt += parseFloat(amount.reserve_amount);
  }
  // Calculate remaining wallet amount
  const total_wallet = walletamt - total_reserve_amt;


  // Fetch the English city names
  const cityNamesResult = await knex('city')
    .where('status', 1)
    .select(knex.raw("STRING_AGG(city_name, ',') as \"cityName\""));
  const cityNameList = cityNamesResult[0].cityName.split(',');

  // Fetch the Arabic city names
  const cityNamesAResult = await knex('city')
    .where('status', 1)
    .select(knex.raw("STRING_AGG(arabic_name, ',') as \"cityName\""));
  const cityNameListA = cityNamesAResult[0].cityName.split(',');

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
      const baseurl = process.env.BUNNY_NET_IMAGE;
      // Check if the address from the latest order exists
      const lastAdd = await knex('address')
        .select('address_id', 'type', 'house_no', 'landmark', 'lat', 'lng', 'society', knex.raw(`CONCAT('${baseurl}', doorimage) as doorimage`))
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
          .whereNot('is_delete', 1)
          .first();
        if (!users_acc_details) {
          users_acc_details = await knex('tbl_user_bank_details')
            .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
            .where('user_id', user_id)
            .where('bank_type', 'totalpay')
            .whereNot('is_delete', 1)
            .first();
        }
      } else {
        users_acc_details = await knex('tbl_user_bank_details')
          .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
          .where('user_id', user_id)
          .where('bank_type', 'totalpay')
          .whereNot('is_delete', 1)
          .first();
      }

    } else {
      lastAddress = [];
      users_acc_details = await knex('tbl_user_bank_details')
        .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
        .where('user_id', user_id)
        .where('bank_type', 'totalpay')
        .whereNot('is_delete', 1)
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

    for (let m = 0; m < dateList.length; m++) {
      const selectedDate = dateList[m];
      let timeslots = [];

      if (today === selectedDate) {
        // Handle today's slots based on current time
        if (currentTimes < "12:00") {
          timeslots = await knex('tbl_time_slots')
            .where('status', 0)
            .where('id', 5) // Modify as per your requirement
            .select('time_slots', 'discount', 'min_amount', 'max_amount')
            .orderBy('seq', 'ASC');
        }
      }

      // Handle tomorrow's slots (only fetch time slots with IDs 2 and 3)
      else if (formattedTomorrow === selectedDate && currentTimes >= "18:00") {

        timeslots = await knex('tbl_time_slots')
          .where('status', 0)
          .whereIn('id', [4, 5]) // Fetch only time slots 2 and 3
          .select('time_slots', 'discount', 'min_amount', 'max_amount')
          .orderBy('seq', 'ASC');
      } else {
        // Handle future dates
        timeslots = await knex('tbl_time_slots')
          .where('status', 0)
          .whereIn('id', [1, 4, 5])  // Show only time slots 1 and 2
          .select('time_slots', 'discount', 'min_amount', 'max_amount')
          .orderBy('seq', 'ASC');
      }

      // Add date and timeslots to the result if there are timeslots
      if (timeslots.length > 0) {
        customizedProductData1.push({
          date: selectedDate,
          timeslots: timeslots
        });
      }
    }


    for (const ProductList of cartItems1) {
      const { varient_id } = ProductList;

      // Handle wishlist, notify me, and cart quantity
      let isSubscriptions = 'false';

      if (user_id !== "null") {
        const StoresDetails = await knex('store_orders')
          .where('varient_id', varient_id)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('store_orders.subscription_flag', 1)
          .first();

        isSubscriptions = StoresDetails ? 'true' : 'false';
      }

      // Update the product list details
      ProductList.isSubscription = isSubscriptions;
    }




    // Remove empty objects and limit to 4 results
    const filteredTimeSlots = customizedProductData1.slice(0, 4);

    const deliveryFlag = await knex('app_settings')
      .where('store_id', 7)
      .select('cod_charges', 'wallet_deduction_percentage', 'quickminorderamount')
      .first();


    const freegiftlist = [];
    const mighthavemissed = [];
    const customizedProduct = {
      timeslotsdata: filteredTimeSlots,
      lastadd: lastAddress || [],
      lastcarddetails: users_acc_details || {},
      wallet_balance: total_wallet,
      discountonmrp: 0.00,
      total_price: sum.totalprice,
      total_mrp: sum.totalmrp,
      saving_price: (sum.totalmrp - sum.totalprice).toFixed(2),
      total_items: Number(sum.count) || 0,
      free_delivery: "0.00",
      total_tax: "0.00",
      avg_tax: "0.00",
      delivery_charge: "0.00",
      subscription_fee: "0.00",
      vat: "0.00",
      data: cartItems1,
      free_gift_list: freegiftlist,
      might_have_missed: mighthavemissed,
      wallet_deduction_percentage: deliveryFlag.wallet_deduction_percentage,
      restricted_city: "Ajman,Sharjah",
      quickminorderamount: deliveryFlag.quickminorderamount
    }
    return customizedProduct;

  } else {
    // Handle the case where no items are found
    return 2;
  }
};

// Function to fetch address details using Google Maps API
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

/** getFormattedAddress with timeout (300ms) - avoids blocking API response */
const getFormattedAddressWithTimeout = (lat, lng, ms = 300) => {
  return Promise.race([
    getFormattedAddress(lat, lng),
    new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms))
  ]).catch(() => null);
};

const showspcatCart = async (appDetatils) => {
  const { user_id, device_id, selected_date, selected_time, platform } = appDetatils;
  let selected_datevaltemp = null;
  let selected_timevaltemp = null;

  // Use Dubai timezone consistently for all "today" comparisons.
  // This prevents UTC date drift (offer products lingering past 23:59 Dubai time).
  const dubaiToday = moment().tz('Asia/Dubai').format('YYYY-MM-DD');
  const updateData = await knex('store_orders')
    .whereNull('subscription_flag')
    .where('store_approval', user_id)
    .andWhere('order_cart_id', 'incart')
    .andWhere('sub_delivery_date', '<', dubaiToday)
    .update({
      sub_delivery_date: null,
      sub_time_slot: null,
    });

  const cartItemsList = await knex('store_orders')
    .where('store_approval', user_id)
    .where('order_cart_id', 'incart')
    .whereNotNull('sub_delivery_date')
    .whereNull('subscription_flag'); // Correctly checking for NULL values

  for (const cartItm of cartItemsList) {

    // Get today's date and current time in Dubai timezone
    const dubaiTime = moment.tz("Asia/Dubai");
    const todayDubai = dubaiTime.format("YYYY-MM-DD");
    const isAfter6PM = dubaiTime.hour() > 16;
    const isAfter12PM = dubaiTime.hour() > 11;  //temp change
    const delivery_date = cartItm.sub_delivery_date;
    const time_slot = cartItm.sub_time_slot;

    // Condition 1: Check if any order has a sub_delivery_date of today
    if (delivery_date === todayDubai && isAfter12PM) {
      const updateData = await knex('store_orders')
        .whereNull('subscription_flag')
        .where('store_approval', user_id)
        .andWhere('order_cart_id', 'incart')
        .update({
          sub_delivery_date: null,
          sub_time_slot: null,
        });
    }

    // Condition 2: If it's after 6 PM in Dubai, prevent placing orders for tomorrow with "06:00 am - 10:00 am" time slot
    if (isAfter6PM) {
      const tomorrowDubai = dubaiTime.add(1, 'day').format("YYYY-MM-DD"); // Get tomorrow's date in Dubai time
      if (delivery_date == tomorrowDubai && time_slot == "06:00 am - 10:00 am") {
        const updateData = await knex('store_orders')
          .whereNull('subscription_flag')
          .where('store_approval', user_id)
          .andWhere('order_cart_id', 'incart')
          .update({
            sub_delivery_date: null,
            sub_time_slot: null,
          });
      }
    }

    // Condition 3: Check if any order has a sub_delivery_date of today
    if (delivery_date === todayDubai && (time_slot == "06:00 am - 10:00 am" || time_slot == "02:00 pm - 05:00 pm" || time_slot == "02:00 pm - 04:00 pm")) {
      const updateData = await knex('store_orders')
        .whereNull('subscription_flag')
        .where('store_approval', user_id)
        .andWhere('order_cart_id', 'incart')
        .where('sub_delivery_date', delivery_date)
        .where('sub_time_slot', time_slot)
        .update({
          sub_delivery_date: null,
          sub_time_slot: null,
        });
    }

  }



  const cartItems = await knex('store_orders')
    .where('store_approval', user_id)
    .where('order_cart_id', 'incart')
    .whereNull('subscription_flag'); // Correctly checking for NULL values


  const sum = await knex('store_orders')
    .where('store_orders.store_approval', user_id)
    .where('store_orders.order_cart_id', 'incart')
    .whereNull('subscription_flag')
    .select(knex.raw('MAX(store_orders.sub_time_slot) as sub_time_slot'), knex.raw('MAX(store_orders.sub_delivery_date) as sub_delivery_date'), knex.raw('SUM(store_orders.total_mrp) as totalmrp'), knex.raw('SUM(store_orders.price) as totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
    .first();

  if (selected_date == "null") {
    selected_datevaltemp = sum?.sub_delivery_date ?? null;
    selected_timevaltemp = sum?.sub_time_slot ?? null;
  } else {
    selected_datevaltemp = selected_date;
    selected_timevaltemp = selected_time;

  }


  const baseurl = process.env.BUNNY_NET_IMAGE;
  const cartItems1 = await knex('store_orders')
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .join('categories', 'categories.cat_id', '=', 'product.cat_id')
    .select(
      'categories.cat_type',
      'product.availability',
      'product.percentage',
      'store_orders.repeated_order_cart',
      'store_orders.product_name',
      'product.hide',
      'product.is_delete',
      knex.raw(`CONCAT('${baseurl}', store_orders.varient_image) as varient_image`),
      'store_orders.quantity',
      'store_orders.unit',
      'store_orders.total_mrp',
      // 'store_orders.price',
      knex.raw('ROUND((store_orders.price / store_orders.qty)::numeric, 2) as price'),
      'store_products.mrp',
      'store_orders.qty as cart_qty',
      'store_orders.total_mrp',
      'store_orders.order_cart_id',
      'store_orders.order_date',
      'store_orders.store_approval',
      'store_orders.store_id',
      'store_orders.varient_id',
      'product.product_id',
      'product.cat_id',
      'store_products.stock',
      'store_orders.tx_per',
      'store_orders.price_without_tax',
      'store_orders.tx_price',
      'store_orders.store_order_id',
      'store_orders.tx_name',
      'store_orders.product_feature_id',
      knex.raw(`CONCAT('${baseurl}', product.product_image) as product_image`), knex.raw(`CONCAT('${baseurl}', product.thumbnail) as thumbnail`), 'product.available_days', 'product_varient.description', 'product.type', 'store_orders.price as ord_price', 'store_orders.repeat_orders as repeat_orders')
    .where('store_orders.store_approval', user_id)
    .whereNull('store_orders.subscription_flag') // Corrected null condition
    .where('store_orders.order_cart_id', 'incart')
    // .where('store_products.stock','>',0)
    .where('store_orders.qty', '>', 0);

  let walletamt = 0;
  let refwalletamt = 0;
  // Check if user_id is valid and fetch wallet balance
  if (user_id !== "null") {
    const walletBalance = await knex('users')
      .select('wallet', 'wallet_balance', 'referral_balance')
      .where('id', user_id)
      .first();
    if (walletBalance) {
      walletamt = walletBalance.wallet_balance || 0;
      refwalletamt = walletBalance.referral_balance || 0;
    } else {
      walletamt = 0;
      refwalletamt = 0;
    }
  } else {
    walletamt = 0;
    refwalletamt = 0;
  }

  // Calculate total reserve amount from subscription orders (PostgreSQL: use aggregate in SELECT with GROUP BY)
  const reserveAmounts = await knex('orders')
    .innerJoin('subscription_order', 'subscription_order.cart_id', '=', 'orders.cart_id')
    .select(knex.raw('MAX(orders.reserve_amount) as reserve_amount'))
    .where('orders.is_subscription', 1)
    .where('orders.user_id', user_id)
    .groupBy('orders.order_id');

  let total_reserve_amt = 0;
  for (const amount of reserveAmounts) {
    total_reserve_amt += parseFloat(amount.reserve_amount);
  }
  // Calculate remaining wallet amount
  const total_wallet = walletamt;


  // Fetch the English city names
  const cityNamesResult = await knex('city')
    .where('status', 1)
    .select(knex.raw("STRING_AGG(city_name, ',') as \"cityName\""));
  const cityNameList = (cityNamesResult[0]?.cityName || '').split(',').filter(Boolean);

  // Fetch the Arabic city names
  const cityNamesAResult = await knex('city')
    .where('status', 1)
    .select(knex.raw("STRING_AGG(arabic_name, ',') as \"cityName\""));
  const cityNameListA = (cityNamesAResult[0]?.cityName || '').split(',').filter(Boolean);

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
    const baseurl = process.env.BUNNY_NET_IMAGE;
    if (orderlist) {
      // Check if the address from the latest order exists
      const lastAdd = await knex('address')
        .select('address_id', 'type', 'house_no', 'landmark', 'lat', 'lng', 'society', knex.raw(`CONCAT('${baseurl}', doorimage) as doorimage`))
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
          .whereNot('is_delete', 1)
          .first();
        if (!users_acc_details) {
          users_acc_details = await knex('tbl_user_bank_details')
            .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
            .where('user_id', user_id)
            .where('bank_type', 'totalpay')
            .whereNot('is_delete', 1)
            .first();
        }
      } else {
        users_acc_details = await knex('tbl_user_bank_details')
          .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
          .where('user_id', user_id)
          .where('bank_type', 'totalpay')
          .whereNot('is_delete', 1)
          .first();
      }


    } else {
      lastAddress = [];
      users_acc_details = await knex('tbl_user_bank_details')
        .select('id', 'user_id', 'si_sub_ref_no', 'card_no')
        .where('user_id', user_id)
        .where('bank_type', 'totalpay')
        .whereNot('is_delete', 1)
        .first();
    }

    //Time slot 
    const dates = [];
    const currentDate = moment().tz('Asia/Dubai');
    const currentTime = new Date();

    // Generate the next 5 days' dates in Dubai time
    for (let i = 0; i <= 4; i++) {
      const date = currentDate.clone().add(i, 'days').format('YYYY-MM-DD');

      // ❌ Skip 2025-12-31
      // if (date === '2025-12-31') {
      //     continue;
      // }

      dates.push(date);
      // dates.push(currentDate.clone().add(i, 'days').format('YYYY-MM-DD'));
    }

    const dateList = dates;
    const today = currentDate.format('YYYY-MM-DD');
    const customizedProductData1 = [];
    const customizedProductData2 = [];

    // Get current time in HH:mm format
    const hours = String(currentTime.getHours()).padStart(2, '0');
    const minutes = String(currentTime.getMinutes()).padStart(2, '0');
    const currentTimeStr = `${hours}:${minutes}`;
    // Get the current date and time in Dubai time zone (UTC+4)
    const currentDateTime = moment().tz("Asia/Dubai");
    // Extract hours and minutes for current time in Dubai
    const currentTimes = currentDateTime.format("HH:mm");


    // Determine tomorrow's date
    // Tomorrow in Dubai timezone (avoid UTC drift)
    const formattedTomorrow = currentDate.clone().add(1, 'day').format('YYYY-MM-DD');

    for (let m = 0; m < dateList.length; m++) {
      const selectedDate = dateList[m];
      let timeslots = [];

      if (today === selectedDate) {
        // Handle today's slots based on current time
        if (currentTimes < "11:00") {  //temp change
          timeslots = await knex('tbl_time_slots')
            .where('status', 0)
            .where('id', 5) // Modify as per your requirement
            .select('time_slots', 'discount', 'min_amount', 'max_amount')
            .orderBy('seq', 'ASC');
        }
      }

      // Handle tomorrow's slots (only fetch time slots with IDs 2 and 3)
      else if (formattedTomorrow === selectedDate && currentTimes >= "17:00") {

        timeslots = await knex('tbl_time_slots')
          .where('status', 0)
          .whereIn('id', [4, 5]) // Fetch only time slots 2 and 3
          .select('time_slots', 'discount', 'min_amount', 'max_amount')
          .orderBy('seq', 'ASC');
      } else {
        // Handle future dates
        timeslots = await knex('tbl_time_slots')
          .where('status', 0)
          .whereIn('id', [1, 4, 5])  // Show only time slots 1 and 2
          .select('time_slots', 'discount', 'min_amount', 'max_amount')
          .orderBy('seq', 'ASC');
      }

      // Add date and timeslots to the result if there are timeslots
      if (timeslots.length > 0) {
        customizedProductData1.push({
          date: selectedDate,
          timeslots: timeslots
        });
      }
    }

    // Remove empty objects and limit to 4 results
    const filteredTimeSlots = customizedProductData1.slice(0, 4);

    for (const ProductList of cartItems1) {
      const { varient_id, product_id, product_feature_id } = ProductList;

      // Handle wishlist, notify me, and cart quantity
      let isSubscriptions = 'false';

      if (user_id !== "null") {
        const StoresDetails = await knex('store_orders')
          .where('varient_id', varient_id)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('store_orders.subscription_flag', 1)
          .first();

        isSubscriptions = StoresDetails ? 'true' : 'false';
      }

      // Update the product list details
      ProductList.isSubscription = isSubscriptions;

      // Offer products must be valid only for today's date in UAE (Asia/Dubai)
      const dubaiNow = moment().tz('Asia/Dubai');
      const currentDate1 = dubaiNow.format('YYYY-MM-DD');
      const offer_products = await knex('product')
        .where('product_id', product_id)
        .where('is_offer_product', 1)
        .where('offer_date', currentDate1)
        .first();

      // Default: assume no active offer today; use original per-unit price
      ProductList.isOfferProduct = 'false';
      const unitPrice = ProductList.store_price != null && ProductList.store_price !== ''
        ? ProductList.store_price
        : (ProductList.cart_qty ? (ProductList.ord_price / ProductList.cart_qty) : ProductList.ord_price);
      const originalTotal = unitPrice * ProductList.cart_qty;
      ProductList.price = unitPrice;
      ProductList.total_mrp = originalTotal;

      // If there is an active offer today, override with offer price
      if (offer_products) {
        ProductList.isOfferProduct = 'true';
        ProductList.price = offer_products.offer_price;
        ProductList.total_mrp = offer_products.offer_price;
      }

      // Persist normalized (non-offer) price back to cart when offer is no longer active
      if (!offer_products && ProductList.store_order_id) {
        await knex('store_orders')
          .where('store_order_id', ProductList.store_order_id)
          .update({
            price: unitPrice * ProductList.cart_qty,
            total_mrp: originalTotal
          });
      }


      ProductList.product_feature_id = (product_feature_id) ? product_feature_id : null;
      ProductList.product_feature_value = null;

      let product_feature = await knex('tbl_feature_value_master')
        .where('id', product_feature_id)
        .first();
      if (product_feature) {
        ProductList.product_feature_value = product_feature.feature_value;
      }

      if (ProductList.hide == 1 || ProductList.is_delete == 1) {
        ProductList.stock = 0;
      }
    }

    // Group by cat_type instead of cat_id
    const groupedByCatType = await cartItems1.reduce(async (accPromise, item) => {
      const acc = await accPromise;

      const catDetails = await knex('categories')
        .where('cat_id', item.cat_id)
        .select('parent')
        .first();

      // Fetch the category details to determine `cat_type`
      const categoryDetails = await knex('categories')
        .where('cat_id', catDetails.parent)
        .select('cat_type', 'title', 'cat_id')
        .first();

      const catType = categoryDetails?.cat_type || 'default';
      const catIdVal = categoryDetails?.cat_id || '';
      const catNameVal = categoryDetails?.title || '';

      if (!acc[catType]) {
        acc[catType] = [];
      }

      acc[catType].push({
        item,
        catIdVal,
        catNameVal,
      });
      return acc;
    }, Promise.resolve({}));


    // Count all categories except 'special' before Promise.all
    let nonSpecialCategoryCount = 0;
    Object.keys(groupedByCatType).forEach((catType) => {
      if (catType !== 'special') {
        nonSpecialCategoryCount += groupedByCatType[catType].length;
      }
    });

    // Assign timeslots to each category type
    const specialcatdata = await Promise.all(
      Object.keys(groupedByCatType).map(async (catType) => {
        const productsWithCatId = groupedByCatType[catType];

        if (catType === 'special') {



          // Group products by `cat_id` for the special category
          const groupedByCatId = productsWithCatId.reduce((acc, { item, catIdVal, catNameVal }) => {
            if (!acc[catIdVal]) {
              acc[catIdVal] = {
                cat_id: catIdVal,
                cat_name: catNameVal,
                products: [],
              };
            }
            acc[catIdVal].products.push(item);
            return acc;
          }, {});

          // Process timeslots for each unique `cat_id`
          const result = await Promise.all(
            Object.values(groupedByCatId).map(async ({ cat_id, cat_name, products }) => {


              const parentTimeslots = await knex('categories')
                .where('cat_id', cat_id)
                .select('timeslots')
                .first();

              const catarray1 = await knex('categories')
                .where('parent', cat_id)
                .pluck('cat_id');

              // Update the timeslots for all products with the specified cat_id
              const vararray1 = await knex('product')
                .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
                .whereIn('product.cat_id', catarray1)
                .pluck('product_varient.varient_id');



              const storeorderlist1 = await knex('store_orders')
                .where('store_approval', user_id)
                .whereIn('varient_id', vararray1)
                .where('order_cart_id', 'incart')
                .whereNull('store_orders.subscription_flag') // Corrected null condition
                .whereNotNull('sub_time_slot')
                .select('sub_delivery_date', 'sub_time_slot')
                .first()

              let spselected_dateval = null;
              let spselected_timeval = null;
              if (storeorderlist1) {
                spselected_dateval = storeorderlist1.sub_delivery_date;
                spselected_timeval = storeorderlist1.sub_time_slot;
              } else {
                spselected_dateval = null;
                spselected_timeval = null;
              }
              let timeslots = [];
              if (nonSpecialCategoryCount != 0) {

                if (parentTimeslots && parentTimeslots.timeslots) {
                  const timeslotIds = JSON.parse(parentTimeslots.timeslots);

                  const customizedProductData2 = [];
                  for (const selectedDate1 of dates) {
                    let currentTimeslots = [];
                    //if (today === selectedDate1 && currentTimes < "12:00") {
                    if (today === selectedDate1) {
                      // Handle today's slots based on current time
                      if (currentTimes < "11:00") {  //temp change
                        const commonElements = [5].filter(value =>
                          timeslotIds.map(Number).includes(value)
                        );
                        currentTimeslots = await knex('tbl_time_slots')
                          .where('status', 0)
                          .whereIn('id', commonElements)
                          .select('time_slots', 'discount', 'min_amount', 'max_amount')
                          .orderBy('seq', 'ASC');
                      }
                    } else if (formattedTomorrow === selectedDate1 && currentTimes >= "17:00") {
                      const commonElements = [4, 5].filter(value =>
                        timeslotIds.map(Number).includes(value)
                      );
                      currentTimeslots = await knex('tbl_time_slots')
                        .where('status', 0)
                        .whereIn('id', commonElements)
                        .select('time_slots', 'discount', 'min_amount', 'max_amount')
                        .orderBy('seq', 'ASC');
                    } else {
                      currentTimeslots = await knex('tbl_time_slots')
                        .where('status', 0)
                        .whereIn('id', timeslotIds)
                        .select('time_slots', 'discount', 'min_amount', 'max_amount')
                        .orderBy('seq', 'ASC');
                    }
                    if (currentTimeslots.length > 0) {
                      if (selected_datevaltemp != null) {
                        if (selected_datevaltemp == selectedDate1) {
                          customizedProductData2.push({
                            selected_date: spselected_dateval,
                            date: selectedDate1,
                            timeslots: currentTimeslots,
                          });
                        }
                      } else
                        if (selected_datevaltemp == null) {
                          customizedProductData2.push({
                            date: selectedDate1,
                            timeslots: currentTimeslots,
                          });
                        }
                    }
                  }

                  timeslots = customizedProductData2.slice(0, 4);
                }
              } else {
                const timeslotIds = JSON.parse(parentTimeslots.timeslots);
                const customizedProductData3 = [];
                for (let m = 0; m < dateList.length; m++) {
                  const selectedDate = dateList[m];
                  if (today === selectedDate) {
                    // Handle today's slots based on current time
                    if (currentTimes < "11:00") {  //temp change
                      const commonElements = [5].filter(value =>
                        timeslotIds.map(Number).includes(value)
                      );
                      currentTimeslots = await knex('tbl_time_slots')
                        .where('status', 0)
                        .whereIn('id', commonElements)
                        .select('time_slots', 'discount', 'min_amount', 'max_amount')
                        .orderBy('seq', 'ASC');
                      // Add date and timeslots to the result if there are timeslots
                      if (currentTimeslots.length > 0) {
                        customizedProductData3.push({
                          date: selectedDate,
                          timeslots: currentTimeslots
                        });
                      }
                    }
                  }
                  // Handle tomorrow's slots (only fetch time slots with IDs 2 and 3)
                  else if (formattedTomorrow === selectedDate && currentTimes >= "17:00") {
                    const commonElements = [4, 5].filter(value =>
                      timeslotIds.map(Number).includes(value)
                    );
                    currentTimeslots = await knex('tbl_time_slots')
                      .where('status', 0)
                      .whereIn('id', commonElements)
                      .select('time_slots', 'discount', 'min_amount', 'max_amount')
                      .orderBy('seq', 'ASC');
                    // Add date and timeslots to the result if there are timeslots
                    if (currentTimeslots.length > 0) {
                      customizedProductData3.push({
                        date: selectedDate,
                        timeslots: currentTimeslots
                      });
                    }

                  } else {
                    // Handle future dates
                    currentTimeslots = await knex('tbl_time_slots')
                      .where('status', 0)
                      .whereIn('id', timeslotIds)
                      .select('time_slots', 'discount', 'min_amount', 'max_amount')
                      .orderBy('seq', 'ASC');

                    // Add date and timeslots to the result if there are timeslots
                    if (currentTimeslots.length > 0) {
                      customizedProductData3.push({
                        date: selectedDate,
                        timeslots: currentTimeslots
                      });
                    }

                  }


                }
                timeslots = customizedProductData3.slice(0, 4);
              }

              return {
                cat_id,
                cat_name,
                cat_type: catType,
                selectedDate: (spselected_dateval && spselected_timeval) ? spselected_dateval : null,
                selectedTime: (spselected_dateval && spselected_timeval) ? spselected_timeval : null,
                // selectedDate:selected_dateval,
                // selectedTime:spselected_timeval,
                timeslotsdata: timeslots,
                products,
              };
            })
          );

          return result; // Add all objects for this `special` category
        } else {
          // Default handling for other categories

          const categories = await knex('categories')
            .whereNull('cat_type')
            .pluck('cat_id');

          const subcategories = await knex('categories')
            .whereIn('parent', categories)
            .pluck('cat_id');

          const vararray = await knex('product')
            .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
            .whereIn('product.cat_id', subcategories)
            .pluck('product_varient.varient_id');

          const storeorderlist = await knex('store_orders')
            .where('store_approval', user_id)
            .whereIn('varient_id', vararray)
            .where('order_cart_id', 'incart')
            .whereNull('store_orders.subscription_flag') // Corrected null condition
            .select('sub_delivery_date', 'sub_time_slot')
            .first()

          if (selected_date == "null") {
            selected_dateval = storeorderlist?.sub_delivery_date ?? null;
            selected_timeval = storeorderlist?.sub_time_slot ?? null;
          } else {
            selected_dateval = selected_date;
            selected_timeval = selected_time;

          }

          return {
            cat_id: 0,
            cat_name: "Other Category",
            cat_type: catType,
            selectedDate: (selected_dateval && selected_timeval) ? selected_dateval : null,
            selectedTime: (selected_dateval && selected_timeval) ? selected_timeval : null,
            timeslotsdata: filteredTimeSlots,
            products: productsWithCatId.map(({ item }) => item),
          };
        }
      })
    );

    // Flatten the result array for `special` categories
    const finalData = specialcatdata.flat();

    const generalCategory = finalData.filter(item => item.cat_type === 'default');
    const otherCategories = finalData.filter(item => item.cat_type !== 'default');
    if (generalCategory) {
      // Step 1: Extract store_order_id values
      const storeOrderIds = generalCategory.flatMap(category => category.products.map(product => product.store_order_id));
      // Extracting store_order_id and creating an array of data
      // Step 2: Query the store_orders table for rows with not NULL sub_time_slot and sub_delivery_date
      const rowToUse = await knex('store_orders')
        .whereIn('store_order_id', storeOrderIds)
        .whereNull('store_orders.subscription_flag') // Corrected null condition
        .whereNotNull('sub_time_slot')
        .whereNotNull('sub_delivery_date')
        .first(); // Fetch the first row to use its values

      if (rowToUse) {
        // Step 3: Update the rows in store_orders for the extracted store_order_id values
        const updatedRows = await knex('store_orders')
          .whereIn('store_order_id', storeOrderIds)
          .update({
            sub_time_slot: rowToUse.sub_time_slot, // Use the time slot from the first matching row
            sub_delivery_date: rowToUse.sub_delivery_date // Use the delivery date from the first matching row
          });
      }
    }

    if (otherCategories && otherCategories.length) {
      for (const category of otherCategories) {
        // Step 1: Extract store_order_id values for this category
        const storeOrderIds = category.products.map(product => product.store_order_id);

        if (storeOrderIds.length === 0) continue; // Skip if no products

        // Step 2: Query the store_orders table for the first row with not NULL sub_time_slot and sub_delivery_date
        const rowToUse = await knex('store_orders')
          .whereIn('store_order_id', storeOrderIds)
          .whereNull('subscription_flag') // Only non-subscription orders
          .whereNotNull('sub_time_slot')
          .whereNotNull('sub_delivery_date')
          .first(); // Get first matching row

        if (rowToUse) {
          // Step 3: Update all store_orders in this category using that row's values
          await knex('store_orders')
            .whereIn('store_order_id', storeOrderIds)
            .update({
              sub_time_slot: rowToUse.sub_time_slot,
              sub_delivery_date: rowToUse.sub_delivery_date
            });
        }
      }
    }



    // Combine them, placing the general category first
    const reorderedData = [...generalCategory, ...otherCategories];
    const deliveryFlag = await knex('app_settings')
      .where('store_id', 7)
      .select('cod_charges', 'wallet_deduction_percentage', 'quickminorderamount', 'oneday_min_order_amount')
      .first();

    const product_ids = reorderedData.length > 0 ? reorderedData[0].products.map(item => item.product_id) : [];
    // Offer products list must show only today's offers in UAE (Asia/Dubai)
    const currentDate1 = moment().tz('Asia/Dubai').format('YYYY-MM-DD');
    let offer_products = await knex('product')
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
      .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
      .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .select('product.product_id', 'product.product_name', 'product.offer_price as price',
        knex.raw(`CONCAT('${baseurl}', product.product_image) as product_image`),
        'product_varient.quantity', 'product_varient.unit', 'product_varient.varient_id',
        'store_products.mrp',
        'tbl_country.country_icon',
        'store_products.stock')
      .where(knex.raw('product.offer_date::date = ?::date', [currentDate1]))

    offer_products = offer_products.map(item => ({
      ...item,
      is_added: product_ids.includes(item.product_id)
    }));

    const varient_ids = offer_products.map(item => item.varient_id);


    const isOfferProductBaught = await knex('store_orders')
      .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
      .whereNot('orders.order_status', 'Cancelled')
      .whereIn('varient_id', varient_ids)
      .where('store_approval', user_id)
      .whereRaw('(store_orders.order_date)::date = ?', [currentDate1])
      .where('order_cart_id', '!=', 'incart');
    if (isOfferProductBaught.length >= 1) {
      offer_products = [];
    }

    const noticeList = await knex('tbl_notice')
      .select('message')
      .where('type', 1)
      .first();

    const freegiftlist = [];
    const mighthavemissed = [];
    const customizedProduct = {
      //timeslotsdata: filteredTimeSlots,
      lastadd: lastAddress || [],
      lastcarddetails: users_acc_details || {},
      wallet_balance: total_wallet,
      referral_balance: refwalletamt,
      discountonmrp: 0.00,
      total_price: Number(sum?.totalprice) || 0,
      total_mrp: Number(sum?.totalmrp) || 0,
      saving_price: ((Number(sum?.totalmrp) || 0) - (Number(sum?.totalprice) || 0)).toFixed(2),
      total_items: Number(sum?.count) || 0,
      free_delivery: "0.00",
      total_tax: "0.00",
      avg_tax: "0.00",
      delivery_charge: "0.00",
      subscription_fee: "0.00",
      vat: "0.00",
      // data:specialcatdata,
      data: reorderedData,
      free_gift_list: freegiftlist,
      might_have_missed: mighthavemissed,
      wallet_deduction_percentage: deliveryFlag?.wallet_deduction_percentage ?? 0,
      codcharges: deliveryFlag?.cod_charges ?? 0,
      restricted_city: "Ajman,Sharjah",
      quickminorderamount: deliveryFlag?.quickminorderamount ?? 0,
      oneday_min_order_amount: deliveryFlag?.oneday_min_order_amount ?? 0,
      offer_product: offer_products,
      noticeList: noticeList,
      morning_thresold_time: "10:59 AM",
      evening_thresold_time: "04:59 PM",
    }
    return customizedProduct;

  } else {
    // Handle the case where no items are found
    return 2;
  }
}

//new api - Optimized for PostgreSQL
const might_have_missed = async (appDetatils) => {
  // Determine user_id - PostgreSQL compatible
  let user_id;
  if (appDetatils.user_id && appDetatils.user_id != "null") {
    user_id = appDetatils.user_id;
  } else {
    user_id = appDetatils.device_id;
  }
  const store_id = appDetatils.store_id;
  const type = appDetatils.type;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const currentDate = new Date();

  // PostgreSQL date comparison - using CURRENT_DATE instead of CURDATE()
  const offerDateCondition = knex.raw(`(product.is_offer_product = 0 OR product.offer_date IS NULL OR product.offer_date::date != CURRENT_DATE)`);

  let CartQtyList = [];

  if (type == 'quick') {
    CartQtyList = await knex('store_orders')
      .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
      .join('product_varient', 'store_orders.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .join('store_products', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .where('orders.user_id', user_id)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .whereNull('store_orders.subscription_flag')
      .where('orders.store_id', store_id)
      .where('store_products.stock', '>', 0)
      .whereIn('product.availability', ['quick', 'all'])
      .whereRaw(offerDateCondition)
      .groupBy([
        'store_orders.varient_id',
        'product_varient.quantity',
        'store_products.mrp',
        'store_products.price',
        'product.fcat_id',
        'store_orders.order_cart_id',
        'store_products.stock',
        'product.available_days',
        'product_varient.product_id',
        'product.percentage',
        'product.availability',
        'store_orders.product_name',
        'product.product_image',
        'store_orders.description',
        'store_orders.type',
        'store_orders.unit',
        'store_orders.qty',
        'product.is_customized'
      ])
      .select(
        'product_varient.quantity',
        'store_products.mrp',
        'store_products.price',
        'product.fcat_id',
        'store_orders.order_cart_id',
        'store_products.stock',
        'product.available_days',
        'product_varient.product_id',
        'product.percentage',
        'product.availability',
        'store_orders.product_name',
        knex.raw(`'${baseurl}' || product.product_image as varient_image`),
        'store_orders.varient_id',
        'store_orders.description',
        'store_orders.type',
        knex.raw('count(store_orders.varient_id) as ordercount'),
        'store_orders.unit',
        'store_orders.qty',
        'product.is_customized'
      )
      .orderBy('ordercount', 'desc')
      .limit(20);
  } else if (type == 'subscription') {
    CartQtyList = await knex('store_orders')
      .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
      .join('product_varient', 'store_orders.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .join('store_products', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .where('orders.user_id', user_id)
      .where('store_products.stock', '>', 0)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('store_orders.subscription_flag', '1')
      .where('orders.store_id', store_id)
      .whereIn('product.availability', ['subscription', 'all'])
      .whereRaw(offerDateCondition)
      .groupBy([
        'store_orders.varient_id',
        'product_varient.quantity',
        'store_products.mrp',
        'store_products.price',
        'product.fcat_id',
        'store_orders.order_cart_id',
        'store_products.stock',
        'product.available_days',
        'product_varient.product_id',
        'product.percentage',
        'product.availability',
        'store_orders.product_name',
        'product.product_image',
        'store_orders.description',
        'store_orders.type',
        'store_orders.unit',
        'store_orders.qty',
        'product.is_customized'
      ])
      .select(
        'product_varient.quantity',
        'store_products.mrp',
        'store_products.price',
        'product.fcat_id',
        'store_orders.order_cart_id',
        'store_products.stock',
        'product.available_days',
        'product_varient.product_id',
        'product.percentage',
        'product.availability',
        'store_orders.product_name',
        knex.raw(`'${baseurl}' || product.product_image as varient_image`),
        'store_orders.varient_id',
        'store_orders.description',
        'store_orders.type',
        knex.raw('count(store_orders.varient_id) as ordercount'),
        'store_orders.unit',
        'store_orders.qty',
        'product.is_customized'
      )
      .orderBy('ordercount', 'desc')
      .limit(20);
  }



  // Declare customizedProductData in outer scope
  let customizedProductData = [];

  // Early return if no products found
  if (CartQtyList.length === 0) {
    // Will fall through to fallback logic below
  } else {
    // Batch fetch all related data upfront for performance optimization
    const variantIds = CartQtyList.map(p => p.varient_id);
    const productIds = [...new Set(CartQtyList.map(p => p.product_id))];
    const allFcatIds = new Set();
    CartQtyList.forEach(p => {
      if (p.fcat_id) {
        p.fcat_id.split(',').forEach(id => allFcatIds.add(parseInt(id)));
      }
    });
    const fcatIdArray = Array.from(allFcatIds).filter(id => !isNaN(id));

    // Parallel batch queries for all products
    const [
      wishListData,
      cartItemsData,
      subscriptionCartData,
      notifyMeData,
      subscriptionProductsData,
      featureCategoriesData,
      productFeaturesData,
      productImagesData,
      dealsData
    ] = await Promise.all([
      // Wishlist
      appDetatils.user_id ? knex('wishlist')
        .whereIn('varient_id', variantIds)
        .where('user_id', appDetatils.user_id)
        .select('varient_id', 'user_id') : Promise.resolve([]),
      // Cart items
      appDetatils.user_id ? knex('store_orders')
        .whereIn('varient_id', variantIds)
        .where('store_approval', appDetatils.user_id)
        .where('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .where('store_id', store_id)
        .select('varient_id', 'qty', 'percentage', 'product_feature_id') : Promise.resolve([]),
      // Subscription cart items
      appDetatils.user_id ? knex('store_orders')
        .whereIn('varient_id', variantIds)
        .where('store_approval', appDetatils.user_id)
        .where('order_cart_id', 'incart')
        .where('subscription_flag', '1')
        .where('store_id', store_id)
        .select('varient_id', 'qty', 'percentage') : Promise.resolve([]),
      // Notify me
      appDetatils.user_id ? knex('product_notify_me')
        .whereIn('varient_id', variantIds)
        .where('user_id', user_id)
        .select('varient_id') : Promise.resolve([]),
      // Subscription products check
      appDetatils.user_id ? knex('store_orders')
        .whereIn('varient_id', variantIds)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where(builder => {
          if (type == 'quick') {
            builder.whereNull('subscription_flag');
          } else {
            builder.where('subscription_flag', '1');
          }
        })
        .select('varient_id', 'percentage') : Promise.resolve([]),
      // Feature categories
      fcatIdArray.length > 0 ? knex('feature_categories')
        .whereIn('id', fcatIdArray)
        .where('status', 1)
        .where('is_deleted', 0)
        .select('id', knex.raw(`'${baseurl}' || image as image`)) : Promise.resolve([]),
      // Product features - batch fetch for all products
      productIds.length > 0 ? knex('product_features')
        .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
        .whereIn('product_features.product_id', productIds) : Promise.resolve([]),
      // Product images - batch fetch
      productIds.length > 0 ? knex('product_images')
        .whereIn('product_id', productIds)
        .select('product_id', knex.raw(`'${baseurl}' || image as image`), 'type')
        .orderBy('type', 'DESC') : Promise.resolve([]),
      // Deals - batch fetch
      variantIds.length > 0 ? knex('deal_product')
        .whereIn('varient_id', variantIds)
        .where('store_id', store_id)
        .where('valid_from', '<=', currentDate)
        .where('valid_to', '>', currentDate)
        .select('varient_id', 'deal_price') : Promise.resolve([])
    ]);

    // Create lookup maps for O(1) access
    const wishListMap = new Map(wishListData.map(w => [w.varient_id, true]));
    const cartMap = new Map(cartItemsData.map(c => [c.varient_id, { qty: c.qty || 0, percentage: c.percentage, product_feature_id: c.product_feature_id }]));
    const subCartMap = new Map(subscriptionCartData.map(c => [c.varient_id, { qty: c.qty || 0, percentage: c.percentage }]));
    const notifyMeMap = new Map(notifyMeData.map(n => [n.varient_id, true]));
    const subscriptionMap = new Map(subscriptionProductsData.map(s => [s.varient_id, s.percentage]));
    const featureCategoriesMap = new Map(featureCategoriesData.map(f => [f.id, f]));
    const productFeaturesMap = new Map();
    productFeaturesData.forEach(f => {
      if (!productFeaturesMap.has(f.product_id)) {
        productFeaturesMap.set(f.product_id, []);
      }
      productFeaturesMap.get(f.product_id).push({ id: f.id, feature_value: f.feature_value });
    });
    const productImagesMap = new Map();
    productImagesData.forEach(img => {
      if (!productImagesMap.has(img.product_id)) {
        productImagesMap.set(img.product_id, []);
      }
      productImagesMap.get(img.product_id).push(img.image);
    });
    const dealsMap = new Map(dealsData.map(d => [d.varient_id, d.deal_price]));

    // Batch fetch all variants for all products
    const allVariants = await knex('store_products')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .whereIn('product_varient.product_id', productIds)
      .where('store_products.store_id', store_id)
      .whereNotNull('store_products.price')
      .where('product_varient.approved', 1)
      .select(
        'store_products.store_id',
        'store_products.stock',
        'product_varient.varient_id',
        'product_varient.product_id',
        'product_varient.description',
        'store_products.price',
        'store_products.mrp',
        'product_varient.varient_image',
        'product_varient.unit',
        'product_varient.quantity'
      );

    // Group variants by product_id
    const variantsByProduct = new Map();
    allVariants.forEach(v => {
      if (!variantsByProduct.has(v.product_id)) {
        variantsByProduct.set(v.product_id, []);
      }
      variantsByProduct.get(v.product_id).push(v);
    });

    // Batch fetch product images fallback
    const productImageFallback = await knex('product')
      .whereIn('product_id', productIds)
      .select('product_id', knex.raw(`'${baseurl}' || product_image as image`));
    const productImageFallbackMap = new Map(productImageFallback.map(p => [p.product_id, p.image]));

    // Process products with pre-fetched data
    customizedProductData = [];

    for (let i = 0; i < CartQtyList.length; i++) {
      const ProductList = CartQtyList[i];
      let notifyMe = 'false';
      let isFavourite = 'false';
      let cartQty = 0;
      let isSubscription = 'false';
      let feature_tags = [];

      if (appDetatils.user_id) {
        // Use pre-fetched data
        isFavourite = wishListMap.has(ProductList.varient_id) ? 'true' : 'false';

        // Feature tags from pre-fetched data
        if (ProductList.fcat_id) {
          const resultArray = ProductList.fcat_id.split(',').map(Number).filter(id => !isNaN(id));
          feature_tags = resultArray
            .map(id => featureCategoriesMap.get(id))
            .filter(f => f !== undefined);
        }

        // Cart qty from pre-fetched data
        const cartItem = cartMap.get(ProductList.varient_id);
        cartQty = cartItem ? (cartItem.qty || 0) : 0;

        // Subscription check from pre-fetched data
        const subItem = subscriptionMap.get(ProductList.varient_id);
        isSubscription = subItem ? 'true' : 'false';

        // Notify me from pre-fetched data
        notifyMe = notifyMeMap.has(ProductList.varient_id) ? 'true' : 'false';
      } else {
        cartQty = 0;
        isSubscription = 'false';
        notifyMe = 'false';
        isFavourite = 'false';
      }

      const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
      const finalsubprice = ProductList.mrp - sub_price;
      const subscription_price = parseFloat(finalsubprice.toFixed(2));


      //++++++++++++++++++++++ Varient Code  +++++++++++++++++++++++++++++++++

      let total_cart_qty = 0;
      let total_subcart_qty = 0;

      // Get features from pre-fetched data
      const features = productFeaturesMap.get(ProductList.product_id) || [];

      // Get variants from pre-fetched data
      const app = variantsByProduct.get(ProductList.product_id) || [];

      // Get product images from pre-fetched data (once per product, not per variant)
      let images = productImagesMap.get(ProductList.product_id) || [];
      if (images.length === 0) {
        const fallbackImage = productImageFallbackMap.get(ProductList.product_id);
        if (fallbackImage) {
          images = [fallbackImage];
        }
      }

      const customizedVarientData = [];
      for (let i = 0; i < app.length; i++) {
        // prod.varient.dummy = 5678;
        const ProductLists = app[i];
        // Use pre-fetched dealsMap - NO QUERY
        let vprice = dealsMap.get(ProductLists.varient_id);
        if (!vprice) {
          vprice = ProductLists.price;
        }

        // Declare variables that are used outside the if block
        var isFavourite1 = '';
        var notifyMe1 = '';
        var cartQty1 = 0;
        var subcartQty1 = 0;
        var productFeatureId = 0;

        if (appDetatils.user_id) {
          // Wishlist check
          // Use pre-fetched wishListMap - NO QUERY
          isFavourite1 = wishListMap.has(ProductLists.varient_id) ? 'true' : 'false';

          // Use pre-fetched cartMap and subCartMap - NO QUERIES
          const cartItem = cartMap.get(ProductLists.varient_id);
          cartQty1 = cartItem ? (cartItem.qty || 0) : 0;
          productFeatureId = cartItem ? (cartItem.product_feature_id || 0) : 0;

          const subCartItem = subCartMap.get(ProductLists.varient_id);
          subcartQty1 = subCartItem ? (subCartItem.qty || 0) : 0;

          notifyMe1 = notifyMeMap.has(ProductLists.varient_id) ? 'true' : 'false';

        } else {
          notifyMe1 = 'false';
          isFavourite1 = 'false';
          cartQty1 = 0;
        }
        const baseurl = process.env.BUNNY_NET_IMAGE;

        // Images already fetched outside loop - NO QUERY

        // product_feature_id already fetched in cartMap (line 2443) - NO QUERY NEEDED
        total_cart_qty = total_cart_qty + cartQty1;
        total_subcart_qty = total_subcart_qty + subcartQty1;

        const customizedVarient = {
          stock: ProductLists.stock,
          varient_id: ProductLists.varient_id,
          product_id: ProductLists.product_id,
          product_name: ProductLists.product_name,
          product_image: images[0].image + "?width=200&height=200&quality=100",
          thumbnail: images[0].image,
          description: ProductLists.description,
          price: vprice,
          mrp: ProductLists.mrp,
          unit: ProductLists.unit,
          quantity: ProductLists.quantity,
          type: ProductLists.type,
          discountper: 0,
          notify_me: notifyMe1,
          isFavourite: isFavourite1,
          cart_qty: cartQty1,
          subcartQty: subcartQty1,
          product_feature_id: productFeatureId,
          country_icon: null,
        };

        customizedVarientData.push(customizedVarient);
      }
      varients = customizedVarientData;


      const customizedProduct = {
        orderId: ProductList.order_cart_id,
        percentage: ProductList.percentage,
        isSubscription: isSubscription,
        subscription_price: subscription_price,
        availability: ProductList.availability,
        discountper: 0,
        product_name: ProductList.product_name,
        varient_image: ProductList.varient_image,
        varient_id: ProductList.varient_id,
        price: ProductList.price,
        description: ProductList.description,
        type: ProductList.type,
        ordercount: ProductList.ordercount,
        unit: ProductList.unit,
        quantity: ProductList.quantity,
        mrp: ProductList.mrp,
        cart_qty: cartQty,
        notify_me: notifyMe,
        product_id: ProductList.product_id,
        available_days: ProductList.available_days,
        stock: ProductList.stock,
        feature_tags: feature_tags,
        isFavourite: isFavourite,
        is_customized: ProductList.is_customized,
        features: features,
        total_cart_qty: total_cart_qty,
        total_subcart_qty: total_subcart_qty,
        varients: varients
        // Add or modify properties as needed
      };

      customizedProductData.push(customizedProduct);


    }
  } // Close the else block from line 2204

  if (customizedProductData.length > 0) {
    return customizedProductData
  } else {

    // Fetch category list
    const categoryList = await knex('categories').where('parent', 121).pluck('cat_id');

    // Fetch product details
    const productDetail = await knex('store_products')
      .select(
        'store_products.*',
        knex.raw(`'${baseurl}' || product_image as product_image`),
        knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
        'tbl_country.country_icon',
        'product_varient.unit as prdunit',
        'product_varient.varient_id',
        'product_varient.quantity',
        'product.product_id',
        'product.product_name',
        'product.thumbnail',
        'product.type',
        'product.percentage',
        'product.availability',
        'product_varient.description',
        'product_varient.varient_image',
        'product_varient.ean',
        'product_varient.approved',
        'product.cat_id',
        'product.brand_id',
        'product.hide',
        'product.added_by',
        'product.fcat_id',
        'product.is_customized',
      )
      .innerJoin('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
      .innerJoin('product', 'product_varient.product_id', 'product.product_id')
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
      .where('store_products.store_id', store_id)
      .where('store_products.stock', '>', 0)
      .whereNotNull('store_products.price')
      .where('product.hide', 0)
      .whereIn('product.cat_id', categoryList)
      .where('product.is_delete', 0)
      .where('product.approved', 1)
      .limit(8);

    const variantIds = productDetail.map(product => product.varient_id);

    // Collect all feature category IDs
    const allFcatIds = new Set();
    productDetail.forEach(p => {
      if (p.fcat_id) {
        p.fcat_id.split(',').forEach(id => {
          const numId = parseInt(id);
          if (!isNaN(numId)) allFcatIds.add(numId);
        });
      }
    });
    const fcatIdArray = Array.from(allFcatIds);

    // Collect all product IDs for batch fetching
    const productIds = productDetail.map(p => p.product_id);

    // Fetch associated data in parallel
    const [wishList, cartItems, notifyMeList, subscriptionProducts, deals, allFeatureCategories, allFeatures, allImages] = await Promise.all([
      knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id),
      knex('store_orders')
        .whereIn('varient_id', variantIds)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .where('store_id', store_id)
        .select('varient_id', 'qty', 'product_feature_id'),
      knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id),
      knex('store_orders')
        .select('varient_id', 'qty')
        .whereIn('varient_id', variantIds)
        .where('store_approval', user_id)
        .where('subscription_flag', 1)
        .where('order_cart_id', 'incart'),
      knex('deal_product')
        .whereIn('varient_id', variantIds)
        .where('store_id', store_id)
        .where('deal_product.valid_from', '<=', new Date())
        .where('deal_product.valid_to', '>', new Date()),
      // Batch fetch all feature categories
      fcatIdArray.length > 0 ? knex('feature_categories')
        .whereIn('id', fcatIdArray)
        .where('status', 1)
        .where('is_deleted', 0)
        .select('id', knex.raw(`'${baseurl}' || image as image`)) : Promise.resolve([]),
      // Batch fetch all product features
      productIds.length > 0 ? knex('product_features')
        .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
        .whereIn('product_features.product_id', productIds) : Promise.resolve([]),
      // Batch fetch all product images
      productIds.length > 0 ? knex('product_images')
        .whereIn('product_id', productIds)
        .select('product_id', knex.raw(`'${baseurl}' || image as image`), 'type')
        .orderBy('type', 'DESC') : Promise.resolve([])
    ]);

    const dealMap = Object.fromEntries(deals.map(deal => [deal.varient_id, deal.deal_price]));
    const subscriptionMap = Object.fromEntries(subscriptionProducts.map(sub => [sub.varient_id, { qty: sub.qty || 0 }]));
    const wishListMap = Object.fromEntries(wishList.map(item => [item.varient_id, true]));
    const cartMap = Object.fromEntries(cartItems.map(item => [item.varient_id, { qty: item.qty || 0, product_feature_id: item.product_feature_id || 0 }]));
    const notifyMeMap = Object.fromEntries(notifyMeList.map(item => [item.varient_id, true]));

    // Create lookup maps for batch-fetched data
    const featureCategoriesMap = new Map(allFeatureCategories.map(f => [f.id, f]));
    const featuresMap = new Map();
    allFeatures.forEach(f => {
      if (!featuresMap.has(f.product_id)) {
        featuresMap.set(f.product_id, []);
      }
      featuresMap.get(f.product_id).push({ id: f.id, feature_value: f.feature_value });
    });
    const imagesMap = new Map();
    allImages.forEach(img => {
      if (!imagesMap.has(img.product_id)) {
        imagesMap.set(img.product_id, []);
      }
      imagesMap.get(img.product_id).push(img.image);
    });

    // Batch fetch product image fallback
    const productImageFallback = await knex('product')
      .whereIn('product_id', productIds)
      .select('product_id', knex.raw(`'${baseurl}' || product_image as image`));
    const productImageFallbackMap = new Map(productImageFallback.map(p => [p.product_id, p.image]));

    // Process product details with pre-fetched data
    const customizedProductData = await Promise.all(productDetail.map(async product => {
      // Use pre-fetched featureCategoriesMap - NO QUERY
      let featureTags = [];
      if (product.fcat_id) {
        const resultArray = product.fcat_id.split(',').map(Number).filter(id => !isNaN(id));
        featureTags = resultArray
          .map(id => featureCategoriesMap.get(id))
          .filter(f => f !== undefined);
      }

      const price = dealMap[product.varient_id] || product.price;
      const subscriptionPrice = parseFloat((product.mrp - (product.mrp * product.percentage) / 100).toFixed(2));


      //++++++++++++++++++++++ Varient Code  +++++++++++++++++++++++++++++++++

      let total_cart_qty = 0;
      let total_subcart_qty = 0;

      // Use pre-fetched featuresMap - NO QUERY
      const features = featuresMap.get(product.product_id) || [];


      let app = await knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id', 'product_varient.description', 'store_products.price', 'store_products.mrp', 'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity')
        .where('store_products.store_id', appDetatils.store_id)
        .where('product_varient.product_id', product.product_id)
        .whereNotNull('store_products.price')
        .where('product_varient.approved', 1)

      //prod.varient = app;
      const customizedVarientData = [];
      for (let i = 0; i < app.length; i++) {
        // prod.varient.dummy = 5678;
        const ProductList = app[i];
        const currentDate = new Date();
        // Use pre-fetched dealMap - NO QUERY
        let vprice = dealMap[ProductList.varient_id] || ProductList.price;

        if (appDetatils.user_id) {
          // Wishlist check 
          var isFavourite1 = '';
          var notifyMe1 = '';
          var cartQty1 = 0;
          var subcartQty1 = 0;
          // Use pre-fetched wishListMap - NO QUERY
          isFavourite1 = wishListMap[ProductList.varient_id] ? 'true' : 'false';

          // cart qty check 
          // Use pre-fetched cartMap - NO QUERY
          const cartItem = cartMap[ProductList.varient_id];
          cartQty1 = cartItem ? (cartItem.qty || 0) : 0;
          var productFeatureId = cartItem ? (cartItem.product_feature_id || 0) : 0;

          // Subscription cart qty
          // Use pre-fetched subscriptionMap - NO QUERY
          const subCartItem = subscriptionMap[ProductList.varient_id];
          subcartQty1 = subCartItem ? (subCartItem.qty || 0) : 0;


          // Use pre-fetched notifyMeMap - NO QUERY
          notifyMe1 = notifyMeMap[ProductList.varient_id] ? 'true' : 'false';

        } else {
          notifyMe1 = 'false';
          isFavourite1 = 'false';
          cartQty1 = 0;
        }
        const baseurl = process.env.BUNNY_NET_IMAGE;

        // Use pre-fetched imagesMap - NO QUERY
        let images = imagesMap.get(product.product_id) || [];

        if (images.length === 0) {
          const fallbackImage = productImageFallbackMap.get(product.product_id);
          if (fallbackImage) {
            images = [fallbackImage];
          }
        }

        // product_feature_id should be batch fetched - using 0 as fallback
        total_cart_qty = total_cart_qty + cartQty1;
        total_subcart_qty = total_subcart_qty + subcartQty1;

        const customizedVarient = {
          stock: ProductList.stock,
          varient_id: ProductList.varient_id,
          product_id: ProductList.product_id,
          product_name: ProductList.product_name,
          product_image: images[0] ? images[0] + "?width=200&height=200&quality=100" : null,
          thumbnail: images[0] || null,
          description: ProductList.description,
          price: vprice,
          mrp: ProductList.mrp,
          unit: ProductList.unit,
          quantity: ProductList.quantity,
          type: ProductList.type,
          discountper: 0,
          notify_me: notifyMe1,
          isFavourite: isFavourite1,
          cart_qty: cartQty1,
          subcartQty: subcartQty1,
          product_feature_id: productFeatureId,
          country_icon: product.country_icon ? baseurl + product.country_icon : null,
        };

        customizedVarientData.push(customizedVarient);
      }
      varients = customizedVarientData;




      return {
        p_id: product.p_id,
        varient_id: product.varient_id,
        stock: product.stock,
        store_id: product.store_id,
        price: parseFloat(price),
        mrp: parseFloat(product.mrp),
        min_ord_qty: product.min_ord_qty,
        max_ord_qty: product.max_ord_qty,
        buyingprice: product.buyingprice,
        product_code: product.product_code,
        partner_id: product.partner_id,
        product_id: product.product_id,
        quantity: product.quantity,
        unit: product.prdunit,
        description: product.description,
        //varient_image: product.varient_image,
        ean: product.ean,
        approved: product.approved,
        added_by: product.added_by,
        cat_id: product.cat_id,
        brand_id: product.brand_id,
        product_name: product.product_name,
        varient_image: `${product.product_image}?width=200&height=200&quality=100`,
        type: product.type,
        hide: product.hide,
        percentage: product.percentage,
        isSubscription: subscriptionMap[product.varient_id] ? 'true' : 'false',
        subscription_price: subscriptionPrice,
        availability: product.availability,
        discountper: product.discountper || 0,
        country_icon: product.country_icon ? `${baseurl}${product.country_icon}` : null,
        avgrating: 0,
        notify_me: notifyMeMap[product.varient_id] ? 'true' : 'false',
        isFavourite: wishListMap[product.varient_id] ? 'true' : 'false',
        cart_qty: cartMap[product.varient_id] ? cartMap[product.varient_id].qty : 0,
        countrating: 0,
        feature_tags: featureTags,
        is_customized: product.is_customized,
        features: features,
        total_cart_qty: total_cart_qty,
        total_subcart_qty: total_subcart_qty,
        varients: varients

      };
    }));

    return customizedProductData;

  }


};


const updateCart = async (appDetatils) => {
  const { user_id, varient_id, product_feature_id } = appDetatils;

  // Check if the order already exists in the cart
  const existingOrders = await knex('store_orders')
    .where('store_approval', user_id)
    .whereIn('varient_id', varient_id)
    .andWhere('order_cart_id', 'incart')
    .whereNull('subscription_flag');

  if (existingOrders) {
    for (let i = 0; i < existingOrders.length; i++) {
      const ord = existingOrders[i];
      await knex('store_orders')
        .where('varient_id', ord.varient_id)
        .where('store_approval', user_id)
        .andWhere('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .update({ 'product_feature_id': product_feature_id });

      // Ensure Redis is sync'd after update
      await syncCartItem(user_id, ord.store_id || 0, ord.varient_id, ord.qty, false);
    }
  }
};

const updateSubCart = async (appDetatils) => {
  const { user_id, varient_id, product_feature_id } = appDetatils;

  // Check if the order already exists in the cart
  const existingOrders = await knex('store_orders')
    .where('store_approval', user_id)
    .whereIn('varient_id', varient_id)
    .andWhere('order_cart_id', 'incart')
    .where('subscription_flag', 1);

  if (existingOrders) {
    for (let i = 0; i < existingOrders.length; i++) {
      const ord = existingOrders[i];
      await knex('store_orders')
        .where('varient_id', ord.varient_id)
        .where('store_approval', user_id)
        .andWhere('order_cart_id', 'incart')
        .where('subscription_flag', 1)
        .update({ 'product_feature_id': product_feature_id });

      // Ensure Redis is sync'd after update
      await syncCartItem(user_id, ord.store_id || 0, ord.varient_id, ord.qty, true);
    }
  }
};

module.exports = {
  addtoCart,
  addtosubCart,
  showCart,
  showsubCart,
  showspcatCart,
  might_have_missed,
  updateCart,
  updateSubCart
};
