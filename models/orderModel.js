// models/orderModel.js
const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const { format } = require('date-fns');
const moment = require('moment-timezone');
const { cancelorderMail } = require('../sendGridService');
const { pauseorderMail } = require('../sendGridService');
const { resumeorderMail } = require('../sendGridService');
const { sendRejectNotification } = require('../sendNotification');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

/** Get next store_order_id using MAX+1 (PostgreSQL: store_order_id is NOT NULL, no default) */
const getNextStoreOrderId = async () => {
  const maxResult = await knex('store_orders').max('store_order_id as maxId').first();
  const maxId = maxResult?.maxId != null ? Number(maxResult.maxId) : 0;
  return maxId + 1;
};

/** Get next w_id for wallet_history (PostgreSQL: w_id is PK, no default) */
const getNextWalletHistoryWId = async (knexInstance = knex) => {
  const maxResult = await knexInstance('wallet_history').max('w_id as maxWId').first();
  const maxId = maxResult?.maxWId != null ? parseInt(maxResult.maxWId, 10) : 0;
  return maxId + 1;
};

const totaldeliveries = async () => {

  return ongoing = await knex('tbl_total_deliveries')
    .where('status', '=', 0)
    .orderBy('total_deliveries', 'ASC')

};

const canautorenewal = async (appDetatils) => {
  userId = appDetatils.user_id
  cart_id = appDetatils.cart_id
  store_order_id = appDetatils.store_order_id

  order_data = await knex('store_orders')
    .where('order_cart_id', cart_id)
    .where('store_order_id', store_order_id)
    .where('store_approval', userId)
    .update({ 'isautorenew': "no" });

  return order_data;

}

/** Normalize date to YYYY-MM-DD for comparison */
const toDateStr = (d) => (d == null) ? '' : (typeof d === 'string' ? d.slice(0, 10) : moment(d).format('YYYY-MM-DD'));

/** Compute groupstatus and condition from subscription rows (one query instead of 10+) */
const computeGroupStatusFromSubRows = (subRows, today) => {
  let groupstatus = '';
  let condition = '';
  const grouplisttotal = subRows || [];
  const pendingFutureOrders = (subRows || []).filter(r => r.order_status === 'Pending' && toDateStr(r.delivery_date) > today);
  const pendingPastOrders = (subRows || []).filter(r => r.order_status === 'Pending' && toDateStr(r.delivery_date) < today);
  const grouplistcancel = (subRows || []).filter(r => r.order_status === 'Cancelled');
  const pausePastOrders = (subRows || []).filter(r => r.order_status === 'Pause' && toDateStr(r.delivery_date) < today);
  const pauseFutureOrders = (subRows || []).filter(r => r.order_status === 'Pause' && toDateStr(r.delivery_date) > today);
  const completedOrders = (subRows || []).filter(r => r.order_status === 'Completed');
  const completedPastOrders = (subRows || []).filter(r => r.order_status === 'Completed' && toDateStr(r.delivery_date) < today);
  const confirmedPastOrders = (subRows || []).filter(r => r.order_status === 'Confirmed' && toDateStr(r.delivery_date) < today);
  const cancelledOrders = (subRows || []).filter(r => r.order_status === 'Cancelled');
  if (pendingFutureOrders.length === grouplisttotal.length) { groupstatus = 'Pending'; condition = 'if1'; }
  else if (pendingPastOrders.length === grouplisttotal.length) { groupstatus = 'Expired'; condition = 'if2'; }
  else if (grouplistcancel.length === grouplisttotal.length) { groupstatus = 'Cancelled'; condition = 'if3'; }
  else if (pausePastOrders.length === grouplisttotal.length) { groupstatus = 'Expired'; condition = 'if4'; }
  else if (pendingFutureOrders.length > 0 && pausePastOrders.length > 0) { groupstatus = 'Pending'; condition = 'if5'; }
  else if (pendingPastOrders.length > 0 && pauseFutureOrders.length > 0) { groupstatus = 'Pause'; condition = 'if6'; }
  else if (pendingFutureOrders.length > 0 && pauseFutureOrders.length > 0) { groupstatus = 'Pending'; condition = 'if7'; }
  else if (completedOrders.length > 0 && pauseFutureOrders.length > 0) { groupstatus = 'Pause'; condition = 'if8'; }
  else if (completedPastOrders.length > 0 && pausePastOrders.length > 0 && pendingFutureOrders.length > 0) { groupstatus = 'Pending'; condition = 'if9'; }
  else if (completedPastOrders.length > 0 && confirmedPastOrders.length > 0) { groupstatus = 'Completed'; condition = 'if12'; }
  else if (pendingPastOrders.length > 0 && confirmedPastOrders.length > 0) { groupstatus = 'Expired'; condition = 'if13'; }
  else if (cancelledOrders.length > 0 && pausePastOrders.length > 0) { groupstatus = 'Cancelled'; condition = 'if14'; }
  return { groupstatus, condition };
};

const mergeOrders = async (appDetatils) => {
  const group_id = appDetatils.group_id;
  const today = moment().format('YYYY-MM-DD');
  const [ongoingRows, cartidlist] = await Promise.all([
    knex('orders')
      .join('store', 'orders.store_id', '=', 'store.id')
      .join('users', 'users.id', 'orders.user_id')
      .join('address', 'orders.address_id', '=', 'address.address_id')
      .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
      .where('orders.group_id', group_id)
      .whereNotNull('orders.order_status')
      .whereNotNull('orders.payment_method')
      .orderBy('orders.order_id', 'DESC'),
    knex('subscription_order').where('group_id', group_id).pluck('cart_id')
  ]);
  const ongoing = ongoingRows;
  const uniqueData = [...new Set(cartidlist)];

  if (ongoing && ongoing.length > 0) {
    const customizedProductData = await Promise.all(ongoing.map(async (ProductList) => {
      let upcomingdeliverydateval, laststoreorderid, lastdeliverydateval, card_number, prodwiseinvoiceurl;
      let ongoings, orderStatus, subdataorder;
      const subRowsAll = await knex('subscription_order').where('cart_id', ProductList.cart_id)
        .select('order_status', knex.raw("TO_CHAR(delivery_date, 'YYYY-MM-DD') as delivery_date"));
      const pending_sub = subRowsAll.filter(r => ['Pending', 'pending', 'Out_For_Delivery'].includes(r.order_status));
      const pause_sub = subRowsAll.filter(r => ['Pause', 'pause'].includes(r.order_status));
      const completed_sub = subRowsAll.filter(r => ['Completed', 'completed'].includes(r.order_status));
      const cancelled_sub = subRowsAll.filter(r => ['Cancelled', 'cancelled'].includes(r.order_status));
      const upcomingRow = subRowsAll.filter(r => r.order_status === 'Pending' && r.delivery_date > today).sort((a, b) => (a.delivery_date || '').localeCompare(b.delivery_date || ''))[0];
      upcomingdeliverydateval = upcomingRow ? upcomingRow.delivery_date : null;

      const [ongoing_sub, lastdeliverydateResult, users_acc_details, ongoingsResult] = await Promise.all([
        knex('subscription_order')
          .leftJoin('delivery_boy', 'subscription_order.dboy_id', '=', 'delivery_boy.dboy_id')
          .where('subscription_order.cart_id', ProductList.cart_id)
          .select('*', knex.raw("TO_CHAR(subscription_order.delivery_date, 'YYYY-MM-DD') as delivery_date"))
          .orderBy('subscription_order.delivery_date', 'ASC'),
        knex('subscription_order').where('subscription_order.cart_id', ProductList.cart_id)
          .where('subscription_order.order_status', 'Pending')
          .select('invoice_path', 'store_order_id', knex.raw("TO_CHAR(subscription_order.delivery_date, 'YYYY-MM-DD') as delivery_date"))
          .orderBy('subscription_order.delivery_date', 'DESC').first(),
        knex('tbl_user_bank_details').where('si_sub_ref_no', ProductList.si_sub_ref_no).select('card_no').first(),
        knex('orders').join('store', 'orders.store_id', '=', 'store.id')
          .join('users', 'users.id', 'orders.user_id')
          .join('address', 'orders.address_id', '=', 'address.address_id')
          .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
          .where('orders.cart_id', ProductList.cart_id)
          .whereNotNull('orders.order_status').whereNotNull('orders.payment_method')
          .orderBy('orders.order_id', 'DESC').first()
      ]);
      if (lastdeliverydateResult) {
        laststoreorderid = lastdeliverydateResult.store_order_id;
        lastdeliverydateval = lastdeliverydateResult.delivery_date;
      } else {
        const lastdeliverydatefinal = await knex('subscription_order')
          .where('subscription_order.cart_id', ProductList.cart_id)
          .select('store_order_id', knex.raw("TO_CHAR(subscription_order.delivery_date, 'YYYY-MM-DD') as delivery_date"))
          .orderBy('subscription_order.delivery_date', 'DESC').first();
        laststoreorderid = lastdeliverydatefinal ? lastdeliverydatefinal.store_order_id : null;
        lastdeliverydateval = lastdeliverydatefinal ? lastdeliverydatefinal.delivery_date : null;
      }
      card_number = users_acc_details ? users_acc_details.card_no : null;
      ongoings = ongoingsResult;
      const onSubData = [];
      for (let i = 0; i < ongoing_sub.length; i++) {
        const ProductList = ongoing_sub[i];

        if (ProductList.order_status == 'Processing_payment') {
          orderStatus = 'Processing Payment';
        } else if (ProductList.order_status == 'Payment_failed') {
          orderStatus = 'Payment Failed';
        } else if (ProductList.order_status != 'Cancelled') {
          orderStatus = ProductList.order_status;
        }

        const timestamp = new Date(ProductList.delivery_date).getTime() / 1000; // Convert the date string to a UNIX timestamp in seconds
        const day = new Date(timestamp * 1000).toLocaleString('en-us', { weekday: 'short' });

        const today = new Date();

        var itemOrderStatus = '';
        if (ProductList.order_status == 'Processing_payment') {
          itemOrderStatus = 'Processing Payment';
        } else if (ProductList.order_status == 'Payment_failed') {
          itemOrderStatus = 'Payment Failed';
        } else if (ProductList.order_status != 'Cancelled') {
          itemOrderStatus = ProductList.order_status;
        }
        ongoing_subs = {
          cart_id: ProductList.cart_id,
          subscription_id: ProductList.id,
          cart_id: ProductList.cart_id,
          delivery_date: ProductList.delivery_date,
          time_slot: ProductList.time_slot,
          day: day,
          store_order_id: ProductList.store_order_id,
          processing_product: ProductList.processing_product,
          order_status: itemOrderStatus

        };
        onSubData.push(ongoing_subs);
      }
      subdataorder = onSubData;

      const baseurl = process.env.BUNNY_NET_IMAGE;
      const order = await knex('store_orders')
        .select(
          'store_orders.store_order_id',
          'store_orders.product_name',
          'product.product_id',
          'store_orders.quantity',
          'store_orders.unit',
          'store_orders.varient_id',
          'store_orders.qty',
          knex.raw('(store_orders.price / store_orders.qty) as price'),
          'store_orders.total_mrp',
          'store_orders.order_cart_id',
          'store_orders.order_date',
          'store_orders.repeat_orders',
          'store_orders.store_approval',
          'store_orders.store_id',
          'store_orders.description',
          'store_orders.tx_per',
          'store_orders.price_without_tax',
          'store_orders.tx_price',
          'store_orders.tx_name',
          'store_orders.type',
          'store_orders.isautorenew',
          'store_orders.product_feature_id',
          //   knex.raw(`CONCAT('${baseurl}', product.product_image) as varient_image`),
          knex.raw(`CONCAT(?::text, product.product_image, ?::text) as varient_image`, [baseurl, '?width=100&height=100&quality=100']),
          'product.thumbnail'
        )
        .join('product_varient', 'store_orders.varient_id', 'product_varient.varient_id')
        .join('product', 'product.product_id', 'product_varient.product_id')
        .where('order_cart_id', '=', ProductList.cart_id)
        //.whereIn('order_cart_id',uniqueData)
        .where('store_order_id', '=', laststoreorderid);

      ongoings.discountonmrp = ongoings.total_products_mrp - ongoings.price_without_delivery;
      ongoings.total_items = order.length;

      const customizedOrdData = [];
      total_delivery_week = 0;

      const storeId = ongoings.store_id;
      const [cartRatingData, productFeaturesList, productRatingsList] = await Promise.all([
        knex('delivery_rating').where('cart_id', ProductList.cart_id).select('cart_id', 'description', 'rating').first(),
        (() => {
          const featureIds = [...new Set(order.map(o => o.product_feature_id).filter(Boolean))];
          return featureIds.length ? knex('tbl_feature_value_master').whereIn('id', featureIds) : Promise.resolve([]);
        })(),
        order.length ? knex('product_rating').where('user_id', ProductList.user_id).where('store_id', storeId).whereIn('varient_id', order.map(o => o.varient_id)) : Promise.resolve([])
      ]);
      const productFeatureMap = {};
      (productFeaturesList || []).forEach(p => { productFeatureMap[p.id] = p; });
      const productRatingMap = {};
      (productRatingsList || []).forEach(r => { productRatingMap[r.varient_id] = r; });

      for (let i = 0; i < order.length; i++) {
        const ordList = order[i];
        const product_feature = ordList.product_feature_id ? productFeatureMap[ordList.product_feature_id] : null;
        ordList.product_feature_value = product_feature ? product_feature.feature_value : null;
        ordList.drating = (cartRatingData && cartRatingData.rating) ? cartRatingData.rating : 0;
        ordList.dreview = (cartRatingData && cartRatingData.description) ? cartRatingData.description : '';
        const getrating = productRatingMap[ordList.varient_id];
        if (getrating) {
          ordList.rating = getrating.rating;
          ordList.rating_description = getrating.description;
        } else {
          ordList.rating = "null";
          ordList.rating_description = "null";
        }
        repeat_orderss = (ProductList.repeat_orders || '').toString().split(',').map(s => s.trim()).filter(Boolean);
        total_delivery_week = repeat_orderss.length * (Number(ProductList.total_delivery) || 0);
      }

      let groupstatus = orderStatus;
      let condition = '';
      const statusResult = computeGroupStatusFromSubRows(subRowsAll, today);
      if (statusResult.groupstatus) groupstatus = statusResult.groupstatus;
      if (statusResult.condition) condition = statusResult.condition;

      const [invoice, specialinstruction, deliverypartnerinstruction] = await Promise.all([
        knex('subscription_order').where('subscription_order.cart_id', ProductList.cart_id).select('invoice_path').first(),
        knex('subscription_order').join('orders', 'orders.cart_id', 'subscription_order.cart_id')
          .where('subscription_order.group_id', group_id).where('orders.order_status', '!=', 'Cancelled')
          .select('orders.order_instruction', 'orders.order_date')
          .distinct('orders.order_instruction', 'orders.order_date').orderBy('orders.order_date', 'asc'),
        knex('subscription_order').join('orders', 'orders.cart_id', 'subscription_order.cart_id')
          .where('subscription_order.group_id', group_id).where('orders.order_status', '!=', 'Cancelled')
          .select('orders.del_partner_instruction', 'orders.order_date')
          .distinct('orders.del_partner_instruction', 'orders.order_date').orderBy('orders.order_date', 'asc')
      ]);
      const invoicePdfUrl = process.env.BUNNY_NET_INVOICE_PDF;
      if (invoice && invoice.invoice_path) {
        const path = invoice.invoice_path;
        if (path.startsWith('https://www.quickart.ae')) {
          prodwiseinvoiceurl = path;
        } else {
          const filename = path.split('/').pop();
          prodwiseinvoiceurl = invoicePdfUrl + filename;
        }
        console.log(`[MergeOrders] Invoice Path found: "${path}" -> Final URL: "${prodwiseinvoiceurl}"`);
      } else {
        prodwiseinvoiceurl = "https://icseindia.org/document/sample.pdf";
        console.log(`[MergeOrders] No invoice path found for cart_id: ${ProductList.cart_id}. Returning placeholder.`);
      }
      const addressParts = [
        ProductList.house_no,
        ProductList.society,
        ProductList.city,
        ProductList.landmark,
        ProductList.state,
        ProductList.pincode
      ].filter(part => part != null);
      let specialinstructionval = "";
      if (specialinstruction.length > 0) {
        const seen = new Set();
        const instructions = specialinstruction.filter(r => {
          if (seen.has(r.order_instruction)) return false;
          seen.add(r.order_instruction);
          return true;
        }).map(r => r.order_instruction);
        specialinstructionval = instructions.join(", ");
      }
      let deliverypartnerinstructionval = "";
      if (deliverypartnerinstruction.length > 0) {
        const seen1 = new Set();
        const instructions1 = deliverypartnerinstruction.filter(r => {
          if (seen1.has(r.del_partner_instruction)) return false;
          seen1.add(r.del_partner_instruction);
          return true;
        }).map(r => r.del_partner_instruction);
        deliverypartnerinstructionval = instructions1.join(", ");
      }


      const delivery_address = addressParts.join(', ');
      const customizedProduct = {
        user_name: ProductList.name,
        total_delivery: ongoings.total_delivery,
        delivery_address: delivery_address,
        // delivery_address:ProductList.building_villa+','+ongoings.street+','+ongoings.society+','+ongoings.city,
        store_name: ProductList.store_name,
        store_owner: ProductList.employee_name,
        store_phone: ProductList.phone_number,
        store_address: ProductList.address,
        order_status: groupstatus,
        condition: condition,
        delivery_date: upcomingdeliverydateval,
        time_slot: ProductList.time_slot,
        payment_method: ProductList.payment_method,
        payment_status: ProductList.payment_status,
        paid_by_wallet: parseFloat(ProductList.paid_by_wallet || 0) + parseFloat(ProductList.paid_by_ref_wallet || 0),
        cart_id: ProductList.cart_id,
        //total_price:(pricess*total_delivery_week).toFixed(2),
        delivery_charge: Number(ProductList.delivery_charge).toFixed(2),
        rem_price: Number(ProductList.rem_price).toFixed(2),
        coupon_discount: Number(ProductList.coupon_discount).toFixed(2),
        dboy_name: ProductList.boy_name,
        dboy_phone: ProductList.boy_phone,
        price_without_delivery: Number(ProductList.price_without_delivery).toFixed(2),
        avg_tax_per: ProductList.avg_tax_per,
        total_tax_price: ProductList.total_tax_price,
        user_id: ProductList.user_id,
        total_products_mrp: ProductList.total_products_mrp,
        discountonmrp: ProductList.discountonmrp,
        cancelling_reason: ProductList.cancelling_reason,
        order_date: ProductList.order_date,
        dboy_id: ProductList.dboy_id,
        user_signature: ProductList.user_signature,
        coupon_id: ProductList.coupon_id,
        dboy_incentive: ProductList.dboy_incentive,
        total_items: ProductList.total_items,
        is_subscription: ProductList.is_subscription,
        pending_order: pending_sub.length,
        pause_order: pause_sub.length,
        completed_order: completed_sub.length,
        cancelled_order: cancelled_sub.length,
        total_order: total_delivery_week, // need to change
        data: order,
        subscription_details: subdataorder,
        lastdeliverydate: lastdeliverydateval,
        card_number: card_number,
        prodwiseinvoice: prodwiseinvoiceurl,
        pastorecentrder: ProductList.pastorecentrder,
        special_instruction: specialinstructionval,
        delivery_partner_instruction: deliverypartnerinstructionval
      };

      return customizedProduct;
    }));

    return customizedProductData;
  } else {
    throw new Error('No Orders Yet');
  }
}

const orderwiselist = async (appDetatils) => {
  const { user_id: rawUserId, device_id, store_id } = appDetatils;
  const user_id = (rawUserId && rawUserId !== "null" && rawUserId !== "") ? rawUserId : device_id;
  if (!user_id || user_id === "" || user_id === "null") {
    return [];
  }
  const today = new Date();
  const formattedToday = format(today, 'yyyy-MM-dd');
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // 1. Fetch the main order groups (Limit 8 as before)
  const orders = await knex("orders")
    .select(
      knex.raw("MAX(orders.pastorecentrder) as pastorecentrder"),
      knex.raw("MAX(orders.cart_id) as cart_id"),
      "orders.group_id",
      knex.raw("MAX(orders.is_subscription) as is_subscription"),
      knex.raw("TO_CHAR(MAX(orders.delivery_date), 'YYYY-MM-DD') as delivery_date"),
      knex.raw("TO_CHAR(MAX(orders.order_date), 'YYYY-MM-DD') as order_date"),
      knex.raw("MAX(orders.order_status) as order_status"),
      knex.raw("SUM(orders.total_products_mrp) as totalProductsMrp")
    )
    .where("orders.user_id", user_id)
    .whereNotNull("orders.payment_method")
    .whereNot("orders.order_status", null)
    .orderByRaw("MAX(orders.order_id) DESC")
    .groupBy("orders.group_id")
    .limit(8);

  if (orders.length === 0) return [];

  const groupIds = orders.map((o) => o.group_id);

  // 2. Batch Fetch Metadata for all groupIds to avoid N+1 queries
  const [orderDetailsBatch, storeOrdersBatch, productSummariesBatch, nextDeliveryDatesBatch, subStatusCountsBatch, futureProdListsBatch] = await Promise.all([
    // A. Cart IDs and MRPs for each group
    knex("orders")
      .whereIn("group_id", groupIds)
      .select("cart_id", "group_id", "total_products_mrp"),

    // B. Store orders for basic product list (Quick orders or fallback)
    knex("store_orders")
      .join("orders", "orders.cart_id", "=", "store_orders.order_cart_id")
      .whereIn("orders.group_id", groupIds)
      .where("store_orders.store_id", store_id)
      .select(
        "order_cart_id",
        "orders.group_id",
        knex.raw(`CONCAT(store_orders.product_name, ' X ', store_orders.qty) as product_name`),
        knex.raw(`CONCAT(?::text, varient_image, ?::text) as varient_image`, [baseurl, '?width=100&height=100&quality=100'])
      ),

    // C. Batch product summary strings (STRING_AGG)
    knex("orders")
      .select(
        "orders.group_id",
        knex.raw(`STRING_AGG(store_orders.product_name || ' X ' || store_orders.qty, ', ') as product_details`)
      )
      .join("store_orders", "orders.cart_id", "store_orders.order_cart_id")
      .whereIn("orders.group_id", groupIds)
      .groupBy("orders.group_id"),

    // D. Batch next delivery dates for subscriptions
    knex("subscription_order")
      .select("group_id", knex.raw("MIN(TO_CHAR(delivery_date, 'YYYY-MM-DD')) as next_date"))
      .whereIn("group_id", groupIds)
      .where('delivery_date', '>=', today)
      .groupBy("group_id"),

    // E. Batch subscription status counts (future pending/completed)
    knex('subscription_order')
      .select(
        'group_id',
        knex.raw("COUNT(*) FILTER (WHERE order_status = 'Pending' AND delivery_date > ?) as future_pending", [formattedToday]),
        knex.raw("COUNT(*) FILTER (WHERE order_status = 'Completed' AND delivery_date > ?) as future_completed", [formattedToday])
      )
      .whereIn('group_id', groupIds)
      .groupBy('group_id'),

    // F. Batch future pending product lists
    knex('subscription_order')
      .select(
        "subscription_order.group_id",
        "store_orders.order_cart_id",
        knex.raw(`CONCAT(store_orders.product_name, ' X ', store_orders.qty) as product_name`),
        knex.raw(`CONCAT(?::text, varient_image, ?::text) as varient_image`, [baseurl, '?width=100&height=100&quality=100'])
      )
      .join('store_orders', 'store_orders.store_order_id', 'subscription_order.store_order_id')
      .whereIn('subscription_order.group_id', groupIds)
      .where('delivery_date', '>', formattedToday)
      .where('subscription_order.order_status', 'Pending')
      .groupBy('subscription_order.group_id', 'store_orders.order_cart_id', 'store_orders.product_name', 'store_orders.qty', 'store_orders.varient_image')
  ]);

  // 3. Map batch results into efficient lookup objects
  const orderDataMap = {};
  orderDetailsBatch.forEach(d => {
    (orderDataMap[d.group_id] = orderDataMap[d.group_id] || []).push(d);
  });

  const storeOrderMap = {};
  storeOrdersBatch.forEach(o => {
    (storeOrderMap[o.group_id] = storeOrderMap[o.group_id] || []).push(o);
  });

  const productSummaryMap = Object.fromEntries(productSummariesBatch.map(s => [s.group_id, s.product_details]));
  const nextDeliveryMap = Object.fromEntries(nextDeliveryDatesBatch.map(d => [d.group_id, d.next_date]));
  const subStatusMap = Object.fromEntries(subStatusCountsBatch.map(s => [s.group_id, s]));

  const futureProdListMap = {};
  futureProdListsBatch.forEach(p => {
    (futureProdListMap[p.group_id] = futureProdListMap[p.group_id] || []).push(p);
  });

  // 4. Assemble final data without any further database hits
  return orders.map((order) => {
    const isSub = order.is_subscription === 1;
    const typeval = isSub ? "Subscription" : "Quick";
    const groupData = orderDataMap[order.group_id] || [];

    // Total MRP calculation
    const totalProductsMrp = order.pastorecentrder === "old"
      ? groupData.reduce((sum, d) => sum + parseFloat(d.total_products_mrp || 0), 0)
      : parseFloat(order.totalProductsMrp || 0);

    // Order status logic
    let finalStatus = order.order_status;
    if (isSub && order.order_status !== 'Cancelled') {
      const stats = subStatusMap[order.group_id];
      const hasFuturePending = stats ? parseInt(stats.future_pending) > 0 : false;
      const hasFutureCompleted = stats ? parseInt(stats.future_completed) > 0 : false;
      finalStatus = hasFuturePending ? 'Pending' : (hasFutureCompleted ? 'Completed' : 'Pending');
    }

    return {
      cart_id: order.cart_id,
      group_id: order.group_id,
      order_date: order.order_date,
      delivery_date: isSub ? (nextDeliveryMap[order.group_id] || "") : "",
      order_status: finalStatus,
      total_mrp: totalProductsMrp.toFixed(2),
      type: typeval,
      is_subscription: order.is_subscription,
      productname: productSummaryMap[order.group_id] || "",
      prodList: (isSub && (futureProdListMap[order.group_id] || []).length > 0)
        ? futureProdListMap[order.group_id]
        : (storeOrderMap[order.group_id] || []),
    };
  });
};



const grpordDetails = async (appDetatils) => {

  const { user_id, group_id } = appDetatils;
  const ongoing = await knex('orders')
    .join('users', 'orders.user_id', '=', 'users.id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .where('orders.group_id', group_id)
    .orderBy('orders.order_id', 'desc');
  const customizedOrderData = [];
  for (let i = 0; i < ongoing.length; i++) {
    const ProductList = ongoing[i];
    const ordersss = await knex('store_orders')
      .where('order_cart_id', ProductList.cart_id)
      .select();

    const subscriptionOrderList = await knex('subscription_order')
      .where('cart_id', ProductList.cart_id)
      .where('order_status', 'Pending')
      .orderBy('delivery_date', 'asc')
      .first();

    const startDeliveryDate = subscriptionOrderList ? subscriptionOrderList.delivery_date : null;

    let total_price = 0;

    for (const store of ordersss) {
      if (store.repeat_orders) {
        const repeat_orderss = store.repeat_orders.split(',').map(order => order.trim());
        total_delivery_week = repeat_orderss.length * ProductList.total_delivery;
      } else {
        const repeat_orderss = 1
        total_delivery_week = repeat_orderss * ProductList.total_delivery;
      }

      total_price += store.price * total_delivery_week;
    }


    const productDetails = await knex
      .select(
        'store_order_id',
        'product_name',
        'varient_image',
        'quantity',
        'unit',
        'varient_id',
        'qty',
        knex.raw('(price/qty) as price'),
        'total_mrp',
        'order_cart_id',
        'order_date',
        'repeat_orders',
        'type'
      )
      .from('store_orders')
      .where('order_cart_id', ProductList.cart_id);

    const customizedProductData = [];
    for (let j = 0; j < productDetails.length; j++) {
      const itemProduct = productDetails[j];

      const dataasss = await knex('product_varient')
        .select('*')
        .join('product', 'product.product_id', '=', 'product_varient.product_id')
        .where('product_varient.varient_id', '=', itemProduct.varient_id)
        .first();

      const currentDate = new Date();
      let next_delivery_date = '';
      const subscription_order_o = await knex('subscription_order')
        .where('store_order_id', itemProduct.store_order_id)
        .where('delivery_date', '>=', currentDate)
        .where('order_status', 'Pending')
        .first();

      next_delivery_date = subscription_order_o ? subscription_order_o.delivery_date : undefined;

      let orderStatusDelivery;

      if (next_delivery_date) {
        next_delivery_date = format(next_delivery_date, 'yyyy-MM-dd');
        orderStatusDelivery = 'Active';
      } else {
        const subscription_order_completed = await knex('subscription_order')
          .where('store_order_id', itemProduct.store_order_id)
          .where('order_status', 'Completed')
          .select('*');

        const subscription_order_pending = await knex('subscription_order')
          .where('store_order_id', itemProduct.store_order_id)
          .where('order_status', 'Pending')
          .select('*');

        if (subscription_order_completed.length > 0 || subscription_order_pending.length > 0) {
          orderStatusDelivery = 'Completed';
        } else {

          const subscription_order_cancelled = await knex('subscription_order')
            .where('store_order_id', itemProduct.store_order_id)
            .where('order_status', 'Cancelled')
            .select('*');

          if (subscription_order_cancelled.length > 0) {
            // orderStatusDelivery = 'Cancelled';
            orderStatusDelivery = '';
          } else {
            orderStatusDelivery = 'Pending';
          }

        }
        next_delivery_date = '';
      }
      //  return ongoing.order_status
      if (ongoing.order_status == 'Cancelled') {
        orderStatusDelivery = 'Cancelled'
      }

      const baseurl = process.env.BUNNY_NET_IMAGE;
      const customizedProduct = {
        store_order_id: itemProduct.store_order_id,
        product_name: itemProduct.product_name,
        varient_image: baseurl + itemProduct.varient_image,
        thumbnail: baseurl + dataasss.thumbnail,
        unit: itemProduct.unit,
        varient_id: itemProduct.orderStatusDeliveryvarient_id,
        qty: itemProduct.qty,
        price: itemProduct.price,
        total_mrp: itemProduct.total_mrp,
        order_date: itemProduct.order_date,
        repeat_orders: itemProduct.repeat_orders,
        type: itemProduct.type,
        order_status_delivery: orderStatusDelivery,
        next_delivery_date: next_delivery_date,
        // Add or modify properties as needed
      };



      customizedProductData.push(customizedProduct);
    }

    let startDeliveryDates = startDeliveryDate ? format(startDeliveryDate, 'yyyy-MM-dd') : "null";
    let order_date = format(ProductList.order_date, 'yyyy-MM-dd');
    const customizedProduct = {
      address_name: ProductList.receiver_name,
      delivery_address: ProductList.building_villa + "," + ProductList.street + "," + ProductList.society + "," + ProductList.city,
      order_status: ProductList.order_status,
      delivery_date: startDeliveryDates,
      time_slot: ProductList.time_slot,
      payment_method: ProductList.si_order == 'no' ? 'Wallet' : 'Card Payment',
      cart_id: ProductList.cart_id,
      total_price: total_price,
      delivery_charge: ProductList.delivery_charge,
      coupon_discount: ProductList.coupon_discount,
      price_without_delivery: total_price,
      user_id: ProductList.user_id,
      total_products_mrp: total_price,
      cancelling_reason: ProductList.cancelling_reason,
      order_date: order_date,
      coupon_id: ProductList.coupon_id,
      is_subscription: ProductList.is_subscription,
      total_delivery: ProductList.total_delivery,
      repeat_orders: ProductList.repeat_orders,
      si_order: ProductList.si_order,
      data: customizedProductData,
    };


    customizedOrderData.push(customizedProduct);

  }

  return customizedOrderData



};

const getCancelquickOrderProd737 = async (appDetatils) => {
  const minCardRefundThreshold = 0.09;
  const user_id = appDetatils.user_id
  const cart_id = appDetatils.cart_id
  const cancel_reason = appDetatils.cancel_reason
  let groupIdForEmail = null; // in scope for email section after if block
  let autoCancelledOfferCartIds = [];
  let autoOfferCancellationApplied = false;

  if (cart_id && user_id) {

    // Check if there are any orders that are not already cancelled
    const existingOrders = await knex('orders')
      .where('cart_id', cart_id)
      .where('order_status', 'Cancelled')
      .first();
    // If there are no orders to cancel, return a message
    if (existingOrders) {
      throw new Error('Order are already cancelled.');
    }


    subcription_data = await knex('subscription_order')
      .where('cart_id', cart_id)
      .where('order_status', 'Pending')
      //.where('si_payment_flag', 'no')
      //.where('processing_product','!=','1')
      .update({
        'cancel_reason': cancel_reason,
        'order_status': "Cancelled"
      });

    order_data = await knex('orders')
      .where('cart_id', cart_id)
      //.where('group_id', group_id)
      .where('order_status', 'Pending')
      // .where('payment_status','!=','success')
      .update({ 'order_status': "Cancelled", 'cancelling_reason': cancel_reason });

    const cancelledCardRefundRow = await knex('orders')
      .where('cart_id', cart_id)
      .select(
        knex.raw("COALESCE(SUM(CASE WHEN orders.payment_method != 'COD' AND orders.payment_status = 'success' THEN GREATEST((orders.total_price::numeric - COALESCE(orders.paid_by_wallet, 0)::numeric - COALESCE(orders.paid_by_ref_wallet, 0)::numeric), 0) ELSE 0 END), 0) as card_paid"),
        knex.raw("COALESCE(SUM(NULLIF(trim(orders.rem_price::text), '')::numeric), 0) as rem_price")
      )
      .first();
    let cancelledCardRefund = Math.max(
      Number(cancelledCardRefundRow?.card_paid || 0),
      Number(cancelledCardRefundRow?.rem_price || 0)
    );
    if (cancelledCardRefund <= minCardRefundThreshold) {
      cancelledCardRefund = 0;
    }

    //After cancelled order wallet amount save 
    const orderDetail = await knex('orders')
      .where('cart_id', cart_id)
      .select('payment_method', 'user_id', 'group_id', 'coupon_code')
      .first();
    if (!orderDetail?.group_id) throw new Error('Order not found for this cart.');
    const groupID = orderDetail.group_id;
    groupIdForEmail = groupID;
    const couponCode = orderDetail.coupon_code ?? '';
    const userId = appDetatils.user_id;
    const offerCancelEligibilityRow = await knex('store_orders as so')
      .join('orders as o', 'o.cart_id', 'so.order_cart_id')
      .join('product_varient as pv', 'pv.varient_id', 'so.varient_id')
      .join('product as p', 'p.product_id', 'pv.product_id')
      .where('so.order_cart_id', cart_id)
      .where('p.is_offer_product', 1)
      .whereRaw('p.offer_date::date = o.order_date::date')
      .first();
    const canApplyOfferThresholdCancellation = !!offerCancelEligibilityRow;

    const orderDetails = await knex('orders')
      .select('*')
      .where('group_id', groupID);



    const storeDetailsAmtAll = await knex('orders')
      .where('group_id', groupID)
      .select(
        knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as \"codCharges\""),
        knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\""),
        knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet'),
        knex.raw('COALESCE(SUM(orders.paid_by_ref_wallet), 0) as paid_by_ref_wallet'),
        knex.raw('MAX(orders.payment_method) as payment_method')
      )
      .first();
    const paidByWallet = Math.round(storeDetailsAmtAll?.paid_by_wallet ?? 0);
    const paidByRefWallet = Math.round(storeDetailsAmtAll?.paid_by_ref_wallet ?? 0);
    let WalletAddtoUserAccount = 0;
    let FinalWalletAmountUse = 0;
    let cashWalletAddtoUserAccount = 0;
    let cashFinalWalletAmountUse = 0;
    for (const orders of orderDetails) {
      const cartID = orders.cart_id;
      if (orders.payment_method == 'COD') {
        //COD Code Write
        const storeOrderDetails = await knex('store_orders')
          .where('order_cart_id', cartID)
          .select('*')
          .first();

        const storeDetailsAmt = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .where('group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .select(knex.raw('SUM(store_orders.total_mrp) as "Totalmrp"'), knex.raw('SUM(store_orders.price) as "Totalprice"'))
          .first();


        const TotalpriceStore = parseFloat(storeDetailsAmt?.Totalprice ?? 0);
        const TotalmrpStore = parseFloat(storeDetailsAmt?.Totalmrp ?? 0);

        // if(TotalpriceStore < 30){

        //     order_data = await knex('orders')
        //       .where('group_id', groupID)
        //       .where('is_offer_product', '1')
        //       .update({'order_status':"Cancelled"});

        //     subcription_data = await knex('subscription_order')
        //             .where('group_id', groupID)
        //             .where('is_offer_product', '1')
        //             .update({'cancel_reason':cancel_reason,
        //             'order_status':"Cancelled"});  

        // }

        const CouponDis = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
          .sum({ total_price: 'store_orders.price' })
          .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
          .where('orders.group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .first();

        if (orders.order_status == 'Cancelled') {
          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          await knex('orders')
            .where('cart_id', cartID)
            .update({
              'order_status': 'Cancelled',
              'cancelling_reason': cancel_reason,
              'total_price': parseFloat(totalPrice).toFixed(2),
              'price_without_delivery': parseFloat(totalPrice).toFixed(2),
              'total_products_mrp': parseFloat(totalPrice).toFixed(2),
              'paid_by_wallet': 0,
              'paid_by_ref_wallet': 0
            });

        } else {

          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          const totalMRP = parseFloat(storeOrderDetails?.total_mrp ?? 0);
          const codCharges = parseFloat(storeDetailsAmtAll?.codCharges ?? 0);
          const delPartnerTip = parseFloat(storeDetailsAmtAll?.delPartnerTip ?? 0);


          let couponDiscount = 0;
          let couponDiscounts = 0;
          if (couponCode) {
            const couponDetails = await knex('coupon')
              .where('coupon_code', couponCode)
              .select('*')
              .first();
            const itemMrp = await knex('store_products')
              .where('varient_id', storeOrderDetails.varient_id)
              .select('mrp')
              .first();
            const itemQty = parseFloat(storeOrderDetails?.qty ?? 0);
            const unitPrice = itemQty > 0 ? (parseFloat(totalPrice) / itemQty) : 0;
            couponDiscount = (itemQty > 0 && unitPrice >= parseFloat(itemMrp?.mrp ?? 0))
              ? ((totalPrice * couponDetails.amount) / 100)
              : 0;
            couponDiscounts = (parseFloat(CouponDis?.total_price ?? 0) > 0) ? ((parseFloat(CouponDis?.total_price ?? 0) * couponDetails.amount) / 100) : 0;
          }

        const TotalPriceOrders = ((parseFloat(TotalpriceStore) - couponDiscounts) + codCharges + delPartnerTip);
        const WalletDiscount = parseFloat(TotalPriceOrders) * 50 / 100;

        WalletAddtoUserAccount = paidByRefWallet > WalletDiscount ? paidByRefWallet - WalletDiscount : 0;
        const WalletUseOrder = WalletAddtoUserAccount == 0 && paidByRefWallet ? paidByRefWallet : paidByRefWallet - WalletDiscount;
        cashWalletAddtoUserAccount = paidByWallet;

        const codorderamt = codCharges ? (totalPrice * codCharges / TotalpriceStore) : 0;
        const delPartnerTipAmt = delPartnerTip ? (totalPrice * delPartnerTip / TotalpriceStore) : 0;
        const totalPriceAmt = (parseFloat(totalPrice) - parseFloat(couponDiscount)) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt);

        let cashFinalWalletAmountUse = 0;
        let FinalWalletAmountUse = 0;

        if (paidByWallet > 0) {
          const walletRef = await knex('wallet_history')
            .where({
              cart_id: cartID,
              type: 'deduction',
              resource: 'order_placed_wallet'
            })
            .select(
              knex.raw(
                'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
              )
            )
            .first();
          cashFinalWalletAmountUse = (walletRef?.total_wallet_used || 0);
        }

        if (paidByRefWallet > 0) {
          const walletRef = await knex('wallet_history')
            .where({
              cart_id: cartID,
              type: 'deduction',
              resource: 'order_placed_wallet_ref'
            })
            .select(
              knex.raw(
                'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_ref_used'
              )
            )
            .first();
          FinalWalletAmountUse = (walletRef?.total_wallet_ref_used || 0);
        }

        await knex('orders')
          .where('cart_id', cartID)
          .update({
            'total_price': parseFloat(totalPriceAmt).toFixed(2),
            'price_without_delivery': parseFloat(totalPriceAmt).toFixed(2),
            'total_products_mrp': parseFloat(totalPriceAmt).toFixed(2),
            'coupon_discount': parseFloat(couponDiscount).toFixed(2),
            'cod_charges': parseFloat(codorderamt).toFixed(2),
            'del_partner_tip': parseFloat(delPartnerTipAmt).toFixed(2),
          });

        }

      } else {

        //CARD Code Write
        const storeOrderDetails = await knex('store_orders')
          .where('order_cart_id', cartID)
          .select('*')
          .first();

        const storeDetailsAmt = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .where('group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .select(knex.raw('SUM(store_orders.total_mrp) as "Totalmrp"'), knex.raw('SUM(store_orders.price) as "Totalprice"'))
          .first();

        const TotalpriceStore = parseFloat(storeDetailsAmt?.Totalprice ?? 0);
        const TotalmrpStore = parseFloat(storeDetailsAmt?.Totalmrp ?? 0);

        if (!autoOfferCancellationApplied && cartID == cart_id && TotalpriceStore < 30 && canApplyOfferThresholdCancellation) {
          const pendingOfferRows = await knex('orders')
            .where('group_id', groupID)
            .where('is_offer_product', 1)
            .where('order_status', 'Pending')
            .select('cart_id');
          autoCancelledOfferCartIds = pendingOfferRows
            .map((row) => row.cart_id)
            .filter((id) => id && id !== cart_id);

          order_data = await knex('orders')
            .where('group_id', groupID)
            .where('is_offer_product', 1)
            .update({ 'order_status': "Cancelled", 'cancelling_reason': cancel_reason });

          subcription_data = await knex('subscription_order')
            .where('group_id', groupID)
            .where('is_offer_product', 1)
            .update({
              'cancel_reason': cancel_reason,
              'order_status': "Cancelled"
            });

          console.log('[cancelledquickorderprod][offer-auto-cancel-triggered]', {
            group_id: groupID,
            requested_cart_id: cart_id,
            trigger_cart_id: cartID,
            total_price_store_after_cancel: Number(TotalpriceStore || 0).toFixed(2),
            threshold: 30,
            can_apply_offer_threshold_cancellation: canApplyOfferThresholdCancellation,
            auto_cancelled_offer_cart_ids: autoCancelledOfferCartIds,
          });

          autoOfferCancellationApplied = true;
        }

        const CouponDis = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
          .sum({ total_price: 'store_orders.price' })
          .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
          .where('orders.group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .first();



        if (orders.order_status == 'Cancelled') {
          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          const totalMrp = parseFloat(storeOrderDetails?.total_mrp ?? 0);
          await knex('orders')
            .where('cart_id', cartID)
            .update({
              'order_status': 'Cancelled',
              'cancelling_reason': cancel_reason,
              'total_price': totalPrice.toFixed(2),
              'price_without_delivery': totalPrice.toFixed(2),
              'total_products_mrp': totalPrice.toFixed(2),
              'paid_by_wallet': 0,
              'paid_by_ref_wallet': 0
            });


          let couponDiscount = 0;
          let couponDiscounts = 0;
          if (couponCode) {
            const couponDetails = await knex('coupon')
              .where('coupon_code', couponCode)
              .select('*')
              .first();
            const itemMrp = await knex('store_products')
              .where('varient_id', storeOrderDetails.varient_id)
              .select('mrp')
              .first();
            const itemQty = parseFloat(storeOrderDetails?.qty ?? 0);
            const unitPrice = itemQty > 0 ? (parseFloat(totalPrice) / itemQty) : 0;
            couponDiscount = (itemQty > 0 && unitPrice >= parseFloat(itemMrp?.mrp ?? 0))
              ? ((totalPrice * couponDetails.amount) / 100)
              : 0;

          }

          if (cartID == cart_id) {
            let deliveryPT = 0;
            const result = await knex('orders')
              .where('group_id', groupID)
              .where('order_status', 'Cancelled')
              .count({ cancelled_count: 'order_id' });

            const cancelledCount = parseInt(result[0]?.cancelled_count ?? 0, 10);

            const resultAll = await knex('orders')
              .where('group_id', groupID)
              .count({ all_count: 'order_id' });

            const allCount = parseInt(resultAll[0]?.all_count ?? 0, 10);

            const OrdersDetailsAmt = await knex('orders')
              .where('group_id', groupID)
              .select(knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\""))
              .first();

            if (cancelledCount === allCount) {
              deliveryPT = parseFloat(OrdersDetailsAmt?.delPartnerTip ?? 0);
            }
            const TotalPriceOrdersAmt = ((parseFloat(totalPrice) - parseFloat(couponDiscount))) + parseFloat(deliveryPT);
            const user = await knex('users')
              .select('user_phone', 'wallet_balance', 'referral_balance')
              .where('id', userId)
              .first();

            let walletUsedRaw = 0;
            let refUsedRaw = 0;
            if (paidByWallet > 0) {
              const walletRef = await knex('wallet_history')
                .where({
                  cart_id: cartID,
                  type: 'deduction',
                  resource: 'order_placed_wallet'
                })
                .select(
                  knex.raw(
                    'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
                  )
                )
                .first();
              walletUsedRaw = Number(walletRef?.total_wallet_used || 0);
            }
            if (paidByRefWallet > 0) {
              const walletRef = await knex('wallet_history')
                .where({
                  cart_id: cartID,
                  type: 'deduction',
                  resource: 'order_placed_wallet_ref'
                })
                .select(
                  knex.raw(
                    'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
                  )
                )
                .first();
              refUsedRaw = Number(walletRef?.total_wallet_used_ref || 0);
            }

            let walletUsed = walletUsedRaw;
            let refUsed = refUsedRaw;
            if (cancelledCount !== allCount) {
              const cancelledCartTipAmount = Math.max(0, Number(orders?.del_partner_tip || 0));
              const totalWalletRefundRaw = Math.max(0, walletUsedRaw + refUsedRaw);
              const tipToExcludeFromWalletRefund = Math.min(cancelledCartTipAmount, totalWalletRefundRaw);
              if (tipToExcludeFromWalletRefund > 0 && totalWalletRefundRaw > 0) {
                const walletShare = walletUsedRaw / totalWalletRefundRaw;
                const refShare = refUsedRaw / totalWalletRefundRaw;
                walletUsed = Math.max(0, walletUsedRaw - (tipToExcludeFromWalletRefund * walletShare));
                refUsed = Math.max(0, refUsedRaw - (tipToExcludeFromWalletRefund * refShare));
              }
            }

            // Handle Cash Wallet Refund
            if (paidByWallet > 0) {
              if (walletUsed > 0) {
                await knex("users")
                  .where("id", userId)
                  .update({
                    wallet_balance: Number(user.wallet_balance || 0) + walletUsed
                  });

                const nextWId = await getNextWalletHistoryWId();
                await knex("wallet_history").insert({
                  w_id: nextWId,
                  user_id: userId,
                  amount: walletUsed.toFixed(2),
                  resource: "order_refund_cancelled",
                  type: "Add",
                  group_id: groupID,
                  cart_id: cartID,
                });
              }
            }

            // Handle Referral Wallet Refund
            if (paidByRefWallet > 0) {
              if (refUsed > 0) {
                const lastTxn = await knex("wallet_history")
                  .where("user_id", userId)
                  .where("group_id", groupID)
                  .where("type", "deduction")
                  .where("resource", "order_placed_wallet_ref")
                  .orderBy("w_id", "desc")
                  .first();

                let walletType = "Add";
                const dubaiTime = moment.tz("Asia/Dubai");
                const todayDubai = dubaiTime.format("YYYY-MM-DD");

                if (
                  lastTxn &&
                  lastTxn.expiry_date &&
                  moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
                ) {
                  walletType = "wallet_expired";
                }

                if (walletType == 'Add') {
                  await knex("users")
                    .where("id", userId)
                    .update({
                      referral_balance: Number(user.referral_balance || 0) + refUsed
                    });
                }

                const nextWId = await getNextWalletHistoryWId();
                await knex("wallet_history").insert({
                  w_id: nextWId,
                  user_id: userId,
                  amount: refUsed.toFixed(2),
                  resource: "order_refund_cancelled_ref",
                  type: walletType,
                  group_id: groupID,
                  cart_id: cartID,
                  expiry_date: lastTxn ? lastTxn.expiry_date : null
                });
              }
            }
          }

        } else {
          const storeDetailsAmt = await knex('orders')
            .where('group_id', groupID)
            .select(
              knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as \"codCharges\""),
              knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\""),
              knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet')
            )
            .first();

          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          const totalMRP = parseFloat(storeOrderDetails?.total_mrp ?? 0);
          const codCharges = parseFloat(storeDetailsAmt?.codCharges ?? 0);
          const delPartnerTip = parseFloat(storeDetailsAmt?.delPartnerTip ?? 0);
          const paidByWallet = parseFloat(storeDetailsAmt?.paid_by_wallet ?? 0);
          const paidByRefWallet = parseFloat(storeDetailsAmt?.paid_by_ref_wallet ?? 0);

          let couponDiscount = 0;
          let couponDiscounts = 0;
          if (couponCode) {
            const couponDetails = await knex('coupon')
              .where('coupon_code', couponCode)
              .select('*')
              .first();
            const itemMrp = await knex('store_products')
              .where('varient_id', storeOrderDetails.varient_id)
              .select('mrp')
              .first();
            const itemQty = parseFloat(storeOrderDetails?.qty ?? 0);
            const unitPrice = itemQty > 0 ? (parseFloat(totalPrice) / itemQty) : 0;
            couponDiscount = (itemQty > 0 && unitPrice >= parseFloat(itemMrp?.mrp ?? 0))
              ? ((totalPrice * couponDetails.amount) / 100)
              : 0;
            couponDiscounts = (parseFloat(CouponDis?.total_price ?? 0) > 0) ? ((parseFloat(CouponDis?.total_price ?? 0) * couponDetails.amount) / 100) : 0;
          }

          const TotalPriceOrders = ((parseFloat(TotalpriceStore) - couponDiscounts) + codCharges + delPartnerTip);
          const WalletDiscount = parseFloat(TotalPriceOrders) * 50 / 100;

          WalletAddtoUserAccount = paidByRefWallet > WalletDiscount ? paidByRefWallet - WalletDiscount : 0;
          const WalletUseOrder = WalletAddtoUserAccount == 0 && paidByRefWallet ? paidByRefWallet : paidByRefWallet - WalletDiscount;
          cashWalletAddtoUserAccount = paidByWallet;

          const codorderamt = codCharges ? (totalPrice * codCharges / TotalpriceStore) : 0;
          const delPartnerTipAmt = delPartnerTip ? (totalPrice * delPartnerTip / TotalpriceStore) : 0;
          const totalPriceAmt = (parseFloat(totalPrice) - parseFloat(couponDiscount)) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt);

          let cashWallet = 0;
          let refWallet = 0;

          if (paidByWallet > 0) {
            const walletRef = await knex('wallet_history')
              .where({
                cart_id: cartID,
                type: 'deduction',
                resource: 'order_placed_wallet'
              })
              .select(
                knex.raw(
                  'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
                )
              )
              .first();
            cashWallet = (walletRef?.total_wallet_used || 0);
          }
          if (paidByRefWallet > 0) {
            const walletRef = await knex('wallet_history')
              .where({
                cart_id: cartID,
                type: 'deduction',
                resource: 'order_placed_wallet_ref'
              })
              .select(
                knex.raw(
                  'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
                )
              )
              .first();
            refWallet = (walletRef?.total_wallet_used_ref || 0);
          }

          await knex('orders').where('cart_id', cartID).update({
            'total_price': parseFloat(totalPriceAmt).toFixed(2),
            'price_without_delivery': parseFloat(totalPriceAmt).toFixed(2),
            'total_products_mrp': parseFloat(totalPriceAmt).toFixed(2),
            'rem_price': 0,
            'coupon_discount': parseFloat(couponDiscount).toFixed(2),
            'cod_charges': parseFloat(codorderamt).toFixed(2),
            'del_partner_tip': parseFloat(delPartnerTipAmt).toFixed(2),
          });


        }




      }

    }

    const result = await knex('orders')
      .where('group_id', groupID)
      .where('order_status', 'Cancelled')
      .count({ cancelled_count: 'order_id' });

    const cancelledCount = parseInt(result[0]?.cancelled_count ?? 0, 10);

    const resultAll = await knex('orders')
      .where('group_id', groupID)
      .count({ all_count: 'order_id' });

    const allCount = parseInt(resultAll[0]?.all_count ?? 0, 10);

    if (autoCancelledOfferCartIds.length > 0) {
      const additionalRefundRow = await knex('orders')
        .whereIn('cart_id', autoCancelledOfferCartIds)
        .select(
          knex.raw("COALESCE(SUM(CASE WHEN orders.payment_method != 'COD' AND orders.payment_status = 'success' THEN GREATEST((orders.total_price::numeric - COALESCE(orders.paid_by_wallet, 0)::numeric - COALESCE(orders.paid_by_ref_wallet, 0)::numeric), 0) ELSE 0 END), 0) as card_paid"),
          knex.raw("COALESCE(SUM(NULLIF(trim(orders.rem_price::text), '')::numeric), 0) as rem_price")
        )
        .first();
      const additionalCardRefund = Math.max(
        Number(additionalRefundRow?.card_paid || 0),
        Number(additionalRefundRow?.rem_price || 0)
      );
      console.log('[cancelledquickorderprod][offer-auto-cancel-refund-calc]', {
        group_id: groupID,
        requested_cart_id: cart_id,
        auto_cancelled_offer_cart_ids: autoCancelledOfferCartIds,
        additional_card_paid: Number(additionalRefundRow?.card_paid || 0).toFixed(2),
        additional_rem_price: Number(additionalRefundRow?.rem_price || 0).toFixed(2),
        additional_card_refund_used: Number(additionalCardRefund || 0).toFixed(2),
        base_cancelled_card_refund_before_addition: Number(cancelledCardRefund || 0).toFixed(2),
      });
      cancelledCardRefund += additionalCardRefund;
    }

    // Prevent refunding delivery tip on partial cancellation:
    // only full-order cancellation should refund tip.
    if (cancelledCount !== allCount && cancelledCardRefund > 0) {
      const cancelledTipRow = await knex('orders')
        .where('cart_id', cart_id)
        .select(
          knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\"")
        )
        .first();

      const cancelledTipAmount = Number(cancelledTipRow?.delPartnerTip || 0);
      if (cancelledTipAmount > 0) {
        // If wallet/ref-wallet + card split happened, wallet/ref-wallet branch already excluded
        // the tip share from its refunds. So for the CARD refund we only exclude the remaining
        // tip that wasn't excluded by wallet/ref-wallet.
        const isWalletCardSplit = (paidByWallet > 0 || paidByRefWallet > 0);
        if (isWalletCardSplit) {
          const walletUsedRow = await knex('wallet_history')
            .where({
              cart_id: cart_id,
              type: 'deduction',
              resource: 'order_placed_wallet',
            })
            .select(knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
            ))
            .first();

          const refUsedRow = await knex('wallet_history')
            .where({
              cart_id: cart_id,
              type: 'deduction',
              resource: 'order_placed_wallet_ref',
            })
            .select(knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
            ))
            .first();

          const walletUsedRaw = Number(walletUsedRow?.total_wallet_used || 0);
          const refUsedRaw = Number(refUsedRow?.total_wallet_used_ref || 0);
          const totalWalletRefundRaw = Math.max(0, walletUsedRaw + refUsedRaw);

          // Remaining tip to exclude from CARD refund.
          // If wallet/ref-wallet refunds had enough money, remaining tip becomes 0.
          const tipToExcludeFromCard = Math.max(0, cancelledTipAmount - totalWalletRefundRaw);
          cancelledCardRefund = Math.max(0, cancelledCardRefund - tipToExcludeFromCard);
        } else {
          cancelledCardRefund = Math.max(0, cancelledCardRefund - cancelledTipAmount);
        }
      }
    }
    if (cancelledCardRefund <= minCardRefundThreshold) {
      cancelledCardRefund = 0;
    }

    // Only refund wallet here for COD; Wallet payment is already refunded in the loop (CARD branch)
    if (storeDetailsAmtAll.payment_method == "COD") {

      const user = await knex("users")
        .select("user_phone", "wallet", "wallet_balance", "referral_balance")
        .where("id", userId)
        .first();

      if (paidByWallet > 0) {
        const walletRef = await knex('wallet_history')
          .where({
            cart_id: cart_id,
            type: 'deduction',
            resource: 'order_placed_wallet'
          })
          .select(
            knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
            )
          )
          .first();

        const walletUsed = Number(walletRef?.total_wallet_used || 0);

        if (walletUsed > 0) {
          await knex("users")
            .where("id", userId)
            .update({
              wallet_balance: Number(user.wallet_balance || 0) + walletUsed
            });

          const nextWId = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWId,
            user_id: userId,
            amount: walletUsed.toFixed(2),
            resource: "order_refund_cancelled",
            type: "Add",
            group_id: groupID,
            cart_id: cart_id,
          });
        }
      }

      if (paidByRefWallet > 0) {
        const walletRef = await knex('wallet_history')
          .where({
            cart_id: cart_id,
            type: 'deduction',
            resource: 'order_placed_wallet_ref'
          })
          .select(
            knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
            )
          )
          .first();

        const refUsed = Number(walletRef?.total_wallet_used_ref || 0);

        if (refUsed > 0) {
          const lastTxn = await knex("wallet_history")
            .where("user_id", userId)
            .where("group_id", groupID)
            .where("type", "deduction")
            .where("resource", "order_placed_wallet_ref")
            .orderBy("w_id", "desc")
            .first();

          let walletType = "Add";
          const dubaiTime = moment.tz("Asia/Dubai");
          const todayDubai = dubaiTime.format("YYYY-MM-DD");

          if (
            lastTxn &&
            lastTxn.expiry_date &&
            moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
          ) {
            walletType = "wallet_expired";
          }

          if (walletType == 'Add') {
            await knex("users")
              .where("id", userId)
              .update({
                referral_balance: Number(user.referral_balance || 0) + refUsed
              });
          }

          const nextWId = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWId,
            user_id: userId,
            amount: refUsed.toFixed(2),
            resource: "order_refund_cancelled_ref",
            type: walletType,
            group_id: groupID,
            cart_id: cart_id,
            expiry_date: lastTxn ? lastTxn.expiry_date : null
          });
        }
      }
    }

    if (cancelledCardRefund > 0) {
      console.log('[cancelledquickorderprod][final-card-refund-before-wallet-credit]', {
        group_id: groupID,
        requested_cart_id: cart_id,
        cancelled_count: cancelledCount,
        total_count: allCount,
        auto_cancelled_offer_cart_ids: autoCancelledOfferCartIds,
        final_cancelled_card_refund: Number(cancelledCardRefund || 0).toFixed(2),
      });

      const userForCardRefund = await knex("users")
        .select("wallet_balance")
        .where("id", userId)
        .first();

      await knex("users")
        .where("id", userId)
        .update({
          wallet_balance: Number(userForCardRefund?.wallet_balance || 0) + cancelledCardRefund
        });

      const nextWId = await getNextWalletHistoryWId();
      await knex("wallet_history").insert({
        w_id: nextWId,
        user_id: userId,
        amount: cancelledCardRefund.toFixed(2),
        resource: "order_refund_cancelled_card_to_wallet",
        type: "Add",
        group_id: groupID,
        cart_id: cart_id,
      });
    }

    const group_id = groupIdForEmail;

    totalCal = await knex('orders')
      .where('group_id', group_id)
      .where('cart_id', cart_id)
      .sum('total_products_mrp as total_products_mrp');
    finalAmount = parseFloat(totalCal[0]?.total_products_mrp ?? 0).toFixed(2);

    totalCartAmount = await knex('orders')
      .where('group_id', group_id)
      .sum('total_products_mrp as total_products_mrp');

    total_cart_amount = parseFloat(totalCartAmount[0]?.total_products_mrp ?? 0).toFixed(2);

    orderFinalAmount = parseFloat(total_cart_amount) - parseFloat(finalAmount);

    const result1 = await knex('orders')
      .where('group_id', group_id)
      .where('order_status', 'Cancelled')
      .count({ cancelled_count: 'order_id' });

    const cancelledCount1 = parseInt(result1[0]?.cancelled_count ?? 0, 10);

    const resultAll1 = await knex('orders')
      .where('group_id', group_id)
      .count({ all_count: 'order_id' });

    const allCount1 = parseInt(resultAll1[0]?.all_count ?? 0, 10);

    const is_addedld = await knex('tbl_luckydraw').where('order_id', group_id).where('is_delete', 0).first();

    // if((is_addedld && orderFinalAmount <= 100) || (is_addedld && orderFinalAmount >= 100 && cancelledCount == allCount)){
    if (is_addedld && orderFinalAmount <= 100 || (is_addedld && orderFinalAmount > 100 && cancelledCount1 == allCount1)) {
      if (finalAmount >= 100) {
        const updateentry = await knex('tbl_luckydraw')
          .where('order_id', group_id)
          .update({ 'is_delete': 1 });
      }

      const usertotalorders = await knex('tbl_luckydraw')
        .distinct('orders.group_id')
        .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
        .where('tbl_luckydraw.user_id', user_id)
        .where('tbl_luckydraw.is_delete', 0)
        .where('orders.order_status', '!=', 'Cancelled');

      const getUserup = await knex('users').where('id', user_id).first();

      const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
      // Convert OTP code to a time-based string
      const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

      const payload = {
        "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
        "campaignName": "CancelOrderIphone",
        "destination": "+" + phone_with_country_code,
        "userName": "Quickart General Trading Co LLC",
        "templateParams": [
          getUserup.name, group_id, finalAmount, `${usertotalorders.length}`
        ],
        "source": "new-landing-page form",
        "media": {},
        "buttons": [],
        "carouselCards": [],
        "location": {},
        "attributes": {},
        "paramsFallbackValue": {
          "FirstName": "user"
        }
      };

      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      const result = await response.json();

      const updateentry = await knex('tbl_luckydraw')
        .where('order_id', group_id)
        .update({ 'is_delete': 1 });
    }

    await knex('orders')
      .where('group_id', groupID)
      .where('order_status', 'Cancelled')
      .update({
        'del_partner_tip': 0,
        'coupon_discount': 0,
        'cod_charges': 0,
        'reserve_amount': 0,
        'paid_by_wallet': 0,
        'paid_by_ref_wallet': 0,
        'rem_price': 0
      });


  }


  //Email Code (groupIdForEmail in scope from function top)
  if (groupIdForEmail) {
    storeOrders = await knex('store_orders')
      .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
      .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
      .where('store_orders.order_cart_id', cart_id)

    // Fetch user for email
    const user = await knex('users')
      .select('name', 'email')
      .where('id', user_id)
      .first();
    let userName = user?.name ?? '';
    let userEmail = user?.email ?? '';

    const group_id = groupIdForEmail;

    const logo = await knex('tbl_web_setting').first();
    const appName = logo ? logo.name : null;
    // Fetching the first record from the 'currency' table
    const currency = await knex('currency').first();
    const currencySign = currency ? currency.currency_sign : null;
    const templateData = {
      baseurl: process.env.BASE_URL,
      user_name: userName,
      user_email: userEmail,
      store_orderss: storeOrders,
      final_amount: "",
      app_name: appName,
      currency_sign: currencySign,
      cart_id: group_id
    };
    const subject = 'Delivery Order Cancelled'
    // Trigger the email after order is placed
    //sendCancelledEmail = await cancelorderMail(userEmail, templateData, subject);
  }

};
const getCancelquickOrderProd = async (appDetatils) => {
  const minCardRefundThreshold = 0.09;
  const user_id = appDetatils.user_id
  const cart_id = appDetatils.cart_id
  const cancel_reason = appDetatils.cancel_reason
  let groupIdForEmail = null; // in scope for email section after if block
  let autoCancelledOfferCartIds = [];
  let autoOfferCancellationApplied = false;

  if (cart_id && user_id) {

    // Check if there are any orders that are not already cancelled
    const existingOrders = await knex('orders')
      .where('cart_id', cart_id)
      .where('order_status', 'Cancelled')
      .first();
    // If there are no orders to cancel, return a message
    if (existingOrders) {
      throw new Error('Order are already cancelled.');
    }


    subcription_data = await knex('subscription_order')
      .where('cart_id', cart_id)
      .where('order_status', 'Pending')
      //.where('si_payment_flag', 'no')
      //.where('processing_product','!=','1')
      .update({
        'cancel_reason': cancel_reason,
        'order_status': "Cancelled"
      });

    order_data = await knex('orders')
      .where('cart_id', cart_id)
      //.where('group_id', group_id)
      .where('order_status', 'Pending')
      // .where('payment_status','!=','success')
      .update({ 'order_status': "Cancelled", 'cancelling_reason': cancel_reason });

    const cancelledCardRefundRow = await knex('orders')
      .where('cart_id', cart_id)
      .select(
        knex.raw("COALESCE(SUM(CASE WHEN orders.payment_method != 'COD' AND orders.payment_status = 'success' THEN GREATEST((orders.total_price::numeric - COALESCE(orders.paid_by_wallet, 0)::numeric - COALESCE(orders.paid_by_ref_wallet, 0)::numeric), 0) ELSE 0 END), 0) as card_paid")
      )
      .first();
    let cancelledCardRefund = Number(cancelledCardRefundRow?.card_paid || 0);
    if (cancelledCardRefund <= minCardRefundThreshold) {
      cancelledCardRefund = 0;
    }

    //After cancelled order wallet amount save 
    const orderDetail = await knex('orders')
      .where('cart_id', cart_id)
      .select('payment_method', 'user_id', 'group_id', 'coupon_code')
      .first();
    if (!orderDetail?.group_id) throw new Error('Order not found for this cart.');
    const groupID = orderDetail.group_id;
    groupIdForEmail = groupID;
    const couponCode = orderDetail.coupon_code ?? '';
    const userId = appDetatils.user_id;
    const offerCancelEligibilityRow = await knex('store_orders as so')
      .join('orders as o', 'o.cart_id', 'so.order_cart_id')
      .join('product_varient as pv', 'pv.varient_id', 'so.varient_id')
      .join('product as p', 'p.product_id', 'pv.product_id')
      .where('so.order_cart_id', cart_id)
      .where('p.is_offer_product', 1)
      .whereRaw('p.offer_date::date = o.order_date::date')
      .first();
    const canApplyOfferThresholdCancellation = !!offerCancelEligibilityRow;

    const orderDetails = await knex('orders')
      .select('*')
      .where('group_id', groupID);



    const storeDetailsAmtAll = await knex('orders')
      .where('group_id', groupID)
      .select(
        knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as \"codCharges\""),
        knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\""),
        knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet'),
        knex.raw('COALESCE(SUM(orders.paid_by_ref_wallet), 0) as paid_by_ref_wallet'),
        knex.raw('MAX(orders.payment_method) as payment_method')
      )
      .first();
    const paidByWallet = Math.round(storeDetailsAmtAll?.paid_by_wallet ?? 0);
    const paidByRefWallet = Math.round(storeDetailsAmtAll?.paid_by_ref_wallet ?? 0);
    let WalletAddtoUserAccount = 0;
    let FinalWalletAmountUse = 0;
    let cashWalletAddtoUserAccount = 0;
    let cashFinalWalletAmountUse = 0;
    for (const orders of orderDetails) {
      const cartID = orders.cart_id;
      if (orders.payment_method == 'COD') {
        //COD Code Write
        const storeOrderDetails = await knex('store_orders')
          .where('order_cart_id', cartID)
          .select('*')
          .first();

        const storeDetailsAmt = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .where('group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .select(knex.raw('SUM(store_orders.total_mrp) as "Totalmrp"'), knex.raw('SUM(store_orders.price) as "Totalprice"'))
          .first();


        const TotalpriceStore = parseFloat(storeDetailsAmt?.Totalprice ?? 0);
        const TotalmrpStore = parseFloat(storeDetailsAmt?.Totalmrp ?? 0);

        // if(TotalpriceStore < 30){

        //     order_data = await knex('orders')
        //       .where('group_id', groupID)
        //       .where('is_offer_product', '1')
        //       .update({'order_status':"Cancelled"});

        //     subcription_data = await knex('subscription_order')
        //             .where('group_id', groupID)
        //             .where('is_offer_product', '1')
        //             .update({'cancel_reason':cancel_reason,
        //             'order_status':"Cancelled"});  

        // }

        const CouponDis = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
          .sum({ total_price: 'store_orders.price' })
          .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
          .where('orders.group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .first();

        if (orders.order_status == 'Cancelled') {
          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          await knex('orders')
            .where('cart_id', cartID)
            .update({
              'order_status': 'Cancelled',
              'cancelling_reason': cancel_reason,
              'total_price': parseFloat(totalPrice).toFixed(2),
              'price_without_delivery': parseFloat(totalPrice).toFixed(2),
              'total_products_mrp': parseFloat(totalPrice).toFixed(2),
              'paid_by_wallet': 0,
              'paid_by_ref_wallet': 0
            });

        } else {

          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          const totalMRP = parseFloat(storeOrderDetails?.total_mrp ?? 0);
          const codCharges = parseFloat(storeDetailsAmtAll?.codCharges ?? 0);
          const delPartnerTip = parseFloat(storeDetailsAmtAll?.delPartnerTip ?? 0);


          let couponDiscount = 0;
          let couponDiscounts = 0;
          if (couponCode) {
            const couponDetails = await knex('coupon')
              .where('coupon_code', couponCode)
              .select('*')
              .first();
            const itemMrp = await knex('store_products')
              .where('varient_id', storeOrderDetails.varient_id)
              .select('mrp')
              .first();
            const itemQty = parseFloat(storeOrderDetails?.qty ?? 0);
            const unitPrice = itemQty > 0 ? (parseFloat(totalPrice) / itemQty) : 0;
            couponDiscount = (itemQty > 0 && unitPrice >= parseFloat(itemMrp?.mrp ?? 0))
              ? ((totalPrice * couponDetails.amount) / 100)
              : 0;
            couponDiscounts = (parseFloat(CouponDis?.total_price ?? 0) > 0) ? ((parseFloat(CouponDis?.total_price ?? 0) * couponDetails.amount) / 100) : 0;
          }

          const TotalPriceOrders = ((parseFloat(TotalpriceStore) - couponDiscounts) + codCharges + delPartnerTip);
          const WalletDiscount = parseFloat(TotalPriceOrders) * 50 / 100;

          WalletAddtoUserAccount = paidByRefWallet > WalletDiscount ? paidByRefWallet - WalletDiscount : 0;
          const WalletUseOrder = WalletAddtoUserAccount == 0 && paidByRefWallet ? paidByRefWallet : paidByRefWallet - WalletDiscount;
          cashWalletAddtoUserAccount = paidByWallet;

          const codorderamt = codCharges ? (totalPrice * codCharges / TotalpriceStore) : 0;
          const delPartnerTipAmt = delPartnerTip ? (totalPrice * delPartnerTip / TotalpriceStore) : 0;
          const totalPriceAmt = (parseFloat(totalPrice) - parseFloat(couponDiscount)) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt);

          let cashFinalWalletAmountUse = 0;
          let FinalWalletAmountUse = 0;

          if (paidByWallet > 0) {
            const walletRef = await knex('wallet_history')
              .where({
                cart_id: cartID,
                type: 'deduction',
                resource: 'order_placed_wallet'
              })
              .select(
                knex.raw(
                  'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
                )
              )
              .first();
            cashFinalWalletAmountUse = (walletRef?.total_wallet_used || 0);
          }

          if (paidByRefWallet > 0) {
            const walletRef = await knex('wallet_history')
              .where({
                cart_id: cartID,
                type: 'deduction',
                resource: 'order_placed_wallet_ref'
              })
              .select(
                knex.raw(
                  'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_ref_used'
                )
              )
              .first();
            FinalWalletAmountUse = (walletRef?.total_wallet_ref_used || 0);
          }

          await knex('orders')
            .where('cart_id', cartID)
            .update({
              'total_price': parseFloat(totalPriceAmt).toFixed(2),
              'price_without_delivery': parseFloat(totalPriceAmt).toFixed(2),
              'total_products_mrp': parseFloat(totalPriceAmt).toFixed(2),
              'coupon_discount': parseFloat(couponDiscount).toFixed(2),
              'cod_charges': parseFloat(codorderamt).toFixed(2),
              'del_partner_tip': parseFloat(delPartnerTipAmt).toFixed(2),
            });

        }

      } else {

        //CARD Code Write
        const storeOrderDetails = await knex('store_orders')
          .where('order_cart_id', cartID)
          .select('*')
          .first();

        const storeDetailsAmt = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .where('group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .select(knex.raw('SUM(store_orders.total_mrp) as "Totalmrp"'), knex.raw('SUM(store_orders.price) as "Totalprice"'))
          .first();

        const TotalpriceStore = parseFloat(storeDetailsAmt?.Totalprice ?? 0);
        const TotalmrpStore = parseFloat(storeDetailsAmt?.Totalmrp ?? 0);

        if (!autoOfferCancellationApplied && cartID == cart_id && TotalpriceStore < 30 && canApplyOfferThresholdCancellation) {
          const pendingOfferRows = await knex('orders')
            .where('group_id', groupID)
            .where('is_offer_product', 1)
            .where('order_status', 'Pending')
            .select('cart_id');
          autoCancelledOfferCartIds = pendingOfferRows
            .map((row) => row.cart_id)
            .filter((id) => id && id !== cart_id);

          order_data = await knex('orders')
            .where('group_id', groupID)
            .where('is_offer_product', 1)
            .update({ 'order_status': "Cancelled", 'cancelling_reason': cancel_reason });

          subcription_data = await knex('subscription_order')
            .where('group_id', groupID)
            .where('is_offer_product', 1)
            .update({
              'cancel_reason': cancel_reason,
              'order_status': "Cancelled"
            });

          console.log('[cancelledquickorderprod][offer-auto-cancel-triggered]', {
            group_id: groupID,
            requested_cart_id: cart_id,
            trigger_cart_id: cartID,
            total_price_store_after_cancel: Number(TotalpriceStore || 0).toFixed(2),
            threshold: 30,
            can_apply_offer_threshold_cancellation: canApplyOfferThresholdCancellation,
            auto_cancelled_offer_cart_ids: autoCancelledOfferCartIds,
          });

          autoOfferCancellationApplied = true;
        }

        const CouponDis = await knex('store_orders')
          .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
          .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
          .sum({ total_price: 'store_orders.price' })
          .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
          .where('orders.group_id', groupID)
          .whereNot('orders.order_status', 'Cancelled')
          .first();



        if (orders.order_status == 'Cancelled') {
          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          const totalMrp = parseFloat(storeOrderDetails?.total_mrp ?? 0);
          await knex('orders')
            .where('cart_id', cartID)
            .update({
              'order_status': 'Cancelled',
              'cancelling_reason': cancel_reason,
              'total_price': totalPrice.toFixed(2),
              'price_without_delivery': totalPrice.toFixed(2),
              'total_products_mrp': totalPrice.toFixed(2),
              'paid_by_wallet': 0,
              'paid_by_ref_wallet': 0
            });


          let couponDiscount = 0;
          let couponDiscounts = 0;
          if (couponCode) {
            const couponDetails = await knex('coupon')
              .where('coupon_code', couponCode)
              .select('*')
              .first();
            const itemMrp = await knex('store_products')
              .where('varient_id', storeOrderDetails.varient_id)
              .select('mrp')
              .first();
            const itemQty = parseFloat(storeOrderDetails?.qty ?? 0);
            const unitPrice = itemQty > 0 ? (parseFloat(totalPrice) / itemQty) : 0;
            couponDiscount = (itemQty > 0 && unitPrice >= parseFloat(itemMrp?.mrp ?? 0))
              ? ((totalPrice * couponDetails.amount) / 100)
              : 0;

          }

          if (cartID == cart_id) {
            let deliveryPT = 0;
            const result = await knex('orders')
              .where('group_id', groupID)
              .where('order_status', 'Cancelled')
              .count({ cancelled_count: 'order_id' });

            const cancelledCount = parseInt(result[0]?.cancelled_count ?? 0, 10);

            const resultAll = await knex('orders')
              .where('group_id', groupID)
              .count({ all_count: 'order_id' });

            const allCount = parseInt(resultAll[0]?.all_count ?? 0, 10);

            const OrdersDetailsAmt = await knex('orders')
              .where('group_id', groupID)
              .select(knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\""))
              .first();

            if (cancelledCount === allCount) {
              deliveryPT = parseFloat(OrdersDetailsAmt?.delPartnerTip ?? 0);
            }
            const TotalPriceOrdersAmt = ((parseFloat(totalPrice) - parseFloat(couponDiscount))) + parseFloat(deliveryPT);
            const user = await knex('users')
              .select('user_phone', 'wallet_balance', 'referral_balance')
              .where('id', userId)
              .first();

            let walletUsedRaw = 0;
            let refUsedRaw = 0;
            if (paidByWallet > 0) {
              const walletRef = await knex('wallet_history')
                .where({
                  cart_id: cartID,
                  type: 'deduction',
                  resource: 'order_placed_wallet'
                })
                .select(
                  knex.raw(
                    'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
                  )
                )
                .first();
              walletUsedRaw = Number(walletRef?.total_wallet_used || 0);
            }
            if (paidByRefWallet > 0) {
              const walletRef = await knex('wallet_history')
                .where({
                  cart_id: cartID,
                  type: 'deduction',
                  resource: 'order_placed_wallet_ref'
                })
                .select(
                  knex.raw(
                    'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
                  )
                )
                .first();
              refUsedRaw = Number(walletRef?.total_wallet_used_ref || 0);
            }

            let walletUsed = walletUsedRaw;
            let refUsed = refUsedRaw;
            if (cancelledCount !== allCount) {
              const cancelledCartTipAmount = Math.max(0, Number(orders?.del_partner_tip || 0));
              const totalWalletRefundRaw = Math.max(0, walletUsedRaw + refUsedRaw);
              const tipToExcludeFromWalletRefund = Math.min(cancelledCartTipAmount, totalWalletRefundRaw);
              if (tipToExcludeFromWalletRefund > 0 && totalWalletRefundRaw > 0) {
                const walletShare = walletUsedRaw / totalWalletRefundRaw;
                const refShare = refUsedRaw / totalWalletRefundRaw;
                walletUsed = Math.max(0, walletUsedRaw - (tipToExcludeFromWalletRefund * walletShare));
                refUsed = Math.max(0, refUsedRaw - (tipToExcludeFromWalletRefund * refShare));
              }
            }

            // Handle Cash Wallet Refund
            if (paidByWallet > 0) {
              if (walletUsed > 0) {
                await knex("users")
                  .where("id", userId)
                  .update({
                    wallet_balance: Number(user.wallet_balance || 0) + walletUsed
                  });

                const nextWId = await getNextWalletHistoryWId();
                await knex("wallet_history").insert({
                  w_id: nextWId,
                  user_id: userId,
                  amount: walletUsed.toFixed(2),
                  resource: "order_refund_cancelled",
                  type: "Add",
                  group_id: groupID,
                  cart_id: cartID,
                });
              }
            }

            // Handle Referral Wallet Refund
            if (paidByRefWallet > 0) {
              if (refUsed > 0) {
                const lastTxn = await knex("wallet_history")
                  .where("user_id", userId)
                  .where("group_id", groupID)
                  .where("type", "deduction")
                  .where("resource", "order_placed_wallet_ref")
                  .orderBy("w_id", "desc")
                  .first();

                let walletType = "Add";
                const dubaiTime = moment.tz("Asia/Dubai");
                const todayDubai = dubaiTime.format("YYYY-MM-DD");

                if (
                  lastTxn &&
                  lastTxn.expiry_date &&
                  moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
                ) {
                  walletType = "wallet_expired";
                }

                if (walletType == 'Add') {
                  await knex("users")
                    .where("id", userId)
                    .update({
                      referral_balance: Number(user.referral_balance || 0) + refUsed
                    });
                }

                const nextWId = await getNextWalletHistoryWId();
                await knex("wallet_history").insert({
                  w_id: nextWId,
                  user_id: userId,
                  amount: refUsed.toFixed(2),
                  resource: "order_refund_cancelled_ref",
                  type: walletType,
                  group_id: groupID,
                  cart_id: cartID,
                  expiry_date: lastTxn ? lastTxn.expiry_date : null
                });
              }
            }
          }

        } else {
          const storeDetailsAmt = await knex('orders')
            .where('group_id', groupID)
            .select(
              knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as \"codCharges\""),
              knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\""),
              knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet')
            )
            .first();

          const totalPrice = parseFloat(storeOrderDetails?.price ?? 0);
          const totalMRP = parseFloat(storeOrderDetails?.total_mrp ?? 0);
          const codCharges = parseFloat(storeDetailsAmt?.codCharges ?? 0);
          const delPartnerTip = parseFloat(storeDetailsAmt?.delPartnerTip ?? 0);
          const paidByWallet = parseFloat(storeDetailsAmt?.paid_by_wallet ?? 0);
          const paidByRefWallet = parseFloat(storeDetailsAmt?.paid_by_ref_wallet ?? 0);

          let couponDiscount = 0;
          let couponDiscounts = 0;
          if (couponCode) {
            const couponDetails = await knex('coupon')
              .where('coupon_code', couponCode)
              .select('*')
              .first();
            const itemMrp = await knex('store_products')
              .where('varient_id', storeOrderDetails.varient_id)
              .select('mrp')
              .first();
            const itemQty = parseFloat(storeOrderDetails?.qty ?? 0);
            const unitPrice = itemQty > 0 ? (parseFloat(totalPrice) / itemQty) : 0;
            couponDiscount = (itemQty > 0 && unitPrice >= parseFloat(itemMrp?.mrp ?? 0))
              ? ((totalPrice * couponDetails.amount) / 100)
              : 0;
            couponDiscounts = (parseFloat(CouponDis?.total_price ?? 0) > 0) ? ((parseFloat(CouponDis?.total_price ?? 0) * couponDetails.amount) / 100) : 0;
          }

          const TotalPriceOrders = ((parseFloat(TotalpriceStore) - couponDiscounts) + codCharges + delPartnerTip);
          const WalletDiscount = parseFloat(TotalPriceOrders) * 50 / 100;

          WalletAddtoUserAccount = paidByRefWallet > WalletDiscount ? paidByRefWallet - WalletDiscount : 0;
          const WalletUseOrder = WalletAddtoUserAccount == 0 && paidByRefWallet ? paidByRefWallet : paidByRefWallet - WalletDiscount;
          cashWalletAddtoUserAccount = paidByWallet;

          const codorderamt = codCharges ? (totalPrice * codCharges / TotalpriceStore) : 0;
          const delPartnerTipAmt = delPartnerTip ? (totalPrice * delPartnerTip / TotalpriceStore) : 0;
          const totalPriceAmt = (parseFloat(totalPrice) - parseFloat(couponDiscount)) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt);

          let cashWallet = 0;
          let refWallet = 0;

          if (paidByWallet > 0) {
            const walletRef = await knex('wallet_history')
              .where({
                cart_id: cartID,
                type: 'deduction',
                resource: 'order_placed_wallet'
              })
              .select(
                knex.raw(
                  'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
                )
              )
              .first();
            cashWallet = (walletRef?.total_wallet_used || 0);
          }
          if (paidByRefWallet > 0) {
            const walletRef = await knex('wallet_history')
              .where({
                cart_id: cartID,
                type: 'deduction',
                resource: 'order_placed_wallet_ref'
              })
              .select(
                knex.raw(
                  'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
                )
              )
              .first();
            refWallet = (walletRef?.total_wallet_used_ref || 0);
          }

          await knex('orders').where('cart_id', cartID).update({
            'total_price': parseFloat(totalPriceAmt).toFixed(2),
            'price_without_delivery': parseFloat(totalPriceAmt).toFixed(2),
            'total_products_mrp': parseFloat(totalPriceAmt).toFixed(2),
            'rem_price': 0,
            'coupon_discount': parseFloat(couponDiscount).toFixed(2),
            'cod_charges': parseFloat(codorderamt).toFixed(2),
            'del_partner_tip': parseFloat(delPartnerTipAmt).toFixed(2),
          });


        }




      }

    }

    const result = await knex('orders')
      .where('group_id', groupID)
      .where('order_status', 'Cancelled')
      .count({ cancelled_count: 'order_id' });

    const cancelledCount = parseInt(result[0]?.cancelled_count ?? 0, 10);

    const resultAll = await knex('orders')
      .where('group_id', groupID)
      .count({ all_count: 'order_id' });

    const allCount = parseInt(resultAll[0]?.all_count ?? 0, 10);

    if (autoCancelledOfferCartIds.length > 0) {
      const additionalRefundRow = await knex('orders')
        .whereIn('cart_id', autoCancelledOfferCartIds)
        .select(
          knex.raw("COALESCE(SUM(CASE WHEN orders.payment_method != 'COD' AND orders.payment_status = 'success' THEN GREATEST((orders.total_price::numeric - COALESCE(orders.paid_by_wallet, 0)::numeric - COALESCE(orders.paid_by_ref_wallet, 0)::numeric), 0) ELSE 0 END), 0) as card_paid")
        )
        .first();
      const additionalCardRefund = Number(additionalRefundRow?.card_paid || 0);
      console.log('[cancelledquickorderprod][offer-auto-cancel-refund-calc]', {
        group_id: groupID,
        requested_cart_id: cart_id,
        auto_cancelled_offer_cart_ids: autoCancelledOfferCartIds,
        additional_card_paid: Number(additionalRefundRow?.card_paid || 0).toFixed(2),
        additional_card_refund_used: Number(additionalCardRefund || 0).toFixed(2),
        base_cancelled_card_refund_before_addition: Number(cancelledCardRefund || 0).toFixed(2),
      });
      cancelledCardRefund += additionalCardRefund;
    }

    // Prevent refunding delivery tip on partial cancellation:
    // only full-order cancellation should refund tip.
    if (cancelledCount !== allCount && cancelledCardRefund > 0) {
      const cancelledTipRow = await knex('orders')
        .where('cart_id', cart_id)
        .select(
          knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as \"delPartnerTip\"")
        )
        .first();

      const cancelledTipAmount = Number(cancelledTipRow?.delPartnerTip || 0);
      if (cancelledTipAmount > 0) {
        // If wallet/ref-wallet + card split happened, wallet/ref-wallet branch already excluded
        // the tip share from its refunds. So for the CARD refund we only exclude the remaining
        // tip that wasn't excluded by wallet/ref-wallet.
        const isWalletCardSplit = (paidByWallet > 0 || paidByRefWallet > 0);
        if (isWalletCardSplit) {
          const walletUsedRow = await knex('wallet_history')
            .where({
              cart_id: cart_id,
              type: 'deduction',
              resource: 'order_placed_wallet',
            })
            .select(knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
            ))
            .first();

          const refUsedRow = await knex('wallet_history')
            .where({
              cart_id: cart_id,
              type: 'deduction',
              resource: 'order_placed_wallet_ref',
            })
            .select(knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
            ))
            .first();

          const walletUsedRaw = Number(walletUsedRow?.total_wallet_used || 0);
          const refUsedRaw = Number(refUsedRow?.total_wallet_used_ref || 0);
          const totalWalletRefundRaw = Math.max(0, walletUsedRaw + refUsedRaw);

          // Remaining tip to exclude from CARD refund.
          // If wallet/ref-wallet refunds had enough money, remaining tip becomes 0.
          const tipToExcludeFromCard = Math.max(0, cancelledTipAmount - totalWalletRefundRaw);
          cancelledCardRefund = Math.max(0, cancelledCardRefund - tipToExcludeFromCard);
        } else {
          cancelledCardRefund = Math.max(0, cancelledCardRefund - cancelledTipAmount);
        }
      }
    }
    if (cancelledCardRefund <= minCardRefundThreshold) {
      cancelledCardRefund = 0;
    }

    // Only refund wallet here for COD; Wallet payment is already refunded in the loop (CARD branch)
    if (storeDetailsAmtAll.payment_method == "COD") {

      const user = await knex("users")
        .select("user_phone", "wallet", "wallet_balance", "referral_balance")
        .where("id", userId)
        .first();

      if (paidByWallet > 0) {
        const walletRef = await knex('wallet_history')
          .where({
            cart_id: cart_id,
            type: 'deduction',
            resource: 'order_placed_wallet'
          })
          .select(
            knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used'
            )
          )
          .first();

        const walletUsed = Number(walletRef?.total_wallet_used || 0);

        if (walletUsed > 0) {
          await knex("users")
            .where("id", userId)
            .update({
              wallet_balance: Number(user.wallet_balance || 0) + walletUsed
            });

          const nextWId = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWId,
            user_id: userId,
            amount: walletUsed.toFixed(2),
            resource: "order_refund_cancelled",
            type: "Add",
            group_id: groupID,
            cart_id: cart_id,
          });
        }
      }

      if (paidByRefWallet > 0) {
        const walletRef = await knex('wallet_history')
          .where({
            cart_id: cart_id,
            type: 'deduction',
            resource: 'order_placed_wallet_ref'
          })
          .select(
            knex.raw(
              'COALESCE(SUM(NULLIF(trim(amount), \'\')::numeric), 0) as total_wallet_used_ref'
            )
          )
          .first();

        const refUsed = Number(walletRef?.total_wallet_used_ref || 0);

        if (refUsed > 0) {
          const lastTxn = await knex("wallet_history")
            .where("user_id", userId)
            .where("group_id", groupID)
            .where("type", "deduction")
            .where("resource", "order_placed_wallet_ref")
            .orderBy("w_id", "desc")
            .first();

          let walletType = "Add";
          const dubaiTime = moment.tz("Asia/Dubai");
          const todayDubai = dubaiTime.format("YYYY-MM-DD");

          if (
            lastTxn &&
            lastTxn.expiry_date &&
            moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
          ) {
            walletType = "wallet_expired";
          }

          if (walletType == 'Add') {
            await knex("users")
              .where("id", userId)
              .update({
                referral_balance: Number(user.referral_balance || 0) + refUsed
              });
          }

          const nextWId = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWId,
            user_id: userId,
            amount: refUsed.toFixed(2),
            resource: "order_refund_cancelled_ref",
            type: walletType,
            group_id: groupID,
            cart_id: cart_id,
            expiry_date: lastTxn ? lastTxn.expiry_date : null
          });
        }
      }
    }

    if (cancelledCardRefund > 0) {
      console.log('[cancelledquickorderprod][final-card-refund-before-wallet-credit]', {
        group_id: groupID,
        requested_cart_id: cart_id,
        cancelled_count: cancelledCount,
        total_count: allCount,
        auto_cancelled_offer_cart_ids: autoCancelledOfferCartIds,
        final_cancelled_card_refund: Number(cancelledCardRefund || 0).toFixed(2),
      });

      const totalWalletPaidForGroup = (paidByWallet || 0) + (paidByRefWallet || 0);
      const groupPaymentMethod = (storeDetailsAmtAll.payment_method || '').toString().toLowerCase();
      const isWalletOnlyGroup =
        totalWalletPaidForGroup > 0 &&
        groupPaymentMethod.includes('wallet') &&
        !groupPaymentMethod.includes('card');

      // Case 1: Pure wallet orders (cash + ref, no real card payment method).
      // Any remaining refund here (including reserved delivery tip) should be
      // split proportionally between cash wallet and referral wallet.
      if (isWalletOnlyGroup) {
        const cashShare = (paidByWallet || 0) / totalWalletPaidForGroup;
        const refShare = (paidByRefWallet || 0) / totalWalletPaidForGroup;

        const extraCashRefund = Number((cancelledCardRefund * cashShare).toFixed(2));
        const extraRefRefund = Number((cancelledCardRefund - extraCashRefund).toFixed(2));

        if (extraCashRefund > 0) {
          const userCashRow = await knex("users")
            .select("wallet_balance")
            .where("id", userId)
            .first();

          await knex("users")
            .where("id", userId)
            .update({
              wallet_balance: Number(userCashRow?.wallet_balance || 0) + extraCashRefund
            });

          const nextWIdCash = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWIdCash,
            user_id: userId,
            amount: extraCashRefund.toFixed(2),
            resource: "order_refund_cancelled",
            type: "Add",
            group_id: groupID,
            cart_id: cart_id,
          });
        }

        if (extraRefRefund > 0) {
          const userRefRow = await knex("users")
            .select("referral_balance")
            .where("id", userId)
            .first();

          await knex("users")
            .where("id", userId)
            .update({
              referral_balance: Number(userRefRow?.referral_balance || 0) + extraRefRefund
            });

          const lastTxn = await knex("wallet_history")
            .where("user_id", userId)
            .where("group_id", groupID)
            .where("type", "deduction")
            .where("resource", "order_placed_wallet_ref")
            .orderBy("w_id", "desc")
            .first();

          let walletType = "Add";
          const dubaiTime = moment.tz("Asia/Dubai");
          const todayDubai = dubaiTime.format("YYYY-MM-DD");

          if (
            lastTxn &&
            lastTxn.expiry_date &&
            moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
          ) {
            walletType = "wallet_expired";
          }

          const nextWIdRef = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWIdRef,
            user_id: userId,
            amount: extraRefRefund.toFixed(2),
            resource: "order_refund_cancelled_ref",
            type: walletType,
            group_id: groupID,
            cart_id: cart_id,
            expiry_date: lastTxn ? lastTxn.expiry_date : null
          });
        }
      } else {
        // Case 2: Card-based orders (wallet + card or pure card).
        // cancelledCardRefund is computed as (line total - paid_by_wallet - paid_by_ref_wallet).
        // On the LAST cancel (entire group cancelled), tip / remainder can sit in that "card"
        // bucket while referral (or cash-wallet) ledger for the group is not yet square — then
        // we re-route from cancelledCardRefund using wallet_history totals.
        //
        // IMPORTANT: Do NOT run that ledger split on PARTIAL cancels. refDeductions are for the
        // whole group while cancelledCardRefund is only this line's card bucket; refRemaining would
        // be ~full referral paid and would incorrectly absorb almost all of the card refund into
        // referral (on top of the per-cart loop's referral refund).
        const userForCardRefund = await knex("users")
          .select("wallet_balance", "referral_balance")
          .where("id", userId)
          .first();

        const isFullGroupCancel = cancelledCount === allCount;

        if (isFullGroupCancel && cancelledCardRefund > 0) {
          const roundMoney2 = (v) => {
            const n = Number(v);
            if (!Number.isFinite(n)) return 0;
            return Number(n.toFixed(2));
          };

          const sumWalletHistoryAmount = async (whereBuilder) => {
            const row = await knex("wallet_history")
              .where("user_id", userId)
              .where("group_id", groupID)
              .modify(whereBuilder)
              .select(
                knex.raw(
                  "COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_amt"
                )
              )
              .first();
            return Number(row?.total_amt || 0);
          };

          const refDedTotal = await sumWalletHistoryAmount((qb) => {
            qb.where("type", "deduction").where("resource", "order_placed_wallet_ref");
          });
          const refAddTotal = await sumWalletHistoryAmount((qb) => {
            qb.where("resource", "order_refund_cancelled_ref").where("type", "Add");
          });
          const cashDedTotal = await sumWalletHistoryAmount((qb) => {
            qb.where("type", "deduction").where("resource", "order_placed_wallet");
          });
          const cashAddTotal = await sumWalletHistoryAmount((qb) => {
            qb.where("resource", "order_refund_cancelled").where("type", "Add");
          });

          const refRemaining = roundMoney2(Math.max(0, refDedTotal - refAddTotal));
          const cashRemaining = roundMoney2(Math.max(0, cashDedTotal - cashAddTotal));

          let routeToRef = roundMoney2(Math.min(cancelledCardRefund, refRemaining));
          let leftAfterRef = roundMoney2(cancelledCardRefund - routeToRef);
          let routeToCashWallet = roundMoney2(Math.min(leftAfterRef, cashRemaining));
          let routeToCard = roundMoney2(leftAfterRef - routeToCashWallet);

          const splitSum = roundMoney2(routeToRef + routeToCashWallet + routeToCard);
          if (splitSum !== roundMoney2(cancelledCardRefund)) {
            routeToCard = roundMoney2(
              roundMoney2(cancelledCardRefund) - routeToRef - routeToCashWallet
            );
          }

          console.log("[cancelledquickorderprod][case2-card-refund-split]", {
            group_id: groupID,
            requested_cart_id: cart_id,
            full_group_cancel: true,
            cancelledCardRefund: roundMoney2(cancelledCardRefund),
            refDedTotal,
            refAddTotal,
            refRemaining,
            cashDedTotal,
            cashAddTotal,
            cashRemaining,
            routeToRef,
            routeToCashWallet,
            routeToCard,
          });

          let nextWalletBal = Number(userForCardRefund?.wallet_balance || 0);
          let nextRefBal = Number(userForCardRefund?.referral_balance || 0);

          if (routeToRef > 0) {
            const lastTxn = await knex("wallet_history")
              .where("user_id", userId)
              .where("group_id", groupID)
              .where("type", "deduction")
              .where("resource", "order_placed_wallet_ref")
              .orderBy("w_id", "desc")
              .first();

            let walletType = "Add";
            const dubaiTime = moment.tz("Asia/Dubai");
            const todayDubai = dubaiTime.format("YYYY-MM-DD");

            if (
              lastTxn &&
              lastTxn.expiry_date &&
              moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
            ) {
              walletType = "wallet_expired";
            }

            if (walletType === "Add") {
              nextRefBal += routeToRef;
            }

            const nextWIdRef = await getNextWalletHistoryWId();
            await knex("wallet_history").insert({
              w_id: nextWIdRef,
              user_id: userId,
              amount: routeToRef.toFixed(2),
              resource: "order_refund_cancelled_ref",
              type: walletType,
              group_id: groupID,
              cart_id: cart_id,
              expiry_date: lastTxn ? lastTxn.expiry_date : null,
            });
          }

          if (routeToCashWallet > 0) {
            nextWalletBal += routeToCashWallet;
            const nextWIdCash = await getNextWalletHistoryWId();
            await knex("wallet_history").insert({
              w_id: nextWIdCash,
              user_id: userId,
              amount: routeToCashWallet.toFixed(2),
              resource: "order_refund_cancelled",
              type: "Add",
              group_id: groupID,
              cart_id: cart_id,
            });
          }

          if (routeToCard > 0) {
            nextWalletBal += routeToCard;
            const nextWId = await getNextWalletHistoryWId();
            await knex("wallet_history").insert({
              w_id: nextWId,
              user_id: userId,
              amount: routeToCard.toFixed(2),
              resource: "order_refund_cancelled_card_to_wallet",
              type: "Add",
              group_id: groupID,
              cart_id: cart_id,
            });
          }

          await knex("users")
            .where("id", userId)
            .update({
              wallet_balance: nextWalletBal,
              referral_balance: nextRefBal,
            });
        } else if (cancelledCardRefund > 0) {
          // Partial cancel (or same): entire card bucket -> cash wallet (original behaviour).
          await knex("users")
            .where("id", userId)
            .update({
              wallet_balance:
                Number(userForCardRefund?.wallet_balance || 0) + cancelledCardRefund,
            });

          const nextWId = await getNextWalletHistoryWId();
          await knex("wallet_history").insert({
            w_id: nextWId,
            user_id: userId,
            amount: cancelledCardRefund.toFixed(2),
            resource: "order_refund_cancelled_card_to_wallet",
            type: "Add",
            group_id: groupID,
            cart_id: cart_id,
          });
        }
      }
    }

    const group_id = groupIdForEmail;

    totalCal = await knex('orders')
      .where('group_id', group_id)
      .where('cart_id', cart_id)
      .sum('total_products_mrp as total_products_mrp');
    finalAmount = parseFloat(totalCal[0]?.total_products_mrp ?? 0).toFixed(2);

    totalCartAmount = await knex('orders')
      .where('group_id', group_id)
      .sum('total_products_mrp as total_products_mrp');

    total_cart_amount = parseFloat(totalCartAmount[0]?.total_products_mrp ?? 0).toFixed(2);

    orderFinalAmount = parseFloat(total_cart_amount) - parseFloat(finalAmount);

    const result1 = await knex('orders')
      .where('group_id', group_id)
      .where('order_status', 'Cancelled')
      .count({ cancelled_count: 'order_id' });

    const cancelledCount1 = parseInt(result1[0]?.cancelled_count ?? 0, 10);

    const resultAll1 = await knex('orders')
      .where('group_id', group_id)
      .count({ all_count: 'order_id' });

    const allCount1 = parseInt(resultAll1[0]?.all_count ?? 0, 10);

    const is_addedld = await knex('tbl_luckydraw').where('order_id', group_id).where('is_delete', 0).first();

    // if((is_addedld && orderFinalAmount <= 100) || (is_addedld && orderFinalAmount >= 100 && cancelledCount == allCount)){
    if (is_addedld && orderFinalAmount <= 100 || (is_addedld && orderFinalAmount > 100 && cancelledCount1 == allCount1)) {
      if (finalAmount >= 100) {
        const updateentry = await knex('tbl_luckydraw')
          .where('order_id', group_id)
          .update({ 'is_delete': 1 });
      }

      const usertotalorders = await knex('tbl_luckydraw')
        .distinct('orders.group_id')
        .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
        .where('tbl_luckydraw.user_id', user_id)
        .where('tbl_luckydraw.is_delete', 0)
        .where('orders.order_status', '!=', 'Cancelled');

      const getUserup = await knex('users').where('id', user_id).first();

      const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
      // Convert OTP code to a time-based string
      const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

      const payload = {
        "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
        "campaignName": "CancelOrderIphone",
        "destination": "+" + phone_with_country_code,
        "userName": "Quickart General Trading Co LLC",
        "templateParams": [
          getUserup.name, group_id, finalAmount, `${usertotalorders.length}`
        ],
        "source": "new-landing-page form",
        "media": {},
        "buttons": [],
        "carouselCards": [],
        "location": {},
        "attributes": {},
        "paramsFallbackValue": {
          "FirstName": "user"
        }
      };

      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      const result = await response.json();

      const updateentry = await knex('tbl_luckydraw')
        .where('order_id', group_id)
        .update({ 'is_delete': 1 });
    }

    await knex('orders')
      .where('group_id', groupID)
      .where('order_status', 'Cancelled')
      .update({
        'del_partner_tip': 0,
        'coupon_discount': 0,
        'cod_charges': 0,
        'reserve_amount': 0,
        'paid_by_wallet': 0,
        'paid_by_ref_wallet': 0,
        'rem_price': 0
      });


  }


  //Email Code (groupIdForEmail in scope from function top)
  if (groupIdForEmail) {
    storeOrders = await knex('store_orders')
      .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
      .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
      .where('store_orders.order_cart_id', cart_id)

    // Fetch user for email
    const user = await knex('users')
      .select('name', 'email')
      .where('id', user_id)
      .first();
    let userName = user?.name ?? '';
    let userEmail = user?.email ?? '';

    const group_id = groupIdForEmail;

    const logo = await knex('tbl_web_setting').first();
    const appName = logo ? logo.name : null;
    // Fetching the first record from the 'currency' table
    const currency = await knex('currency').first();
    const currencySign = currency ? currency.currency_sign : null;
    const templateData = {
      baseurl: process.env.BASE_URL,
      user_name: userName,
      user_email: userEmail,
      store_orderss: storeOrders,
      final_amount: "",
      app_name: appName,
      currency_sign: currencySign,
      cart_id: group_id
    };
    const subject = 'Delivery Order Cancelled'
    // Trigger the email after order is placed
    //sendCancelledEmail = await cancelorderMail(userEmail, templateData, subject);
  }

};


const getSubresumeorder = async (appDetatils) => {
  // cart_id=appDetatils.cart_id;
  subscription_id = appDetatils.subscription_id;
  cart_id = appDetatils.cart_id;
  // time_slot = appDetatils.time_slot;
  delivery_date = appDetatils.delivery_date;
  today_date = new Date();

  const pausecount = await knex('subscription_order')
    .where('cart_id', cart_id)
    .where('id', subscription_id)
    .where('order_status', 'Pause');

  const subscriptionOrdersDetails = await knex('subscription_order')
    .where('cart_id', cart_id)
    .first();

  if (pausecount.length > 0) {
    //   const oldsData = await knex('subscription_order')
    //   .where('subscription_id',subscription_id)
    //   .where('cart_id', cart_id)
    //   .where('order_status','Pending')
    //   .delete();

    const Pending_Orders = await knex('subscription_order')
      .where('cart_id', cart_id)
      .where('id', subscription_id)
      .where('order_status', 'Pause')
      .update({
        'order_status': "Pending",
        'delivery_date': delivery_date,
        'time_slot': subscriptionOrdersDetails.time_slot,
        'delivery_unique_code': null
      });

    //Email Code
    const groupID = (subscriptionOrdersDetails.group_id) ? subscriptionOrdersDetails.group_id : subscriptionOrdersDetails.cart_id;
    const date = new Date(delivery_date);
    // Format the date to keep only the date part (Day, Month, Date, Year)
    const resumeformattedDate = date.toDateString();
    const resumedayOfWeek = date.toLocaleDateString('en-US', { weekday: 'short' });
    const storeOrders = await knex('store_orders')
      .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
      .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
      .where('orders.cart_id', cart_id)

    let userName = "Store";
    let userEmail = 'store1@quickart.ae';
    const logo = await knex('tbl_web_setting').first();
    const appName = logo ? logo.name : null;
    // Fetching the first record from the 'currency' table
    const currency = await knex('currency').first();
    const currencySign = currency ? currency.currency_sign : null;
    const templateData = {
      baseurl: process.env.BASE_URL,
      user_name: userName,
      user_email: userEmail,
      store_orderss: storeOrders,
      final_amount: "",
      app_name: appName,
      currency_sign: currencySign,
      cart_id: groupID,
      resume_data_deliveryss: resumeformattedDate,
      resumedayOfWeek: resumedayOfWeek
    };
    const subject = 'Order Resumed'
    // Trigger the email after order is placed
    //sendPausedEmail = await resumeorderMail(userEmail, templateData, subject);
    return 1
  } else {
    throw new Error('No Orders Pause Yet');
  }
};


const getCancelquickOrder737 = async (appDetatils) => {
  const minCardRefundThreshold = 0.09;
  const user_id = appDetatils.user_id
  // cart_id =appDetatils.cart_id
  const group_id = appDetatils.group_id
  const cancel_reason = appDetatils.cancel_reason;
  const groupID = appDetatils.group_id

  if (!group_id || !user_id) {
    throw new Error('group_id and user_id are required.');
  }

  // Check if there are any orders that are not already cancelled (single round-trip)
  const existingOrders = await knex('orders')
    .where('group_id', group_id)
    .whereNot('order_status', 'Cancelled')
    .pluck('order_id');

  if (existingOrders.length === 0) {
    throw new Error('All orders are already cancelled.');
  }

  // Pending-order aggregates for refund (PG: non-aggregated columns must use aggregate)
  const orderDetails = await knex('orders')
    .where('group_id', group_id)
    .where('order_status', 'Pending')
    .select(
      knex.raw('COALESCE(SUM(orders.total_products_mrp), 0) as total_products_mrp'),
      knex.raw('COALESCE(SUM(orders.rem_price), 0) as rem_price'),
      knex.raw("COALESCE(SUM(CASE WHEN orders.payment_method != 'COD' AND orders.payment_status = 'success' THEN GREATEST((orders.total_price::numeric - COALESCE(orders.paid_by_wallet, 0)::numeric - COALESCE(orders.paid_by_ref_wallet, 0)::numeric), 0) ELSE 0 END), 0) as card_paid"),
      knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet'),
      knex.raw('COALESCE(SUM(orders.paid_by_ref_wallet), 0) as paid_by_ref_wallet'),
      knex.raw('MAX(orders.payment_method) as payment_method'),
      knex.raw('MAX(orders.user_id) as user_id')
    )
    .first();

  if (!orderDetails || (orderDetails.user_id == null && orderDetails.payment_method == null)) {
    throw new Error('No pending orders to cancel.');
  }

  const paymentMethod = orderDetails.payment_method;
  const userId = orderDetails.user_id;
  const totalProductsMrp = parseFloat(orderDetails.total_products_mrp ?? 0);
  const refundTo3dp = (v) => {
    const n = parseFloat(v ?? 0);
    if (!Number.isFinite(n)) return 0;
    return parseFloat(n.toFixed(3));
  };
  const paidByWallet = refundTo3dp(orderDetails.paid_by_wallet);
  const paidByRefWallet = refundTo3dp(orderDetails.paid_by_ref_wallet);
  let paidByCard = Math.max(
    refundTo3dp(orderDetails.card_paid),
    refundTo3dp(orderDetails.rem_price)
  );
  if (paidByCard <= minCardRefundThreshold) {
    paidByCard = 0;
  }

  // Single aggregate query for totals (PG: cast text columns to numeric)
  const totalCal = await knex('orders')
    .where('group_id', group_id)
    .select(
      knex.raw('COALESCE(SUM(orders.coupon_discount), 0) as coupon_discount'),
      knex.raw('COALESCE(SUM(orders.rem_price), 0) as rem_price'),
      knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet'),
      knex.raw('COALESCE(SUM(orders.paid_by_ref_wallet), 0) as paid_by_ref_wallet'),
      knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as cod_charges"),
      knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as del_partner_tip"),
      knex.raw('COALESCE(SUM(orders.total_products_mrp), 0) as total_products_mrp')
    )
    .first();
  const finalAmount = parseFloat(totalCal?.total_products_mrp ?? 0).toFixed(2);




  order_data = await knex('orders')
    .where('group_id', group_id)
    .pluck('order_id');

  subcription_data = await knex('subscription_order')
    .whereIn('order_id', order_data)
    .where('order_status', 'Pending')
    .update({
      'cancel_reason': cancel_reason,
      'order_status': "Cancelled"
    });

  order_data = await knex('orders')
    .where('group_id', group_id)
    .where('order_status', 'Pending')
    .update({
      'order_status': "Cancelled",
      'cancelling_reason': cancel_reason,
      'paid_by_wallet': 0,
      'paid_by_ref_wallet': 0
    });


  // return paymentMethod;
  if (paymentMethod != 'COD') {
    const user = await knex('users')
      .select('user_phone', 'wallet', 'wallet_balance', 'referral_balance')
      .where('id', userId)
      .first();

    let actualCashWallet = Number(user.wallet_balance || 0) + paidByWallet + paidByCard;
    let actualRefWallet = Number(user.referral_balance || 0) + paidByRefWallet;

    if (paidByWallet > 0 || paidByCard > 0) {
      await knex('users')
        .where('id', userId)
        .update({ 'wallet_balance': actualCashWallet });
    }

    if (paidByWallet > 0) {
      const nextWId = await getNextWalletHistoryWId();
      await knex('wallet_history').insert({
        w_id: nextWId,
        user_id: userId,
        amount: paidByWallet.toFixed(3),
        resource: 'order_refund_cancelled',
        type: 'Add',
        group_id: group_id,
        cart_id: ''
      });
    }

    if (paidByCard > 0) {
      const nextWId = await getNextWalletHistoryWId();
      await knex('wallet_history').insert({
        w_id: nextWId,
        user_id: userId,
        amount: paidByCard.toFixed(3),
        resource: 'order_refund_cancelled_card_to_wallet',
        type: 'Add',
        group_id: group_id,
        cart_id: ''
      });
    }

    if (paidByRefWallet > 0) {
      const lastTxn = await knex("wallet_history")
        .where("user_id", userId)
        .where("group_id", group_id)
        .where("type", "deduction")
        .where("resource", "order_placed_wallet_ref")
        .orderBy("w_id", "desc")
        .first();

      let walletType = "Add";
      const dubaiTime = moment.tz("Asia/Dubai");
      const todayDubai = dubaiTime.format("YYYY-MM-DD");

      if (
        lastTxn &&
        lastTxn.expiry_date &&
        moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
      ) {
        walletType = "wallet_expired";
      }

      if (walletType === "Add") {
        await knex("users")
          .where("id", userId)
          .update({
            referral_balance: actualRefWallet
          });
      }

      const nextWId = await getNextWalletHistoryWId();
      await knex("wallet_history").insert({
        w_id: nextWId,
        user_id: userId,
        amount: paidByRefWallet.toFixed(3),
        resource: "order_refund_cancelled_ref",
        type: walletType,
        group_id: group_id,
        cart_id: "",
        expiry_date: lastTxn ? lastTxn.expiry_date : null
      });
    }
  }
  else {
    if (paidByWallet > 0 || paidByRefWallet > 0 || paidByCard > 0) {
      const user = await knex('users')
        .select('user_phone', 'wallet', 'wallet_balance', 'referral_balance')
        .where('id', userId)
        .first();

      if (paidByWallet > 0 || paidByCard > 0) {
        let actualCashWallet = Number(user.wallet_balance || 0) + paidByWallet + paidByCard;
        await knex('users')
          .where('id', userId)
          .update({ 'wallet_balance': actualCashWallet });
      }

      if (paidByWallet > 0) {
        const nextWId = await getNextWalletHistoryWId();
        await knex('wallet_history').insert({
          w_id: nextWId,
          user_id: userId,
          amount: paidByWallet.toFixed(3),
          resource: 'order_refund_cancelled',
          type: 'Add',
          group_id: group_id,
          cart_id: ''
        });
      }

      if (paidByCard > 0) {
        const nextWId = await getNextWalletHistoryWId();
        await knex('wallet_history').insert({
          w_id: nextWId,
          user_id: userId,
          amount: paidByCard.toFixed(3),
          resource: 'order_refund_cancelled_card_to_wallet',
          type: 'Add',
          group_id: group_id,
          cart_id: ''
        });
      }

      if (paidByRefWallet > 0) {
        let actualRefWallet = Number(user.referral_balance || 0) + paidByRefWallet;
        const lastTxn = await knex("wallet_history")
          .where("user_id", userId)
          .where("group_id", group_id)
          .where("type", "deduction")
          .where("resource", "order_placed_wallet_ref")
          .orderBy("w_id", "desc")
          .first();

        let walletType = "Add";
        const dubaiTime = moment.tz("Asia/Dubai");
        const todayDubai = dubaiTime.format("YYYY-MM-DD");

        if (
          lastTxn &&
          lastTxn.expiry_date &&
          moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
        ) {
          walletType = "wallet_expired";
        }

        if (walletType === "Add") {
          await knex("users")
            .where("id", userId)
            .update({
              referral_balance: actualRefWallet
            });
        }

        const nextWId = await getNextWalletHistoryWId();
        await knex("wallet_history").insert({
          w_id: nextWId,
          user_id: userId,
          amount: paidByRefWallet.toFixed(3),
          resource: "order_refund_cancelled_ref",
          type: walletType,
          group_id: group_id,
          cart_id: "",
          expiry_date: lastTxn ? lastTxn.expiry_date : null
        });
      }
    }
  }

  const is_addedld = await knex('tbl_luckydraw').where('order_id', group_id).where('is_delete', 0).first();

  if (is_addedld) {
    const usertotalorders = await knex('tbl_luckydraw')
      .distinct('orders.group_id')
      .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
      .where('tbl_luckydraw.user_id', user_id)
      .where('tbl_luckydraw.is_delete', 0)
      .where('orders.order_status', '!=', 'Cancelled');

    const getUserup = await knex('users').where('id', user_id).first();

    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
    // Convert OTP code to a time-based string
    const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

    const payload = {
      "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      "campaignName": "CancelOrderIphone",
      "destination": "+" + phone_with_country_code,
      "userName": "Quickart General Trading Co LLC",
      "templateParams": [
        getUserup.name, group_id, finalAmount, `${usertotalorders.length}`
      ],
      "source": "new-landing-page form",
      "media": {},
      "buttons": [],
      "carouselCards": [],
      "location": {},
      "attributes": {},
      "paramsFallbackValue": {
        "FirstName": "user"
      }
    };

    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    const updateentry = await knex('tbl_luckydraw')
      .where('order_id', group_id)
      .update({ 'is_delete': 1 });
  }

  await knex('orders')
    .where('group_id', groupID)
    .where('order_status', 'Cancelled')
    .update({
      'paid_by_wallet': 0,
      'paid_by_ref_wallet': 0,
      'rem_price': 0
    });


  //Email Code
  storeOrders = await knex('store_orders')
    .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
    .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('orders.group_id', group_id)

  // Fetch user for email
  const user = await knex('users')
    .select('name', 'email')
    .where('id', user_id)
    .first();
  let userName = user?.name ?? '';
  let userEmail = user?.email ?? '';

  const logo = await knex('tbl_web_setting').first();
  const appName = logo ? logo.name : null;
  // Fetching the first record from the 'currency' table
  const currency = await knex('currency').first();
  const currencySign = currency ? currency.currency_sign : null;
  const templateData = {
    baseurl: process.env.BASE_URL,
    user_name: userName,
    user_email: userEmail,
    store_orderss: storeOrders,
    final_amount: "",
    app_name: appName,
    currency_sign: currencySign,
    cart_id: group_id
  };
  const subject = 'Delivery Order Cancelled'
  // Trigger the email after order is placed
  //sendCancelledEmail = await cancelorderMail(userEmail, templateData, subject);



};

const getCancelquickOrder = async (appDetatils) => {
  const minCardRefundThreshold = 0.09;
  const user_id = appDetatils.user_id
  // cart_id =appDetatils.cart_id
  const group_id = appDetatils.group_id
  const cancel_reason = appDetatils.cancel_reason;
  const groupID = appDetatils.group_id

  if (!group_id || !user_id) {
    throw new Error('group_id and user_id are required.');
  }

  // Check if there are any orders that are not already cancelled (single round-trip)
  const existingOrders = await knex('orders')
    .where('group_id', group_id)
    .whereNot('order_status', 'Cancelled')
    .pluck('order_id');

  if (existingOrders.length === 0) {
    throw new Error('All orders are already cancelled.');
  }

  // Pending-order aggregates for refund (PG: non-aggregated columns must use aggregate)
  const orderDetails = await knex('orders')
    .where('group_id', group_id)
    .where('order_status', 'Pending')
    .select(
      knex.raw('COALESCE(SUM(orders.total_products_mrp), 0) as total_products_mrp'),
      knex.raw("COALESCE(SUM(CASE WHEN orders.payment_method != 'COD' AND orders.payment_status = 'success' THEN GREATEST((orders.total_price::numeric - COALESCE(orders.paid_by_wallet, 0)::numeric - COALESCE(orders.paid_by_ref_wallet, 0)::numeric), 0) ELSE 0 END), 0) as card_paid"),
      knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet'),
      knex.raw('COALESCE(SUM(orders.paid_by_ref_wallet), 0) as paid_by_ref_wallet'),
      knex.raw('MAX(orders.payment_method) as payment_method'),
      knex.raw('MAX(orders.user_id) as user_id')
    )
    .first();

  if (!orderDetails || (orderDetails.user_id == null && orderDetails.payment_method == null)) {
    throw new Error('No pending orders to cancel.');
  }

  const paymentMethod = orderDetails.payment_method;
  const userId = orderDetails.user_id;
  const totalProductsMrp = parseFloat(orderDetails.total_products_mrp ?? 0);
  const refundTo3dp = (v) => {
    const n = parseFloat(v ?? 0);
    if (!Number.isFinite(n)) return 0;
    return parseFloat(n.toFixed(3));
  };
  const paidByWallet = refundTo3dp(orderDetails.paid_by_wallet);
  const paidByRefWallet = refundTo3dp(orderDetails.paid_by_ref_wallet);
  let paidByCard = refundTo3dp(orderDetails.card_paid);
  if (paidByCard <= minCardRefundThreshold) {
    paidByCard = 0;
  }
  const sumGroupWalletHistoryAmount = async (whereBuilder) => {
    const row = await knex("wallet_history")
      .where("user_id", userId)
      .where("group_id", group_id)
      .modify(whereBuilder)
      .select(
        knex.raw(
          "COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_amt"
        )
      )
      .first();
    return Number(row?.total_amt || 0);
  };

  let routeCardToRef = 0;
  let routeCardToCashWallet = 0;
  let routeCardToCardWallet = paidByCard;
  if (paidByCard > 0) {
    const refDedTotal = await sumGroupWalletHistoryAmount((qb) => {
      qb.where("type", "deduction").where("resource", "order_placed_wallet_ref");
    });
    const refAddTotal = await sumGroupWalletHistoryAmount((qb) => {
      qb.where("resource", "order_refund_cancelled_ref").where("type", "Add");
    });
    const cashDedTotal = await sumGroupWalletHistoryAmount((qb) => {
      qb.where("type", "deduction").where("resource", "order_placed_wallet");
    });
    const cashAddTotal = await sumGroupWalletHistoryAmount((qb) => {
      qb.where("resource", "order_refund_cancelled").where("type", "Add");
    });

    const refRemainingAfterDirect = Math.max(
      0,
      refundTo3dp(refDedTotal - refAddTotal - paidByRefWallet)
    );
    const cashRemainingAfterDirect = Math.max(
      0,
      refundTo3dp(cashDedTotal - cashAddTotal - paidByWallet)
    );

    routeCardToRef = refundTo3dp(Math.min(paidByCard, refRemainingAfterDirect));
    let leftAfterRef = refundTo3dp(paidByCard - routeCardToRef);
    routeCardToCashWallet = refundTo3dp(
      Math.min(leftAfterRef, cashRemainingAfterDirect)
    );
    routeCardToCardWallet = refundTo3dp(leftAfterRef - routeCardToCashWallet);

    const splitTotal = refundTo3dp(
      routeCardToRef + routeCardToCashWallet + routeCardToCardWallet
    );
    if (splitTotal !== paidByCard) {
      routeCardToCardWallet = refundTo3dp(
        paidByCard - routeCardToRef - routeCardToCashWallet
      );
    }

    console.log("[cancelledquickorder][card-refund-routing]", {
      group_id,
      paidByWallet,
      paidByRefWallet,
      paidByCard,
      refDedTotal,
      refAddTotal,
      cashDedTotal,
      cashAddTotal,
      refRemainingAfterDirect,
      cashRemainingAfterDirect,
      routeCardToRef,
      routeCardToCashWallet,
      routeCardToCardWallet,
    });
  }

  // Single aggregate query for totals (PG: cast text columns to numeric)
  const totalCal = await knex('orders')
    .where('group_id', group_id)
    .select(
      knex.raw('COALESCE(SUM(orders.coupon_discount), 0) as coupon_discount'),
      knex.raw('COALESCE(SUM(orders.rem_price), 0) as rem_price'),
      knex.raw('COALESCE(SUM(orders.paid_by_wallet), 0) as paid_by_wallet'),
      knex.raw('COALESCE(SUM(orders.paid_by_ref_wallet), 0) as paid_by_ref_wallet'),
      knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as cod_charges"),
      knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as del_partner_tip"),
      knex.raw('COALESCE(SUM(orders.total_products_mrp), 0) as total_products_mrp')
    )
    .first();
  const finalAmount = parseFloat(totalCal?.total_products_mrp ?? 0).toFixed(2);




  order_data = await knex('orders')
    .where('group_id', group_id)
    .pluck('order_id');

  subcription_data = await knex('subscription_order')
    .whereIn('order_id', order_data)
    .where('order_status', 'Pending')
    .update({
      'cancel_reason': cancel_reason,
      'order_status': "Cancelled"
    });

  order_data = await knex('orders')
    .where('group_id', group_id)
    .where('order_status', 'Pending')
    .update({
      'order_status': "Cancelled",
      'cancelling_reason': cancel_reason,
      'paid_by_wallet': 0,
      'paid_by_ref_wallet': 0
    });


  // return paymentMethod;
  if (paymentMethod != 'COD') {
    const user = await knex('users')
      .select('user_phone', 'wallet', 'wallet_balance', 'referral_balance')
      .where('id', userId)
      .first();

    const totalCashWalletRefund = refundTo3dp(paidByWallet + routeCardToCashWallet);
    const totalRefRefund = refundTo3dp(paidByRefWallet + routeCardToRef);

    if (totalCashWalletRefund > 0 || routeCardToCardWallet > 0) {
      const actualCashWallet = Number(user.wallet_balance || 0) + totalCashWalletRefund + routeCardToCardWallet;
      await knex('users')
        .where('id', userId)
        .update({ 'wallet_balance': actualCashWallet });
    }

    if (totalCashWalletRefund > 0) {
      const nextWId = await getNextWalletHistoryWId();
      await knex('wallet_history').insert({
        w_id: nextWId,
        user_id: userId,
        amount: totalCashWalletRefund.toFixed(3),
        resource: 'order_refund_cancelled',
        type: 'Add',
        group_id: group_id,
        cart_id: ''
      });
    }

    if (routeCardToCardWallet > 0) {
      const nextWId = await getNextWalletHistoryWId();
      await knex('wallet_history').insert({
        w_id: nextWId,
        user_id: userId,
        amount: routeCardToCardWallet.toFixed(3),
        resource: 'order_refund_cancelled_card_to_wallet',
        type: 'Add',
        group_id: group_id,
        cart_id: ''
      });
    }

    if (totalRefRefund > 0) {
      const lastTxn = await knex("wallet_history")
        .where("user_id", userId)
        .where("group_id", group_id)
        .where("type", "deduction")
        .where("resource", "order_placed_wallet_ref")
        .orderBy("w_id", "desc")
        .first();

      let walletType = "Add";
      const dubaiTime = moment.tz("Asia/Dubai");
      const todayDubai = dubaiTime.format("YYYY-MM-DD");

      if (
        lastTxn &&
        lastTxn.expiry_date &&
        moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
      ) {
        walletType = "wallet_expired";
      }

      if (walletType === "Add") {
        const actualRefWallet = Number(user.referral_balance || 0) + totalRefRefund;
        await knex("users")
          .where("id", userId)
          .update({
            referral_balance: actualRefWallet
          });
      }

      const nextWId = await getNextWalletHistoryWId();
      await knex("wallet_history").insert({
        w_id: nextWId,
        user_id: userId,
        amount: totalRefRefund.toFixed(3),
        resource: "order_refund_cancelled_ref",
        type: walletType,
        group_id: group_id,
        cart_id: "",
        expiry_date: lastTxn ? lastTxn.expiry_date : null
      });
    }
  }
  else {
    if (paidByWallet > 0 || paidByRefWallet > 0 || paidByCard > 0) {
      const user = await knex('users')
        .select('user_phone', 'wallet', 'wallet_balance', 'referral_balance')
        .where('id', userId)
        .first();

      const totalCashWalletRefund = refundTo3dp(paidByWallet + routeCardToCashWallet);
      const totalRefRefund = refundTo3dp(paidByRefWallet + routeCardToRef);

      if (totalCashWalletRefund > 0 || routeCardToCardWallet > 0) {
        let actualCashWallet = Number(user.wallet_balance || 0) + totalCashWalletRefund + routeCardToCardWallet;
        await knex('users')
          .where('id', userId)
          .update({ 'wallet_balance': actualCashWallet });
      }

      if (totalCashWalletRefund > 0) {
        const nextWId = await getNextWalletHistoryWId();
        await knex('wallet_history').insert({
          w_id: nextWId,
          user_id: userId,
          amount: totalCashWalletRefund.toFixed(3),
          resource: 'order_refund_cancelled',
          type: 'Add',
          group_id: group_id,
          cart_id: ''
        });
      }

      if (routeCardToCardWallet > 0) {
        const nextWId = await getNextWalletHistoryWId();
        await knex('wallet_history').insert({
          w_id: nextWId,
          user_id: userId,
          amount: routeCardToCardWallet.toFixed(3),
          resource: 'order_refund_cancelled_card_to_wallet',
          type: 'Add',
          group_id: group_id,
          cart_id: ''
        });
      }

      if (totalRefRefund > 0) {
        const lastTxn = await knex("wallet_history")
          .where("user_id", userId)
          .where("group_id", group_id)
          .where("type", "deduction")
          .where("resource", "order_placed_wallet_ref")
          .orderBy("w_id", "desc")
          .first();

        let walletType = "Add";
        const dubaiTime = moment.tz("Asia/Dubai");
        const todayDubai = dubaiTime.format("YYYY-MM-DD");

        if (
          lastTxn &&
          lastTxn.expiry_date &&
          moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
        ) {
          walletType = "wallet_expired";
        }

        if (walletType === "Add") {
          let actualRefWallet = Number(user.referral_balance || 0) + totalRefRefund;
          await knex("users")
            .where("id", userId)
            .update({
              referral_balance: actualRefWallet
            });
        }

        const nextWId = await getNextWalletHistoryWId();
        await knex("wallet_history").insert({
          w_id: nextWId,
          user_id: userId,
          amount: totalRefRefund.toFixed(3),
          resource: "order_refund_cancelled_ref",
          type: walletType,
          group_id: group_id,
          cart_id: "",
          expiry_date: lastTxn ? lastTxn.expiry_date : null
        });
      }
    }
  }

  const is_addedld = await knex('tbl_luckydraw').where('order_id', group_id).where('is_delete', 0).first();

  if (is_addedld) {
    const usertotalorders = await knex('tbl_luckydraw')
      .distinct('orders.group_id')
      .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
      .where('tbl_luckydraw.user_id', user_id)
      .where('tbl_luckydraw.is_delete', 0)
      .where('orders.order_status', '!=', 'Cancelled');

    const getUserup = await knex('users').where('id', user_id).first();

    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
    // Convert OTP code to a time-based string
    const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

    const payload = {
      "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      "campaignName": "CancelOrderIphone",
      "destination": "+" + phone_with_country_code,
      "userName": "Quickart General Trading Co LLC",
      "templateParams": [
        getUserup.name, group_id, finalAmount, `${usertotalorders.length}`
      ],
      "source": "new-landing-page form",
      "media": {},
      "buttons": [],
      "carouselCards": [],
      "location": {},
      "attributes": {},
      "paramsFallbackValue": {
        "FirstName": "user"
      }
    };

    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    const updateentry = await knex('tbl_luckydraw')
      .where('order_id', group_id)
      .update({ 'is_delete': 1 });
  }

  await knex('orders')
    .where('group_id', groupID)
    .where('order_status', 'Cancelled')
    .update({
      'paid_by_wallet': 0,
      'paid_by_ref_wallet': 0,
      'rem_price': 0
    });


  //Email Code
  storeOrders = await knex('store_orders')
    .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
    .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('orders.group_id', group_id)

  // Fetch user for email
  const user = await knex('users')
    .select('name', 'email')
    .where('id', user_id)
    .first();
  let userName = user?.name ?? '';
  let userEmail = user?.email ?? '';

  const logo = await knex('tbl_web_setting').first();
  const appName = logo ? logo.name : null;
  // Fetching the first record from the 'currency' table
  const currency = await knex('currency').first();
  const currencySign = currency ? currency.currency_sign : null;
  const templateData = {
    baseurl: process.env.BASE_URL,
    user_name: userName,
    user_email: userEmail,
    store_orderss: storeOrders,
    final_amount: "",
    app_name: appName,
    currency_sign: currencySign,
    cart_id: group_id
  };
  const subject = 'Delivery Order Cancelled'
  // Trigger the email after order is placed
  //sendCancelledEmail = await cancelorderMail(userEmail, templateData, subject);



};

const getCancelquickOrderold = async (appDetatils) => {
  const user_id = appDetatils.user_id
  // cart_id =appDetatils.cart_id
  const group_id = appDetatils.group_id
  const cancel_reason = appDetatils.cancel_reason;
  const groupID = appDetatils.group_id


  const user = await knex('users')
    .select('name', 'email')
    .where('id', user_id)
    .first();

  sendCancelnotification = await sendRejectNotification(cancel_reason, user, group_id, user_id);
  return sendCancelnotification;

  //   //After cancelled order wallet amount save 
  //   const orderDetails = await knex('orders')
  //   .where('group_id', group_id)
  //   .where('order_status', 'Pending')
  //   .select(knex.raw('SUM(orders.total_products_mrp) as total_products_mrp'),knex.raw('SUM(orders.paid_by_wallet) as paid_by_wallet'),'payment_method','user_id')
  //   .first(); 
  //   const paymentMethod=orderDetails.payment_method;
  //   const userId=orderDetails.user_id;
  //   const totalProductsMrp=orderDetails.total_products_mrp;
  //   const paidByWallet=orderDetails.paid_by_wallet;

  //   if(group_id && user_id ){

  //   // Check if there are any orders that are not already cancelled
  //   const existingOrders = await knex('orders')
  //   .where('group_id', group_id)
  //   .whereNot('order_status', 'Cancelled')
  //   .pluck('order_id');

  //   // If there are no orders to cancel, return a message
  //   if (existingOrders.length === 0) {
  //   throw new Error('All orders are already cancelled.');
  //   }


  //     order_data = await knex('orders')
  //               .where('group_id', group_id)
  //               .pluck('order_id');

  //     subcription_data = await knex('subscription_order')
  //     //.where('cart_id', cart_id)
  //     .whereIn('order_id',order_data)
  //     .where('order_status', 'Pending')
  //     .where('si_payment_flag', 'no')
  //     .where('processing_product','!=','1')
  //     .update({'cancel_reason':cancel_reason,
  //               'order_status':"Cancelled"});

  //   order_data = await knex('orders')
  //               //.where('cart_id', cart_id)
  //               .where('group_id', group_id)
  //               .where('order_status', 'Pending')
  //              // .where('payment_status','!=','success')
  //               .update({'order_status':"Cancelled"});


  //   // return paymentMethod;
  //   if(paymentMethod != 'COD')
  //   {

  //     const user = await knex('users')
  //     .select('user_phone', 'wallet')
  //     .where('id', userId)
  //     .first();
  //     let actualWallet=user.wallet+totalProductsMrp;

  //     await knex('users')
  //     .where('id', userId)
  //     .update({
  //     'wallet':actualWallet,
  //     }); 

  //     await knex('wallet_history').insert({
  //     user_id: userId,
  //     amount: totalProductsMrp.toFixed(2),
  //     resource: 'order_refund_cancelled',
  //     type:'Add',
  //     group_id:group_id,
  //     cart_id:''
  //     });
  //   }else
  //   {
  //     if(paidByWallet > 0){
  //     const user = await knex('users')
  //     .select('user_phone', 'wallet')
  //     .where('id', userId)
  //     .first();
  //     let actualWallet=parseFloat(user.wallet)+parseFloat(paidByWallet);

  //     await knex('users')
  //     .where('id', userId)
  //     .update({
  //     'wallet':actualWallet,
  //     }); 

  //     await knex('wallet_history').insert({
  //     user_id: userId,
  //     amount: paidByWallet.toFixed(2),
  //     resource: 'order_refund_cancelled',
  //     type:'Add',
  //     group_id:group_id,
  //     cart_id:''
  //     });
  //   }
  //   }

  //   }

  //   await knex('orders')
  //   .where('group_id',groupID)
  //   .where('order_status','Cancelled')
  //   .update({
  //   'paid_by_wallet':0,
  //   'rem_price':0
  //   }); 


  // //Email Code
  // storeOrders = await knex('store_orders')
  // .select('store_orders.*','orders.group_id','orders.time_slot')
  // .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
  // .where('orders.group_id', group_id)

  // // Fetch user phone and wallet
  // const user = await knex('users')
  // .select('name','email')
  // .where('id', user_id)
  // .first();
  // let userName=user.name;
  // let userEmail=user.email;

  // const logo = await knex('tbl_web_setting').first();
  // const appName = logo ? logo.name : null;
  // // Fetching the first record from the 'currency' table
  // const currency = await knex('currency').first();
  // const currencySign = currency ? currency.currency_sign : null;
  // const templateData = {
  // baseurl: process.env.BASE_URL,
  // user_name:userName,
  // user_email:userEmail,
  // store_orderss:storeOrders,
  // final_amount:"",
  // app_name:appName,
  // currency_sign:currencySign,
  // cart_id:group_id
  // };
  // const subject = 'Delivery Order Cancelled'
  // // Trigger the email after order is placed
  // sendCancelledEmail = await cancelorderMail(userEmail, templateData,subject);
  // sendCancelnotification = await sendRejectNotification(cancel_reason, user, group_id, user_id);
};

const getMydailyOrder = async (appDetails) => {
  // Input validation and type coercion for PostgreSQL and safety
  const userId = appDetails.user_id != null ? String(appDetails.user_id) : null;
  const page = Math.max(1, parseInt(appDetails.page, 10) || 1);
  const perpage = Math.min(Math.max(1, parseInt(appDetails.perpage, 10) || 10), 100);
  const offset = (page - 1) * perpage;

  if (!userId) {
    throw new Error('user_id is required');
  }

  // 1) Single query: paginated grouped orders (PostgreSQL-compatible GROUP BY + aggregates)
  const groupedOrders = await knex('orders')
    .select(
      knex.raw('MAX(orders.order_type) as order_type'),
      'orders.group_id',
      knex.raw('MAX(orders.time_slot) as time_slot'),
      knex.raw("to_char(MAX(orders.delivery_date), 'YYYY-MM-DD') as delivery_date"),
      knex.raw('MAX(orders.cart_id) as cart_id'),
      knex.raw("to_char(MAX(orders.order_date), 'YYYY-MM-DD') as order_date"),
      knex.raw('MAX(orders.coupon_discount) as coupon_discount'),
      knex.raw('MAX(orders.total_products_mrp) as price_without_delivery'),
      knex.raw('MAX(orders.user_id) as user_id'),
      knex.raw('MAX(orders.is_subscription) as is_subscription'),
      knex.raw('MAX(orders.si_order) as si_order'),
      knex.raw('MAX(orders.bank_id) as bank_id'),
      knex.raw('MAX(orders.order_status) as order_status')
    )
    .where('orders.user_id', userId)
    .whereNotNull('orders.order_status')
    // Do not return abandoned orders in the daily-orders response.
    .whereNot('orders.order_status', 'Order_abandoned')
    .where(function () {
      this.where('orders.is_subscription', 0).orWhereNull('orders.is_subscription');
    })
    .whereNotNull('orders.payment_method')
    .groupBy('orders.group_id')
    .orderByRaw('MAX(orders.order_id) DESC')
    .offset(offset)
    .limit(perpage);

  if (groupedOrders.length === 0) {
    return [];
  }

  const groupIds = groupedOrders.map((r) => r.group_id);

  // 2) Single query: all order statuses for these group_ids (for display status)
  const statusRows = await knex('orders')
    .select('orders.group_id', 'orders.order_status', 'orders.order_id')
    .whereIn('orders.group_id', groupIds)
    .whereNot('orders.order_status', 'Order_abandoned')
    .whereIn('orders.order_status', [
      'Completed',
      'Confirmed',
      'Out_For_Delivery',
      'Pending',
      'Processing Payment',
      'Order Not Placed',
      'Processing_payment',
      'Payment_failed'
    ])
    .orderBy('orders.order_id', 'asc');

  const statusByGroup = new Map();
  for (const row of statusRows) {
    if (!statusByGroup.has(row.group_id)) {
      let display = row.order_status;
      if (row.order_status === 'Pending') display = 'In Progress';
      else if (row.order_status === 'Processing_payment') display = 'Processing Payment';
      else if (row.order_status === 'Payment_failed') display = 'Payment Failed';
      statusByGroup.set(row.group_id, display);
    }
  }

  // 3) Single query: totals per group_id (PostgreSQL: cast text to numeric for cod_charges, del_partner_tip)
  const totalsRows = await knex('orders')
    .select('orders.group_id')
    .sum('orders.total_products_mrp as total_products_mrp')
    .sum('orders.rem_price as rem_price')
    .sum('orders.paid_by_wallet as paid_by_wallet')
    .sum('orders.coupon_discount as coupon_discount')
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(orders.cod_charges), '')::numeric), 0) as cod_charges"))
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(orders.del_partner_tip), '')::numeric), 0) as del_partner_tip"))
    .whereNot('orders.order_status', 'Order_abandoned')
    .whereIn('orders.group_id', groupIds)
    .groupBy('orders.group_id');

  const totalsByGroupMap = new Map();
  for (const row of totalsRows) {
    totalsByGroupMap.set(row.group_id, {
      total_products_mrp: parseFloat(row.total_products_mrp || 0),
      rem_price: row.rem_price,
      paid_by_wallet: row.paid_by_wallet,
      cod_charges: row.cod_charges,
      coupon_discount: row.coupon_discount,
      del_partner_tip: row.del_partner_tip
    });
  }

  // 4) Single query: product_details per group_id (STRING_AGG with qty::text)
  const productRows = await knex('orders')
    .join('store_orders', 'orders.cart_id', 'store_orders.order_cart_id')
    .whereNot('orders.order_status', 'Order_abandoned')
    .whereIn('orders.group_id', groupIds)
    .select('orders.group_id')
    .select(knex.raw("STRING_AGG(store_orders.product_name || ' X ' || store_orders.qty::text, ',') as product_details"))
    .groupBy('orders.group_id');

  const productByGroup = new Map();
  for (const row of productRows) {
    productByGroup.set(row.group_id, row.product_details || '');
  }

  const customizedProductData = groupedOrders.map((prd) => {
    const ordstatus = statusByGroup.get(prd.group_id) || 'Cancelled';
    const totals = totalsByGroupMap.get(prd.group_id);
    const famount = totals ? parseFloat((totals.total_products_mrp || 0).toFixed(2)) : 0;
    const productDetails = productByGroup.get(prd.group_id) || '';

    return {
      group_id: prd.group_id,
      cart_id: prd.cart_id,
      order_date: prd.order_date,
      coupon_discount: prd.coupon_discount,
      price_without_delivery: famount,
      user_id: prd.user_id,
      is_subscription: prd.is_subscription,
      si_order: prd.si_order,
      bank_id: prd.bank_id,
      order_status: ordstatus,
      time_slot: prd.time_slot,
      delivery_date: prd.delivery_date,
      productname: productDetails,
      orderType: prd.order_type || 'normal'
    };
  });

  return customizedProductData;
};


const getSubpauseorder = async (appDetatils) => {
  // cart_id=appDetatils.cart_id;
  let order_id;
  subscription_id = appDetatils.subscription_id;
  pause_reason = appDetatils.pause_reason;
  cart_id = appDetatils.cart_id;
  store_order_id = typeof appDetatils.store_order_id === 'number' ? appDetatils.store_order_id : parseInt(appDetatils.store_order_id, 10);
  group_id = appDetatils.group_id;
  today_date = new Date();
  // Extract the date parts
  const year = today_date.getFullYear();
  const month = String(today_date.getMonth() + 1).padStart(2, '0'); // getMonth() is zero-based
  const day = String(today_date.getDate()).padStart(2, '0');

  // Format the date as 'YYYY-MM-DD'
  const formattedDate = `${year}-${month}-${day}`;

  const oldsData = await knex('subscription_order')
    .select('id')
    .where({
      store_order_id: store_order_id,
      order_status: 'Pause'
    });

  // Step 2: Extract `id` values from the query result
  const subscriptionIdOlds = oldsData.map(row => row.id);


  // Step 3: Create comma-separated strings and arrays
  const subscriptionIdOld = subscriptionIdOlds.join(',');
  const subscriptionIdNew = subscription_id.split(',');

  // Step 4: Convert to arrays and sort
  let subscriptionIdOld1 = subscriptionIdOld.split(',');
  let subscriptionIdNew1 = subscription_id.split(',');

  subscriptionIdOld1.sort();
  subscriptionIdNew1.sort();

  // Optional: re-index arrays (sort already returns a re-indexed array in JavaScript)
  subscriptionIdOld1 = Array.from(subscriptionIdOld1);
  subscriptionIdNew1 = Array.from(subscriptionIdNew1);



  //Pause Order functionality  Start

  let new_arr_pause = [];
  for (let i = 0; i < subscriptionIdNew1.length; i++) {
    let subscriptionIdNew12 = '';
    for (let kj = 0; kj < subscriptionIdOld1.length; kj++) {
      if (subscriptionIdNew1[i].trim() === subscriptionIdOld1[kj].trim()) {
        subscriptionIdNew12 = subscriptionIdOld1[kj].trim();
        break;
      }
    }
    if (subscriptionIdNew12 !== subscriptionIdNew1[i].trim()) {
      new_arr_pause.push(subscriptionIdNew1[i].trim());
    }
  }


  //Pause Order functionality  End 

  //Resume Order functionality  Start 


  new_arr_resume = [];
  for (let ii = 0; ii < subscriptionIdOld1.length; ii++) {
    //return subscriptionIdNew1
    subscription_id_new122 = '';
    for (let kjj = 0; kjj < subscriptionIdNew1.length; kjj++) {
      if (subscriptionIdOld1[ii].trim() == subscriptionIdNew1[kjj].trim()) {
        subscription_id_new122 = subscriptionIdNew1[kjj].trim();
      }
    }
    if (subscription_id_new122 != subscriptionIdOld1[ii].trim()) {
      new_arr_resume.push(subscriptionIdOld1[ii].trim());
    }
  }


  pause_data_deliveryss1 = new_arr_pause;
  resume_data_deliveryss1 = new_arr_resume;

  //  return resume_data_deliveryss1

  // return pause_data_deliveryss1;
  let resume_idss = resume_data_deliveryss1.join("','");
  let pause_idss = pause_data_deliveryss1.join("','");
  let resume_idsss = "'" + resume_idss + "'";
  let pause_idsss = "'" + pause_idss + "'";

  const pauseIdsInt = pause_data_deliveryss1.map(id => parseInt(id, 10)).filter(n => !Number.isNaN(n));
  const pause_dataa = await knex('subscription_order')
    .select('delivery_date', 'time_slot', 'user_id')
    .whereIn('id', pauseIdsInt.length ? pauseIdsInt : pause_data_deliveryss1)

  // Process the results
  const pause_data_delivery = pause_dataa.map(row => moment(row.delivery_date).format('DD MMMM'));

  const pause_data_deliveryss12 = pause_data_delivery.join(' & ');

  const resumeIdsInt = resume_data_deliveryss1.map(id => parseInt(id, 10)).filter(n => !Number.isNaN(n));
  const resumeDataa = await knex('subscription_order')
    .select('delivery_date', 'time_slot', 'user_id')
    .whereIn('id', resumeIdsInt.length ? resumeIdsInt : resume_data_deliveryss1)

  // Process the results
  const resume_data_delivery = resumeDataa.map(row => moment(row.delivery_date).format('DD MMMM'));

  // Join the formatted dates with " & "
  const resume_data_deliveryss12 = resume_data_delivery.join(' & ');



  //  Pending_Orders= await knex('subscription_order')
  //  .where('cart_id',cart_id)
  //  .where('store_order_id',store_order_id)
  //  //.where('delivery_date','>',today_date)
  //  .where('delivery_date','>',formattedDate)
  //  .where('order_status1','!=','Cancelled')
  //  .update({'order_status': "Pending",
  //  'pause_reason' :""});  

  //  const Pending_Orders = await knex('subscription_order')
  //   .where('cart_id',cart_id)
  //   .where('store_order_id',store_order_id)
  //   .where('delivery_date', '>', formattedDate)
  //   .where('order_status','!=','Cancelled')
  //   .update({
  //     'pause_reason': "abcd",
  //     'order_status': "Pending"
  //   });

  //  sub_order=subscription_id.split(',');

  //  for (let i = 0; i < sub_order.length; i++) 
  //  {

  //  sub_order_id=sub_order[i];
  //  pause_order= await knex('subscription_order')
  //  .where('id',sub_order_id)
  //  .where('delivery_date','>',today_date)
  //  .update({'pause_reason':pause_reason,
  //  'order_status':"Pause"});
  //  } 

  const sub_order = subscription_id.split(',').map(id => id.trim()).filter(Boolean);
  const subOrderIdsInt = sub_order.map(id => parseInt(id, 10)).filter(n => !Number.isNaN(n));
  if (subOrderIdsInt.length > 0) {
    await knex('subscription_order')
      .whereIn('id', subOrderIdsInt)
      .where('delivery_date', '>', formattedDate)
      .update({
        pause_reason: pause_reason,
        order_status: 'Pause'
      });
  }


  // Fetch ongoing orders (PostgreSQL: orders.user_id is text, users.id is integer)
  const ongoings = await knex('orders')
    .join('store', 'orders.store_id', '=', 'store.id')
    .join('users', 'users.id', 'orders.user_id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
    .where('orders.cart_id', cart_id)
    .whereNot('orders.order_status', 'NULL')
    .whereNotNull('orders.payment_method')
    .orderBy('orders.order_id', 'DESC')
    .first();

  //return ongoings

  // Fetch subscription orders
  const subscription_orders = await knex('subscription_order')
    .where('subscription_order.cart_id', cart_id)
    .where('store_order_id', store_order_id)
    .orderBy('subscription_order.id', 'DESC')
    .first();

  //  return subscription_orders

  // Fetch store details
  const store_details = await knex('store_orders')
    .where('store_order_id', store_order_id)
    .first();



  // Subscription Module
  // return sub_order
  order_id = ongoings.order_id;
  datess = subscription_orders.delivery_date;
  repeat_orders = store_details.repeat_orders;

  total_delivery = sub_order.length;
  is_subscription = 1;
  created_at = today_date;

  // $pause_total = DB::table('subscription_order')
  // ->where('subscription_order.cart_id',$cart_id)
  // ->where('store_order_id',$store_order_id)
  // ->where(function ($query) {
  // $query->where('subscription_order.order_status','=','Pause')
  // ->orWhere('subscription_order.order_status','=','pause');
  // })            
  // ->get();

  const pause_total = await knex('subscription_order')
    .where('subscription_order.cart_id', cart_id)
    .andWhere('store_order_id', store_order_id)
    .andWhere(function () {
      this.where('subscription_order.order_status', '=', 'Pause')
        .orWhere('subscription_order.order_status', '=', 'pause');
    })
    .select();


  other_total = await knex('subscription_order')
    .where('subscription_order.cart_id', cart_id)
    .where('store_order_id', store_order_id);

  orderssss = await knex('orders')
    .where('orders.cart_id', cart_id)
    .first();

  repeat_orderss = repeat_orders.split(",");
  repeat_tatal = repeat_orderss.length;
  // return repeat_tatal
  total_records = orderssss.total_delivery * repeat_tatal + pause_total.length;
  delete_recordss = other_total.length - total_records;
  // return total_records
  if (delete_recordss > 0) {
    const recordsToDelete = await knex('subscription_order')
      .where('cart_id', cart_id)
      .andWhere('store_order_id', store_order_id)
      .orderBy('id', 'DESC')
      .limit(delete_recordss)
      .select('id');

    // Extract the ids of the records to delete
    const idsToDelete = recordsToDelete.map(record => record.id);
    //return idsToDelete
    //  return 888;
    // Delete the records with the selected ids
    //snehalon
    // await knex('subscription_order')
    //   .whereIn('id', idsToDelete)
    //   .delete();

  }


  if (is_subscription == 1) {


    let k = 0;
    for (let i = 1; i < 1000; i++) {
      const addOneDay = (dateString) => {
        const date = new Date(dateString);
        date.setDate(date.getDate() + i);
        const year = date.getFullYear();
        const month = String(date.getMonth() + i).padStart(2, '0'); // Months are zero-based in JS
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
      };

      const givenDate = datess; // Example given date
      const delivery_dates = addOneDay(givenDate);

      repeat_orderss = repeat_orders.split(",");

      repeat_tatal = repeat_orderss.length;

      // return repeat_orderss.length

      for (let j = 0; j < repeat_orderss.length; j++) {


        //let delivery_dates = "2024-05-20"; // Example date
        let timestamp = new Date(delivery_dates).getTime();
        let day = moment(timestamp).format('ddd');
        let days_name = repeat_orderss[j].trim();


        // $timestamp = strtotime($delivery_dates);
        // $day=date('D', $timestamp);

        // $days_name=trim($repeat_orderss[$j]);

        //   if(days_name.toLowerCase() == day.toLowerCase()){




        const pause_total = await knex('subscription_order')
          .where('cart_id', cart_id)
          .andWhere('store_order_id', store_order_id)
          .andWhere(function () {
            this.where('order_status', 'Pause')
              .orWhere('order_status', 'pause');
          })
          .select();



        other_total = await knex('subscription_order')
          .where('subscription_order.cart_id', cart_id)
          .where('subscription_order.store_order_id', store_order_id);

        orderssss = await knex('orders')
          .where('orders.cart_id', cart_id)
          .first();

        total_delivery = orderssss.total_delivery * repeat_tatal;

        //   return total_delivery
        other_delivery = other_total.length - pause_total.length;

        if (total_delivery > other_delivery) {

          //Pause order subscripion

          numbers = subscription_id.split(",");
          numbers.sort();
          arrlength = numbers.length;
          subscription_ids = '';
          for (let xm = 0; xm < arrlength; xm++) {

            sub_order_id = numbers[xm].trim();
            get_total = await knex('subscription_order')
              .where('store_order_id', store_order_id)
              .andWhere(function () {
                this.where('order_status', 'Pending')
                  .orWhere('order_status', 'Completed')
                  .orWhere('order_status', 'Out_For_Delivery');;
              })
              .select();


            if (get_total.length == 0) {
              gets_total = await knex('subscription_order')
                .where('id', sub_order_id)
                .first();
              subscription_ids = gets_total.subscription_id;
            }

          }





          // subscription_order =  await knex('subscription_order')
          // .insert({
          //     cart_id:subscription_orders.cart_id,
          //     user_id:subscription_orders.user_id,
          //     order_id:subscription_orders.order_id,
          //     store_id:subscription_orders.store_id,
          //     delivery_date:delivery_dates,
          //     time_slot:subscription_orders.time_slot,
          //     created_date: created_at,
          //     order_status:'Pending',
          //     store_order_id:subscription_orders.store_order_id,
          //     subscription_id:subscription_ids,
          //     group_id:group_id
          // });


        }
        k++;

        //}
      }

      if (k == total_delivery) {
        break;
      }
    }
  }


  // PostgreSQL: no SELECT * with GROUP BY; use orderBy + first() for one row per max subscription_id
  data_last = await knex('subscription_order')
    .select('id', 'group_id', 'subscription_id', 'delivery_date')
    .where('subscription_order.cart_id', cart_id)
    .where('subscription_order.store_order_id', store_order_id)
    .orderBy('subscription_id', 'desc')
    .first();

  last_id = data_last.id;

  const data = await knex('subscription_order')
    .select('id', 'subscription_id', 'delivery_date', 'group_id')
    .where('subscription_order.cart_id', cart_id)
    .where('subscription_order.store_order_id', store_order_id)
    .where('subscription_order.subscription_id', subscription_id)
    .orderBy('subscription_id', 'desc')
    .first();
  const pausedate = data.delivery_date;
  const date2 = new Date(pausedate);
  // Get the year, month, and day in the required format
  const year2 = date2.getFullYear();
  const month2 = String(date2.getMonth() + 1).padStart(2, '0'); // Months are zero-based, so add 1
  const day2 = String(date2.getDate()).padStart(2, '0');
  // Combine into the desired format
  const pauseformattedDate = `${year2}-${month2}-${day2}`;
  if (data_last.group_id) {
    groupID = data_last.group_id;
  } else {
    groupID = data_last.cart_id;
  }



  data_second = await knex('subscription_order')
    .select('*')
    .where('id', '>', last_id)
    .where('subscription_order.cart_id', cart_id)
    .where('store_order_id', store_order_id)
    .andWhere(function () {
      this.where('order_status', 'Pending')
        .orWhere('order_status', 'Completed')
        .orWhere('order_status', 'Out_For_Delivery');;
    })
    .select();

  //return subscription_id
  numbers = subscription_id.split(",");
  //return numbers
  for (let i = 0; i < data_second.length; i++) {
    // let a = 0
    const datasecond = data_second[i];
    numbers = numbers.sort();
    numbers = Array.from(numbers);
    arrlength = numbers.length;
    // sub_order_id=numbers[0].trim();
    sub_order_id = numbers[0]
    // return datasecond
    if (arrlength != 0) {
      pause_order = await knex('subscription_order')
        .where('id', datasecond.id)
        .update({ subscription_id: sub_order_id });
    }
    numbers.splice(0, 1)
  }

  data_last_delete = await knex('subscription_order')
    .select('*')
    .where('subscription_order.cart_id', cart_id)
    .where('subscription_order.store_order_id', store_order_id)
    .orderBy('id', 'desc')
  // return data_last_delete
  for (let l = 0; l < data_last_delete.length; l++) {

    const datalastdelete = data_last_delete[l];

    if (datalastdelete.order_status == 'Pause') {
      // return datalastdelete
      //snehaloff
      // delete_records =await knex('subscription_order')
      // .where('id',datalastdelete.id)
      // .delete();
    }

  }

  let resume_data_deliveryss1s = [];
  for (let jkj = 0; jkj < pause_data_deliveryss1.length; jkj++) {
    let subscription_ordersssss = await knex('subscription_order')
      .select('delivery_date')
      .where('subscription_id', pause_data_deliveryss1[jkj])
      .where('order_status', 'Pending')
      .first();

    if (subscription_ordersssss && subscription_ordersssss.delivery_date) {
      let formattedDate = new Date(subscription_ordersssss.delivery_date).toLocaleDateString('en-GB', {
        day: '2-digit',
        month: 'long'
      });
      resume_data_deliveryss1s.push(formattedDate);
    }
  }

  pause_data_deliveryss = pause_data_deliveryss12;
  let resume_data_deliveryss = (resume_data_deliveryss1s || []).join(' & ');
  resume_data_deliveryss_new_sms = resume_data_deliveryss12;

  pause_by = "user";
  if (pause_data_deliveryss && resume_data_deliveryss) {
    orders_detailss = await knex('orders')
      .select('time_slot', 'user_id')
      .where('cart_id', cart_id)
      .first();
    time_slot = orders_detailss.time_slot;
    user_id = orders_detailss.user_id;
    //Customer mail & sms
    // $this->subscription_order_pause_mail($cart_id, $store_order_id, $user_id, $pause_data_deliveryss, $resume_data_deliveryss,$time_slot,$pause_by);
    // $successmsg = $this->subsscription_order_pause_sms($cart_id,$store_order_id,$user_id,$pause_data_deliveryss,$resume_data_deliveryss,$time_slot,$pause_by);
    //Store Mail & sms
    // $this->subscription_order_pause_mailstore($cart_id, $store_order_id, $user_id, $pause_data_deliveryss, $resume_data_deliveryss,$time_slot,$pause_by);
    //$this->subsscription_order_pause_smsstore($cart_id,$store_order_id,$user_id,$pause_data_deliveryss,$resume_data_deliveryss,$time_slot,$pause_by);
  }

  orders_detailss = await knex('orders')
    .select('time_slot', 'user_id')
    .where('cart_id', cart_id)
    .first();
  time_slot = orders_detailss.time_slot;
  user_id = orders_detailss.user_id;

  if (resume_data_deliveryss_new_sms) {

    //Customer mail & sms
    //  $successmsg = $this->subsscription_order_resume_sms($cart_id,$store_order_id,$user_id,$pause_data_deliveryss,$resume_data_deliveryss_new_sms,$time_slot,$pause_by);
    //  $this->subscription_order_resume_mail($cart_id, $store_order_id, $user_id, $pause_data_deliveryss, $resume_data_deliveryss_new_sms,$time_slot,$pause_by);

    //Store Mail & sms
    // $this->subsscription_order_resume_smsstore($cart_id,$store_order_id,$user_id,$pause_data_deliveryss,$resume_data_deliveryss_new_sms,$time_slot,$pause_by);
    // $this->subscription_order_resume_mailstore($cart_id, $store_order_id, $user_id, $pause_data_deliveryss, $resume_data_deliveryss_new_sms,$time_slot,$pause_by);
  }

  //Email Code
  const storeOrders = await knex('store_orders')
    .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
    .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('orders.cart_id', cart_id);

  const userIdInt = user_id != null ? parseInt(user_id, 10) : null;
  const user = userIdInt != null && !Number.isNaN(userIdInt)
    ? await knex('users').select('name', 'email').where('id', userIdInt).first()
    : null;

  let userName = (user && user.name) || '';
  let userEmail = (user && user.email) || 'store1@quickart.ae';
  
  const logo = await knex('tbl_web_setting').first();
  const appName = logo ? logo.name : null;
  // Fetching the first record from the 'currency' table
  const currency = await knex('currency').first();
  const currencySign = currency ? currency.currency_sign : null;
  const templateData = {
    baseurl: process.env.BASE_URL,
    user_name: userName,
    user_email: userEmail,
    store_orderss: storeOrders,
    final_amount: "",
    app_name: appName,
    currency_sign: currencySign,
    cart_id: groupID,
    pause_data_deliveryss: pauseformattedDate
  };
  const subject = 'Order Paused'
  // Trigger the email after order is placed
  //sendPausedEmail = await pauseorderMail(userEmail, templateData, subject);
};

const getCancelprdOrder737 = async (appDetatils) => {
  const minCardRefundThreshold = 0.09;
  let userId = appDetatils.user_id
  cart_id = appDetatils.cart_id
  store_order_id = appDetatils.store_order_id;
  cancel_reason = appDetatils.cancel_reason;

  if (cart_id && userId && store_order_id) {

    subcription_data = await knex('subscription_order')
      .where('store_order_id', store_order_id)
      .where('order_status', 'Pending')
      // .where('si_payment_flag', 'no')
      // .where('processing_product','!=','1')
      .update({ 'cancel_reason': cancel_reason, 'order_status': "Cancelled" });

    order_data = await knex('orders')
      .where('cart_id', cart_id)
      .where('order_status', 'Pending')
      .update({ 'order_status': "Cancelled" });
  }

  const pendingDeliveries = await knex("subscription_order")
    .where("store_order_id", store_order_id)
    .where("order_status", "Pending")
    .count({ count: "id" })
    .first();

  const cancelDeliveries = await knex("subscription_order")
    .where("store_order_id", store_order_id)
    .where("order_status", "Cancelled")
    .count({ count: "id" })
    .first();

  const totalDeliveries = await knex("subscription_order")
    .where("store_order_id", store_order_id)
    .count({ count: "id" })
    .first();

  const completedDeliveries = await knex("subscription_order")
    .where("store_order_id", store_order_id)
    .where("order_status", "Completed")
    .count({ count: "id" })
    .first();

  const storeDetails = await knex('store_orders')
    .where('store_order_id', store_order_id)
    .first();

  const OrdersDetails = await knex('orders')
    .where('cart_id', cart_id)
    .first();

  const result = await knex('subscription_order')
    .where('store_order_id', store_order_id)
    .where('si_payment_flag', 'yes')
    .where('order_status', 'Cancelled')
    .count({ cancelled_count: 'id' });
  const cancelledCount = result[0].cancelled_count;

  const repeatOrders = storeDetails.repeat_orders;
  const subTotalDelivery = storeDetails.sub_total_delivery;
  const repeatOrderCount = repeatOrders.trim().split(',');
  const totalDeliveryWeek = repeatOrderCount.length * subTotalDelivery;
  const totalPrice = storeDetails.price / totalDeliveryWeek;
  const reserveAmount = 0;
  const TotalPaidAmount = (cancelledCount) ? (cancelledCount * totalPrice) : 0;
  const TotalPriceOrdersAmt = (parseFloat(reserveAmount) + parseFloat(TotalPaidAmount));
  const totalWalletPaid = (OrdersDetails.paid_by_wallet) ? Number(OrdersDetails.paid_by_wallet) : 0;
  const totalRefWalletPaid = (OrdersDetails.paid_by_ref_wallet) ? Number(OrdersDetails.paid_by_ref_wallet) : 0;
  const cardPaidBySplit = (
    OrdersDetails &&
    OrdersDetails.payment_method != 'COD' &&
    OrdersDetails.payment_status == 'success'
  )
    ? Math.max(
      Number(OrdersDetails.total_price || 0) -
      Number(OrdersDetails.paid_by_wallet || 0) -
      Number(OrdersDetails.paid_by_ref_wallet || 0),
      0
    )
    : 0;
  const totalCardPaid = Math.max(
    Number(OrdersDetails?.rem_price || 0),
    cardPaidBySplit
  );

  const user = await knex("users")
    .select("user_phone", "wallet", "wallet_balance", "referral_balance", "name", "email")
    .where("id", userId)
    .first();

  const totalDeliveryCount = Number(totalDeliveries.count || 0);
  const cancelCount = Number(cancelDeliveries.count || 0);

  let walletAmt = 0;
  let refWalletAmt = 0;

  // Deduction logic based on user's requirements
  const divisor = totalDeliveryCount - cancelCount;
  const walletPerDelivery = totalWalletPaid / (divisor > 0 ? divisor : 1);
  const refWalletPerDelivery = totalRefWalletPaid / (divisor > 0 ? divisor : 1);
  const cardPerDelivery = totalCardPaid / (divisor > 0 ? divisor : 1);
  walletAmt = walletPerDelivery;
  refWalletAmt = refWalletPerDelivery;
  let cardAmt = cardPerDelivery;

  const walletDeductionRow = await knex("wallet_history")
    .where("user_id", userId)
    .where("resource", "order_placed_wallet")
    .where("type", "deduction")
    .andWhere(function () {
      this.where("cart_id", cart_id)
        .orWhere(function () {
          this.where("group_id", OrdersDetails.group_id || "")
            .andWhere("cart_id", "");
        });
    })
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_deduction"))
    .first();

  const walletRefundRow = await knex("wallet_history")
    .where("user_id", userId)
    .where("resource", "order_refund_cancelled")
    .where("type", "Add")
    .andWhere(function () {
      this.where("cart_id", cart_id)
        .orWhere(function () {
          this.where("group_id", OrdersDetails.group_id || "")
            .andWhere("cart_id", "");
        });
    })
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_refund"))
    .first();

  const refDeductionRow = await knex("wallet_history")
    .where("user_id", userId)
    .where("resource", "order_placed_wallet_ref")
    .where("type", "deduction")
    .andWhere(function () {
      this.where("cart_id", cart_id)
        .orWhere(function () {
          this.where("group_id", OrdersDetails.group_id || "")
            .andWhere("cart_id", "");
        });
    })
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_deduction"))
    .first();

  const refRefundRow = await knex("wallet_history")
    .where("user_id", userId)
    .where("resource", "order_refund_cancelled_ref")
    .whereIn("type", ["Add", "wallet_expired"])
    .andWhere(function () {
      this.where("cart_id", cart_id)
        .orWhere(function () {
          this.where("group_id", OrdersDetails.group_id || "")
            .andWhere("cart_id", "");
        });
    })
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_refund"))
    .first();

  const cardDeductionTotal = Math.max(0, Number(totalCardPaid || 0));
  const cardRefundRow = await knex("wallet_history")
    .where("user_id", userId)
    .where("resource", "order_refund_cancelled_card_to_wallet")
    .where("type", "Add")
    .andWhere(function () {
      this.where("cart_id", cart_id)
        .orWhere(function () {
          this.where("group_id", OrdersDetails.group_id || "")
            .andWhere("cart_id", "");
        });
    })
    .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_refund"))
    .first();

  const walletRemainingRefundable = Math.max(
    0,
    Number(walletDeductionRow?.total_deduction || 0) - Number(walletRefundRow?.total_refund || 0)
  );
  const refRemainingRefundable = Math.max(
    0,
    Number(refDeductionRow?.total_deduction || 0) - Number(refRefundRow?.total_refund || 0)
  );
  const cardRemainingRefundable = Math.max(
    0,
    cardDeductionTotal - Number(cardRefundRow?.total_refund || 0)
  );

  walletAmt = Math.min(Number(walletAmt || 0), walletRemainingRefundable);
  refWalletAmt = Math.min(Number(refWalletAmt || 0), refRemainingRefundable);
  cardAmt = Math.min(Number(cardAmt || 0), cardRemainingRefundable);
  if (cardAmt <= minCardRefundThreshold) {
    cardAmt = 0;
  }

  const remainingWallet = totalWalletPaid - walletAmt;
  const remainingRefWallet = totalRefWalletPaid - refWalletAmt;
  const remainingCard = totalCardPaid - cardAmt;

  let actualWallet = Number(user.wallet_balance || 0) + walletAmt;
  let actualRefWallet = Number(user.referral_balance || 0) + refWalletAmt;


  if (walletAmt > 0) {
    await knex("users").where("id", userId).update({
      wallet_balance: actualWallet.toFixed(2),
    });
    const nextWId = await getNextWalletHistoryWId();
    await knex("wallet_history").insert({
      w_id: nextWId,
      user_id: userId,
      amount: walletAmt.toFixed(2),
      resource: "order_refund_cancelled",
      type: "Add",
      group_id: "",
      cart_id: cart_id,
    });
  }

  if (Number(refWalletAmt) > 0) {
    const lastTxn = await knex("wallet_history")
      .where("user_id", userId)
      .where("group_id", OrdersDetails.group_id)
      .where("cart_id", cart_id)
      .where("type", "deduction")
      .where("resource", "order_placed_wallet_ref")
      .orderBy("w_id", "desc")
      .first();

    let walletType = "Add";
    const dubaiTime = moment.tz("Asia/Dubai");
    const todayDubai = dubaiTime.format("YYYY-MM-DD");

    if (
      lastTxn &&
      lastTxn.expiry_date &&
      moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
    ) {
      walletType = "wallet_expired";
    }

    // Update user wallet only if wallet is valid
    if (walletType === "Add") {
      await knex("users")
        .where("id", userId)
        .update({
          referral_balance: actualRefWallet.toFixed(2)
        });
    }

    const nextWId = await getNextWalletHistoryWId();
    await knex("wallet_history").insert({
      w_id: nextWId,
      user_id: userId,
      amount: refWalletAmt.toFixed(2),
      resource: "order_refund_cancelled_ref",
      type: walletType,
      group_id: OrdersDetails.group_id,
      cart_id: cart_id,
      expiry_date: lastTxn?.expiry_date || null
    });
  }

  if (Number(cardAmt) > 0) {
    const latestUserWallet = await knex("users")
      .select("wallet_balance")
      .where("id", userId)
      .first();

    await knex("users")
      .where("id", userId)
      .update({
        wallet_balance: (Number(latestUserWallet?.wallet_balance || 0) + Number(cardAmt)).toFixed(2),
      });

    const nextWId = await getNextWalletHistoryWId();
    await knex("wallet_history").insert({
      w_id: nextWId,
      user_id: userId,
      amount: cardAmt.toFixed(2),
      resource: "order_refund_cancelled_card_to_wallet",
      type: "Add",
      group_id: OrdersDetails.group_id || "",
      cart_id: cart_id,
    });
  }


  await knex('orders')
    .where('cart_id', cart_id)
    .where('order_status', 'Cancelled')
    .update({
      'del_partner_tip': 0,
      'cod_charges': 0,
      'reserve_amount': 0,
      'paid_by_wallet': parseFloat(remainingWallet.toFixed(2)),
      'paid_by_ref_wallet': parseFloat(remainingRefWallet.toFixed(2)),
      'rem_price': parseFloat(Math.max(0, remainingCard).toFixed(2))
    });

  //Email Code
  storeOrders = await knex('store_orders')
    .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
    .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('store_orders.order_cart_id', cart_id)

  let userName = user.name;
  let userEmail = user.email;

  const logo = await knex('tbl_web_setting').first();
  const appName = logo ? logo.name : null;
  // Fetching the first record from the 'currency' table
  const currency = await knex('currency').first();
  const currencySign = currency ? currency.currency_sign : null;

  const orderlist = await knex('orders')
    .where('cart_id', cart_id)
    .select('group_id')
    .first();

  const group_id = orderlist.group_id;

  totalCal = await knex('orders')
    .where('group_id', group_id)
    .where('cart_id', cart_id)
    .sum('total_products_mrp as total_products_mrp');
  finalAmount = parseFloat(totalCal[0].total_products_mrp).toFixed(2);

  totalCartAmount = await knex('orders')
    .where('group_id', group_id)
    .sum('total_products_mrp as total_products_mrp');
  total_cart_amount = parseFloat(totalCartAmount[0].total_products_mrp).toFixed(2);

  orderFinalAmount = total_cart_amount - finalAmount;


  const result1 = await knex('orders')
    .where('group_id', group_id)
    .where('order_status', 'Cancelled')
    .count({ cancelled_count: 'order_id' });

  const cancelledCount1 = result1[0].cancelled_count;

  const resultAll1 = await knex('orders')
    .where('group_id', group_id)
    .count({ all_count: 'order_id' });

  const allCount1 = resultAll1[0].all_count;

  const is_addedld = await knex('tbl_luckydraw').where('order_id', group_id).where('is_delete', 0).first();

  // && cancelledCount == allCount
  if (is_addedld && orderFinalAmount < 200 || (is_addedld && orderFinalAmount > 200 && cancelledCount1 == allCount1)) {
    if (finalAmount >= 200) {
      const updateentry = await knex('tbl_luckydraw')
        .where('order_id', group_id)
        .update({ 'is_delete': 1 });
    }

    const usertotalorders = await knex('tbl_luckydraw')
      .distinct('orders.group_id')
      .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
      .where('tbl_luckydraw.user_id', userId)
      .where('tbl_luckydraw.is_delete', 0)
      .where('orders.order_status', '!=', 'Cancelled');

    const getUserup = await knex('users').where('id', userId).first();

    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
    // Convert OTP code to a time-based string
    const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

    const payload = {
      "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      "campaignName": "CancelOrderIphone",
      "destination": "+" + phone_with_country_code,
      "userName": "Quickart General Trading Co LLC",
      "templateParams": [
        getUserup.name, group_id, finalAmount, `${usertotalorders.length}`
      ],
      "source": "new-landing-page form",
      "media": {},
      "buttons": [],
      "carouselCards": [],
      "location": {},
      "attributes": {},
      "paramsFallbackValue": {
        "FirstName": "user"
      }
    };

    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    const updateentry = await knex('tbl_luckydraw')
      .where('order_id', group_id)
      .update({ 'is_delete': 1 });
  }

  const templateData = {
    baseurl: process.env.BASE_URL,
    user_name: userName,
    user_email: userEmail,
    store_orderss: storeOrders,
    final_amount: "",
    app_name: appName,
    currency_sign: currencySign,
    cart_id: orderlist.group_id
  };
  const subject = 'Delivery Order Cancelled'
  // Trigger the email after order is placed
  //sendCancelledEmail = await cancelorderMail(userEmail, templateData, subject);

};

const getCancelprdOrder = async (appDetatils) => {
  const minCardRefundThreshold = 0.09;
  let userId = appDetatils.user_id
  cart_id = appDetatils.cart_id
  store_order_id = appDetatils.store_order_id;
  cancel_reason = appDetatils.cancel_reason;

  // Check if there are any orders that are not already cancelled
  const existingOrders = await knex("orders")
    .where("cart_id", cart_id)
    .where("order_status", "Cancelled")
    .first();
  // If there are no orders to cancel, return a message
  if (existingOrders) {
    throw new Error("Order are already cancelled.");
  }
  ////// new end////////////

  const parseCancelSubscriptionIds = (raw) => {
    if (raw === undefined || raw === null || raw === '') return [];
    const parts = String(raw).split(',').map((s) => s.trim()).filter(Boolean);
    const nums = parts.map((id) => parseInt(id, 10)).filter((n) => !Number.isNaN(n));
    return [...new Set(nums)];
  };

  const normalizeUpdateRowCount = (result) => {
    if (typeof result === 'number' && Number.isFinite(result)) return result;
    if (result && typeof result === 'object' && 'rowCount' in result && Number.isFinite(Number(result.rowCount))) {
      return Number(result.rowCount);
    }
    const n = parseInt(String(result), 10);
    return Number.isFinite(n) ? n : 0;
  };

  // Only refund when this call actually moves subscription rows to Cancelled (Pending → Cancelled).
  let cancelledSubscriptionRowsThisRequest = 0;
  if (cart_id && userId && store_order_id) {

    const cancelSubIds = parseCancelSubscriptionIds(appDetatils.subscription_id);
    let subUpdateBuilder = knex('subscription_order')
      .where('store_order_id', store_order_id)
      .where('order_status', 'Pending');
    if (cancelSubIds.length > 0) {
      subUpdateBuilder = subUpdateBuilder.whereIn('id', cancelSubIds);
    }

    const subUpdateResult = await subUpdateBuilder.update({
      cancel_reason: cancel_reason,
      order_status: 'Cancelled',
    });
    cancelledSubscriptionRowsThisRequest = normalizeUpdateRowCount(subUpdateResult);
    subcription_data = subUpdateResult;

    const pendingOrPauseLeft = await knex('subscription_order')
      .where('store_order_id', store_order_id)
      .whereIn('order_status', ['Pending', 'Pause'])
      .count({ count: 'id' })
      .first();
    const activeSubsLeft = Number(pendingOrPauseLeft?.count || 0);

    if (activeSubsLeft === 0) {
      order_data = await knex('orders')
        .where('cart_id', cart_id)
        .where('order_status', 'Pending')
        .update({ order_status: 'Cancelled' });
    }
  }

  const shouldApplySubscriptionRefund = cancelledSubscriptionRowsThisRequest > 0;

  const totalDeliveries = await knex("subscription_order")
    .where("store_order_id", store_order_id)
    .count({ count: "id" })
    .first();

  const storeDetails = await knex('store_orders')
    .where('store_order_id', store_order_id)
    .first();

  const OrdersDetails = await knex('orders')
    .where('cart_id', cart_id)
    .first();

  const result = await knex('subscription_order')
    .where('store_order_id', store_order_id)
    .where('si_payment_flag', 'yes')
    .where('order_status', 'Cancelled')
    .count({ cancelled_count: 'id' });
  const cancelledCount = result[0].cancelled_count;

  const repeatOrders = storeDetails.repeat_orders;
  const subTotalDelivery = storeDetails.sub_total_delivery;
  const repeatOrderCount = repeatOrders.trim().split(',');
  const totalDeliveryWeek = repeatOrderCount.length * subTotalDelivery;
  const totalPrice = storeDetails.price / totalDeliveryWeek;
  const reserveAmount = 0;
  const TotalPaidAmount = (cancelledCount) ? (cancelledCount * totalPrice) : 0;
  const TotalPriceOrdersAmt = (parseFloat(reserveAmount) + parseFloat(TotalPaidAmount));
  const totalWalletPaid = (OrdersDetails.paid_by_wallet) ? Number(OrdersDetails.paid_by_wallet) : 0;
  const totalRefWalletPaid = (OrdersDetails.paid_by_ref_wallet) ? Number(OrdersDetails.paid_by_ref_wallet) : 0;
  const cardPaidBySplit = (
    OrdersDetails &&
    OrdersDetails.payment_method != 'COD' &&
    OrdersDetails.payment_status == 'success'
  )
    ? Math.max(
      Number(OrdersDetails.total_price || 0) -
      Number(OrdersDetails.paid_by_wallet || 0) -
      Number(OrdersDetails.paid_by_ref_wallet || 0),
      0
    )
    : 0;
  const totalCardPaid = Math.max(
    Number(OrdersDetails?.rem_price || 0),
    cardPaidBySplit
  );

  const isPayPerDelivery =
    OrdersDetails?.payment_type &&
    String(OrdersDetails.payment_type).toLowerCase() === "payperdelivery";

  /** Scope wallet_history rows to this subscription line: trim cart_id, and treat NULL cart as '' for legacy rows. */
  const applyWalletHistoryOrderScope = (qb) => {
    const cartIdTrim = String(cart_id ?? "").trim();
    const gidRaw = OrdersDetails?.group_id;
    const gidTrim =
      gidRaw !== undefined && gidRaw !== null && String(gidRaw).trim() !== ""
        ? String(gidRaw).trim()
        : "";
    qb.where(function () {
      this.whereRaw("trim(coalesce(cart_id::text, '')) = ?", [cartIdTrim]);
      if (gidTrim) {
        this.orWhere(function () {
          this.whereRaw("trim(coalesce(group_id::text, '')) = ?", [gidTrim]).where(function () {
            this.whereNull("cart_id").orWhereRaw("trim(coalesce(cart_id::text, '')) = ?", [""]);
          });
        });
      }
    });
  };

  const user = await knex("users")
    .select("user_phone", "wallet", "wallet_balance", "referral_balance", "name", "email")
    .where("id", userId)
    .first();

  // Refund wallet / card only when subscription_order rows were moved to Cancelled (Pending → Cancelled) in this request.
  if (shouldApplySubscriptionRefund) {
    const totalDeliveryCount = Number(totalDeliveries.count || 0);
    // Split payment evenly across ALL subscription rows for this line item (Pending, Pause, Completed, Cancelled all count).
    // Refund only for rows cancelled in this request. Using (total - cancelCount) wrongly attributes 100% of payment
    // to the last non-cancelled row (e.g. one Paused + one Cancelled → divisor 1 → full refund).
    const deliveriesForSplit = totalDeliveryCount > 0 ? totalDeliveryCount : 1;
    const nCancelledThisCall = Math.min(
      Math.max(0, Number(cancelledSubscriptionRowsThisRequest) || 0),
      deliveriesForSplit
    );

    const walletDeductionResources = isPayPerDelivery
      ? ["order_placed_wallet", "order_wallet_deduction"]
      : ["order_placed_wallet"];
    const refDeductionResources = isPayPerDelivery
      ? ["order_placed_wallet_ref", "order_referral_deduction"]
      : ["order_placed_wallet_ref"];

    let walletAmt = 0;
    let refWalletAmt = 0;

    const walletPerDelivery = totalWalletPaid / deliveriesForSplit;
    const refWalletPerDelivery = totalRefWalletPaid / deliveriesForSplit;
    const cardPerDelivery = totalCardPaid / deliveriesForSplit;
    walletAmt = walletPerDelivery * nCancelledThisCall;
    refWalletAmt = refWalletPerDelivery * nCancelledThisCall;
    let cardAmt = cardPerDelivery * nCancelledThisCall;

    const walletDeductionRow = await knex("wallet_history")
      .where("user_id", userId)
      .whereIn("resource", walletDeductionResources)
      .where("type", "deduction")
      .where(function () {
        applyWalletHistoryOrderScope(this);
      })
      .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_deduction"))
      .first();

    const walletRefundRow = await knex("wallet_history")
      .where("user_id", userId)
      .where("resource", "order_refund_cancelled")
      .where("type", "Add")
      .where(function () {
        applyWalletHistoryOrderScope(this);
      })
      .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_refund"))
      .first();

    const refDeductionRow = await knex("wallet_history")
      .where("user_id", userId)
      .whereIn("resource", refDeductionResources)
      .where("type", "deduction")
      .where(function () {
        applyWalletHistoryOrderScope(this);
      })
      .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_deduction"))
      .first();

    const refRefundRow = await knex("wallet_history")
      .where("user_id", userId)
      .where("resource", "order_refund_cancelled_ref")
      .whereIn("type", ["Add", "wallet_expired"])
      .where(function () {
        applyWalletHistoryOrderScope(this);
      })
      .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_refund"))
      .first();

    const cardDeductionTotal = Math.max(0, Number(totalCardPaid || 0));
    const cardRefundRow = await knex("wallet_history")
      .where("user_id", userId)
      .where("resource", "order_refund_cancelled_card_to_wallet")
      .where("type", "Add")
      .where(function () {
        applyWalletHistoryOrderScope(this);
      })
      .select(knex.raw("COALESCE(SUM(NULLIF(trim(amount), '')::numeric), 0) as total_refund"))
      .first();

    const walletDeductionMatched = Number(walletDeductionRow?.total_deduction || 0);
    const walletRefundSoFar = Number(walletRefundRow?.total_refund || 0);
    const walletRemainingRefundable = Math.max(0, walletDeductionMatched - walletRefundSoFar);

    const refDeductionMatched = Number(refDeductionRow?.total_deduction || 0);
    const refRefundSoFar = Number(refRefundRow?.total_refund || 0);
    const refRemainingRefundable = Math.max(0, refDeductionMatched - refRefundSoFar);
    const cardRemainingRefundable = Math.max(
      0,
      cardDeductionTotal - Number(cardRefundRow?.total_refund || 0)
    );

    walletAmt = Math.min(Number(walletAmt || 0), walletRemainingRefundable);
    refWalletAmt = Math.min(Number(refWalletAmt || 0), refRemainingRefundable);
    cardAmt = Math.min(Number(cardAmt || 0), cardRemainingRefundable);
    if (cardAmt <= minCardRefundThreshold) {
      cardAmt = 0;
    }

    let actualWallet = Number(user.wallet_balance || 0) + walletAmt;
    let actualRefWallet = Number(user.referral_balance || 0) + refWalletAmt;


    if (walletAmt > 0) {
      await knex("users").where("id", userId).update({
        wallet_balance: actualWallet.toFixed(2),
      });
      const nextWId = await getNextWalletHistoryWId();
      await knex("wallet_history").insert({
        w_id: nextWId,
        user_id: userId,
        amount: walletAmt.toFixed(2),
        resource: "order_refund_cancelled",
        type: "Add",
        group_id: "",
        cart_id: cart_id,
      });
    }

    if (Number(refWalletAmt) > 0) {
      const lastTxn = await knex("wallet_history")
        .where("user_id", userId)
        .where("type", "deduction")
        .whereIn("resource", refDeductionResources)
        .where(function () {
          applyWalletHistoryOrderScope(this);
        })
        .orderBy("w_id", "desc")
        .first();

      if (!lastTxn) {
        refWalletAmt = 0;
        actualRefWallet = Number(user.referral_balance || 0);
      } else {
        let walletType = "Add";
        const dubaiTime = moment.tz("Asia/Dubai");
        const todayDubai = dubaiTime.format("YYYY-MM-DD");

        if (
          lastTxn.expiry_date &&
          moment(lastTxn.expiry_date).tz("Asia/Dubai").format("YYYY-MM-DD") < todayDubai
        ) {
          walletType = "wallet_expired";
        }

        if (walletType === "Add") {
          await knex("users")
            .where("id", userId)
            .update({
              referral_balance: actualRefWallet.toFixed(2)
            });
        }

        const nextWId = await getNextWalletHistoryWId();
        await knex("wallet_history").insert({
          w_id: nextWId,
          user_id: userId,
          amount: refWalletAmt.toFixed(2),
          resource: "order_refund_cancelled_ref",
          type: walletType,
          group_id: OrdersDetails.group_id,
          cart_id: cart_id,
          expiry_date: lastTxn.expiry_date || null
        });
      }
    }

    if (Number(cardAmt) > 0) {
      const latestUserWallet = await knex("users")
        .select("wallet_balance")
        .where("id", userId)
        .first();

      await knex("users")
        .where("id", userId)
        .update({
          wallet_balance: (Number(latestUserWallet?.wallet_balance || 0) + Number(cardAmt)).toFixed(2),
        });

      const nextWId = await getNextWalletHistoryWId();
      await knex("wallet_history").insert({
        w_id: nextWId,
        user_id: userId,
        amount: cardAmt.toFixed(2),
        resource: "order_refund_cancelled_card_to_wallet",
        type: "Add",
        group_id: OrdersDetails.group_id || "",
        cart_id: cart_id,
      });
    }

    const remainingWallet = totalWalletPaid - walletAmt;
    const remainingRefWallet = totalRefWalletPaid - refWalletAmt;
    const remainingCard = totalCardPaid - cardAmt;

    await knex('orders')
      .where('cart_id', cart_id)
      .where('order_status', 'Cancelled')
      .update({
        'del_partner_tip': 0,
        'cod_charges': 0,
        'reserve_amount': 0,
        'paid_by_wallet': parseFloat(remainingWallet.toFixed(2)),
        'paid_by_ref_wallet': parseFloat(remainingRefWallet.toFixed(2)),
        'rem_price': parseFloat(Math.max(0, remainingCard).toFixed(2))
      });
  }

  //Email Code
  storeOrders = await knex('store_orders')
    .select('store_orders.*', 'orders.group_id', 'orders.time_slot')
    .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('store_orders.order_cart_id', cart_id)

  let userName = user.name;
  let userEmail = user.email;

  const logo = await knex('tbl_web_setting').first();
  const appName = logo ? logo.name : null;
  // Fetching the first record from the 'currency' table
  const currency = await knex('currency').first();
  const currencySign = currency ? currency.currency_sign : null;

  const orderlist = await knex('orders')
    .where('cart_id', cart_id)
    .select('group_id')
    .first();

  const group_id = orderlist.group_id;

  totalCal = await knex('orders')
    .where('group_id', group_id)
    .where('cart_id', cart_id)
    .sum('total_products_mrp as total_products_mrp');
  finalAmount = parseFloat(totalCal[0].total_products_mrp).toFixed(2);

  totalCartAmount = await knex('orders')
    .where('group_id', group_id)
    .sum('total_products_mrp as total_products_mrp');
  total_cart_amount = parseFloat(totalCartAmount[0].total_products_mrp).toFixed(2);

  orderFinalAmount = total_cart_amount - finalAmount;


  const result1 = await knex('orders')
    .where('group_id', group_id)
    .where('order_status', 'Cancelled')
    .count({ cancelled_count: 'order_id' });

  const cancelledCount1 = result1[0].cancelled_count;

  const resultAll1 = await knex('orders')
    .where('group_id', group_id)
    .count({ all_count: 'order_id' });

  const allCount1 = resultAll1[0].all_count;

  const is_addedld = await knex('tbl_luckydraw').where('order_id', group_id).where('is_delete', 0).first();

  // && cancelledCount == allCount
  if (is_addedld && orderFinalAmount < 200 || (is_addedld && orderFinalAmount > 200 && cancelledCount1 == allCount1)) {
    if (finalAmount >= 200) {
      const updateentry = await knex('tbl_luckydraw')
        .where('order_id', group_id)
        .update({ 'is_delete': 1 });
    }

    const usertotalorders = await knex('tbl_luckydraw')
      .distinct('orders.group_id')
      .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
      .where('tbl_luckydraw.user_id', userId)
      .where('tbl_luckydraw.is_delete', 0)
      .where('orders.order_status', '!=', 'Cancelled');

    const getUserup = await knex('users').where('id', userId).first();

    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
    // Convert OTP code to a time-based string
    const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

    const payload = {
      "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      "campaignName": "CancelOrderIphone",
      "destination": "+" + phone_with_country_code,
      "userName": "Quickart General Trading Co LLC",
      "templateParams": [
        getUserup.name, group_id, finalAmount, `${usertotalorders.length}`
      ],
      "source": "new-landing-page form",
      "media": {},
      "buttons": [],
      "carouselCards": [],
      "location": {},
      "attributes": {},
      "paramsFallbackValue": {
        "FirstName": "user"
      }
    };

    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const result = await response.json();

    const updateentry = await knex('tbl_luckydraw')
      .where('order_id', group_id)
      .update({ 'is_delete': 1 });
  }

  const templateData = {
    baseurl: process.env.BASE_URL,
    user_name: userName,
    user_email: userEmail,
    store_orderss: storeOrders,
    final_amount: "",
    app_name: appName,
    currency_sign: currencySign,
    cart_id: orderlist.group_id
  };
  const subject = 'Delivery Order Cancelled'
  // Trigger the email after order is placed
  //sendCancelledEmail = await cancelorderMail(userEmail, templateData, subject);

};

const getCancelOrder = async (appDetatils) => {
  user_id = appDetatils.user_id
  ongoing = await knex('orders')
    .join('store', 'orders.store_id', '=', 'store.id')
    .join('users', 'orders.user_id', '=', 'users.id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
    .where('orders.user_id', user_id)
    .whereNotNull('orders.order_status')
    .whereNotNull('orders.payment_method')
    .where('orders.order_status', 'Cancelled')
    .orderBy('orders.order_id', 'DESC')


  const customizedProductData = [];
  for (let i = 0; i < ongoing.length; i++) {
    const ProductList = ongoing[i];

    order = await knex('store_orders')
      .where('order_cart_id', ProductList.cart_id)


    const baseurl = process.env.BUNNY_NET_IMAGE;
    const customizedProduct = {
      user_name: ProductList.name,
      delivery_address: ProductList.house_no + "," + ProductList.society + "," + ProductList.city + "," + ProductList.landmark + "," + ProductList.state + "," + ProductList.pincode,
      store_name: ProductList.store_name,
      store_owner: ProductList.employee_name,
      store_phone: ProductList.phone_number,
      store_email: ProductList.email,
      store_address: ProductList.address,
      order_status: ProductList.order_status,
      delivery_date: ProductList.delivery_date,
      time_slot: ProductList.time_slot,
      payment_method: ProductList.payment_method,
      payment_status: ProductList.payment_status,
      paid_by_wallet: parseFloat(ProductList.paid_by_wallet || 0) + parseFloat(ProductList.paid_by_ref_wallet || 0),
      cart_id: ProductList.cart_id,
      price: ProductList.total_price,
      delivery_charge: ProductList.delivery_charge,
      rem_price: ProductList.rem_price,
      coupon_discount: ProductList.coupon_discount,
      dboy_name: ProductList.boy_name,
      dboy_phone: ProductList.boy_phone,
      sub_total: ProductList.price_without_delivery,
      avg_tax_per: ProductList.avg_tax_per,
      total_tax_price: ProductList.total_tax_price,
      data: order
      // Add or modify properties as needed
    };

    customizedProductData.push(customizedProduct);
  }
  return customizedProductData

};

const getRepeatedplaceorder = async (appDetatils) => {
  // PostgreSQL: explicit variable declarations (no implicit globals)
  const groupID = appDetatils.cart_id;
  const user_id = appDetatils.user_id;
  const order_type = appDetatils.order_type;
  const replace_status = parseInt(appDetatils.replace_status, 10);

  if (!user_id || (replace_status !== 0 && replace_status !== 1)) {
    throw new Error('Invalid parameters.');
  }

  const ordersAll = await knex('orders')
    .where('group_id', '=', groupID);

  for (let j = 0; j < ordersAll.length; j++) {
    const ordersList = ordersAll[j];
    const cart_id = ordersList.cart_id;
    const orders_detailsss = await knex('store_orders')
      .where('repeated_order_cart', '=', cart_id)
      .where('order_cart_id', '=', 'incart');

    if (orders_detailsss.length === 0) {
      const orders_details = await knex('store_orders')
        .select('store_orders.*', 'product.availability')
        .join('store_products', 'store_products.varient_id', 'store_orders.varient_id')
        .join('product_varient', 'product_varient.varient_id', 'store_products.varient_id')
        .join('product', 'product.product_id', 'product_varient.product_id')
        .where('order_cart_id', '=', cart_id);

      // replace_status 0 : replace , 1 : update
      // PostgreSQL: store_orders.subscription_flag is TEXT, use '1'
      if (replace_status === 0) {
        await knex('store_orders')
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('store_orders.subscription_flag', '1')
          .delete();
      }

      for (let i = 0; i < orders_details.length; i++) {
        const ordersdetails = orders_details[i];
        const rec = await knex('store_orders')
          .where('store_approval', user_id)
          .where('varient_id', ordersdetails.varient_id)
          .where('order_cart_id', 'incart');

        if (rec.length > 0) {
          await knex('store_orders')
            .where('store_approval', user_id)
            .where('varient_id', ordersdetails.varient_id)
            .where('store_orders.subscription_flag', '1')
            .where('order_cart_id', 'incart')
            .delete();
        }

        const varient_details = await knex('store_products')
          .where('varient_id', '=', ordersdetails.varient_id)
          .first();

        // Stock validation: if stock is 0/null (or missing row), abort with special error
        const availableStock = varient_details?.stock;
        if (availableStock === null || availableStock === undefined || Number(availableStock) <= 0) {
          throw new Error('OUT_OF_STOCK');
        }
        if (Number(ordersdetails.qty || 0) > Number(availableStock)) {
          throw new Error('OUT_OF_STOCK');
        }

        // PostgreSQL: null-safe repeat_orders (TEXT column may be null)
        const repeatOrders = (ordersdetails.repeat_orders || '').trim().split(',').filter(Boolean);
        const totalMrp = repeatOrders.length * (varient_details.price * ordersdetails.qty);
        const availability = (ordersdetails.availability === 'quick') ? null : 1;
        const currentDate = new Date();

        if (availability === 1) {
          // PostgreSQL: store_order_id is NOT NULL; generate next id
          const store_order_id = await getNextStoreOrderId();
          await knex('store_orders').insert({
            store_order_id,
            product_name: ordersdetails.product_name,
            varient_image: ordersdetails.varient_image,
            quantity: ordersdetails.quantity,
            unit: ordersdetails.unit,
            varient_id: ordersdetails.varient_id,
            qty: ordersdetails.qty,
            price: (ordersdetails.qty * varient_details.price),
            total_mrp: totalMrp,
            order_cart_id: 'incart',
            order_date: currentDate,
            repeat_orders: ordersdetails.repeat_orders,
            store_approval: user_id,
            store_id: ordersdetails.store_id,
            description: ordersdetails.description,
            tx_per: ordersdetails.tx_per,
            price_without_tax: varient_details.price,
            tx_price: ordersdetails.tx_price,
            type: ordersdetails.type,
            repeated_order_cart: ordersdetails.order_cart_id,
            buying_price: ordersdetails.buying_price,
            base_mrp: ordersdetails.base_mrp,
            sub_time_slot: ordersdetails.sub_time_slot,
            sub_total_delivery: ordersdetails.sub_total_delivery,
            percentage: ordersdetails.percentage,
            sub_delivery_date: ordersdetails.sub_delivery_date,
            subscription_flag: availability != null ? '1' : null,
          });
        }
      }
    }
  }
  return 1;
};

const getcancelOrderres = async () => {
  return cancel_reason = await knex('cancel_for')

};

const ordsubDetails = async (appDetatils) => {

  cart_id = appDetatils.cart_id;
  store_order_id = appDetatils.store_order_id;
  subscription_id = appDetatils.subscription_id;
  ongoing = await knex('orders')
    .join('store', 'orders.store_id', '=', 'store.id')
    .join('users', 'orders.user_id', '=', 'users.id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
    //   .where('orders.user_id',$user_id)
    .where('orders.cart_id', cart_id)
    .whereNotNull('orders.order_status')
    .whereNotNull('orders.payment_method')
    .orderBy('orders.order_id', 'DESC')

  const pending_sub = await knex('subscription_order')
    .where('cart_id', cart_id)
    .where('order_status', 'Pending');

  const pause_sub = await knex('subscription_order')
    .where('cart_id', cart_id)
    .where('order_status', 'Pause');

  const completed_sub = await knex('subscription_order')
    .where('cart_id', cart_id)
    .where('order_status', 'Completed');

  const cancelled_sub = await knex('subscription_order')
    .where('cart_id', cart_id)
    .where('order_status', 'Cancelled');

  ongoing_sub = await knex('subscription_order')
    .leftJoin('delivery_boy', 'subscription_order.dboy_id', '=', 'delivery_boy.dboy_id')
    .where('cart_id', cart_id)
    .where('store_order_id', store_order_id)
    .orderBy('subscription_order.delivery_date', 'ASC')
    .select('*', knex.raw('DATE_FORMAT(subscription_order.delivery_date, "%Y-%m-%d") as delivery_date'))
  // .where('subscription_id',subscription_id)

  const onSubData = [];
  for (let i = 0; i < ongoing_sub.length; i++) {
    const ProductList = ongoing_sub[i];

    const timestamp = new Date(ProductList.deliveryDate).getTime() / 1000; // Convert the date string to a UNIX timestamp in seconds
    const day = new Date(timestamp * 1000).toLocaleString('en-us', { weekday: 'short' });
    ongoing_subs = {
      cart_id: ProductList.cart_id,
      subscription_id: ProductList.id,
      cart_id: ProductList.cart_id,
      delivery_date: ProductList.delivery_date,
      time_slot: ProductList.time_slot,
      order_status: ProductList.order_status,
      day: day,
      store_order_id: ProductList.store_order_id,
      processing_product: ProductList.processing_product

    };
    onSubData.push(ongoing_subs);
  }
  subdataorder = onSubData;

  if (ongoing) {

    const customizedProductData = [];
    for (let i = 0; i < ongoing.length; i++) {
      const ProductList = ongoing[i];

      ongoings = await knex('orders')
        .join('store', 'orders.store_id', '=', 'store.id')
        .join('users', 'orders.user_id', '=', 'users.id')
        .join('address', 'orders.address_id', '=', 'address.address_id')
        .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
        .where('orders.cart_id', cart_id)
        .whereNotNull('orders.order_status')
        .whereNotNull('orders.payment_method')
        .orderBy('orders.order_id', 'DESC')
        .first();


      const baseurl = process.env.BUNNY_NET_IMAGE;

      const order = await knex('store_orders')
        .join('product_varient', 'store_orders.varient_id', 'product_varient.varient_id')
        .join('product', 'product.product_id', 'product_varient.product_id')
        // .join('store_products', 'store_products.varient_id', 'product_varient.varient_id')
        .where('store_orders.order_cart_id', '=', appDetatils.cart_id)
        //  .where('store_orders.store_order_id', '=', appDetatils.store_order_id)
        .select(
          'store_orders.store_order_id',
          'store_orders.product_name',
          'product.product_id',
          'store_orders.quantity',
          'store_orders.unit',
          'store_orders.varient_id',
          'store_orders.qty',
          // 'store_orders.price as price',
          knex.raw('((store_orders.price / store_orders.qty)/store_orders.sub_total_delivery) as price'),
          'store_orders.total_mrp',
          'store_orders.order_cart_id',
          'store_orders.order_date',
          'store_orders.repeat_orders',
          'store_orders.store_approval',
          'store_orders.store_id',
          'store_orders.description',
          'store_orders.tx_per',
          'store_orders.price_without_tax',
          'store_orders.tx_price',
          'store_orders.tx_name',
          'store_orders.type',
          knex.raw(`CONCAT('${baseurl}', product.product_image) as varient_image`),
          'product.thumbnail',
          knex.raw(`LENGTH(store_orders.repeat_orders) - LENGTH(REPLACE(store_orders.repeat_orders, ',', '')) + 1 as repeat_orders_count`)
        )
        .groupBy('store_orders.varient_id');


      ongoings.discountonmrp = ongoings.total_products_mrp - ongoings.price_without_delivery;
      ongoings.total_items = order.length;

      const customizedOrdData = [];
      //  return order

      for (let i = 0; i < order.length; i++) {

        const ordList = order[i];
        checkstore = await knex('orders')
          .where('cart_id', ProductList.cart_id)
          .first();
        getrating = await knex('product_rating')
          .where('varient_id', ordList.varient_id)
          .where('user_id', ProductList.user_id)
          .where('store_id', checkstore.store_id)
          .first();

        if (getrating) {

          rating = getrating.rating;
          rating_description = getrating.description;
        }
        else {
          rating = "null";
          rating_description = "null";
        }

        //  const trimmedString = ProductList.repeat_orders.trim();



        if (ProductList.repeat_orders != null) {
          repeat_orderss = ProductList.repeat_orders.split(',').map(s => s.trim());
          total_delivery_week = repeat_orderss.length * order.total_delivery;
          finalrepeat_orderss = repeat_orderss.length
        } else {
          repeat_orderss = '';
          total_delivery_week = 1;
          finalrepeat_orderss = null;
        }

      }


      const pendingResult = await knex('subscription_order')
        .where('cart_id', ProductList.cart_id)
        .where('order_status', 'Pending')
        .count({ pending_count: 'order_id' });

      const AllResult = await knex('subscription_order')
        .where('cart_id', ProductList.cart_id)
        .andWhere(function () {
          this.where('order_status', 'Pending')
            .orWhere('order_status', 'Completed')
            .orWhere('order_status', 'Confirmed')
            .orWhere('order_status', 'Out_For_Delivery');
        })
        .count({ all_count: 'order_id' });

      const pendingCount = pendingResult[0].pending_count;
      const allCount = AllResult[0].all_count;
      order_status = (pendingCount > 0) ? "Pending" : "Completed";

      const cancelledResult = await knex('subscription_order')
        .where('cart_id', ProductList.cart_id)
        .where('order_status', 'Cancelled')
        .count({ cancelled_count: 'order_id' });
      const cancelledCount = cancelledResult[0].cancelled_count;
      if (cancelledCount > 0 && allCount <= 0) {
        order_status = "Cancelled";
      }

      const completedResult = await knex('subscription_order')
        .where('cart_id', ProductList.cart_id)
        .where('order_status', 'Completed')
        .count({ completed_count: 'order_id' });
      const completedCount = completedResult[0].completed_count;
      if (completedCount > 0 && pendingCount > 0) {
        order_status = "Inprogress";
      }

      const TotalOrderResult = await knex('subscription_order')
        .where('cart_id', ProductList.cart_id)
        .count({ total_count: 'order_id' });
      const totalOrderCount = TotalOrderResult[0].total_count;


      store_order_idwww = await knex('store_orders')
        .where('order_cart_id', appDetatils.cart_id)
        .first();
      pricess = store_order_idwww.price;
      // return finalrepeat_orderss;
      finaltotal_price = pricess * finalrepeat_orderss * ProductList.total_delivery;
      let ProductListOrderDate = (ProductList.order_date) ? format(ProductList.order_date, 'yyyy-MM-dd') : "null";
      const customizedProduct = {
        user_name: ProductList.name,
        total_delivery: ProductList.total_delivery,
        //delivery_address:ProductList.building_villa+','+ongoings.street+','+ongoings.society+','+ongoings.city,
        delivery_address: ProductList.house_no,
        store_name: ProductList.store_name,
        store_owner: ProductList.employee_name,
        store_phone: ProductList.phone_number,
        store_address: ProductList.address,
        order_status: order_status,
        delivery_date: ProductList.delivery_date,
        time_slot: ProductList.time_slot,
        payment_method: ProductList.payment_method,
        payment_status: ProductList.payment_status,
        paid_by_wallet: ProductList.paid_by_wallet,
        cart_id: ProductList.cart_id,
        // total_price:finaltotal_price.toFixed(2),
        total_price: ProductList.total_price.toFixed(2),
        delivery_charge: ProductList.delivery_charge.toFixed(2),
        rem_price: ProductList.rem_price.toFixed(2),
        coupon_discount: ProductList.coupon_discount.toFixed(2),
        dboy_name: ProductList.boy_name,
        dboy_phone: ProductList.boy_phone,
        price_without_delivery: ProductList.price_without_delivery.toFixed(2),
        avg_tax_per: ProductList.avg_tax_per,
        total_tax_price: ProductList.total_tax_price,
        user_id: ProductList.user_id,
        total_products_mrp: ProductList.total_products_mrp,
        discountonmrp: ProductList.discountonmrp,
        cancelling_reason: ProductList.cancelling_reason,
        order_date: ProductListOrderDate,
        dboy_id: ProductList.dboy_id,
        user_signature: ProductList.user_signature,
        coupon_id: ProductList.coupon_id,
        dboy_incentive: ProductList.dboy_incentive,
        total_items: ProductList.total_items,
        is_subscription: ProductList.is_subscription,
        pending_order: pending_sub.length,
        pause_order: pause_sub.length,
        completed_order: completed_sub.length,
        cancelled_order: cancelled_sub.length,
        //total_order:total_delivery_week, // need to change
        total_order: totalOrderCount,
        data: order,
        //subscription_details:ongoing_subs
        subscription_details: subdataorder
      }


      customizedProductData.push(customizedProduct);
    }

    return customizedProductData;

  } else {
    throw new Error('No Orders Yet');
  }

};

const ordsubDetailsold = async (appDetatils) => {

  cart_id = appDetatils.cart_id;
  store_order_id = appDetatils.store_order_id;
  subscription_id = appDetatils.subscription_id;
  ongoing = await knex('orders')
    .join('store', 'orders.store_id', '=', 'store.id')
    .join('users', 'orders.user_id', '=', 'users.id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
    //   .where('orders.user_id',$user_id)
    .where('orders.cart_id', cart_id)
    .whereNotNull('orders.order_status')
    .whereNotNull('orders.payment_method')
    .orderBy('orders.order_id', 'DESC')



  const pending_sub = await knex('subscription_order')
    .where('subscription_id', subscription_id)
    .where(function () {
      this.where('order_status', 'Pending').orWhere('order_status', 'pending');
    })

  const pause_sub = await knex('subscription_order')
    .where('subscription_id', subscription_id)
    .where(function () {
      this.where('order_status', 'Pause').orWhere('order_status', 'pause');
    })
  const completed_sub = await knex('subscription_order')
    .where('subscription_id', subscription_id)
    .where(function () {
      this.where('order_status', 'Completed').orWhere('order_status', 'completed');
    })

  const cancelled_sub = await knex('subscription_order')
    .where('subscription_id', subscription_id)
    .where(function () {
      this.where('order_status', 'Cancelled').orWhere('order_status', 'cancelled');
    })

  ongoing_sub = await knex('subscription_order')
    .leftJoin('delivery_boy', 'subscription_order.dboy_id', '=', 'delivery_boy.dboy_id')
    .where('cart_id', cart_id)
    // .where('store_order_id',store_order_id)
    .where('subscription_id', subscription_id)

  const onSubData = [];
  for (let i = 0; i < ongoing_sub.length; i++) {
    const ProductList = ongoing_sub[i];

    const timestamp = new Date(ProductList.deliveryDate).getTime() / 1000; // Convert the date string to a UNIX timestamp in seconds
    const day = new Date(timestamp * 1000).toLocaleString('en-us', { weekday: 'short' });
    ongoing_subs = {
      cart_id: ProductList.cart_id,
      subscription_id: ProductList.id,
      cart_id: ProductList.cart_id,
      delivery_date: ProductList.delivery_date,
      time_slot: ProductList.time_slot,
      order_status: ProductList.order_status,
      day: day,
      store_order_id: ProductList.store_order_id,
      processing_product: ProductList.processing_product

    };
    onSubData.push(ongoing_subs);
  }
  subdataorder = onSubData;

  if (ongoing) {
    const customizedProductData = [];
    for (let i = 0; i < ongoing.length; i++) {
      const ProductList = ongoing[i];

      ongoings = await knex('orders')
        .join('store', 'orders.store_id', '=', 'store.id')
        .join('users', 'orders.user_id', '=', 'users.id')
        .join('address', 'orders.address_id', '=', 'address.address_id')
        .leftJoin('delivery_boy', 'orders.dboy_id', '=', 'delivery_boy.dboy_id')
        .where('orders.cart_id', cart_id)
        .whereNotNull('orders.order_status')
        .whereNotNull('orders.payment_method')
        .orderBy('orders.order_id', 'DESC')
        .first();
      const baseurl = process.env.BUNNY_NET_IMAGE;
      const order = await knex('store_orders')
        .select(
          'store_orders.store_order_id',
          'store_orders.product_name',
          'store_orders.quantity',
          'store_orders.unit',
          'store_orders.varient_id',
          'store_orders.qty',
          knex.raw('(store_orders.price / store_orders.qty) as price'),
          'store_orders.total_mrp',
          'store_orders.order_cart_id',
          'store_orders.order_date',
          'store_orders.repeat_orders',
          'store_orders.store_approval',
          'store_orders.store_id',
          'store_orders.description',
          'store_orders.tx_per',
          'store_orders.price_without_tax',
          'store_orders.tx_price',
          'store_orders.tx_name',
          'store_orders.type',
          knex.raw(`CONCAT('${baseurl}', product.product_image) as varient_image`),
          'product.thumbnail'
        )
        .join('product_varient', 'store_orders.varient_id', 'product_varient.varient_id')
        .join('product', 'product.product_id', 'product_varient.product_id')
        .where('order_cart_id', '=', cart_id)
        .where('store_order_id', '=', store_order_id);

      ongoings.discountonmrp = ongoings.total_products_mrp - ongoings.price_without_delivery;
      ongoings.total_items = order.length;

      const customizedOrdData = [];


      for (let i = 0; i < order.length; i++) {
        const ordList = order[i];
        checkstore = await knex('orders')
          .where('cart_id', ProductList.cart_id)
          .first();
        getrating = await knex('product_rating')
          .where('varient_id', ordList.varient_id)
          .where('user_id', ProductList.user_id)
          .where('store_id', checkstore.store_id)
          .first();

        if (getrating) {

          rating = getrating.rating;
          rating_description = getrating.description;
        }
        else {
          rating = "null";
          rating_description = "null";
        }

        //  const trimmedString = ProductList.repeat_orders.trim();

        // Step 2: Split the trimmed string by commas to get an array of values
        //  const repeat_orderss = trimmedString.split(',');
        //repeat_orderss=explode(",",trim(ProductList.repeat_orders));
        // total_delivery_week=repeat_orderss.length*order.total_delivery;

        const repeat_orderss = ProductList.repeat_orders.split(',').map(s => s.trim());
        const total_delivery_week = repeat_orderss.length * order.total_delivery;

      }

      store_order_idwww = await knex('store_orders')
        .where('store_order_id', store_order_id)
        .first();
      pricess = store_order_idwww.price;
      const customizedProduct = {
        user_name: ProductList.name,
        total_delivery: ongoings.total_delivery,
        delivery_address: ProductList.building_villa + ',' + ongoings.street + ',' + ongoings.society + ',' + ongoings.city,
        store_name: ProductList.store_name,
        store_owner: ProductList.employee_name,
        store_phone: ProductList.phone_number,
        store_address: ProductList.address,
        order_status: ProductList.order_status,
        delivery_date: ProductList.delivery_date,
        time_slot: ProductList.time_slot,
        payment_method: ProductList.payment_method,
        payment_status: ProductList.payment_status,
        paid_by_wallet: ProductList.paid_by_wallet,
        cart_id: ProductList.cart_id,
        total_price: (pricess * total_delivery_week).toFixed(2),
        delivery_charge: ProductList.delivery_charge.toFixed(2),
        rem_price: ProductList.rem_price.toFixed(2),
        coupon_discount: ProductList.coupon_discount.toFixed(2),
        dboy_name: ProductList.boy_name,
        dboy_phone: ProductList.boy_phone,
        price_without_delivery: ProductList.price_without_delivery.toFixed(2),
        avg_tax_per: ProductList.avg_tax_per,
        total_tax_price: ProductList.total_tax_price,
        user_id: ProductList.user_id,
        total_products_mrp: ProductList.total_products_mrp,
        discountonmrp: ProductList.discountonmrp,
        cancelling_reason: ProductList.cancelling_reason,
        order_date: ProductList.order_date,
        dboy_id: ProductList.dboy_id,
        user_signature: ProductList.user_signature,
        coupon_id: ProductList.coupon_id,
        dboy_incentive: ProductList.dboy_incentive,
        total_items: ProductList.total_items,
        is_subscription: ProductList.is_subscription,
        pending_order: pending_sub.length,
        pause_order: pause_sub.length,
        completed_order: completed_sub.length,
        cancelled_order: cancelled_sub.length,
        total_order: total_delivery_week, // need to change
        data: order,
        //subscription_details:ongoing_subs
        subscription_details: subdataorder
      }


      customizedProductData.push(customizedProduct);
    }

    return customizedProductData;

  } else {
    throw new Error('No Orders Yet');
  }

};

const getOngoingsub = async (appDetatils) => {
  // PostgreSQL: no MySQL sql_mode; use valid GROUP BY

  const { user_id, store_id, page: pageFilter, perpage: perPage } = appDetatils;
  if (!user_id || user_id === "" || user_id === "null") {
    return [];
  }
  const offset = (pageFilter - 1) * perPage;

  // Base query for ongoing subscriptions
  const ongoingQuery = knex('orders')
    .select(
      knex.raw('SUM(orders.total_products_mrp) as totalProductsMrp'),
      'orders.group_id',
      'orders.cart_id',
      knex.raw('DATE_FORMAT(orders.delivery_date, "%Y-%m-%d") as delivery_date'),
      knex.raw('DATE_FORMAT(orders.order_date, "%Y-%m-%d") as order_date'),
      'orders.total_delivery',
      'store_orders.price',
      'store_orders.store_order_id',
      'store_orders.product_name',
      'store_orders.total_mrp',
      'store_orders.repeat_orders',
      'orders.order_status',
      'orders.si_order',
      'orders.si_sub_ref_no',
      'orders.pastorecentrder'
    )
    .join('store_orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('order_cart_id', '!=', 'incart')
    .where('orders.user_id', user_id)
    .where('orders.store_id', store_id)
    .whereNotNull('orders.order_status')
    .where('orders.is_subscription', 1)
    .whereNotNull('orders.payment_method')
    .groupBy('orders.group_id')
    .orderBy('orders.order_id', 'DESC')
    .limit(perPage)
    .offset(offset);

  const ongoing = await ongoingQuery;

  // Extract cart_ids for batch processing
  const cartIds = ongoing.map(o => o.cart_id);
  const storeOrderIds = ongoing.map(o => o.store_order_id);

  // Fetch subscription orders and order details in parallel
  const [subscriptionOrders, orderDetails, deliveryRatings] = await Promise.all([
    knex('subscription_order')
      .whereIn('cart_id', cartIds)
      .whereIn('store_order_id', storeOrderIds)
      .where('order_status', 'Pending')
      .orderBy('id', 'DESC')
      .select('cart_id', 'store_order_id', 'subscription_id', 'delivery_date'),
    knex('orders')
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'total_products_mrp', 'pastorecentrder'),
    knex('delivery_rating')
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'rating', 'description')
  ]);

  // Convert deliveryRatings array into a map for efficient lookup
  const deliveryRatingsMap = deliveryRatings.reduce((map, rating) => {
    map[rating.cart_id] = { rating: rating.rating, description: rating.description };
    return map;
  }, {});


  // Process ongoing subscriptions
  const customizedProductData = ongoing.map(product => {
    const subscriptionOrder = subscriptionOrders.find(o => o.cart_id === product.cart_id && o.store_order_id === product.store_order_id);
    const formattedDeliveryDate = subscriptionOrder ? format(new Date(subscriptionOrder.delivery_date), 'yyyy-MM-dd') : '';
    const orderDetail = orderDetails.find(o => o.cart_id === product.cart_id);
    const ratingData = deliveryRatingsMap[product.cart_id] || { rating: null, description: null };
    const totalProductsMrp = product.pastorecentrder === 'old' ? orderDetail.total_products_mrp : product.totalProductsMrp;
    const deliveryDate = new Date(product.delivery_date);
    const today = new Date();
    // let orderStatus = deliveryDate > today ? 'Active' : 'Inactive';
    let orderStatus = deliveryDate > today ? 'Pending' : 'Completed';
    if (product.order_status === 'Cancelled') orderStatus = 'Cancelled';

    const repeatOrders = product.repeat_orders.split(',').map(item => item.trim());
    const totalDeliveryWeek = repeatOrders.length * product.total_delivery;

    return {
      cart_id: product.group_id,
      order_date: product.order_date,
      total_mrp: parseFloat(totalProductsMrp).toFixed(2),
      subscription_id: subscriptionOrder ? subscriptionOrder.subscription_id : '',
      si_order: product.si_order,
      si_sub_ref_no: product.si_sub_ref_no,
      order_status: orderStatus,
      drating: ratingData.rating,
      dreview: ratingData.description
    };
  });

  return customizedProductData;
};

const getrepeatOrder = async (appDetatils) => {
  const startTime = Date.now();

  // Keep original request behavior
  const user_id = (appDetatils.user_id != "null") ? appDetatils.user_id : appDetatils.device_id;
  const store_id = appDetatils.store_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // STEP 1 (ultra-optimized): get valid variant_ids first, then count orders (2-step approach)
  const topStart = Date.now();
  // Step 1a: Get all valid variant_ids that meet product/store_products criteria (one-time query)
  const validVariants = await knex('product_varient')
    .join('product', 'product_varient.product_id', 'product.product_id')
    .join('store_products', function () {
      this.on('store_products.varient_id', '=', 'product_varient.varient_id')
        .andOn('store_products.store_id', '=', knex.raw('?', [store_id]));
    })
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('store_products.stock', '>', 0)
    .whereRaw(`(product.is_offer_product = 0 OR product.offer_date IS NULL OR product.offer_date::date != CURRENT_DATE)`)
    .select('product_varient.varient_id')
    .distinct();

  const validVariantIds = validVariants.map(v => v.varient_id);
  if (validVariantIds.length === 0) {
    return [];
  }

  // Step 1b: Count orders for valid variants only (much faster with WHERE IN)
  const topVariants = await knex('store_orders')
    .join('orders', 'orders.cart_id', 'store_orders.order_cart_id')
    .where('orders.user_id', user_id)
    .where('orders.store_id', store_id)
    .whereIn('store_orders.varient_id', validVariantIds)
    .select('store_orders.varient_id')
    .count({ ordercount: 'store_orders.varient_id' })
    .groupBy('store_orders.varient_id')
    .orderBy('ordercount', 'desc')
    .limit(20);
  const topTime = Date.now() - topStart;

  if (!topVariants || topVariants.length === 0) {
    return [];
  }

  const topVariantIds = topVariants.map(r => r.varient_id);
  const orderCountMap = new Map();
  for (let i = 0; i < topVariants.length; i++) {
    const r = topVariants[i];
    orderCountMap.set(r.varient_id, Number(r.ordercount) || 0);
  }

  // STEP 2 (optimized): fetch variant mapping first, then parallel fetch rest
  const baseStart = Date.now();
  // Get variant-product mapping (very fast, only up to 20 rows)
  const variantRows = await knex('product_varient')
    .whereIn('varient_id', topVariantIds)
    .select('varient_id', 'product_id');

  // Extract product IDs immediately
  const variantToProductMap = new Map();
  const productIdsSet = new Set();
  for (let i = 0; i < variantRows.length; i++) {
    const v = variantRows[i];
    variantToProductMap.set(v.varient_id, v.product_id);
    productIdsSet.add(v.product_id);
  }
  const productIds = Array.from(productIdsSet);

  // Now fetch everything else in parallel (we know product_ids now)
  const [productRows, storeProductRows, orderMetadata] = await Promise.all([
    // Fetch product details (direct WHERE IN, very fast)
    knex('product')
      .whereIn('product_id', productIds)
      .where('hide', 0)
      .where('is_delete', 0)
      .select('product_id', 'available_days', 'percentage', 'availability', 'fcat_id', 'is_customized', 'product_image', 'thumbnail'),
    // Fetch store_products details (filtered by stock)
    knex('store_products')
      .where('store_id', store_id)
      .where('stock', '>', 0)
      .whereIn('varient_id', topVariantIds)
      .select('varient_id', 'price', 'mrp', 'stock'),
    // Fetch store_orders metadata (lightweight)
    knex('store_orders')
      .whereIn('varient_id', topVariantIds)
      .select('varient_id', 'product_name', 'description', 'type', 'unit', 'qty')
  ]);

  const baseTime = Date.now() - baseStart;

  // Build lookup maps for O(1) merging
  const productMap = new Map();
  for (let i = 0; i < productRows.length; i++) {
    productMap.set(productRows[i].product_id, productRows[i]);
  }

  const storeProductMap = new Map();
  for (let i = 0; i < storeProductRows.length; i++) {
    storeProductMap.set(storeProductRows[i].varient_id, storeProductRows[i]);
  }

  const orderMetaMap = new Map();
  for (let i = 0; i < orderMetadata.length; i++) {
    orderMetaMap.set(orderMetadata[i].varient_id, orderMetadata[i]);
  }

  // Merge all data: only include variants that have valid product AND store_product
  const baseRows = [];
  for (let i = 0; i < topVariantIds.length; i++) {
    const vid = topVariantIds[i];
    const productId = variantToProductMap.get(vid);
    if (!productId) continue;

    const product = productMap.get(productId);
    if (!product) continue;

    const storeProduct = storeProductMap.get(vid);
    if (!storeProduct) continue;

    const om = orderMetaMap.get(vid) || {};
    baseRows.push({
      varient_id: vid,
      product_name: om.product_name || null,
      description: om.description || null,
      type: om.type || null,
      unit: om.unit || null,
      qty: om.qty || null,
      product_id: productId,
      available_days: product.available_days,
      percentage: product.percentage,
      availability: product.availability,
      fcat_id: product.fcat_id,
      is_customized: product.is_customized,
      price: storeProduct.price,
      mrp: storeProduct.mrp,
      stock: storeProduct.stock,
      product_image: product.product_image,
      thumbnail: product.thumbnail
    });
  }

  // Maintain top-20 ordering based on ordercount
  const baseByVariantId = new Map();
  for (let i = 0; i < baseRows.length; i++) baseByVariantId.set(baseRows[i].varient_id, baseRows[i]);
  const orderedBase = [];
  for (let i = 0; i < topVariantIds.length; i++) {
    const row = baseByVariantId.get(topVariantIds[i]);
    if (row) orderedBase.push(row);
  }

  // Collect productIds for bulk fetches (from filtered orderedBase)
  const bulkProductIdsSet = new Set();
  for (let i = 0; i < orderedBase.length; i++) bulkProductIdsSet.add(orderedBase[i].product_id);
  const bulkProductIds = Array.from(bulkProductIdsSet);

  // STEP 3: bulk fetch everything needed for response building
  const bulkStart = Date.now();
  const hasUser = !!appDetatils.user_id && appDetatils.user_id !== 'null';

  const [
    cartAggRows,
    notifyRows,
    wishlistRows,
    featuresRows,
    storeVariantsRows,
    productImagesRows
  ] = await Promise.all([
    // Cart + subscription cart in ONE query (skip entirely for guest requests)
    hasUser
      ? knex('store_orders')
        .select('varient_id')
        .select(knex.raw(`COALESCE(SUM(CASE WHEN subscription_flag IS NULL THEN qty ELSE 0 END), 0) AS cart_qty`))
        .select(knex.raw(`COALESCE(SUM(CASE WHEN subscription_flag = '1' THEN qty ELSE 0 END), 0) AS subcart_qty`))
        .whereIn('varient_id', topVariantIds)
        .where('store_approval', appDetatils.user_id)
        .where('order_cart_id', 'incart')
        .where('store_id', store_id)
        .groupBy('varient_id')
      : Promise.resolve([]),
    // notify-me (skip for guest)
    hasUser
      ? knex('product_notify_me')
        .select('varient_id')
        .whereIn('varient_id', topVariantIds)
        .where('user_id', user_id)
      : Promise.resolve([]),
    // wishlist (skip for guest)
    hasUser
      ? knex('wishlist')
        .select('varient_id')
        .whereIn('varient_id', topVariantIds)
        .where('user_id', user_id)
      : Promise.resolve([]),
    // product features (fix bad join column)
    knex('product_features')
      .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', 'product_features.feature_value_id')
      .whereIn('product_features.product_id', bulkProductIds),
    // all variants for these products in this store (optimized: fetch variants first, then store_products)
    knex('product_varient')
      .join('store_products', function () {
        this.on('store_products.varient_id', '=', 'product_varient.varient_id')
          .andOn('store_products.store_id', '=', knex.raw('?', [store_id]));
      })
      .select(
        'product_varient.product_id',
        'store_products.stock',
        'product_varient.varient_id',
        'product_varient.description',
        'store_products.price',
        'store_products.mrp',
        'product_varient.varient_image',
        'product_varient.unit',
        'product_varient.quantity'
      )
      .whereIn('product_varient.product_id', bulkProductIds)
      .whereNotNull('store_products.price')
      .where('product_varient.approved', 1),
    // product images: fetch ONLY 1 row per product_id (Postgres DISTINCT ON)
    knex('product_images')
      .distinctOn('product_id')
      .select('product_id', 'image', 'type')
      .whereIn('product_id', bulkProductIds)
      .orderBy('product_id', 'asc')
      .orderBy('type', 'desc')
  ]);
  const bulkTime = Date.now() - bulkStart;

  // Build quick lookup maps
  const cartQtyMap = new Map();
  const subCartQtyMap = new Map();
  for (let i = 0; i < cartAggRows.length; i++) {
    const r = cartAggRows[i];
    const vid = r.varient_id;
    cartQtyMap.set(vid, Number(r.cart_qty) || 0);
    subCartQtyMap.set(vid, Number(r.subcart_qty) || 0);
  }
  const notifySet = new Set(notifyRows.map(r => r.varient_id));
  const wishlistSet = new Set(wishlistRows.map(r => r.varient_id));

  const featuresByProductId = new Map();
  for (let i = 0; i < featuresRows.length; i++) {
    const r = featuresRows[i];
    if (!featuresByProductId.has(r.product_id)) featuresByProductId.set(r.product_id, []);
    featuresByProductId.get(r.product_id).push({ id: r.id, feature_value: r.feature_value });
  }

  const variantsByProductId = new Map();
  for (let i = 0; i < storeVariantsRows.length; i++) {
    const r = storeVariantsRows[i];
    if (!variantsByProductId.has(r.product_id)) variantsByProductId.set(r.product_id, []);
    variantsByProductId.get(r.product_id).push(r);
  }

  const firstImageByProductId = new Map();
  for (let i = 0; i < productImagesRows.length; i++) {
    const r = productImagesRows[i];
    if (!firstImageByProductId.has(r.product_id)) firstImageByProductId.set(r.product_id, r.image);
  }

  // Feature tags: batch fetch feature_categories referenced by fcat_id strings
  const fcatIdsSet = new Set();
  for (let i = 0; i < orderedBase.length; i++) {
    const f = orderedBase[i].fcat_id;
    if (!f) continue;
    const parts = String(f).split(',');
    for (let j = 0; j < parts.length; j++) {
      const n = Number(parts[j].trim());
      if (Number.isFinite(n)) fcatIdsSet.add(n);
    }
  }

  let featureCatMap = new Map();
  if (fcatIdsSet.size > 0) {
    const fcatIds = Array.from(fcatIdsSet);
    const featureCats = await knex('feature_categories')
      .select('id', 'image')
      .whereIn('id', fcatIds)
      .where('status', 1)
      .where('is_deleted', 0);
    featureCatMap = new Map();
    for (let i = 0; i < featureCats.length; i++) {
      const r = featureCats[i];
      featureCatMap.set(r.id, { id: r.id, image: `${baseurl}${r.image}` });
    }
  }

  // STEP 4: Build response (same structure as original)
  const buildStart = Date.now();
  const customizedProductData = [];

  for (let i = 0; i < orderedBase.length; i++) {
    const p = orderedBase[i];

    const cartQty = appDetatils.user_id ? (cartQtyMap.get(p.varient_id) || 0) : 0;
    const subCartQty = appDetatils.user_id ? (subCartQtyMap.get(p.varient_id) || 0) : 0;
    const notifyMe = appDetatils.user_id ? (notifySet.has(p.varient_id) ? 'true' : 'false') : 'false';
    const isSubscription = appDetatils.user_id ? (subCartQty > 0 ? 'true' : 'false') : 'false';

    const mrp = Number(p.mrp) || 0;
    const perc = Number(p.percentage) || 0;
    const subscription_price = parseFloat((mrp - (mrp * perc) / 100).toFixed(2));

    // Keep old "integer price + .001" behavior but numeric
    const basePrice = Number(p.price) || 0;
    const priceval = Number.isInteger(basePrice) ? (basePrice + 0.001) : basePrice;

    // Feature tags
    let feature_tags = [];
    if (p.fcat_id) {
      const parts = String(p.fcat_id).split(',');
      const temp = [];
      for (let j = 0; j < parts.length; j++) {
        const n = Number(parts[j].trim());
        const fc = featureCatMap.get(n);
        if (fc) temp.push(fc);
      }
      feature_tags = temp;
    }

    const features = featuresByProductId.get(p.product_id) || [];

    // Variants (build with preloaded maps)
    const variants = variantsByProductId.get(p.product_id) || [];
    let total_cart_qty = 0;
    let total_subcart_qty = 0;
    const customizedVarientData = new Array(variants.length);

    const img = firstImageByProductId.get(p.product_id);
    const imgFull = img ? `${baseurl}${img}` : null;

    for (let k = 0; k < variants.length; k++) {
      const v = variants[k];
      const vCartQty = appDetatils.user_id ? (cartQtyMap.get(v.varient_id) || 0) : 0;
      const vSubCartQty = appDetatils.user_id ? (subCartQtyMap.get(v.varient_id) || 0) : 0;
      total_cart_qty += vCartQty;
      total_subcart_qty += vSubCartQty;

      customizedVarientData[k] = {
        stock: v.stock,
        varient_id: v.varient_id,
        product_id: v.product_id,
        product_name: p.product_name,
        product_image: imgFull ? `${imgFull}?width=200&height=200&quality=100` : '',
        thumbnail: imgFull || '',
        description: v.description,
        price: v.price,
        mrp: v.mrp,
        unit: v.unit,
        quantity: v.quantity,
        type: p.type,
        discountper: 0,
        notify_me: appDetatils.user_id ? (notifySet.has(v.varient_id) ? 'true' : 'false') : 'false',
        isFavourite: appDetatils.user_id ? (wishlistSet.has(v.varient_id) ? 'true' : 'false') : 'false',
        cart_qty: vCartQty,
        subcartQty: vSubCartQty,
        country_icon: null
      };
    }

    customizedProductData.push({
      percentage: p.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: p.availability,
      discountper: 0,
      product_name: p.product_name,
      varient_image: p.product_image ? `${baseurl}${p.product_image}` : '',
      varient_id: p.varient_id,
      price: parseFloat(priceval),
      description: p.description,
      type: p.type,
      ordercount: orderCountMap.get(p.varient_id) || 0,
      unit: p.unit,
      qty: p.qty,
      total_mrp: p.mrp,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      total_subcart_qty: total_subcart_qty,
      notify_me: notifyMe,
      product_id: p.product_id,
      available_days: p.available_days,
      feature_tags: feature_tags,
      stock: p.stock,
      is_customized: p.is_customized,
      features: features,
      varients: customizedVarientData
    });
  }

  const buildTime = Date.now() - buildStart;
  const totalTime = Date.now() - startTime;
  console.log(`📊 repeat_orders timings: top=${topTime}ms base=${baseTime}ms bulk=${bulkTime}ms build=${buildTime}ms total=${totalTime}ms (items=${customizedProductData.length})`);

  return customizedProductData;

};

const getMyOrder = async (appDetatils) => {
  const { user_id, page } = appDetatils;
  const pageFilter = page; // You can adjust the page number dynamically
  const perPage = 50;

  let ongoing = await knex('orders')
    .select('orders.group_id', 'orders.cart_id', knex.raw('DATE_FORMAT(orders.order_date, "%Y-%m-%d") as order_date'), 'orders.coupon_discount', 'orders.total_products_mrp as price_without_delivery', 'orders.user_id', 'orders.is_subscription', 'orders.si_order', 'orders.bank_id', 'orders.order_status')
    .where('orders.user_id', user_id)
    .whereNot('orders.group_id', null)
    .whereNot('orders.order_status', null)
    .whereNotNull('orders.payment_method')
    .orderBy('orders.order_id', 'desc')
    .offset((pageFilter - 1) * perPage)
    .limit(perPage);

  const customizedProductData = [];
  for (let j = 0; j < ongoing.length; j++) {
    const prd = ongoing[j];
    if (prd.order_status == 'Cancelled') {
      ordstatus = 'Cancelled'
    } else
      if (prd.order_status == 'Completed') {
        ordstatus = 'Completed'
      } else {
        ordstatus = 'In Progress'
      }
    const customizedProduct = {
      cart_id: prd.cart_id,
      group_id: prd.group_id,
      order_date: prd.order_date,
      coupon_discount: prd.coupon_discount,
      price_without_delivery: prd.price_without_delivery,
      user_id: prd.user_id,
      is_subscription: prd.is_subscription,
      si_order: prd.si_order,
      bank_id: prd.bank_id,
      //order_status: ordstatus,
      order_status: prd.order_status
    };



    customizedProductData.push(customizedProduct);
  }
  return customizedProductData
  //return ongoing;
};

const ordersDetails = async (appDetatils) => {
  const { user_id, group_id } = appDetatils || {};
  if (group_id == null || group_id === '') return [];

  const customizedProductData = [];
  const userJoinRaw = knex.raw('users.id = orders.user_id');

  const cartIdsSubquery = () => knex('orders').select('cart_id').where('group_id', group_id);

  const [ongoing, ongoinglist, ongoing1, allSubOrdersByCart, allStoreOrdersByCart, trailStoreOrdersByCart, allProductDetailsRows] = await Promise.all([
    knex('orders')
      .join('users', userJoinRaw)
      .join('address', 'orders.address_id', '=', 'address.address_id')
      .leftJoin('coupon', 'coupon.coupon_id', '=', 'orders.coupon_id')
      .where('orders.group_id', group_id)
      .orderBy('orders.order_id', 'desc')
      .first(),
    knex('orders')
      .where('group_id', group_id)
      .sum('rem_price as rem_price')
      .sum('paid_by_wallet as paid_by_wallet')
      .sum({ cod_charges: knex.raw('COALESCE(cod_charges::numeric, 0)') }),
    knex('orders')
      .join('users', userJoinRaw)
      .join('address', 'orders.address_id', '=', 'address.address_id')
      .where('orders.group_id', group_id)
      .orderBy('orders.order_id', 'desc'),
    knex('subscription_order').whereIn('cart_id', cartIdsSubquery()).select('*'),
    knex('store_orders').whereIn('order_cart_id', cartIdsSubquery()).select('*'),
    knex('store_orders').whereIn('order_cart_id', cartIdsSubquery()).where('order_type', 'trail').select('trail_id', 'order_cart_id'),
    knex
      .select(
        'store_orders.store_order_id',
        'store_orders.product_name',
        'store_orders.varient_image',
        'store_orders.quantity',
        'store_orders.unit',
        'store_orders.varient_id',
        'store_orders.qty',
        knex.raw('(store_orders.price/store_orders.qty) as price'),
        'store_orders.total_mrp',
        'store_orders.order_cart_id',
        'store_orders.order_date',
        'store_orders.repeat_orders',
        'store_orders.type',
        'orders.order_status',
        'orders.trail_id',
        'orders.trail_discount',
        'store_orders.sub_total_delivery',
        'store_orders.subscription_flag',
        'store_orders.order_type',
        'subscription_order.order_status as prodorderstatus',
        'subscription_order.time_slot as prodwisetime_slot',
        'subscription_order.driver_photo',
        'subscription_order.cancel_reason',
        'orders.is_offer_product',
        'store_orders.product_feature_id'
      )
      .from('store_orders')
      .join('orders', 'store_orders.order_cart_id', 'orders.cart_id')
      .join('subscription_order', 'store_orders.order_cart_id', 'subscription_order.cart_id')
      .whereIn('store_orders.order_cart_id', cartIdsSubquery())
  ]);

  if (!ongoing) return [];
  const orderDateRaw = ongoing.order_date;
  if (orderDateRaw == null) return [];
  const orderDate = typeof orderDateRaw === 'object' && orderDateRaw instanceof Date
    ? format(orderDateRaw, 'yyyy-MM-dd')
    : String(orderDateRaw).slice(0, 10) || '';
  const firstSums = (ongoinglist && ongoinglist[0]) ? ongoinglist[0] : {};
  let price_without_delivery = Number(firstSums.rem_price || 0) + Number(firstSums.paid_by_wallet || 0) + Number(firstSums.cod_charges || 0);

  const cartIds = (ongoing1 || []).map(o => o.cart_id);
  if (cartIds.length === 0) return [];

  const productDetailsByCartId = {};
  (allProductDetailsRows || []).forEach(row => {
    const cid = row.order_cart_id;
    if (!productDetailsByCartId[cid]) productDetailsByCartId[cid] = [];
    productDetailsByCartId[cid].push(row);
  });

  const allVarientIds = [];
  const allStoreOrderIds = [];
  const allFeatureIds = [];
  (allProductDetailsRows || []).forEach(row => {
    if (row.varient_id) allVarientIds.push(row.varient_id);
    if (row.store_order_id) allStoreOrderIds.push(row.store_order_id);
    if (row.product_feature_id) allFeatureIds.push(row.product_feature_id);
  });
  const uniqVarientIds = [...new Set(allVarientIds)];
  const uniqStoreOrderIds = [...new Set(allStoreOrderIds)];
  const uniqFeatureIds = [...new Set(allFeatureIds)];

  const trailIds = [...new Set((trailStoreOrdersByCart || []).map(r => r.trail_id).filter(Boolean))].map(t => parseInt(t, 10)).filter(n => !isNaN(n));
  const trailDiscountPairs = (allProductDetailsRows || []).filter(r => r.trail_id).map(r => ({ trail_id: r.trail_id, varient_id: r.varient_id }));
  const pairs = trailDiscountPairs.length > 0 ? [...new Map(trailDiscountPairs.map(p => [`${p.trail_id}|${p.varient_id}`, p]).values()).values()] : [];

  const [varientProductMap, subOrdersByStoreOrderId, productRatingMap, featureValueMap, trailPacks, trailDiscountRows] = await Promise.all([
    uniqVarientIds.length === 0 ? Promise.resolve([]) : knex('product_varient').select('*').join('product', 'product.product_id', '=', 'product_varient.product_id').whereIn('product_varient.varient_id', uniqVarientIds),
    uniqStoreOrderIds.length === 0 ? Promise.resolve([]) : knex('subscription_order').whereIn('store_order_id', uniqStoreOrderIds).select('*'),
    (uniqVarientIds.length === 0 || !user_id) ? Promise.resolve([]) : knex('product_rating').where('user_id', user_id).whereIn('cart_id', cartIds).whereIn('varient_id', uniqVarientIds).select('cart_id', 'varient_id', 'description', 'rating'),
    uniqFeatureIds.length === 0 ? Promise.resolve([]) : knex('tbl_feature_value_master').whereIn('id', uniqFeatureIds).select('id', 'feature_value'),
    trailIds.length === 0 ? Promise.resolve([]) : knex('tbl_trail_pack_basic').whereIn('id', trailIds).select('id', 'discount_percentage'),
    (pairs.length > 0 && user_id) ? knex('store_orders')
      .join('product_varient', 'product_varient.varient_id', 'store_orders.varient_id')
      .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .join('tbl_trail_pack_basic', knex.raw('store_orders.trail_id::int = tbl_trail_pack_basic.id'))
      .where('store_orders.store_approval', String(user_id))
      .whereIn(['store_orders.trail_id', 'store_orders.varient_id'], pairs.map(p => [p.trail_id, p.varient_id]))
      .select('store_orders.trail_id', 'store_orders.varient_id', knex.raw('ROUND(SUM(store_products.price * (1 - tbl_trail_pack_basic.discount_percentage::numeric / 100))::numeric, 2) as total_discounted_price'))
      .groupBy('store_orders.trail_id', 'store_orders.varient_id') : Promise.resolve([])
  ]);

  let trailDiscountByTrailId = {};
  (trailPacks || []).forEach(t => { trailDiscountByTrailId[String(t.id)] = t.discount_percentage; });

  const varientProductByVarientId = {};
  (varientProductMap || []).forEach(row => { varientProductByVarientId[row.varient_id] = row; });
  const subOrdersBySoid = {};
  (subOrdersByStoreOrderId || []).forEach(row => {
    if (!subOrdersBySoid[row.store_order_id]) subOrdersBySoid[row.store_order_id] = [];
    subOrdersBySoid[row.store_order_id].push(row);
  });
  const productRatingByCartVarient = {};
  (productRatingMap || []).forEach(row => { productRatingByCartVarient[`${row.cart_id}|${row.varient_id}`] = row; });
  const featureValueById = {};
  (featureValueMap || []).forEach(row => { featureValueById[row.id] = row.feature_value; });

  const trailDiscountMap = {};
  (trailDiscountRows || []).forEach(row => {
    trailDiscountMap[`${row.trail_id}|${row.varient_id}`] = row.total_discounted_price;
  });

  const customizedOrderData = [];
  let total_price = 0;
  let coupon_discount = 0;
  let startDeliveryDate = null;
  let trailpackdiscount = 0;
  let discounted_priceval = 0;
  let orderStatus;

  const currentDate = new Date();

  for (let i = 0; i < ongoing1.length; i++) {
    const ProductList = ongoing1[i];
    if (ProductList.order_status != 'Cancelled') orderStatus = ProductList.order_status;

    const subOrdersForCart = (allSubOrdersByCart || []).filter(so => so.cart_id === ProductList.cart_id);
    const subscriptionOrderList = subOrdersForCart.filter(so => so.order_status === 'Pending').sort((a, b) => new Date(a.delivery_date) - new Date(b.delivery_date))[0] || null;
    startDeliveryDate = subscriptionOrderList ? subscriptionOrderList.delivery_date : null;
    const subOrdList = subOrdersForCart[0] || null;
    const suborderstatus = subOrdList?.order_status ?? null;

    const trailstoreorder = (trailStoreOrdersByCart || []).find(t => t.order_cart_id === ProductList.cart_id);
    if (trailstoreorder && trailstoreorder.trail_id != null) {
      trailpackdiscount = trailDiscountByTrailId[String(trailstoreorder.trail_id)] ?? 0;
    } else {
      trailpackdiscount = 0;
    }

    const ordersss = (allStoreOrdersByCart || []).filter(so => so.order_cart_id === ProductList.cart_id);
    for (const store of ordersss) {
      let total_delivery_week;
      if (store.repeat_orders) {
        const repeat_orderss = store.repeat_orders.split(',').map(o => o.trim());
        total_delivery_week = repeat_orderss.length * ProductList.total_delivery;
      } else {
        total_delivery_week = 1 * ProductList.total_delivery;
      }
      total_price += store.price * total_delivery_week;
    }
    coupon_discount += ProductList.coupon_discount || 0;

    const productDetails = productDetailsByCartId[ProductList.cart_id] || [];


    let discounted_priceval = 0;
    for (let j = 0; j < productDetails.length; j++) {
      const itemProduct = productDetails[j];
      if (!itemProduct) continue;

      const dataasss = varientProductByVarientId[itemProduct.varient_id] || null;

      const subOrdersForItem = subOrdersBySoid[itemProduct.store_order_id] || [];
      const subscription_order_o = subOrdersForItem.find(so => so.order_status === 'Pending' && new Date(so.delivery_date) >= currentDate);
      let next_delivery_date = subscription_order_o ? subscription_order_o.delivery_date : undefined;

      let orderStatusDelivery;
      if (next_delivery_date) {
        next_delivery_date = format(next_delivery_date, 'yyyy-MM-dd');
        orderStatusDelivery = 'Active';
      } else {
        const completed = subOrdersForItem.filter(so => so.order_status === 'Completed');
        const pending = subOrdersForItem.filter(so => so.order_status === 'Pending');
        if (completed.length > 0 || pending.length > 0) {
          orderStatusDelivery = 'Completed';
        } else {
          const cancelled = subOrdersForItem.filter(so => so.order_status === 'Cancelled');
          orderStatusDelivery = cancelled.length > 0 ? '' : 'Pending';
        }
        next_delivery_date = '';
      }
      if (ongoing.order_status == 'Cancelled') orderStatusDelivery = 'Cancelled';

      let ordstatus = itemProduct.order_status === 'Pending' ? 'In Progress' : itemProduct.order_status === 'Processing_payment' ? 'Processing Payment' : itemProduct.order_status === 'Payment_failed' ? 'Payment Failed' : itemProduct.order_status;

      const prodrating = productRatingByCartVarient[`${ProductList.cart_id}|${itemProduct.varient_id}`] || null;
      const rating = prodrating ? prodrating.rating : null;
      const review = prodrating ? prodrating.description : null;

      if (itemProduct.trail_id) {
        discounted_priceval = trailDiscountMap[`${itemProduct.trail_id}|${itemProduct.varient_id}`] ?? itemProduct.price;
      } else {
        discounted_priceval = itemProduct.price;
      }

      itemProduct.product_feature_value = (itemProduct.product_feature_id != null && featureValueById[itemProduct.product_feature_id] != null) ? featureValueById[itemProduct.product_feature_id] : null;
      const productOfferDate = dataasss?.offer_date ? String(dataasss.offer_date).slice(0, 10) : '';
      const itemOrderDate = itemProduct.order_date ? String(itemProduct.order_date).slice(0, 10) : '';
      const isOfferProductForResponse =
        Number(dataasss?.is_offer_product) === 1 && productOfferDate !== '' && productOfferDate === itemOrderDate ? 1 : 0;

      const baseurl = process.env.BUNNY_NET_IMAGE;



      const customizedProduct = {
        store_order_id: itemProduct.store_order_id,
        varient_id: itemProduct.varient_id,
        product_name: itemProduct.product_name,
        varient_image: baseurl + itemProduct.varient_image,
        thumbnail: dataasss?.thumbnail ? baseurl + dataasss.thumbnail : baseurl + 'default.png',
        unit: itemProduct.quantity + itemProduct.unit,
        // varient_id: itemProduct.orderStatusDeliveryvarient_id,
        product_id: dataasss ? dataasss.product_id : 0,
        qty: itemProduct.qty,
        price: itemProduct.price,
        discounted_price: discounted_priceval,
        total_mrp: itemProduct.total_mrp,
        trail_discount: itemProduct.trail_discount,
        order_date: itemProduct.order_date,
        repeat_orders: itemProduct.repeat_orders,
        repeat_orders: itemProduct.repeat_orders,
        type: itemProduct.type,
        rating: rating,
        review: review,
        // order_status_delivery:orderStatusDelivery,
        prodorder_status: ordstatus, //itemProduct.prodorderstatus,
        prodwisetime_slot: itemProduct.prodwisetime_slot,
        order_status_delivery: itemProduct.order_status,
        order_cart_id: itemProduct.order_cart_id,
        cancel_reason: itemProduct.cancel_reason,
        is_offer_product: isOfferProductForResponse,
        product_feature_id: (itemProduct.product_feature_id) ? itemProduct.product_feature_id : null,
        product_feature_value: itemProduct.product_feature_value,
        //  next_delivery_date:next_delivery_date,
        // Add or modify properties as needed
      };



      customizedProductData.push(customizedProduct);
    }

  }



  const startDeliveryDates = (ongoing.delivery_date) ? format(ongoing.delivery_date, 'yyyy-MM-dd') : "null";

  const [ongoinglistnew, grouplistcancel, grouplisttotal, ratingData, driverphoto, subinvoice, specialinstruction, deliverypartnerinstruction] = await Promise.all([
    knex('orders').where('group_id', group_id)
      .sum('total_products_mrp as total_products_mrp')
      .sum('rem_price as rem_price')
      .sum('paid_by_wallet as paid_by_wallet')
      .sum('paid_by_ref_wallet as paid_by_ref_wallet')
      .sum({ cod_charges: knex.raw('COALESCE(cod_charges::numeric, 0)') })
      .sum('coupon_discount as coupon_discount')
      .sum({ del_partner_tip: knex.raw('COALESCE(del_partner_tip::numeric, 0)') })
      .sum({ trail_discount: knex.raw('COALESCE(trail_discount::numeric, 0)') }),
    knex('orders').where('group_id', group_id).where('order_status', 'Cancelled').select('group_id'),
    knex('orders').where('group_id', group_id).select('group_id'),
    knex('delivery_rating').where('user_id', user_id).whereIn('cart_id', cartIds).select('cart_id', 'description', 'rating').first(),
    knex('subscription_order').where('group_id', group_id).select('driver_photo').first(),
    knex('subscription_order').where('cart_id', ongoing.cart_id).select('invoice_path').first(),
    knex('subscription_order')
      .join('orders', 'orders.cart_id', 'subscription_order.cart_id')
      .where('subscription_order.group_id', group_id)
      .where('orders.order_status', '!=', 'Cancelled')
      .distinct('orders.order_instruction')
      .select('orders.order_instruction'),
    knex('subscription_order')
      .join('orders', 'orders.cart_id', 'subscription_order.cart_id')
      .where('subscription_order.group_id', group_id)
      .where('orders.order_status', '!=', 'Cancelled')
      .distinct('orders.del_partner_instruction')
      .select('orders.del_partner_instruction')
  ]);

  const newSums = (ongoinglistnew && ongoinglistnew[0]) ? ongoinglistnew[0] : {};
  price_without_delivery = Number(newSums.total_products_mrp) || 0;
  const total_products_mrp = Number(newSums.total_products_mrp) || 0;
  const cod_charges = Number(newSums.cod_charges) || 0;
  const del_partner_tip = Number(newSums.del_partner_tip) || 0;
  const coupon_discounts = Number(newSums.coupon_discount) || 0;
  const paid_by_wallets = Number(newSums.paid_by_wallet) || 0;
  const paid_by_ref_wallets = Number(newSums.paid_by_ref_wallet) || 0;
  let trail_discount = newSums.trail_discount != null ? Number(newSums.trail_discount).toFixed(2) : '0';

  let groupstatus;
  if (grouplistcancel.length === grouplisttotal.length) {
    groupstatus = 'Cancelled';
  } else if (orderStatus) {
    groupstatus = orderStatus;
  } else {
    groupstatus = 'Pending';
  }

  let orderType = ongoing.order_type;
  orderType = orderType === 'trail' ? 'trail' : 'daily';
  let finalprice_without_delivery;
  if (orderType === 'trail') {
    finalprice_without_delivery = parseFloat(price_without_delivery) + parseFloat(trail_discount);
  } else {
    finalprice_without_delivery = parseFloat(price_without_delivery);
  }

  // Adjust price_without_delivery at the end by subtracting coupon_discount, trail_discount, and paid_by_wallet.
  // NOTE: In checkout flows, `orders.total_products_mrp` is already stored AFTER coupon/trail discounts.
  // To avoid double-deduction, treat `price_without_delivery` (from SUM(total_products_mrp)) as the net value.
  // (paid_by_wallet includes paid_by_ref_wallet)
  const paidByWalletTotal = Number(paid_by_wallets) + Number(paid_by_ref_wallets);
  const trailDiscountNum = Number(trail_discount) || 0;
  const grossBeforeDiscounts = Number(price_without_delivery) + Number(coupon_discounts) + Number(trailDiscountNum);
  finalprice_without_delivery =
    Number(grossBeforeDiscounts) - Number(coupon_discounts) - Number(trailDiscountNum) - Number(paidByWalletTotal);

  let specialinstructionval = '';
  if (specialinstruction && specialinstruction.length > 0) {
    specialinstructionval = specialinstruction.map(row => row.order_instruction).join(', ');
  }

  let deliverypartnerinstructionval = '';
  if (deliverypartnerinstruction && deliverypartnerinstruction.length > 0) {
    deliverypartnerinstructionval = deliverypartnerinstruction.map(row => row.del_partner_instruction).join(', ');
  }

  const imgurl = process.env.BASE_URL;
  const baseurl2 = process.env.BUNNY_NET_IMAGE;

  const clampToZeroMoney = (val) => {
    const num = Number(val);
    if (!Number.isFinite(num)) return '0.00';
    return (num < 0 ? 0 : num).toFixed(2);
  };

  const customizedData = {
    group_id: ongoing.group_id,
    address_name: ongoing.receiver_name,
    //delivery_address: ongoing.building_villa+","+ongoing.street+","+ongoing.society+","+ongoing.city,
    delivery_address: ongoing.house_no,
    order_status: groupstatus,
    delivery_date: startDeliveryDates,
    time_slot: ongoing.time_slot,
    // payment_method: ongoing.si_order == 'no' ? 'Wallet' : 'Card Payment',
    payment_method: ongoing.payment_method,
    cart_id: ongoing.cart_id,
    total_price: clampToZeroMoney(parseFloat(price_without_delivery) + parseFloat(coupon_discounts)),
    delivery_charge: ongoing.delivery_charge,

    // price_without_delivery:total_price,
    price_without_delivery: clampToZeroMoney(finalprice_without_delivery),
    cod_charges: Math.round(cod_charges),
    del_partner_tip: Math.round(del_partner_tip),
    user_id: ongoing.user_id,
    total_products_mrp: total_price,
    cancelling_reason: ongoing.cancelling_reason,
    //order_date: order_date,
    order_date: orderDate,
    coupon_code: ongoing.coupon_code,
    coupon_discount: coupon_discounts.toFixed(2),
    paid_by_wallet: (parseFloat(paid_by_wallets) + parseFloat(paid_by_ref_wallets)).toFixed(2),
    is_subscription: ongoing.is_subscription,
    trail_discount: trail_discount,
    traildiscountpercentage: trailpackdiscount,
    total_delivery: ongoing.total_delivery,
    repeat_orders: ongoing.repeat_orders,
    si_order: ongoing.si_order,
    vat: '0.0',
    drating: (ratingData) ? ratingData.rating : 0,
    dreview: (ratingData) ? ratingData.description : '',
    order_type: orderType,
    prodwiseinvoice: (() => {
      const path = subinvoice?.invoice_path;
      if (path) {
        let fullUrl;
        if (path.startsWith('https://www.quickart.ae')) {
          fullUrl = path;
        } else {
          const filename = path.split('/').pop();
          fullUrl = process.env.BUNNY_NET_INVOICE_PDF + filename;
        }
        console.log(`[OrdersDetails] Invoice Path found: "${path}" -> Final URL: "${fullUrl}"`);
        return fullUrl;
      }
      console.log(`[OrdersDetails] No invoice path found for cart_id: ${ongoing.cart_id}`);
      return null;
    })(),
    delivery_proof: driverphoto?.driver_photo ? baseurl2 + driverphoto.driver_photo : null,
    special_instruction: specialinstructionval,
    delivery_partner_instruction: deliverypartnerinstructionval,
    data: customizedProductData,

  };
  customizedOrderData.push(customizedData);
  return customizedOrderData;
};

const ordersDetailsold = async (appDetatils) => {

  const { user_id, cart_id } = appDetatils;
  const customizedProductData = [];
  const ongoing = await knex('orders')
    .join('users', 'orders.user_id', '=', 'users.id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .where('orders.cart_id', cart_id)
    .orderBy('orders.order_id', 'desc')
    .first();

  const subscriptionOrderList = await knex('subscription_order')
    .where('cart_id', cart_id)
    .where('order_status', 'Pending')
    .orderBy('delivery_date', 'asc')
    .first();

  const startDeliveryDate = subscriptionOrderList ? subscriptionOrderList.delivery_date : null;


  const ordersss = await knex('store_orders')
    .where('order_cart_id', cart_id)
    .select();

  let total_price = 0;

  for (const store of ordersss) {
    if (store.repeat_orders) {
      const repeat_orderss = store.repeat_orders.split(',').map(order => order.trim());
      total_delivery_week = repeat_orderss.length * ongoing.total_delivery;
    } else {
      const repeat_orderss = 1
      total_delivery_week = repeat_orderss * ongoing.total_delivery;
    }

    total_price += store.price * total_delivery_week;
  }

  const productDetails = await knex
    .select(
      'store_order_id',
      'product_name',
      'varient_image',
      'quantity',
      'unit',
      'varient_id',
      'qty',
      knex.raw('(price/qty) as price'),
      'total_mrp',
      'order_cart_id',
      'order_date',
      'repeat_orders',
      'type'
    )
    .from('store_orders')
    .where('order_cart_id', ongoing.cart_id);

  const customizedOrderData = [];
  for (let j = 0; j < productDetails.length; j++) {
    const itemProduct = productDetails[j];

    const dataasss = await knex('product_varient')
      .select('*')
      .join('product', 'product.product_id', '=', 'product_varient.product_id')
      .where('product_varient.varient_id', '=', itemProduct.varient_id)
      .first();

    const currentDate = new Date();
    let next_delivery_date = '';
    const subscription_order_o = await knex('subscription_order')
      .where('store_order_id', itemProduct.store_order_id)
      .where('delivery_date', '>=', currentDate)
      .where('order_status', 'Pending')
      .first();

    next_delivery_date = subscription_order_o ? subscription_order_o.delivery_date : undefined;

    let orderStatusDelivery;

    if (next_delivery_date) {
      next_delivery_date = format(next_delivery_date, 'yyyy-MM-dd');
      orderStatusDelivery = 'Active';
    } else {
      const subscription_order_completed = await knex('subscription_order')
        .where('store_order_id', itemProduct.store_order_id)
        .where('order_status', 'Completed')
        .select('*');

      const subscription_order_pending = await knex('subscription_order')
        .where('store_order_id', itemProduct.store_order_id)
        .where('order_status', 'Pending')
        .select('*');

      if (subscription_order_completed.length > 0 || subscription_order_pending.length > 0) {
        orderStatusDelivery = 'Completed';
      } else {

        const subscription_order_cancelled = await knex('subscription_order')
          .where('store_order_id', itemProduct.store_order_id)
          .where('order_status', 'Cancelled')
          .select('*');

        if (subscription_order_cancelled.length > 0) {
          //orderStatusDelivery = 'Cancelled';
          orderStatusDelivery = '';
        } else {
          orderStatusDelivery = 'Pending';
        }

      }
      next_delivery_date = '';
    }

    if (ongoing.order_status == 'Cancelled') {
      orderStatusDelivery = 'Cancelled'
    }
    const baseurl = process.env.BUNNY_NET_IMAGE;
    const customizedProduct = {
      store_order_id: itemProduct.store_order_id,
      product_name: itemProduct.product_name,
      varient_image: baseurl + itemProduct.varient_image,
      thumbnail: baseurl + dataasss.thumbnail,
      unit: itemProduct.unit,
      varient_id: itemProduct.orderStatusDeliveryvarient_id,
      qty: itemProduct.qty,
      price: itemProduct.price,
      total_mrp: itemProduct.total_mrp,
      order_date: itemProduct.order_date,
      repeat_orders: itemProduct.repeat_orders,
      type: itemProduct.type,
      order_status_delivery: orderStatusDelivery,
      next_delivery_date: next_delivery_date,
      // Add or modify properties as needed
    };



    customizedProductData.push(customizedProduct);
  }

  let startDeliveryDates = startDeliveryDate ? format(startDeliveryDate, 'yyyy-MM-dd') : "null";
  let order_date = format(ongoing.order_date, 'yyyy-MM-dd');

  const customizedData = {
    address_name: ongoing.receiver_name,
    delivery_address: ongoing.building_villa + "," + ongoing.street + "," + ongoing.society + "," + ongoing.city,
    order_status: ongoing.order_status,
    delivery_date: startDeliveryDates,
    time_slot: ongoing.time_slot,
    payment_method: ongoing.si_order == 'no' ? 'Wallet' : 'Card Payment',
    cart_id: ongoing.cart_id,
    total_price: total_price,
    delivery_charge: ongoing.delivery_charge,
    coupon_discount: ongoing.coupon_discount,
    price_without_delivery: total_price,
    user_id: ongoing.user_id,
    total_products_mrp: total_price,
    cancelling_reason: ongoing.cancelling_reason,
    order_date: order_date,
    coupon_id: ongoing.coupon_id,
    is_subscription: ongoing.is_subscription,
    total_delivery: ongoing.total_delivery,
    repeat_orders: ongoing.repeat_orders,
    si_order: ongoing.si_order,
    data: customizedProductData,
  };
  customizedOrderData.push(customizedData);
  return customizedOrderData;

};

const getQuickRepeatedPlaceOrder = async (appDetails) => {
  // Destructuring variables from appDetails
  const { cart_id, user_id, order_type, replace_status } = appDetails;

  // Check if required parameters are present
  if (cart_id && user_id && (replace_status === 0 || replace_status === 1)) {
    // Fetch orders that have the cart_id as repeated_order_cart
    const orders_detailsss = await knex('store_orders')
      .where('repeated_order_cart', '=', cart_id)
      .where('order_cart_id', '=', 'incart');

    // If no existing repeated orders, proceed
    if (orders_detailsss.length === 0) {
      // Fetch orders with the current cart_id

      const ordersDetails = await knex('orders').where('cart_id', '=', cart_id).first();
      const groupID = ordersDetails.group_id;
      const orderData = await knex('orders').where('group_id', groupID).where('is_offer_product', 0).pluck('cart_id');
      const orders_details = await knex('store_orders').whereIn('order_cart_id', orderData);
      // If replace_status is 0, delete existing in-cart orders for the user
      if (replace_status === 0) {
        await knex('store_orders')
          .where('store_approval', user_id)
          .where('order_cart_id', '=', 'incart')
          .whereNull('subscription_flag')
          .delete();
      }

      // Iterate over each order detail to insert or update records
      for (const ordersdetails of orders_details) {
        // Check if the order with the same varient_id already exists
        const rec = await knex('store_orders')
          .where('store_approval', user_id)
          .where('varient_id', ordersdetails.varient_id)
          .where('order_cart_id', '=', 'incart');

        // If record exists, delete it to update with new values
        if (rec.length > 0) {
          await knex('store_orders')
            .where('store_approval', user_id)
            .where('varient_id', ordersdetails.varient_id)
            .where('order_cart_id', '=', 'incart')
            .whereNull('subscription_flag')
            .delete();
        }

        // Fetch variant details
        const varient_details = await knex('store_products')
          .where('varient_id', '=', ordersdetails.varient_id)
          .first();

        // Stock validation: if stock is 0/null (or missing row), abort with special error
        const availableStock = varient_details?.stock;
        if (availableStock === null || availableStock === undefined || Number(availableStock) <= 0) {
          throw new Error('OUT_OF_STOCK');
        }
        if (Number(ordersdetails.qty || 0) > Number(availableStock)) {
          throw new Error('OUT_OF_STOCK');
        }

        let totalMrp;
        // Calculate total MRP based on order type
        if (order_type === 'subscription') {
          const repeatOrders = (ordersdetails.repeat_orders || '').trim().split(',').filter(Boolean);
          totalMrp = repeatOrders.length * (ordersdetails.qty * varient_details.price);
        } else if (order_type === 'quick') {
          totalMrp = (ordersdetails.qty * varient_details.price);
        }

        // PostgreSQL: store_order_id is NOT NULL; generate next id
        const store_order_id = await getNextStoreOrderId();
        // Insert new order with current details
        await knex('store_orders').insert({
          store_order_id,
          product_name: ordersdetails.product_name,
          varient_image: ordersdetails.varient_image,
          quantity: ordersdetails.quantity,
          unit: ordersdetails.unit,
          varient_id: ordersdetails.varient_id,
          qty: ordersdetails.qty,
          price: (ordersdetails.qty * varient_details.price),
          total_mrp: totalMrp,
          order_cart_id: 'incart',
          order_date: new Date(), // Uses the current date and time
          repeat_orders: ordersdetails.repeat_orders,
          store_approval: user_id,
          store_id: ordersdetails.store_id,
          description: ordersdetails.description,
          tx_per: ordersdetails.tx_per,
          price_without_tax: varient_details.price,
          tx_price: ordersdetails.tx_price,
          type: ordersdetails.type,
          repeated_order_cart: ordersdetails.order_cart_id,
          buying_price: ordersdetails.buying_price,
          base_mrp: ordersdetails.base_mrp,
          subscription_flag: ordersdetails.subscription_flag != null ? String(ordersdetails.subscription_flag) : null,
          sub_total_delivery: ordersdetails.sub_total_delivery,
          sub_time_slot: null,
          sub_delivery_date: null,
          percentage: ordersdetails.percentage,
          is_offer_product: ordersdetails.is_offer_product,
        });
      }

      return 1; // Success
    } else {
      throw new Error('Order already added.');
    }
  } else {
    throw new Error('Invalid parameters.');
  }
};

const getProductOngoingsub = async (appDetatils) => {
  const user_id = appDetatils.user_id;
  const store_id = appDetatils.store_id;
  const groupID = appDetatils.group_id;
  if (!user_id || user_id === "" || user_id === "null") {
    return [];
  }
  const userIdStr = user_id != null ? String(user_id) : '';

  // PostgreSQL: cannot GROUP BY cart_id only while selecting bare store_orders columns.
  // Sum line prices per cart; pick one representative line (highest store_order_id) via LATERAL;
  // one orders row per cart (highest order_id) via DISTINCT ON — matches former MySQL loose GROUP BY.
  const { rows: ongoing } = await knex.raw(
    `
    SELECT * FROM (
      SELECT DISTINCT ON (o.cart_id)
        so_pick.varient_id,
        o.cart_id,
        TO_CHAR(o.delivery_date::date, 'YYYY-MM-DD') AS delivery_date,
        TO_CHAR(o.order_date::date, 'YYYY-MM-DD') AS order_date,
        o.total_delivery,
        lt.sum_price AS price,
        so_pick.store_order_id,
        so_pick.product_name,
        so_pick.total_mrp,
        so_pick.repeat_orders,
        o.order_status,
        o.si_order,
        o.si_sub_ref_no,
        o.pastorecentrder,
        so_pick.varient_image,
        o.order_id
      FROM orders o
      INNER JOIN (
        SELECT order_cart_id, SUM(price) AS sum_price
        FROM store_orders
        GROUP BY order_cart_id
      ) lt ON o.cart_id = lt.order_cart_id
      INNER JOIN LATERAL (
        SELECT so.*
        FROM store_orders so
        WHERE so.order_cart_id = o.cart_id
        ORDER BY so.store_order_id DESC NULLS LAST
        LIMIT 1
      ) so_pick ON true
      WHERE o.cart_id != 'incart'
        AND o.user_id::text = ?
        AND o.store_id = ?
        AND o.group_id = ?
        AND o.order_status IS NOT NULL
        AND o.is_subscription = 1
        AND o.payment_method IS NOT NULL
      ORDER BY o.cart_id, o.order_id DESC
    ) sub
    ORDER BY sub.order_id DESC
    `,
    [userIdStr, store_id, groupID]
  );

  const customizedProductData = [];
  for (let i = 0; i < ongoing.length; i++) {
    const ProductList = ongoing[i];
    ordersssss = await knex('subscription_order')
      .where('cart_id', ProductList.cart_id)
      .where('store_order_id', ProductList.store_order_id)
      .where('order_status', 'Pending')
      .orderBy('id', 'DESC')
      .first();
    if (ordersssss) {
      subscription_id = ordersssss.subscription_id
    } else {
      subscription_id = '';
    }

    // const  subscriptionOrder= await knex('subscription_order')
    // .where('cart_id',ProductList.cart_id)
    // // .where('store_order_id',ProductList.store_order_id)
    // .where('order_status','Pending')
    // .orderBy('id','ASC')
    // .first();

    let today = new Date();
    let yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);

    const subscriptionOrder = await knex("subscription_order")
      .select(knex.raw("TO_CHAR(delivery_date::date, 'YYYY-MM-DD') as delivery_date"))
      .where('cart_id', ProductList.cart_id)
      // .where('delivery_date', '>=', today) // Fetch orders with delivery date today or later
      .where('delivery_date', '>=', yesterday)
      .orderBy('delivery_date', 'asc') // Sort by nearest delivery date
      .first();

    //  const  deliveryDate = (subscriptionOrder) ? subscriptionOrder.delivery_date : '';
    const formattedDeliveryDate = subscriptionOrder ? format(new Date(subscriptionOrder.delivery_date), 'yyyy-MM-dd') : '';

    const pendingResult = await knex('subscription_order')
      .where('cart_id', ProductList.cart_id)
      .where('order_status', 'Pending')
      .count({ pending_count: 'order_id' });


    const AllResult = await knex('subscription_order')
      .where('cart_id', ProductList.cart_id)
      .andWhere(function () {
        this.where('order_status', 'Pending')
          .orWhere('order_status', 'Completed')
          .orWhere('order_status', 'Confirmed')
          .orWhere('order_status', 'Out_For_Delivery');
      })
      .count({ all_count: 'order_id' });


    const pendingCount = pendingResult[0].pending_count;
    const allCount = AllResult[0].all_count;

    delivery_date = ProductList.delivery_date;

    if (pendingCount > 0) {
      order_status = "Pending";
    }
    else {
      order_status = "Completed";
    }

    const cancelledResult = await knex('subscription_order')
      .where('cart_id', ProductList.cart_id)
      .where('order_status', 'Cancelled')
      .count({ cancelled_count: 'order_id' });
    const cancelledCount = cancelledResult[0].cancelled_count;
    if (cancelledCount > 0 && allCount <= 0) {
      order_status = "Cancelled";
    }

    const completedResult = await knex('subscription_order')
      .where('cart_id', ProductList.cart_id)
      .where('order_status', 'Completed')
      .count({ completed_count: 'order_id' });
    const completedCount = completedResult[0].completed_count;
    if (completedCount > 0 && pendingCount > 0) {
      order_status = "Inprogress";
    }


    const pastorecentrder = ProductList.pastorecentrder;

    parentOrders = await knex('orders')
      .where('cart_id', ProductList.cart_id)
      .first();
    let totalMRP = (pastorecentrder == 'old') ? (parentOrders.total_products_mrp) : ProductList.price;
    const baseurl = process.env.BUNNY_NET_IMAGE;
    const prodrating = await knex('product_rating')
      .where('user_id', user_id)
      .where('cart_id', ProductList.cart_id)
      .where('varient_id', ProductList.varient_id)
      .select('description', 'rating')
      .first();
    if (prodrating) {
      rating = prodrating.rating;
      review = prodrating.description;
    } else {
      rating = null;
      review = null;
    }

    prodvar = await knex('product_varient')
      .where('varient_id', ProductList.varient_id)
      .select('product_id')
      .first();

    const customizedProduct = {
      cart_id: ProductList.cart_id,
      order_date: ProductList.order_date,
      delivery_date: formattedDeliveryDate,
      total_delivery: ProductList.total_delivery,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.varient_image + "?width=200&height=200&quality=100",
      total_mrp: totalMRP,
      repeat_orders: ProductList.repeat_orders,
      order_status: order_status,
      store_order_id: ProductList.store_order_id,
      subscription_id: subscription_id,
      si_order: ProductList.si_order,
      si_sub_ref_no: ProductList.si_sub_ref_no,
      pastorecentrder: ProductList.pastorecentrder,
      review: review,
      rating: rating,
      product_id: prodvar.product_id,
      varient_id: ProductList.varient_id,
      // Add or modify properties as needed
    };

    customizedProductData.push(customizedProduct);
  }
  return customizedProductData;
};


const getOngoingsublist = async (appDetails) => {
  const { user_id, store_id, page: pageFilter, perpage: perPage } = appDetails || {};
  if (!user_id || user_id === "" || user_id === "null") {
    return [];
  }
  const page = Math.max(1, parseInt(pageFilter, 10) || 1);
  const perPageVal = Math.min(100, Math.max(1, parseInt(perPage, 10) || 10));
  const offset = (page - 1) * perPageVal;
  const userIdStr = user_id != null ? String(user_id) : '';
  const userIdInt = parseInt(user_id, 10) || 0;

  // 1) Get paginated group_ids (PostgreSQL: valid GROUP BY, no sql_mode)
  const pageGroupIds = await knex('orders')
    .select('orders.group_id')
    .where('orders.cart_id', '!=', 'incart')
    .where('orders.user_id', userIdStr)
    .where('orders.store_id', store_id)
    .whereNotNull('orders.order_status')
    .whereNot('orders.order_status', 'Order_abandoned')
    .where('orders.is_subscription', 1)
    .whereNotNull('orders.payment_method')
    .groupBy('orders.group_id')
    .orderByRaw('MAX(orders.order_id) DESC')
    .limit(perPageVal)
    .offset(offset);

  const groupIds = pageGroupIds.map(r => r.group_id).filter(Boolean);
  if (groupIds.length === 0) {
    return [];
  }

  // 2) Full rows for these group_ids (PostgreSQL date formatting)
  const rows = await knex('orders')
    .select(
      'orders.order_id',
      'orders.group_id',
      'orders.cart_id',
      knex.raw("to_char(orders.delivery_date::date, 'YYYY-MM-DD') as delivery_date"),
      knex.raw("to_char(orders.order_date::date, 'YYYY-MM-DD') as order_date"),
      'orders.total_delivery',
      'orders.total_products_mrp',
      'store_orders.price',
      'store_orders.store_order_id',
      'store_orders.product_name',
      'store_orders.total_mrp',
      'store_orders.repeat_orders',
      'orders.order_status',
      'orders.si_order',
      'orders.si_sub_ref_no',
      'orders.pastorecentrder'
    )
    .join('store_orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .whereIn('orders.group_id', groupIds)
    .where('orders.cart_id', '!=', 'incart')
    .whereNot('orders.order_status', 'Order_abandoned')
    .orderBy('orders.order_id', 'DESC');

  // Build one row per group with correct SUM(total_products_mrp) per order (once per order_id)
  const byGroup = {};
  const summedOrderIdsByGroup = {};
  for (const r of rows) {
    if (!byGroup[r.group_id]) {
      byGroup[r.group_id] = { ...r, totalProductsMrp: 0 };
      summedOrderIdsByGroup[r.group_id] = new Set();
    }
    if (!summedOrderIdsByGroup[r.group_id].has(r.order_id)) {
      summedOrderIdsByGroup[r.group_id].add(r.order_id);
      byGroup[r.group_id].totalProductsMrp += parseFloat(r.total_products_mrp || 0);
    }
  }
  const ongoing = Object.values(byGroup);
  const cartIds = ongoing.map(o => o.cart_id);
  const storeOrderIds = ongoing.map(o => o.store_order_id);
  const formattedToday = format(new Date(), 'yyyy-MM-dd');

  // 3) Single batch of parallel queries (no N+1)
  const [
    subscriptionOrders,
    orderDetails,
    deliveryRatings,
    pendingGroupIdsRows,
    completedGroupIdsRows,
    productDetailsRows
  ] = await Promise.all([
    knex('subscription_order')
      .whereIn('cart_id', cartIds)
      .whereIn('store_order_id', storeOrderIds)
      .where('order_status', 'Pending')
      .orderBy('id', 'DESC')
      .select('cart_id', 'store_order_id', 'subscription_id', 'delivery_date', 'order_status'),
    knex('orders')
      .whereIn('cart_id', cartIds)
      .whereNot('order_status', 'Order_abandoned')
      .select('cart_id', 'total_products_mrp', 'pastorecentrder'),
    knex('delivery_rating')
      .where('user_id', userIdInt)
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'description', 'rating'),
    knex('subscription_order')
      .distinct('group_id')
      .whereIn('group_id', groupIds)
      .where('order_status', 'Pending')
      .whereRaw('delivery_date >= ?::date', [formattedToday]),
    knex('subscription_order')
      .distinct('group_id')
      .whereIn('group_id', groupIds)
      .where('order_status', 'Completed'),
    knex('orders')
      .select('orders.group_id', knex.raw("STRING_AGG(store_orders.product_name || ' X ' || store_orders.qty::text, ',') as product_details"))
      .join('store_orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
      .whereIn('orders.group_id', groupIds)
      .whereNot('orders.order_status', 'Order_abandoned')
      .groupBy('orders.group_id')
  ]);

  const groupIdsWithPending = new Set((pendingGroupIdsRows || []).map(r => r.group_id));
  const groupIdsWithCompleted = new Set((completedGroupIdsRows || []).map(r => r.group_id));
  const deliveryRatingsMap = Object.fromEntries(
    (deliveryRatings || []).map(rating => [rating.cart_id, { rating: rating.rating, description: rating.description }])
  );
  const productDetailsMap = Object.fromEntries(
    (productDetailsRows || []).map(r => [r.group_id, r.product_details || ''])
  );

  // 4) In-memory mapping only (no per-row DB calls)
  const customizedProductData = ongoing.map(product => {
    const subscriptionOrder = (subscriptionOrders || []).find(
      o => o.cart_id === product.cart_id && o.store_order_id === product.store_order_id
    );
    const orderDetail = (orderDetails || []).find(o => o.cart_id === product.cart_id);
    const ratingData = deliveryRatingsMap[product.cart_id] || { rating: null, description: null };

    const totalProductsMrp =
      product.pastorecentrder === 'old'
        ? (orderDetail?.total_products_mrp || 0)
        : product.totalProductsMrp;

    let orderStatus = groupIdsWithPending.has(product.group_id) ? 'Pending' : 'Completed';
    if (orderStatus === 'Completed' && groupIdsWithCompleted.has(product.group_id)) orderStatus = 'Completed';
    else if (orderStatus === 'Completed') orderStatus = 'Pending';
    if (product.order_status === 'Cancelled') orderStatus = 'Cancelled';

    return {
      group_id: product.group_id,
      cart_id: product.cart_id,
      order_date: product.order_date,
      delivery_date: product.delivery_date,
      total_mrp: parseFloat(totalProductsMrp).toFixed(2),
      subscription_id: subscriptionOrder ? subscriptionOrder.subscription_id : '',
      order_status: orderStatus,
      si_order: product.si_order,
      si_sub_ref_no: product.si_sub_ref_no,
      drating: ratingData.rating,
      dreview: ratingData.description,
      productname: productDetailsMap[product.group_id] || ''
    };
  });

  return customizedProductData;
};

const getActiveOrders = async (appDetails) => {
  //quick orders - PostgreSQL: no MySQL sql_mode

  const { user_phone } = appDetails;
  //   const offset = (page - 1) * perpage;

  const user = await knex('users').select('id').where('user_phone', user_phone).first();
  var user_id = (user) ? user.id : 0;

  if (user_id == 0) {
    throw new Error('No user found');
  }

  const today = new Date();
  const groupedOrders = await knex('orders')
    .select(
      'orders.order_type',
      'orders.group_id',
      'orders.time_slot',
      knex.raw('DATE_FORMAT(orders.delivery_date, "%Y-%m-%d") as delivery_date'),
      'orders.cart_id',
      knex.raw('DATE_FORMAT(orders.order_date, "%Y-%m-%d") as order_date'),
      'orders.coupon_discount',
      'orders.total_products_mrp as price_without_delivery',
      'orders.user_id',
      'orders.is_subscription',
      'orders.si_order',
      'orders.bank_id',
      'orders.order_status'
    )
    .where({ user_id })
    .whereNotNull('orders.order_status')
    .whereNull('orders.is_subscription')
    .whereNotNull('orders.payment_method')
    .where('orders.delivery_date', '>', today)
    .groupBy('orders.group_id')
    .orderBy('orders.order_id', 'desc');
  // .offset(offset)
  // .limit(perpage);

  const customizedProductData = [];

  for (const prd of groupedOrders) {
    let ordstatus = 'Cancelled';

    const orderGroup = await knex('orders').where('group_id', prd.group_id);
    for (const o of orderGroup) {
      if (['Completed', 'Confirmed', 'Out_For_Delivery', 'Pending', 'Processing Payment', 'Order Not Placed', 'Processing_payment', 'Payment_failed'].includes(o.order_status)) {
        //  ordstatus = o.order_status === 'Pending' ? 'In Progress' : o.order_status;
        if (o.order_status === 'Pending') {
          ordstatus = 'In Progress';
        } else if (o.order_status == 'Processing_payment') {
          ordstatus = 'Processing Payment';
        } else if (o.order_status == 'Payment_failed') {
          ordstatus = 'Payment Failed';
        } else {
          ordstatus = o.order_status;
        }

        break;
      }
    }

    const totals = await knex('orders')
      .where('group_id', prd.group_id)
      .sum('total_products_mrp as total_products_mrp')
      .sum('rem_price as rem_price')
      .sum('paid_by_wallet as paid_by_wallet')
      .sum('cod_charges as cod_charges')
      .sum('coupon_discount as coupon_discount')
      .sum('del_partner_tip as del_partner_tip')
      .first();

    const famount = parseFloat((totals.total_products_mrp || 0).toFixed(2));

    const productDetails = await knex('orders')
      .join('store_orders', 'orders.cart_id', 'store_orders.order_cart_id')
      .where('orders.group_id', prd.group_id)
      .select(knex.raw(`STRING_AGG(store_orders.product_name || ' X ' || store_orders.qty, ',') as product_details`))
      .first();

    customizedProductData.push({
      group_id: prd.group_id,
      cart_id: prd.cart_id,
      order_type: prd.order_type || 'Quick',
      order_status: ordstatus,
      order_date: prd.order_date,
      order_amount: famount + ' AED',
      order_timeslot: prd.time_slot,
      items: productDetails?.product_details || '',
      //   cart_id: prd.cart_id,
      //   coupon_discount: prd.coupon_discount,
      //   user_id: prd.user_id,
      //   is_subscription: prd.is_subscription,
      //   si_order: prd.si_order,
      //   bank_id: prd.bank_id,
      //   time_slot: prd.time_slot,
      //   delivery_date: prd.delivery_date,
      //   productname: productDetails?.product_details || '',
      //   orderType: prd.order_type || 'normal'
    });
  }

  quickOrderData = customizedProductData;

  //subscription orders    
  // Disable ONLY_FULL_GROUP_BY mode
  //   await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');

  //   const { user_id, store_id, page: pageFilter, perpage: perPage } = appDetails;
  //   const offset = (pageFilter - 1) * perPage;

  // Base query for ongoing subscriptions
  const ongoingQuery = knex('orders')
    .select(
      knex.raw('SUM(orders.total_products_mrp) as totalProductsMrp'),
      'orders.group_id',
      'orders.cart_id',
      knex.raw('DATE_FORMAT(orders.delivery_date, "%Y-%m-%d") as delivery_date'),
      knex.raw('DATE_FORMAT(orders.order_date, "%Y-%m-%d") as order_date'),
      'orders.time_slot',
      'orders.total_delivery',
      'store_orders.price',
      'store_orders.store_order_id',
      'store_orders.product_name',
      'store_orders.total_mrp',
      'store_orders.repeat_orders',
      'orders.order_status',
      'orders.si_order',
      'orders.si_sub_ref_no',
      'orders.pastorecentrder'
    )
    .join('store_orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
    .where('orders.cart_id', '!=', 'incart')
    .where('orders.user_id', user_id)
    .whereNotNull('orders.order_status')
    .where('orders.is_subscription', 1)
    .where('orders.delivery_date', '>', today)
    .whereNotNull('orders.payment_method')
    .groupBy('orders.group_id')
    .orderBy('orders.order_id', 'DESC');
  // .limit(perPage)
  // .offset(offset);

  const ongoing = await ongoingQuery;

  if (ongoing.length === 0) {
    return []; // Return early if no ongoing subscriptions are found
  }

  // Extract cart_ids and store_order_ids for batch processing
  const cartIds = ongoing.map(o => o.cart_id);
  const storeOrderIds = ongoing.map(o => o.store_order_id);

  // Fetch subscription orders, order details, and ratings in parallel
  const [subscriptionOrders, orderDetails, deliveryRatings] = await Promise.all([
    knex('subscription_order')
      .whereIn('cart_id', cartIds)
      .whereIn('store_order_id', storeOrderIds)
      .where('order_status', 'Pending')
      .orderBy('id', 'DESC')
      .select('cart_id', 'store_order_id', 'subscription_id', 'delivery_date', 'order_status'),
    knex('orders')
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'total_products_mrp', 'pastorecentrder'),
    knex('delivery_rating')
      .where('user_id', user_id)
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'description', 'rating')
  ]);

  // Create a map for delivery ratings by cart_id
  const deliveryRatingsMap = Object.fromEntries(
    deliveryRatings.map(rating => [rating.cart_id, { rating: rating.rating, description: rating.description }])
  );

  // Process ongoing subscriptions
  var customizedProductData1 = await Promise.all(
    ongoing.map(async product => {
      // Find the corresponding subscription order
      const subscriptionOrder = subscriptionOrders.find(
        o => o.cart_id === product.cart_id && o.store_order_id === product.store_order_id
      );

      // Format delivery date
      const formattedDeliveryDate = subscriptionOrder
        ? format(new Date(subscriptionOrder.delivery_date), 'yyyy-MM-dd')
        : '';

      // Find the order details
      const orderDetail = orderDetails.find(o => o.cart_id === product.cart_id);
      const ratingData = deliveryRatingsMap[product.cart_id] || { rating: null, description: null };

      // Determine total products MRP
      const totalProductsMrp =
        product.pastorecentrder === 'old'
          ? orderDetail?.total_products_mrp || 0
          : product.totalProductsMrp;

      // Determine order status
      const deliveryDate = new Date(product.delivery_date);

      const today = new Date();
      // Format as 'YYYY-MM-DD'
      const formattedToday = format(today, 'yyyy-MM-dd');

      // Now use it in Knex
      const resultPending = await knex('subscription_order')
        .select('*')
        .where('group_id', product.group_id)
        .where('order_status', 'Pending')
        .where('delivery_date', '>', formattedToday);

      const resultCompleted = await knex('subscription_order')
        .select('*')
        .where('group_id', product.group_id)
        .where('order_status', 'Completed');

      let orderStatus1 = resultPending.length > 0 ? 'Pending' : 'Completed';
      let orderStatus = (orderStatus1 == 'Completed' && resultCompleted.length > 0) ? 'Completed' : 'Pending';
      if (product.order_status === 'Cancelled') orderStatus = 'Cancelled';

      // Fetch product details for the group
      const productDetails = await knex('orders')
        .select(knex.raw(`STRING_AGG(store_orders.product_name || ' X ' || store_orders.qty, ',') as product_details`))
        .join('store_orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
        .where('orders.group_id', product.group_id)
        .first();


      return {
        group_id: product.group_id,
        cart_id: product.cart_id,
        order_type: 'Subscription',
        order_status: orderStatus,
        order_date: product.order_date,
        order_amount: parseFloat(totalProductsMrp).toFixed(2) + ' AED',
        order_timeslot: product.time_slot,
        items: productDetails?.product_details || ''
        // cart_id: product.cart_id,
        // delivery_date:product.delivery_date,
        // subscription_id: subscriptionOrder ? subscriptionOrder.subscription_id : '',
        // si_order: product.si_order,
        // si_sub_ref_no: product.si_sub_ref_no,
        // drating: ratingData.rating,
        // dreview: ratingData.description,
        // productname: productDetails?.product_details || ''
      };
    })
  );

  subOrderData = customizedProductData1;

  // Merge arrays
  const mergedOrders = [...subOrderData, ...quickOrderData];

  const resultString = mergedOrders
    .map(order =>
      `*Order ID:* ${order.cart_id}
*Order Type:* ${order.order_type}
*Status:* ${order.order_status}
*Date:* ${order.order_date}
*Amount:* ${order.order_amount}
*Timeslot:* ${order.order_timeslot}
*Items:* ${order.items}
\n`
    )
    .join('\n');

  return resultString;

  //return {subscriptionOrders:subOrderData,quickOrders:quickOrderData};
};

module.exports = {
  getMyOrder,
  ordersDetails,
  getrepeatOrder,
  getOngoingsub,
  totaldeliveries,
  ordsubDetails,
  getcancelOrderres,
  getRepeatedplaceorder,
  getCancelOrder,
  getCancelprdOrder,
  getSubpauseorder,
  getMydailyOrder,
  getCancelquickOrder,
  getCancelquickOrderProd,
  getSubresumeorder,
  grpordDetails,
  getQuickRepeatedPlaceOrder,
  getProductOngoingsub,
  getOngoingsublist,
  orderwiselist,
  mergeOrders,
  canautorenewal,
  getActiveOrders
};
