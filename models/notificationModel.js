const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library


const notificationlist = async (appDetails) => {
  const user = appDetails.user_id;

  // Optimized: Fetch only required columns and use PostgreSQL date functions for better performance
  // Fetch notifications from the database, ordered by created_at in DESCENDING order
  const notifybys = await knex('user_notification')
    .select('noti_id', 'noti_title', 'image', 'noti_message', 'read_by_user', 'created_at')
    .where('user_id', user)
    .orderBy('created_at', 'DESC')
    .limit(1000); // Limit to prevent excessive data processing

  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Optimized: Use Map for O(1) lookups instead of object
  const monthlyDataMap = new Map();

  if (notifybys.length > 0) {
    // Pre-define month names array for faster lookup
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    // Optimized: Single pass through data with efficient date parsing
    for (const notification of notifybys) {
      const createdAt = new Date(notification.created_at);
      const year = createdAt.getFullYear();
      const month = monthNames[createdAt.getMonth()]; // Direct array lookup instead of toLocaleString
      const monthYearKey = `${month} - ${year}`;

      // Optimized: Simplified image validation
      const imageval = (!notification.image ||
        notification.image.trim() === '' ||
        notification.image === "/N/A" ||
        notification.image === "N/A")
        ? null
        : baseurl + notification.image;

      const customizedProduct = {
        noti_id: notification.noti_id,
        user_id: user,
        noti_title: notification.noti_title || "Quickart",
        image: imageval,
        noti_message: notification.noti_message,
        read_by_user: notification.read_by_user,
        created_at: notification.created_at,
      };

      // Grouping data by "Month - Year" using Map
      if (!monthlyDataMap.has(monthYearKey)) {
        monthlyDataMap.set(monthYearKey, []);
      }
      monthlyDataMap.get(monthYearKey).push(customizedProduct);
    }

    // Optimized: Sort by date directly instead of parsing strings
    const sortedMonthlyData = Array.from(monthlyDataMap.entries())
      .map(([title, notification_listing]) => {
        const [monthStr, yearStr] = title.split(" - ");
        const monthIndex = monthNames.indexOf(monthStr);
        const year = parseInt(yearStr);
        // Create sort key: year * 100 + month (e.g., 202401 for Jan 2024)
        const sortKey = year * 100 + monthIndex;
        return { title, notification_listing, sortKey };
      })
      .sort((a, b) => b.sortKey - a.sortKey) // Sort descending
      .map(({ title, notification_listing }) => ({
        title,
        notification_listing,
      }));

    return { data: sortedMonthlyData };
  } else {
    return { data: [] }; // Return an empty array if no notifications are found
  }
};



const notificationlistold = async (appDetatils) => {
  // return 678
  //  const userLat = appDetatils.userLat
  const user = appDetatils.user_id;
  const currentDate = new Date();
  notifybys = await knex('user_notification')
    .where('user_id', user)
    .orderBy('noti_id', 'DESC')
  const customizedProductData = [];
  if (notifybys.length > 0) {

    for (let i = 0; i < notifybys.length; i++) {

      const ProductList = notifybys[i];
      const baseurl = process.env.BUNNY_NET_IMAGE;
      const imageval = (ProductList.image === "" || ProductList.image === " " || ProductList.image === "/N/A" || ProductList.image === "N/A" || ProductList.image === null || ProductList.image === undefined)
        ? null : baseurl + ProductList.image;
      const customizedProduct = {
        noti_id: ProductList.noti_id,
        user_id: user,
        noti_title: (ProductList.noti_title) ? ProductList.noti_title : "Quickart",
        image: imageval,
        noti_message: ProductList.noti_message,
        read_by_user: ProductList.read_by_user,
        created_at: ProductList.created_at
        // Add or modify properties as needed
      };
      customizedProductData.push(customizedProduct);
    }
    return customizedProductData;
  } else {

    return customizedProductData
  }

}

const seosource = async (appDetatils) => {
  const utm_source = appDetatils.utm_source;
  const utm_campaign = appDetatils.utm_campaign;
  const utm_network = appDetatils.utm_network;
  const utm_medium = appDetatils.utm_medium;
  const utm_keyword = appDetatils.utm_keyword;
  const placement = appDetatils.placement;
  const user_id = appDetatils.user_id;
  const device_id = appDetatils.device_id;
  const fcm_token = appDetatils.fcm_token;
  const platform = appDetatils.platform;

  // Get max utm_id and use max+1 for primary key (table has no sequence/default)
  const maxRow = await knex('seo_source').max('utm_id as max_id').first();
  const nextUtmId = (maxRow && maxRow.max_id != null) ? Number(maxRow.max_id) + 1 : 1;

  const insert = await knex('seo_source')
    .insert({
      utm_id: nextUtmId,
      utm_source: utm_source,
      utm_campaign: utm_campaign,
      utm_network: utm_network,
      utm_medium: utm_medium,
      utm_keyword: utm_keyword,
      placement: placement,
      user_id: user_id,
      device_id: device_id,
      fcm_token: fcm_token,
      platform: platform
    });
  return insert;
}

const InsertPaymentNotification = async (appDetatils, ipnLogId = 'IPN') => {

  const PaymentDetatils = appDetatils;
  const group_id = PaymentDetatils.order_number || PaymentDetatils.order_id || PaymentDetatils.group_id || PaymentDetatils.order?.number;
  const ordertype = PaymentDetatils.ordertype || 'unknown'; // Default to unknown if missing

  console.log(`[${ipnLogId}] InsertPaymentNotification START - group_id: ${group_id} (from order_number/order_id/group_id), ordertype: ${ordertype}`);
  if (!group_id) {
    console.log(`[${ipnLogId}] WARNING: group_id is empty - IPN may use different field. Available keys: ${Object.keys(PaymentDetatils).join(', ')}`);
  }
  console.log(`[${ipnLogId}] Healing check - card: ${PaymentDetatils.card ? 'PRESENT' : 'MISSING'}, id: ${PaymentDetatils.id || 'MISSING'}`);

  // Ensure we store stringified JSON to avoid [object Object] issues
  const jsonString = JSON.stringify(PaymentDetatils);
  console.log(`[${ipnLogId}] Step 1: Inserting into payment_notification_details`);

  // Manual ID generation for table without sequence/default
  const maxRow = await knex('payment_notification_details').max('id as max_id').first();
  const nextId = (maxRow && maxRow.max_id != null) ? Number(maxRow.max_id) + 1 : 1;
  console.log(`[${ipnLogId}] Insert id: ${nextId}`);

  const insert = await knex('payment_notification_details').insert({
    id: nextId,
    json_data: jsonString,
    group_id: group_id,
    order_type: ordertype,
    added_on: new Date()
  });

  console.log(`[${ipnLogId}] Step 2: Insert complete. Now checking healing logic.`);

  // Healing Logic: If this IPN has a card number and ID, update the placeholder in tbl_user_bank_details
  const healTransId = PaymentDetatils.id || PaymentDetatils.payment_id || PaymentDetatils.trans_id;
  if (PaymentDetatils.card && healTransId) {
    try {
      console.log(`[${ipnLogId}] Step 3: Healing - attempting update tbl_user_bank_details WHERE recurring_init_trans_id = '${healTransId}'`);
      const rowsBefore = await knex('tbl_user_bank_details')
        .where('recurring_init_trans_id', healTransId)
        .select('id', 'user_id', 'card_no', 'recurring_init_trans_id');
      console.log(`[${ipnLogId}] Rows matching recurring_init_trans_id: ${rowsBefore.length}`, rowsBefore);

      const updated = await knex('tbl_user_bank_details')
        .where('recurring_init_trans_id', healTransId)
        .update({
          card_no: PaymentDetatils.card
        });
      if (updated) {
        console.log(`[${ipnLogId}] HEAL SUCCESS: Updated ${updated} row(s) with real card number for trans_id: ${healTransId}`);
      } else {
        console.log(`[${ipnLogId}] HEAL NO-MATCH: No rows found with recurring_init_trans_id='${healTransId}' - card may not be saved yet or ID mismatch`);
      }
    } catch (err) {
      console.error(`[${ipnLogId}] HEAL ERROR:`, err.message, err.stack);
    }
  } else {
    console.log(`[${ipnLogId}] HEAL SKIPPED: card=${!!PaymentDetatils.card}, healTransId=${healTransId || 'MISSING'} - need both for healing`);
  }

  console.log(`[${ipnLogId}] InsertPaymentNotification END`);
  return insert
}

const successData = async (appDetatils) => {
  const PaymentDetatils = appDetatils;
  // const insert=await knex('payment_notification_details').insert({
  // json_data:PaymentDetatils,
  // added_on: new Date()
  // });      
  // return insert
  return PaymentDetatils
}

const failureData = async (appDetatils) => {
  const PaymentDetatils = appDetatils;
  // const insert=await knex('payment_notification_details').insert({
  // json_data:PaymentDetatils,
  // : new Date()
  // });      
  return PaymentDetatils
}

const getNotification = async (getnotification) => {
  const group_id = getnotification.order_id || getnotification.group_id || getnotification.order_number;

  const notifybys = await knex('payment_notification_details')
    .where('group_id', group_id)
    .first();

  if (!notifybys) {
    return null;
  }

  const dataString = notifybys.json_data;
  const data = (typeof dataString === 'string') ? JSON.parse(dataString) : dataString;

  return data ? data.custom_data : null;
}

const findByGroupId = async (getnotification) => {
  const group_id = getnotification.order_id || getnotification.group_id || getnotification.order_number;
  const notifybys = await knex('payment_notification_details')
    .where('group_id', group_id)
    .first();

  return notifybys;
}

const findByOrderIdFromRequests = async (orderId) => {
  const request = await knex('payment_order_request_details')
    .where('group_id', orderId)
    .first();
  if (request && request.json_data) {
    try {
      const parsed = JSON.parse(request.json_data);
      if (parsed.custom_data) {
        return parsed.custom_data;
      }
      // If it was a recurring session request, it might have the data directly in its mainJson
      return parsed;
    } catch (e) {
      return null;
    }
  }
  return null;
}

module.exports = {
  notificationlist,
  InsertPaymentNotification,
  successData,
  failureData,
  seosource,
  getNotification,
  findByGroupId,
  findByOrderIdFromRequests
};
