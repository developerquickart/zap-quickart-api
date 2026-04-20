const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const { format } = require('date-fns');
const moment = require('moment');
const crypto = require('crypto');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
require('moment-timezone');
const uaeTime = moment.tz(new Date(), "Asia/Dubai");
const axios = require('axios');
require('dotenv').config();
const { codorderplacedMail } = require('../sendGridService');
const logToFile = require("../utils/logger");
const userModel = require('./userModel');

const getQuickordercheckout = async (appDetails) => {
    try {
        logToFile("getQuickordercheckout STARTED for groupId: " + appDetails.group_id);
        console.log("[getQuickordercheckout] STARTED for appDetails:", JSON.stringify(appDetails).substring(0, 500) + "...");

        // Destructure other details from appDetails
        const {
            user_id: userId,
            address_id: addressId,
            delivery_date: deliveryDate,
            time_slot: timeSlot,
            store_id: storeId,
            del_partner_tip: delPartnerTips,
            del_partner_instruction: delPartnerInstruction,
            order_instruction: order_instruction,
            payment_method: paymentMethod,
            payment_status: paymentStatus,
            wallet,
            totalwalletamt: totalWalletAmt,
            totalrefwalletamt: totalRefWalletAmt,
            coupon_id: couponId,
            coupon_code: couponCode,
            payment_id: paymentId,
            payment_gateway: paymentGateway,
            si_sub_ref_no: siSubRefNo,
            bank_id: bankId,
            payment_type: paymentType,
            group_id: paymentOrderId,
            platform: platform,
            browser: browser,
            exp_eta: expEta,
        } = appDetails;


        const delPartnerTip = delPartnerTips || 0;
        const walletNew = (wallet || '').toString().trim().toLowerCase();
        const safePaymentMethod = (paymentMethod || '').toString().trim().toLowerCase();
        const parsedExpEta = Number(expEta);
        let paymentMethodNew = '';

        if (!Number.isInteger(parsedExpEta)) {
            throw new Error('exp_eta is required and must be an integer');
        }


        // Determine siStatus based on siSubRefNo
        const siStatus = siSubRefNo ? "yes" : "no";
        const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
        const hashSubstring = hash.substring(0, 2); // Adjust substring length as needed
        // Combine all parts to form group_id
        const groupId = (paymentOrderId) ? paymentOrderId : generateRandomLetters(6) + generateRandomDigits(2) + hashSubstring;
        // Query address from the database
        const ar = await knex('address')
            .select('society', 'city', 'city_id', 'society_id', 'lat', 'lng', 'address_id', 'house_no', 'receiver_email', 'landmark')
            .where('user_id', userId) // Check if this is the correct condition for your use case
            .where('address_id', addressId)
            .first();

        if (!ar) {
            logToFile("No address found for the provided user ID and address ID ${userId} - ${addressId}");
            throw new Error('No address found for the provided user ID and address ID');
        }


        // Get today's date and current time in Dubai timezone
        const dubaiTime = moment.tz("Asia/Dubai");
        const todayDubai = dubaiTime.format("YYYY-MM-DD");
        const orderDateTimeDubai = dubaiTime.toDate();
        const isAfter6PM = dubaiTime.hour() >= 17;
        // const isAfter12PM = dubaiTime.hour() >= 14;
        const isAfter12PM = dubaiTime.isSameOrAfter(
            moment.tz("Asia/Dubai").hour(11).minute(0).second(0)
        );
        // const cutoff11AM = dubaiTime.clone()
        // .startOf('day')
        // .hour(11)
        // .minute(0)
        // .second(0);

        // const isAfter12PM = dubaiTime.isSameOrAfter(cutoff11AM);

        // if (!appDetails.time_slot || appDetails.time_slot === undefined || appDetails.time_slot === null || appDetails.time_slot === '' || appDetails.time_slot === 'undefined') {
        //     //   || deliveryDate === undefined || deliveryDate === null || deliveryDate === '' || deliveryDate === 'undefined'
        //     logToFile("time slot is blank or undefined");
        //     throw new Error("This timeslot is not available for selected products delivery, kindly select different delivery date");
        // }

        // Condition 1: Check if any order has a sub_delivery_date of today
        // if (deliveryDate === todayDubai && isAfter12PM) {
        //      logToFile("Order blocked: after 12PM for today");
        // throw new Error("Unable to place order for selected date time. Please select different date and time.");
        // }


        // // Condition 2: If it's after 6 PM in Dubai, prevent placing orders for tomorrow with "06:00 am - 10:00 am" time slot
        // if (isAfter6PM) {
        // const tomorrowDubai = dubaiTime.add(1, 'day').format("YYYY-MM-DD"); // Get tomorrow's date in Dubai time
        // if (deliveryDate == tomorrowDubai && timeSlot == "06:00 am - 10:00 am") {
        //     logToFile("Order blocked:  If it's after 6 PM in Dubai, prevent placing orders for tomorrow with 06:00 am - 10:00 am time slot");
        // throw new Error(`Unable to place order for selected date time. Please select different date and time.`);
        // }
        // }

        // // Condition 3: Check if any order has a sub_delivery_date of today
        // if (deliveryDate === todayDubai && (timeSlot == "06:00 am - 10:00 am" || timeSlot == "02:00 pm - 05:00 pm" || timeSlot == "02:00 pm - 04:00 pm")) {
        //     logToFile("Order blocked:  Check if any order has a sub_delivery_date of today");
        // throw new Error("Unable to place order for selected date time. Please select different date and time.");
        // }

        //stock 0 Or not check 
        // const storeItemList = await knex('store_orders')
        // .select('store_products.stock','store_orders.*')
        // .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
        // .join('product_varient','store_products.varient_id','=','product_varient.varient_id')
        // .join('product','product_varient.product_id','=','product.product_id')
        // .where('store_orders.store_approval', userId)
        // .where('store_orders.order_cart_id', 'incart')
        // .whereNull('subscription_flag');
        // // Iterate through the list of items and check the stock
        // for (const storeItem of storeItemList) {
        // if (storeItem.stock === 0) {
        // throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
        // }
        // }

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
            .where('store_orders.store_approval', userId)
            .where('store_orders.order_cart_id', 'incart')
            .whereNull('subscription_flag');
        // Iterate through the list of items and check the stock
        if (storeItemList.length > 0 || storeItemList != null) {
            for (const storeItem of storeItemList) {
                if (storeItem.stock === 0 || storeItem.product_hide == 1 || storeItem.product_delete == 1) {
                    logToFile("One or more items in your cart are unavailable/out of stock. Unable to proceed with the order. ");
                    throw new Error("One or more items in your cart are unavailable/out of stock. Unable to proceed with the order.");
                }
                const stock = parseInt(storeItem.stock || 0);
                const cartQty = parseInt(storeItem.qty || 0);
                if (cartQty > stock) {
                    logToFile(`Stock insufficient: ${storeItem.product_name}`);
                    throw new Error(
                        `Only ${stock} quantity available for ${storeItem.product_name}. Please revise the selected quantity.`
                    );
                }

                if (!storeItem.sub_delivery_date || storeItem.sub_delivery_date === undefined || storeItem.sub_delivery_date === null || storeItem.sub_delivery_date === '' || storeItem.sub_delivery_date === 'undefined'
                    || storeItem.sub_time_slot === undefined || storeItem.sub_time_slot === null || storeItem.sub_time_slot === '' || storeItem.sub_time_slot === 'undefined') {
                    logToFile("time slot is blank or undefined");
                    throw new Error("This timeslot is not available for selected products delivery, kindly select different delivery date");
                }

                // Date/time slot cut-off blocks removed as requested.
            }
        }




        // Fetch user phone and wallet
        const user = await knex('users')
            .select('user_phone', 'wallet', 'wallet_balance', 'referral_balance', 'country_code', 'name', 'email')
            .where('id', userId)
            .first();
        // let actualWallet = user.wallet;
        // const walletEnabled = wallet.toLowerCase() === 'yes' && parseFloat(totalWalletAmt || 0) > 0;
        // let WalletBalanace = walletEnabled ? user.wallet - totalWalletAmt : user.wallet;
        let actualWallet = parseFloat(user.wallet_balance || 0);
        let actualRefWallet = parseFloat(user.referral_balance || 0);

        let WalletBalance = user.wallet_balance - totalWalletAmt;
        let RefWalletBalance = user.referral_balance - totalRefWalletAmt;

        let userPhone = user.country_code + user.user_phone;
        let userName = user.name;
        let userEmail = (user.email) ? user.email : ar.receiver_email;

        if (!user) {
            logToFile("User not found ");
            throw new Error('User not found');
        }
        const cartItems = await knex('store_orders')
            .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
            .join('product', 'product_varient.product_id', '=', 'product.product_id')
            .select('store_orders.*', 'product.product_name as product_name')
            .where('store_orders.store_approval', userId)
            .whereNull('subscription_flag')
            .where('store_orders.order_cart_id', "incart");

        for (const productList of cartItems) {

            const varientId = productList.varient_id;
            const orderQty = parseInt(productList.qty || 0);
            const productName = productList.product_name || 'this product';

            if (!varientId || !storeId) {
                throw new Error('Invalid product or store');
            }

            const updatedRows = await knex('store_products')
                .where('varient_id', varientId)
                .andWhere('store_id', storeId)
                .andWhere('stock', '>=', orderQty)
                .decrement('stock', orderQty);

            if (!updatedRows) {
                const currentStockRow = await knex('store_products')
                    .where('varient_id', varientId)
                    .andWhere('store_id', storeId)
                    .select('stock')
                    .first();

                const availableStock = currentStockRow ? currentStockRow.stock : 0;

                throw new Error(
                    `Only ${availableStock} quantity available for ${productName}`
                );
            }
        }
        //store order details
        const storeDetailsAmt = await knex('store_orders')
            .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
            .join('product', 'product_varient.product_id', '=', 'product.product_id')
            .where('store_orders.store_approval', userId)
            .where('store_orders.order_cart_id', 'incart')
            .whereNull('subscription_flag')
            .select(knex.raw('SUM(store_orders.total_mrp) as Totalmrp'), knex.raw('SUM(store_orders.price) as Totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
            .first();

        // Fetch delivery charges
        const deliveryFlag = await knex('app_settings')
            .where('store_id', storeId)
            .select('cod_charges')
            .first();

        const requestedCodCharges = (paymentMethod.toLowerCase() === 'cod') ? deliveryFlag.cod_charges : 0;

        let couponPriceAmount = 0;
        if (couponCode) {
            const CouponDiscount = await knex('store_orders')
                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                .sum({ total_price: 'store_orders.price' })
                .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
                .where('store_orders.order_cart_id', 'incart')
                .where('store_orders.store_approval', userId)
                .whereNull('store_orders.subscription_flag')
                .first();

            const CouponDetails = await knex('coupon')
                .where('coupon_code', couponCode)
                .first();
            if (CouponDetails) {
                couponPriceAmount = (parseFloat(CouponDiscount.total_price) * parseFloat(CouponDetails.amount)) / 100;
            } else {
                couponPriceAmount = 0;
            }
        } else {
            couponPriceAmount = 0;
        }
        logToFile(`[DEBUG] storeDetailsAmt: ${JSON.stringify(storeDetailsAmt)}`);
        logToFile(`[DEBUG] couponPriceAmount: ${couponPriceAmount}, TotalWalletAmt: ${totalWalletAmt}`);

        // Postgres returns aliases in lowercase
        // const basePrice = parseFloat(storeDetailsAmt?.totalprice || storeDetailsAmt?.Totalprice || 0);
        // const tip = parseFloat(delPartnerTip || 0);
        // const cod = parseFloat(codCharges || 0);
        // const coupon = parseFloat(couponPriceAmount || 0);
        // const walletAmt = parseFloat(totalWalletAmt || 0);

        // let TotalpriceAmount = ((basePrice + tip + cod) - (coupon + walletAmt));

        // PostgreSQL returns unquoted aliases in lowercase (totalprice, totalmrp, count)
        const storeTotalPrice = parseFloat(storeDetailsAmt?.totalprice ?? storeDetailsAmt?.Totalprice ?? 0);
        const totalWalletProvided =
            (parseFloat(totalWalletAmt || 0) + parseFloat(totalRefWalletAmt || 0));

        // Base amount excluding COD charges (items + tip - coupon). If wallet covers this fully and
        // the request was COD, we convert the order to Wallet and waive COD charges.
        const baseBeforeCod = Math.max(
            0,
            (
                Math.round(storeTotalPrice * 100) +
                Math.round(parseFloat(delPartnerTip) * 100) -
                Math.round(parseFloat(couponPriceAmount) * 100)
            ) / 100
        );

        const waiveCodCharges = (safePaymentMethod === 'cod' && walletNew === 'yes' && totalWalletProvided >= baseBeforeCod);
        if (waiveCodCharges) {
            paymentMethodNew = 'Wallet';
        }

        const effectiveCodCharges = (safePaymentMethod === 'cod' && paymentMethodNew !== 'Wallet')
            ? requestedCodCharges
            : 0;

        const total =
            Math.round(storeTotalPrice * 100) +
            Math.round(parseFloat(delPartnerTip) * 100) +
            Math.round(parseFloat(effectiveCodCharges) * 100);

        const deduction =
            Math.round(parseFloat(couponPriceAmount) * 100) +
            Math.round(parseFloat(totalWalletAmt) * 100) +
            Math.round(parseFloat(totalRefWalletAmt) * 100);

        let TotalpriceAmount = (total - deduction) / 100;

        // Defensive check for NaN and negative values
        if (isNaN(TotalpriceAmount)) {
            TotalpriceAmount = 0;
        }
        // Prevent negative card amount (e.g. when wallet sent exceeds 50% cap)
        TotalpriceAmount = Math.max(0, TotalpriceAmount);

        logToFile(`[DEBUG] Final TotalpriceAmount: ${TotalpriceAmount}`);

        // Distribute wallet/ref-wallet proportionally across items.
        // If COD charges are waived (order converted to Wallet), use baseBeforeCod as denominator.
        const baseBeforeWallets = Math.max(
            0,
            (
                Math.round(storeTotalPrice * 100) +
                Math.round(parseFloat(delPartnerTip) * 100) +
                Math.round(parseFloat(effectiveCodCharges) * 100) -
                Math.round(parseFloat(couponPriceAmount) * 100)
            ) / 100
        );
        const WithWalletAmount = baseBeforeWallets;
        const WithRefWalletAmount = baseBeforeWallets;

        const WalletDiscountAmount = ((((storeTotalPrice + parseFloat(delPartnerTip) + parseFloat(effectiveCodCharges)) - couponPriceAmount) * 50) / 100).toFixed(2);
        const WalletStatus = (walletNew === 'yes' && actualWallet >= parseFloat(WalletDiscountAmount)) ? 'percentage' : 'fixed';

        if (parseFloat(TotalpriceAmount) <= 0) {
            paymentMethodNew = 'Wallet';
        }

        // Decide what we actually store in orders.payment_method based on remaining card amount
        // AND whether wallet was explicitly used:
        // - If user selected COD, always store COD.
        // - If wallet/referral was used AND there is no remaining amount to be paid by card,
        //   store 'Wallet'.
        // - Otherwise, store the gateway/payment method (e.g. card).
        const nonWalletPortion = Math.max(0, parseFloat(TotalpriceAmount || 0));

        const walletUsedFlag =
            (walletNew === 'yes') ||
            parseFloat(totalWalletAmt || 0) > 0 ||
            parseFloat(totalRefWalletAmt || 0) > 0;

        const orderPaymentMethodStored =
            safePaymentMethod === 'cod'
                ? paymentMethod
                : (walletUsedFlag && nonWalletPortion <= 0 ? 'Wallet' : paymentMethod);

        const totalrefwalletamt = totalRefWalletAmt;
        if (siStatus == 'yes' || safePaymentMethod == 'cod' || paymentMethodNew == 'Wallet') {
            const number = groupId;
            const description = "";
            const amount = (TotalpriceAmount) ? TotalpriceAmount : "0";
            const orderJson = { number, description, amount };
            const ordertype = "quick";
            const payment_status = (paymentMethodNew === 'Wallet' || safePaymentMethod !== 'cod') ? 'success' : 'Pending';
            const group_id = groupId;
            const user_id = userId;
            const bank_id = 0;
            const si_sub_ref_no = siSubRefNo;
            const store_id = storeId;
            const payment_method = orderPaymentMethodStored;
            const payment_gateway = paymentGateway;
            const payment_id = paymentId;
            const coupon_id = couponId;
            const coupon_code = couponCode;
            const discount_amount = 0;
            const delivery_date = deliveryDate;
            const time_slot = timeSlot;
            const del_partner_tip = delPartnerTips;
            const del_partner_instruction = delPartnerInstruction;
            const device_id = "";
            const totalwalletamt = totalWalletAmt;
            const is_subscription = null;
            const payment_type = paymentType;
            const address_id = addressId;
            const custom_data = { address_id, ordertype, payment_status, group_id, user_id, bank_id, si_sub_ref_no, store_id, payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount, delivery_date, time_slot, del_partner_tip, del_partner_instruction, order_instruction, device_id, totalwalletamt, totalrefwalletamt, is_subscription, payment_type, platform, browser, exp_eta: parsedExpEta, storeItemList };

            const mainJsonSaveRequest = {
                merchant_key: "",
                operation: 'purchase',
                methods: orderPaymentMethodStored,
                success_url: "",
                cancel_url: "",
                hash: "",
                order: orderJson,
                customer: "",
                billing_address: "",
                custom_data: custom_data
            };

            const maxIdResult = await knex('payment_order_request_details').max('id as maxId').first();
            const nextId = (maxIdResult?.maxId ? parseInt(maxIdResult.maxId, 10) : 0) + 1;

            const insert = await knex('payment_order_request_details').insert({
                id: nextId,
                json_data: mainJsonSaveRequest,
                group_id: group_id,
                order_type: ordertype,
                added_on: orderDateTimeDubai
            });

            logToFile("COD Order Detail: " + JSON.stringify(mainJsonSaveRequest));
        }


        if (siStatus == 'yes' && safePaymentMethod != 'cod' && paymentMethodNew != 'Wallet') {

            const BankDetails = await knex('tbl_user_bank_details')
                .where('si_sub_ref_no', siSubRefNo)
                .where('user_id', userId)
                .first();

            if (BankDetails) {
                // Success: BankDetails found   
                // TotalPay Credentials for Test/Live
                const recurring_init_trans_id = BankDetails.recurring_init_trans_id;
                // TotalPay Credentials for Test
                const merchantKey = '968abd2e-79ce-11ef-8430-ee2650fd5759';
                const merchantpassword = 'abdf10a546b5197cdf81508a3d3c9e23';
                // TotalPay Credentials for Live
                //  const merchantKey = '7f066f26-36b4-11ee-8433-eecb8191d36e';
                //  const merchantpassword = '96bb03851c3553fd132339acc06ce060';
                const recurring_token = BankDetails.si_sub_ref_no;
                const pay_amounts = (TotalpriceAmount) ? TotalpriceAmount : 0;
                const orderNumber = groupId;
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
                        number: groupId,
                        amount: (amount) ? amount : 0,
                        description: orderDescription
                    }
                };

                const jsonData = JSON.stringify(mainJson);
                const checkoutUrl = 'https://checkout.totalpay.global/api/v1/payment/recurring';

                if (TotalpriceAmount > 0) {
                    try {
                        const fetch = (await import('node-fetch')).default;

                        const response = await fetch(checkoutUrl, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: jsonData,
                        });

                        const data = await response.json();
                        logToFile(`[RECURRING PAYMENT] Response: ${JSON.stringify(data)}`);

                        if (!data || !data.status) {
                            throw new Error(`Invalid response from payment gateway: ${JSON.stringify(data)}`);
                        }

                        const status = data.status.toLowerCase();
                        if (status === 'decline' || status === 'pending') {
                            const errorMessage = status === 'decline'
                                ? 'Card issue detected! Status: decline'
                                : `HTTP error! Status: ${data.status}`;
                            const cartItems = await knex('store_orders')
                                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                                .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                                .join('product', 'product_varient.product_id', '=', 'product.product_id')
                                .select('store_orders.*', 'product.product_name as product_name')
                                .where('store_orders.store_approval', userId)
                                .whereNull('subscription_flag')
                                .where('store_orders.order_cart_id', "incart");

                            for (const productList of cartItems) {
                                const varientId = productList.varient_id;
                                const orderQty = parseInt(productList.qty || 0);
                                const productName = productList.product_name || 'this product';
                                if (!varientId || !storeId) {
                                    throw new Error('Invalid product or store');
                                }
                                const updatedRows = await knex('store_products')
                                    .where('varient_id', varientId)
                                    .andWhere('store_id', storeId)
                                    .andWhere('stock', '>=', orderQty)
                                    .increment('stock', orderQty);
                            }
                            throw new Error(errorMessage);
                        }

                        if (!response.ok) {
                            const cartItems = await knex('store_orders')
                                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                                .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                                .join('product', 'product_varient.product_id', '=', 'product.product_id')
                                .select('store_orders.*', 'product.product_name as product_name')
                                .where('store_orders.store_approval', userId)
                                .whereNull('subscription_flag')
                                .where('store_orders.order_cart_id', "incart");

                            for (const productList of cartItems) {
                                const varientId = productList.varient_id;
                                const orderQty = parseInt(productList.qty || 0);
                                const productName = productList.product_name || 'this product';
                                if (!varientId || !storeId) {
                                    throw new Error('Invalid product or store');
                                }
                                const updatedRows = await knex('store_products')
                                    .where('varient_id', varientId)
                                    .andWhere('store_id', storeId)
                                    .andWhere('stock', '>=', orderQty)
                                    .increment('stock', orderQty);
                            }
                            throw new Error(`HTTP error! Status: ${response.status}`);
                        }

                        const maxSiIdResult = await knex('tbl_si_deduction_log').max('id as maxId').first();
                        const nextSiId = (maxSiIdResult?.maxId ? parseInt(maxSiIdResult.maxId, 10) : 0) + 1;

                        await knex('tbl_si_deduction_log').insert({
                            id: nextSiId,
                            si_sub_ref_no: siSubRefNo,
                            user_id: userId,
                            amount: (TotalpriceAmount) ? parseFloat(TotalpriceAmount).toFixed(2) : 0,
                            payment_method: 'SI',
                            payment_status: 0,
                            card_id: groupId,
                        });
                        //return await response.json();
                    } catch (error) {
                        logToFile(`checkoutModel fun getQuickordercheckout error: ${error instanceof Error ? error.stack || error.message : JSON.stringify(error)}`);
                        const cartItems = await knex('store_orders')
                            .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                            .join('product', 'product_varient.product_id', '=', 'product.product_id')
                            .select('store_orders.*', 'product.product_name as product_name')
                            .where('store_orders.store_approval', userId)
                            .whereNull('subscription_flag')
                            .where('store_orders.order_cart_id', "incart");

                        for (const productList of cartItems) {
                            const varientId = productList.varient_id;
                            const orderQty = parseInt(productList.qty || 0);
                            const productName = productList.product_name || 'this product';
                            if (!varientId || !storeId) {
                                throw new Error('Invalid product or store');
                            }
                            const updatedRows = await knex('store_products')
                                .where('varient_id', varientId)
                                .andWhere('store_id', storeId)
                                .andWhere('stock', '>=', orderQty)
                                .increment('stock', orderQty);
                        }
                        console.error('Error sending payment data:', error);
                        throw error; // Re-throw for handling in the calling code
                    }
                } else {
                    logToFile(`[CHECKOUT] Skipping TotalPay Recurring API for order ${groupId} as amount is ${TotalpriceAmount} (fully paid by wallet/coupons).`);
                }


            } else {
                // No BankDetails found
                logToFile("No bank details found for the given criteria.");
                const cartItems = await knex('store_orders')
                    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                    .join('product', 'product_varient.product_id', '=', 'product.product_id')
                    .select('store_orders.*', 'product.product_name as product_name')
                    .where('store_orders.store_approval', userId)
                    .whereNull('subscription_flag')
                    .where('store_orders.order_cart_id', "incart");

                for (const productList of cartItems) {
                    const varientId = productList.varient_id;
                    const orderQty = parseInt(productList.qty || 0);
                    const productName = productList.product_name || 'this product';
                    if (!varientId || !storeId) {
                        throw new Error('Invalid product or store');
                    }
                    const updatedRows = await knex('store_products')
                        .where('varient_id', varientId)
                        .andWhere('store_id', storeId)
                        .andWhere('stock', '>=', orderQty)
                        .increment('stock', orderQty);
                }
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
            .where('store_orders.order_cart_id', "incart");

        let price2 = 0;
        let tax_p = 0;
        let tax_price = 0;
        let TotalpriceStore = storeDetailsAmt?.totalprice || storeDetailsAmt?.Totalprice || 0;
        // const walletPerProduct = (totalWalletAmt / TotalpriceStore); // This was not in user request but implied by logic below
        const walletPerProduct = totalWalletAmt; // User snippet uses totalWalletAmt directly or differently?
        // Actually user snippet has: paidByWallet = Math.min(actualWallet, walletPerProduct);
        // I will use what's in the snippet.
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
                .whereNull('subscription_flag')
                .first();

            if (!product) {
                throw new Error('Product not found');
            }

            let offer_product = await knex('product')
                .leftJoin('tbl_country', knex.raw('CAST(product.country_id AS INTEGER)'), '=', 'tbl_country.id')
                .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
                .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
                .select('product.product_id', 'product.product_name', 'product.offer_price as price',
                    'product_varient.quantity', 'product_varient.unit',
                    'store_products.mrp',
                    'tbl_country.country_icon',
                    'store_products.stock')
                .where('product_varient.varient_id', varientId)
                .where('product.offer_date', todayDubai)
                .first();

            const { price, total_mrp, mrp, tx_per: taxPer, tx_price: taxPrice, min_ord_qty: minOrderQty, max_ord_qty: maxOrderQty, stock, product_name: productName, quantity, unit, varient_image: varientImage } = product;
            const totalMrp = (offer_product) ? offer_product.price : mrp; // Adjust this if needed
            price2 = (offer_product) ? offer_product.price : price;

            const codorderamt = effectiveCodCharges ? (price2 * effectiveCodCharges / TotalpriceStore) : 0;
            const delPartnerTipAmt = delPartnerTip ? (price2 * delPartnerTip / TotalpriceStore) : 0;
            let couponPriceProduct = 0;
            const CouponDiscounts = await knex('store_orders')
                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                .sum({ total_price: 'store_orders.price' })
                .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
                .where('store_orders.order_cart_id', 'incart')
                .where('store_orders.varient_id', varientId)
                .where('store_orders.store_approval', userId)
                .whereNull('subscription_flag')
                .first();

            if (couponCode && (CouponDiscounts.total_price > 0)) {
                const CouponDiscount = await knex('store_orders')
                    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                    .sum({ total_price: 'store_orders.price' })
                    .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
                    .where('store_orders.order_cart_id', 'incart')
                    .where('store_orders.store_approval', userId)
                    .whereNull('store_orders.subscription_flag')
                    .first();

                const CouponDetails = await knex('coupon')
                    .where('coupon_code', couponCode)
                    .first();
                if (CouponDetails) {
                    couponPriceProduct = (parseFloat(CouponDiscounts.total_price || 0) * parseFloat(CouponDetails.amount)) / 100;
                } else {
                    couponPriceProduct = 0;
                }
            } else {
                couponPriceProduct = 0;
            }
            const totalPrice = (parseFloat(price2) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt)) - parseFloat(couponPriceProduct);

            let paidByWallet = 0;
            let paidByRefWallet = 0;

            // Apply split when main/ref amounts are sent, not only when wallet === 'yes' (referral-only was skipped before)
            const useWalletDeductions =
                walletNew === 'yes' ||
                parseFloat(totalWalletAmt || 0) > 0 ||
                parseFloat(totalRefWalletAmt || 0) > 0;
            if (useWalletDeductions) {
                const maxWalletUsage = (totalPrice * 50) / 100;
                const walletPerProduct = (parseFloat(totalPrice) * parseFloat(totalWalletAmt || 0)) / parseFloat(WithWalletAmount || 1);
                const refWalletPerProduct = (parseFloat(totalPrice) * parseFloat(totalRefWalletAmt || 0)) / parseFloat(WithRefWalletAmount || 1);

                paidByWallet = Math.min(actualWallet, walletPerProduct);
                paidByRefWallet = Math.min(actualRefWallet, refWalletPerProduct, maxWalletUsage);
            }

            const paymentStatus = (paymentMethodNew === 'Wallet') ? 'success' : ((safePaymentMethod === 'cod') ? 'Pending' : 'success');
            const remPrice = (paymentMethodNew === 'Wallet') ? 0 : ((safePaymentMethod === 'cod') ? (totalPrice - paidByWallet - paidByRefWallet) : 0);
            //const remPrice = totalPrice;

            const timeslotval = (productList.sub_time_slot) ? productList.sub_time_slot : timeSlot;
            // For quick orders we always persist today's date (Dubai timezone),
            // regardless of what the client/request sent.
            var deliverydateval = todayDubai;

            //fetch timeslot discount
            const timeslotdata = await knex('tbl_time_slots')
                .where('time_slots', timeslotval)
                .first();

            var is_offer_product = (productList.is_offer_product == 1) ? 1 : 0;

            if (couponId && couponId != null && couponId != 'null') {
                var couponID = couponId;
                var couponcode = couponCode;
            } else {
                var couponID = 0;
                var couponcode = null;
            }

            const maxOrderIdResult = await knex('orders').max('order_id as maxOrderId').first();
            const nextOrderId = (maxOrderIdResult?.maxOrderId ? parseInt(maxOrderIdResult.maxOrderId, 10) : 0) + 1;

            logToFile(`[getQuickordercheckout] Inserting into orders table for groupId ${groupId}: ` + JSON.stringify({
                order_id: nextOrderId,
                cart_id: cartId,
                total_price: totalPrice.toFixed(2),
                user_id: userId,
                store_id: storeId,
                group_id: groupId,
                payment_method: paymentMethod,
                payment_status: paymentStatus
            }));
            console.log(`[getQuickordercheckout] Inserting into orders table for groupId ${groupId}`);

            const orderID = await knex('orders')
                .insert({
                    order_id: nextOrderId,
                    cart_id: cartId,
                    total_price: totalPrice.toFixed(2),
                    price_without_delivery: totalPrice.toFixed(2),
                    total_products_mrp: totalPrice.toFixed(2),
                    delivery_charge: 0,
                    user_id: userId,
                    store_id: storeId,
                    rem_price: (remPrice) ? parseFloat(remPrice).toFixed(2) : 0,
                    order_date: orderDateTimeDubai,
                    delivery_date: deliverydateval,
                    time_slot: timeslotval,
                    address_id: addressId,
                    avg_tax_per: 0,
                    total_tax_price: 0,
                    total_delivery: 1, // Adjust if needed
                    del_partner_tip: (delPartnerTipAmt) ? parseFloat(delPartnerTipAmt).toFixed(2) : 0,
                    del_partner_instruction: delPartnerInstruction,
                    order_instruction: order_instruction,
                    group_id: groupId, // Use groupId for group_id or adjust as needed
                    is_subscription: null,
                    cod_charges: (codorderamt) ? parseFloat(codorderamt).toFixed(2) : 0,
                    paid_by_wallet: paidByWallet.toFixed(2),
                    paid_by_ref_wallet: paidByRefWallet.toFixed(2),
                    payment_method: orderPaymentMethodStored,
                    si_sub_ref_no: siSubRefNo,
                    coupon_id: couponID,
                    coupon_code: couponcode,
                    coupon_discount: (couponPriceProduct) ? parseFloat(couponPriceProduct).toFixed(2) : 0,
                    payment_status: paymentStatus,
                    payment_type: paymentType,
                    pastorecentrder: 'new',
                    platform: (platform) ? platform : '',
                    browser: (browser) ? browser : '',
                    is_offer_product: is_offer_product,
                    order_status: 'Pending',
                    exp_eta: parsedExpEta,
                    is_zap_order: true,
                }).returning('order_id');

            logToFile(`[getQuickordercheckout] SUCCESSFULLY INSERTED into orders table for groupId ${groupId}`);
            console.log(`[getQuickordercheckout] SUCCESSFULLY INSERTED into orders table for groupId ${groupId}`);

            await knex('store_orders')
                .where('store_order_id', storeOrderId)
                .update({
                    'order_cart_id': cartId,
                });

            const getSub = await knex('subscription_order')
                .select('id')
                .orderBy('id', 'DESC')
                .first();
            let subscription_id = (getSub) ? getSub.id : 0;
            subscription_id = subscription_id + 1;

            logToFile(`[getQuickordercheckout] Inserting into subscription_order for groupId ${groupId}: ` + JSON.stringify({
                id: subscription_id,
                store_order_id: storeOrderId,
                cart_id: cartId,
                user_id: userId,
                order_id: nextOrderId
            }));
            console.log(`[getQuickordercheckout] Inserting into subscription_order for groupId ${groupId}`);

            const subscriptionID = await knex('subscription_order')
                .insert({
                    id: subscription_id,
                    'store_order_id': storeOrderId,
                    'cart_id': cartId,
                    'user_id': userId,
                    'order_id': nextOrderId,
                    'store_id': storeId,
                    'delivery_date': deliverydateval,
                    'time_slot': timeslotval,
                    'time_slot_discount': (timeslotdata) ? timeslotdata.discount : 0,
                    'created_date': orderDateTimeDubai,
                    'order_status': 'Pending',
                    'si_payment_flag': (paymentMethod.toLowerCase() === 'cod') ? "no" : "yes",
                    'group_id': groupId,
                    'subscription_id': subscription_id,
                    'platform': (platform) ? platform : '',
                    'browser': (browser) ? browser : '',
                    is_zap_order: true,
                });

            logToFile(`[getQuickordercheckout] SUCCESSFULLY INSERTED into subscription_order (id: ${subscription_id}) for groupId ${groupId}`);
            console.log(`[getQuickordercheckout] SUCCESSFULLY INSERTED into subscription_order (id: ${subscription_id})`);

            // Generate invoice automatically after order is placed
            await userModel.generateInvoice({ user_id: userId, cart_id: cartId });

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

            if (paidByRefWallet > 0) {
                const nearestExpiry = await knex("wallet_history")
                    .select("expiry_date")
                    .where("user_id", userId)
                    .whereNotNull("expiry_date")
                    .where("expiry_date", ">", knex.fn.now())
                    .orderBy("expiry_date", "asc")
                    .first();

                const maxWIdResultRef = await knex('wallet_history').max('w_id as maxWId').first();
                const nextWIdRef = (maxWIdResultRef?.maxWId != null ? parseInt(maxWIdResultRef.maxWId, 10) : 0) + 1;

                await knex('wallet_history').insert({
                    w_id: nextWIdRef,
                    user_id: userId,
                    amount: paidByRefWallet.toFixed(2),
                    resource: 'order_placed_wallet_ref',
                    type: 'deduction',
                    group_id: groupId,
                    cart_id: cartId,
                    expiry_date: nearestExpiry ? nearestExpiry.expiry_date : null,
                });
            }

        }

        //Wallet amount update for and insert wallet (only when wallet is used)
        // if (walletEnabled) {
        //     await knex('users')
        //         .where('id', userId)
        //         .update({ 'wallet': WalletBalanace });
        // }
        await knex('users')
            .where('id', userId)
            .update({
                'wallet_balance': WalletBalance,
                'referral_balance': RefWalletBalance
            })

        totalCal = await knex('orders')
            .where('group_id', groupId)
            .select(
                knex.raw('COALESCE(SUM(coupon_discount::double precision), 0) as coupon_discount'),
                knex.raw('COALESCE(SUM(rem_price::double precision), 0) as rem_price'),
                knex.raw('COALESCE(SUM(paid_by_wallet::double precision), 0) as paid_by_wallet'),
                knex.raw('COALESCE(SUM(paid_by_ref_wallet::double precision), 0) as paid_by_ref_wallet'),
                knex.raw("COALESCE(SUM(COALESCE(NULLIF(TRIM(COALESCE(cod_charges, '')), '')::double precision, 0)), 0) as cod_charges"),
                knex.raw('COALESCE(SUM(total_products_mrp::double precision), 0) as total_products_mrp')
            )
            .first();
        finalAmount = parseFloat(totalCal.total_products_mrp).toFixed(2);

        //check offer for lucky draw
        const lastofferdate = process.env.LAST_OFFER_DATE;
        // && paymentStatus == 'success' && paymentStatus == 'success'
        if (todayDubai <= lastofferdate && finalAmount >= 100) {
            const maxLuckydrawResult = await knex('tbl_luckydraw').max('id as maxId').first();
            const nextLuckydrawId = (maxLuckydrawResult?.maxId ? parseInt(maxLuckydrawResult.maxId, 10) : 0) + 1;
            await knex('tbl_luckydraw').insert({
                id: nextLuckydrawId,
                user_id: userId,
                order_id: groupId,
                order_type: 'quick',
            });

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
                "campaignName": "updateQuickOrderAppleGiveaway",
                "destination": "+" + phone_with_country_code,
                "userName": "Quickart General Trading Co LLC",
                "templateParams": [
                    getUserup.name, groupId, finalAmount, `${usertotalorders.length}`
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
            logToFile("ai sensy response: " + JSON.stringify(result));
        }

        //SMS Code
        message = "Thank you for your order! Your order " + groupId + " is now being processed & will be delivered right on schedule.  "
        sendSMSOrder(userPhone, message);

        //Email Code 
        totalCal = await knex('orders')
            .where('group_id', groupId)
            .select(
                knex.raw('COALESCE(SUM(coupon_discount::double precision), 0) as coupon_discount'),
                knex.raw('COALESCE(SUM(rem_price::double precision), 0) as rem_price'),
                knex.raw('COALESCE(SUM(paid_by_wallet::double precision), 0) as paid_by_wallet'),
                knex.raw('COALESCE(SUM(paid_by_ref_wallet::double precision), 0) as paid_by_ref_wallet'),
                knex.raw("COALESCE(SUM(COALESCE(NULLIF(TRIM(COALESCE(cod_charges, '')), '')::double precision, 0)), 0) as cod_charges"),
                knex.raw('COALESCE(SUM(total_products_mrp::double precision), 0) as total_products_mrp')
            )
            .first();

        storeOrders = await knex('store_orders')
            .select('store_orders.*', 'orders.group_id', 'orders.time_slot', 'orders.total_delivery')
            .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
            .where('orders.group_id', groupId);


        const logo = await knex('tbl_web_setting').first();
        const appName = logo ? logo.name : null;
        // Fetching the first record from the 'currency' table
        const currency = await knex('currency').first();
        const currencySign = currency ? currency.currency_sign : null;

        const formattedDate = todayDubai;

        const templateData = {
            baseurl: process.env.BASE_URL,
            group_id: groupId,
            user_name: userName,
            user_email: userEmail,
            paymentMethod: paymentMethod,
            delivery_date: deliverydateval,
            orderss_address: ar.house_no + ', ' + ar.landmark + ', ' + ar.society,
            store_orderss: storeOrders,
            coupon_discount: totalCal.coupon_discount.toFixed(2),
            paid_by_wallet: totalCal.paid_by_wallet.toFixed(2),
            paid_by_ref_wallet: totalCal.paid_by_ref_wallet.toFixed(2),
            cod_charges: totalCal.cod_charges.toFixed(2),
            final_amount: finalAmount,
            app_name: appName,
            currency_sign: currencySign,
            order_type: "quick",
            order_date: formattedDate,
        };
        const subject = 'Order Successfully Placed'
        // Trigger the email after order is placed
        //  sendMail = await codorderplacedMail(userEmail,templateData,subject,groupId);
        logToFile("ai sensy response: " + JSON.stringify(templateData));

        try {
            await knex('performance_checkpoints').insert({
                group_id: groupId,
                checkout_ts: knex.raw("now() AT TIME ZONE 'Asia/Dubai'")
            });
        } catch (pcErr) {
            logToFile(`[getQuickordercheckout] performance_checkpoints insert failed for groupId ${groupId}: ${pcErr.message}`);
        }

        return "success";
    } catch (error) {
        logToFile(`[getQuickordercheckout] CRITICAL ERROR for groupId ${appDetails.group_id}: ${error.stack}`);
        console.error(`[getQuickordercheckout] CRITICAL ERROR:`, error);
        throw error;
    }
};

const getQuickOrderCheckoutSdk = async (appDetails) => {
    try {
        logToFile("getQuickOrderCheckoutSdk STARTED for groupId: " + appDetails.group_id);
        console.log("[getQuickOrderCheckoutSdk] STARTED for appDetails:", JSON.stringify(appDetails).substring(0, 500) + "...");
        logToFile("getQuickordercheckout STARTED" + JSON.stringify(appDetails));

        const current = new Date();

        // Destructure other details from appDetails
        const {
            user_id: userId,
            address_id: addressId,
            delivery_date: deliveryDate,
            time_slot: timeSlot,
            store_id: storeId,
            del_partner_tip: delPartnerTips,
            del_partner_instruction: delPartnerInstruction,
            order_instruction: order_instruction,
            payment_method: paymentMethod,
            payment_status: paymentStatus,
            wallet,
            totalwalletamt: totalWalletAmt,
            coupon_id: couponId,
            coupon_code: couponCode,
            payment_id: paymentId,
            payment_gateway: paymentGateway,
            si_sub_ref_no: siSubRefNo,
            bank_id: bankId,
            payment_type: paymentType,
            group_id: paymentOrderId,
            platform: platform,
            browser: browser,
        } = appDetails;


        const delPartnerTip = delPartnerTips || 0;


        // Determine siStatus based on siSubRefNo
        const siStatus = siSubRefNo ? "yes" : "no";
        const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
        const hashSubstring = hash.substring(0, 2); // Adjust substring length as needed
        // Combine all parts to form group_id
        const groupId = (paymentOrderId) ? paymentOrderId : generateRandomLetters(6) + generateRandomDigits(2) + hashSubstring;
        // Query address from the database
        const ar = await knex('address')
            .select('society', 'city', 'city_id', 'society_id', 'lat', 'lng', 'address_id', 'house_no', 'receiver_email', 'landmark')
            .where('user_id', userId) // Check if this is the correct condition for your use case
            .where('address_id', addressId)
            .first();

        if (!ar) {
            logToFile("No address found for the provided user ID and address ID ${userId} - ${addressId}");
            throw new Error('No address found for the provided user ID and address ID');
        }


        // Get today's date and current time in Dubai timezone
        const dubaiTime = moment.tz("Asia/Dubai");
        const todayDubai = dubaiTime.format("YYYY-MM-DD");
        const orderDateTimeDubai = dubaiTime.toDate();
        const isAfter6PM = dubaiTime.hour() >= 17;
        // const isAfter12PM = dubaiTime.hour() >= 14;
        const isAfter12PM = dubaiTime.isSameOrAfter(
            moment.tz("Asia/Dubai").hour(11).minute(0).second(0)
        );
        // const cutoff11AM = dubaiTime.clone()
        // .startOf('day')
        // .hour(11)
        // .minute(0)
        // .second(0);

        // const isAfter12PM = dubaiTime.isSameOrAfter(cutoff11AM);

        // if (!appDetails.time_slot || appDetails.time_slot === undefined || appDetails.time_slot === null || appDetails.time_slot === '' || appDetails.time_slot === 'undefined') {
        //     //   || deliveryDate === undefined || deliveryDate === null || deliveryDate === '' || deliveryDate === 'undefined'
        //     logToFile("time slot is blank or undefined");
        //     throw new Error("This timeslot is not available for selected products delivery, kindly select different delivery date");
        // }

        // Condition 1: Check if any order has a sub_delivery_date of today
        // if (deliveryDate === todayDubai && isAfter12PM) {
        //      logToFile("Order blocked: after 12PM for today");
        // throw new Error("Unable to place order for selected date time. Please select different date and time.");
        // }


        // // Condition 2: If it's after 6 PM in Dubai, prevent placing orders for tomorrow with "06:00 am - 10:00 am" time slot
        // if (isAfter6PM) {
        // const tomorrowDubai = dubaiTime.add(1, 'day').format("YYYY-MM-DD"); // Get tomorrow's date in Dubai time
        // if (deliveryDate == tomorrowDubai && timeSlot == "06:00 am - 10:00 am") {
        //     logToFile("Order blocked:  If it's after 6 PM in Dubai, prevent placing orders for tomorrow with 06:00 am - 10:00 am time slot");
        // throw new Error(`Unable to place order for selected date time. Please select different date and time.`);
        // }
        // }

        // // Condition 3: Check if any order has a sub_delivery_date of today
        // if (deliveryDate === todayDubai && (timeSlot == "06:00 am - 10:00 am" || timeSlot == "02:00 pm - 05:00 pm" || timeSlot == "02:00 pm - 04:00 pm")) {
        //     logToFile("Order blocked:  Check if any order has a sub_delivery_date of today");
        // throw new Error("Unable to place order for selected date time. Please select different date and time.");
        // }

        //stock 0 Or not check 
        // const storeItemList = await knex('store_orders')
        // .select('store_products.stock','store_orders.*')
        // .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
        // .join('product_varient','store_products.varient_id','=','product_varient.varient_id')
        // .join('product','product_varient.product_id','=','product.product_id')
        // .where('store_orders.store_approval', userId)
        // .where('store_orders.order_cart_id', 'incart')
        // .whereNull('subscription_flag');
        // // Iterate through the list of items and check the stock
        // for (const storeItem of storeItemList) {
        // if (storeItem.stock === 0) {
        // throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
        // }
        // }

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
            .where('store_orders.store_approval', userId)
            .where('store_orders.order_cart_id', 'incart')
            .whereNull('subscription_flag');
        // Iterate through the list of items and check the stock
        if (storeItemList.length > 0 || storeItemList != null) {
            for (const storeItem of storeItemList) {
                if (storeItem.stock === 0 || storeItem.product_hide == 1 || storeItem.product_delete == 1) {
                    logToFile("One or more items in your cart are unavailable/out of stock. Unable to proceed with the order. ");
                    throw new Error("One or more items in your cart are unavailable/out of stock. Unable to proceed with the order.");
                }

                if (!storeItem.sub_delivery_date || storeItem.sub_delivery_date === undefined || storeItem.sub_delivery_date === null || storeItem.sub_delivery_date === '' || storeItem.sub_delivery_date === 'undefined'
                    || storeItem.sub_time_slot === undefined || storeItem.sub_time_slot === null || storeItem.sub_time_slot === '' || storeItem.sub_time_slot === 'undefined') {
                    logToFile("time slot is blank or undefined");
                    throw new Error("This timeslot is not available for selected products delivery, kindly select different delivery date");
                }

                // Date/time slot cut-off blocks removed as requested.
            }
        }




        // Fetch user phone and wallet
        const user = await knex('users')
            .select('user_phone', 'wallet', 'country_code', 'name', 'email')
            .where('id', userId)
            .first();
        let actualWallet = user.wallet;
        const walletEnabled = wallet.toLowerCase() === 'yes' && parseFloat(totalWalletAmt || 0) > 0;
        let WalletBalanace = walletEnabled ? user.wallet - totalWalletAmt : user.wallet;
        let userPhone = user.country_code + user.user_phone;
        let userName = user.name;
        let userEmail = (user.email) ? user.email : ar.receiver_email;

        if (!user) {
            logToFile("User not found ");
            throw new Error('User not found');
        }
        //store order details
        const storeDetailsAmt = await knex('store_orders')
            .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
            .join('product', 'product_varient.product_id', '=', 'product.product_id')
            .where('store_orders.store_approval', userId)
            .where('store_orders.order_cart_id', 'incart')
            .whereNull('subscription_flag')
            .select(knex.raw('SUM(store_orders.total_mrp) as Totalmrp'), knex.raw('SUM(store_orders.price) as Totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
            .first();

        // Fetch delivery charges
        const deliveryFlag = await knex('app_settings')
            .where('store_id', storeId)
            .select('cod_charges')
            .first();

        const codCharges = (paymentMethod.toLowerCase() === 'cod') ? deliveryFlag.cod_charges : 0;

        let couponPriceAmount = 0;
        if (couponCode) {
            const CouponDiscount = await knex('store_orders')
                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                .sum({ total_price: 'store_orders.price' })
                .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
                .where('store_orders.order_cart_id', 'incart')
                .where('store_orders.store_approval', userId)
                .whereNull('store_orders.subscription_flag')
                .first();

            const CouponDetails = await knex('coupon')
                .where('coupon_code', couponCode)
                .first();
            if (CouponDetails) {
                couponPriceAmount = (parseFloat(CouponDiscount.total_price) * parseFloat(CouponDetails.amount)) / 100;
            } else {
                couponPriceAmount = 0;
            }
        } else {
            couponPriceAmount = 0;
        }
        const storeTotalPriceSub = parseFloat(storeDetailsAmt?.totalprice ?? storeDetailsAmt?.Totalprice ?? 0);
        const TotalpriceAmount = ((storeTotalPriceSub + parseFloat(delPartnerTip) + parseFloat(codCharges)) - (parseFloat(couponPriceAmount) + parseFloat(totalWalletAmt)));
        const WithWalletAmount = parseFloat(TotalpriceAmount) + parseFloat(totalWalletAmt);
        const WalletDiscountAmount = ((((storeTotalPriceSub + parseFloat(delPartnerTip) + parseFloat(codCharges)) - couponPriceAmount) * 50) / 100).toFixed(2);
        const WalletStatus = (wallet.toLowerCase() === 'yes' && actualWallet >= WalletDiscountAmount) ? 'percentage' : 'fixed';


        if (siStatus == 'yes' || paymentMethod == 'COD') {
            const number = groupId;
            const description = "";
            const amount = (TotalpriceAmount) ? TotalpriceAmount : "0";
            const orderJson = { number, description, amount };
            const ordertype = "quick";
            const payment_status = (paymentMethod.toLowerCase() === 'cod') ? 'Pending' : 'success';
            const group_id = groupId;
            const user_id = userId;
            const bank_id = 0;
            const si_sub_ref_no = siSubRefNo;
            const store_id = storeId;
            const payment_method = paymentMethod;
            const payment_gateway = paymentGateway;
            const payment_id = paymentId;
            const coupon_id = couponId;
            const coupon_code = couponCode;
            const discount_amount = 0;
            const delivery_date = deliveryDate;
            const time_slot = timeSlot;
            const del_partner_tip = delPartnerTips;
            const del_partner_instruction = delPartnerInstruction;
            const device_id = "";
            const totalwalletamt = totalWalletAmt;
            const is_subscription = null;
            const payment_type = paymentType;
            const address_id = addressId;
            const custom_data = { address_id, ordertype, payment_status, group_id, user_id, bank_id, si_sub_ref_no, store_id, payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount, delivery_date, time_slot, del_partner_tip, del_partner_instruction, order_instruction, device_id, totalwalletamt, is_subscription, payment_type, platform, browser, storeItemList };

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

            const maxIdResult = await knex('payment_order_request_details').max('id as maxId').first();
            const nextId = (maxIdResult?.maxId ? parseInt(maxIdResult.maxId, 10) : 0) + 1;

            const insert = await knex('payment_order_request_details').insert({
                id: nextId,
                json_data: mainJsonSaveRequest,
                group_id: group_id,
                order_type: ordertype,
                added_on: new Date()
            });

            logToFile("COD Order Detail: " + JSON.stringify(mainJsonSaveRequest));
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
                // TotalPay Credentials for Test
                const merchantKey = '968abd2e-79ce-11ef-8430-ee2650fd5759';
                const merchantpassword = 'abdf10a546b5197cdf81508a3d3c9e23';
                // TotalPay Credentials for Live
                //  const merchantKey = '7f066f26-36b4-11ee-8433-eecb8191d36e';
                //  const merchantpassword = '96bb03851c3553fd132339acc06ce060';
                const recurring_token = BankDetails.si_sub_ref_no;
                const pay_amounts = (TotalpriceAmount) ? TotalpriceAmount : 0;
                const orderNumber = groupId;
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
                        number: groupId,
                        amount: (amount) ? amount : 0,
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
                    const status = data.status.toLowerCase();
                    if (status === 'decline' || status === 'pending') {
                        const errorMessage = status === 'decline'
                            ? 'Card issue detected! Status: decline'
                            : `HTTP error! Status: ${data.status}`;
                        throw new Error(errorMessage);
                    }

                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }

                    const maxSiLogResult = await knex('tbl_si_deduction_log').max('id as maxId').first();
                    const nextSiLogId = (maxSiLogResult?.maxId ? parseInt(maxSiLogResult.maxId, 10) : 0) + 1;
                    await knex('tbl_si_deduction_log').insert({
                        id: nextSiLogId,
                        si_sub_ref_no: siSubRefNo,
                        user_id: userId,
                        amount: (TotalpriceAmount) ? parseFloat(TotalpriceAmount).toFixed(2) : 0,
                        payment_method: 'SI',
                        payment_status: 0,
                        card_id: groupId,
                    });
                    //return await response.json();
                } catch (error) {
                    logToFile(`checkoutModel fun getQuickordercheckout error: ${error instanceof Error ? error.stack || error.message : JSON.stringify(error)}`);
                    console.error('Error sending payment data:', error);
                    throw error; // Re-throw for handling in the calling code
                }


            } else {
                // No BankDetails found
                logToFile("No bank details found for the given criteria.");
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
            .where('store_orders.order_cart_id', "incart");

        let price2 = 0;
        let tax_p = 0;
        let tax_price = 0;
        let TotalpriceStore = (storeDetailsAmt?.totalprice ?? storeDetailsAmt?.Totalprice ?? 0);
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
                .whereNull('subscription_flag')
                .first();

            if (!product) {
                throw new Error('Product not found');
            }

            const currentDate1 = new Date().toISOString().split('T')[0];
            let offer_product = await knex('product')
                .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
                .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
                .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
                .select('product.product_id', 'product.product_name', 'product.offer_price as price',
                    'product_varient.quantity', 'product_varient.unit',
                    'store_products.mrp',
                    'tbl_country.country_icon',
                    'store_products.stock')
                .where('product_varient.varient_id', varientId)
                .where('product.offer_date', currentDate1)
                .first();

            const { price, total_mrp, mrp, tx_per: taxPer, tx_price: taxPrice, min_ord_qty: minOrderQty, max_ord_qty: maxOrderQty, stock, product_name: productName, quantity, unit, varient_image: varientImage } = product;
            const totalMrp = (offer_product) ? offer_product.price : mrp; // Adjust this if needed
            price2 = (offer_product) ? offer_product.price : price;

            const codorderamt = codCharges ? (price2 * codCharges / TotalpriceStore) : 0;
            const delPartnerTipAmt = delPartnerTip ? (price2 * delPartnerTip / TotalpriceStore) : 0;
            let couponPriceProduct = 0;
            const CouponDiscounts = await knex('store_orders')
                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                .sum({ total_price: 'store_orders.price' })
                .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
                .where('store_orders.order_cart_id', 'incart')
                .where('store_orders.varient_id', varientId)
                .where('store_orders.store_approval', userId)
                .whereNull('subscription_flag')
                .first();

            if (couponCode && (CouponDiscounts.total_price > 0)) {
                const CouponDiscount = await knex('store_orders')
                    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                    .sum({ total_price: 'store_orders.price' })
                    .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
                    .where('store_orders.order_cart_id', 'incart')
                    .where('store_orders.store_approval', userId)
                    .whereNull('store_orders.subscription_flag')
                    .first();

                const CouponDetails = await knex('coupon')
                    .where('coupon_code', couponCode)
                    .first();
                if (CouponDetails) {
                    couponPriceProduct = (parseFloat(CouponDiscounts.total_price || 0) * parseFloat(CouponDetails.amount)) / 100;
                } else {
                    couponPriceProduct = 0;
                }
            } else {
                couponPriceProduct = 0;
            }
            const totalPrice = (parseFloat(price2) + parseFloat(codorderamt) + parseFloat(delPartnerTipAmt)) - parseFloat(couponPriceProduct);

            let paidByWallet = 0;

            if (wallet.toLowerCase() === 'yes') {
                if (WalletStatus == 'percentage') {
                    paidByWallet = (parseFloat(totalPrice) * 50) / 100;
                } else {
                    paidByWallet = (parseFloat(totalPrice) * parseFloat(totalWalletAmt)) / parseFloat(WithWalletAmount);
                }
            }

            const paymentStatus = (paymentMethod.toLowerCase() === 'cod') ? 'Pending' : 'success';
            const remPrice = (paymentMethod.toLowerCase() === 'cod') ? (totalPrice - paidByWallet) : 0;

            const timeslotval = (productList.sub_time_slot) ? productList.sub_time_slot : timeSlot;
            // For quick orders we always persist today's date (Dubai timezone),
            // regardless of what the client/request sent.
            var deliverydateval = todayDubai;

            //fetch timeslot discount
            const timeslotdata = await knex('tbl_time_slots')
                .where('time_slots', timeslotval)
                .first();

            var is_offer_product = (productList.is_offer_product == 1) ? 1 : 0;

            if (couponId && couponId != null && couponId != 'null') {
                var couponID = couponId;
                var couponcode = couponCode;
            } else {
                var couponID = 0;
                var couponcode = null;
            }

            const maxOrderIdResult = await knex('orders').max('order_id as maxOrderId').first();
            const nextOrderId = (maxOrderIdResult?.maxOrderId ? parseInt(maxOrderIdResult.maxOrderId, 10) : 0) + 1;

            logToFile(`[getQuickOrderCheckoutSdk] Inserting into orders table for groupId ${groupId}: ` + JSON.stringify({
                order_id: nextOrderId,
                cart_id: cartId,
                total_price: totalPrice.toFixed(2),
                user_id: userId,
                store_id: storeId,
                group_id: groupId,
                payment_method: paymentMethod,
                payment_status: paymentStatus
            }));
            console.log(`[getQuickOrderCheckoutSdk] Inserting into orders table for groupId ${groupId}`);

            const orderID = await knex('orders')
                .insert({
                    order_id: nextOrderId,
                    cart_id: cartId,
                    total_price: totalPrice.toFixed(2),
                    price_without_delivery: totalPrice.toFixed(2),
                    total_products_mrp: totalPrice.toFixed(2),
                    delivery_charge: 0,
                    user_id: userId,
                    store_id: storeId,
                    rem_price: (remPrice) ? parseFloat(remPrice).toFixed(2) : 0,
                    order_date: current,
                    delivery_date: deliverydateval,
                    time_slot: timeslotval,
                    address_id: addressId,
                    avg_tax_per: 0,
                    total_tax_price: 0,
                    total_delivery: 1, // Adjust if needed
                    del_partner_tip: (delPartnerTipAmt) ? parseFloat(delPartnerTipAmt).toFixed(2) : 0,
                    del_partner_instruction: delPartnerInstruction,
                    order_instruction: order_instruction,
                    group_id: groupId, // Use groupId for group_id or adjust as needed
                    is_subscription: null,
                    cod_charges: (codorderamt) ? parseFloat(codorderamt).toFixed(2) : 0,
                    paid_by_wallet: (paidByWallet) ? parseFloat(paidByWallet).toFixed(2) : 0,
                    payment_method: paymentMethod,
                    si_sub_ref_no: siSubRefNo,
                    coupon_id: couponID,
                    coupon_code: couponcode,
                    coupon_discount: (couponPriceProduct) ? parseFloat(couponPriceProduct).toFixed(2) : 0,
                    payment_status: paymentStatus,
                    payment_type: paymentType,
                    pastorecentrder: 'new',
                    platform: (platform) ? platform : '',
                    browser: (browser) ? browser : '',
                    is_offer_product: is_offer_product,
                    order_status: 'Pending',
                    is_zap_order: true,
                }).returning('order_id');

            logToFile(`[getQuickOrderCheckoutSdk] SUCCESSFULLY INSERTED into orders table for groupId ${groupId}`);
            console.log(`[getQuickOrderCheckoutSdk] SUCCESSFULLY INSERTED into orders table for groupId ${groupId}`);

            await knex('store_orders')
                .where('store_order_id', storeOrderId)
                .update({
                    'order_cart_id': cartId,
                });

            const getSub = await knex('subscription_order')
                .select('id')
                .orderBy('id', 'DESC')
                .first();
            let subscription_id = (getSub) ? getSub.id : 0;
            subscription_id = subscription_id + 1;

            logToFile(`[getQuickOrderCheckoutSdk] Inserting into subscription_order for groupId ${groupId}: ` + JSON.stringify({
                id: subscription_id,
                store_order_id: storeOrderId,
                cart_id: cartId,
                user_id: userId,
                order_id: nextOrderId
            }));
            console.log(`[getQuickOrderCheckoutSdk] Inserting into subscription_order for groupId ${groupId}`);

            const subscriptionID = await knex('subscription_order')
                .insert({
                    id: subscription_id,
                    'store_order_id': storeOrderId,
                    'cart_id': cartId,
                    'user_id': userId,
                    'order_id': nextOrderId,
                    'store_id': storeId,
                    'delivery_date': deliverydateval,
                    'time_slot': timeslotval,
                    'time_slot_discount': (timeslotdata) ? timeslotdata.discount : 0,
                    'created_date': current,
                    'order_status': 'Pending',
                    'si_payment_flag': (paymentMethod.toLowerCase() === 'cod') ? "no" : "yes",
                    'group_id': groupId,
                    'subscription_id': subscription_id,
                    'platform': (platform) ? platform : '',
                    'browser': (browser) ? browser : '',
                    is_zap_order: true,
                });

            logToFile(`[getQuickOrderCheckoutSdk] SUCCESSFULLY INSERTED into subscription_order (id: ${subscription_id}) for groupId ${groupId}`);
            console.log(`[getQuickOrderCheckoutSdk] SUCCESSFULLY INSERTED into subscription_order (id: ${subscription_id})`);

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

        }

        //Wallet amount update for and insert wallet (only when wallet is used)
        if (walletEnabled) {
            await knex('users')
                .where('id', userId)
                .update({ 'wallet': WalletBalanace });
        }

        totalCal = await knex('orders')
            .where('group_id', groupId)
            .select(
                knex.raw('COALESCE(SUM(coupon_discount::double precision), 0) as coupon_discount'),
                knex.raw('COALESCE(SUM(rem_price::double precision), 0) as rem_price'),
                knex.raw('COALESCE(SUM(paid_by_wallet::double precision), 0) as paid_by_wallet'),
                knex.raw("COALESCE(SUM(COALESCE(NULLIF(TRIM(COALESCE(cod_charges, '')), '')::double precision, 0)), 0) as cod_charges"),
                knex.raw('COALESCE(SUM(total_products_mrp::double precision), 0) as total_products_mrp')
            )
            .first();
        finalAmount = parseFloat(totalCal.total_products_mrp).toFixed(2);

        //check offer for lucky draw
        const lastofferdate = process.env.LAST_OFFER_DATE;
        const currentDate = new Date().toISOString().split('T')[0];
        // && paymentStatus == 'success' && paymentStatus == 'success'
        if (currentDate <= lastofferdate && finalAmount >= 100) {
            const maxLuckydrawResult = await knex('tbl_luckydraw').max('id as maxId').first();
            const nextLuckydrawId = (maxLuckydrawResult?.maxId ? parseInt(maxLuckydrawResult.maxId, 10) : 0) + 1;
            await knex('tbl_luckydraw').insert({
                id: nextLuckydrawId,
                user_id: userId,
                order_id: groupId,
                order_type: 'quick',
            });

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
                "campaignName": "updateQuickOrderAppleGiveaway",
                "destination": "+" + phone_with_country_code,
                "userName": "Quickart General Trading Co LLC",
                "templateParams": [
                    getUserup.name, groupId, finalAmount, `${usertotalorders.length}`
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
            logToFile("ai sensy response: " + JSON.stringify(result));
        }

        //SMS Code
        message = "Thank you for your order! Your order " + groupId + " is now being processed & will be delivered right on schedule.  "
        sendSMSOrder(userPhone, message);

        //Email Code 
        totalCal = await knex('orders')
            .where('group_id', groupId)
            .select(
                knex.raw('COALESCE(SUM(coupon_discount::double precision), 0) as coupon_discount'),
                knex.raw('COALESCE(SUM(rem_price::double precision), 0) as rem_price'),
                knex.raw('COALESCE(SUM(paid_by_wallet::double precision), 0) as paid_by_wallet'),
                knex.raw("COALESCE(SUM(COALESCE(NULLIF(TRIM(COALESCE(cod_charges, '')), '')::double precision, 0)), 0) as cod_charges"),
                knex.raw('COALESCE(SUM(total_products_mrp::double precision), 0) as total_products_mrp')
            )
            .first();

        storeOrders = await knex('store_orders')
            .select('store_orders.*', 'orders.group_id', 'orders.time_slot', 'orders.total_delivery')
            .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
            .where('orders.group_id', groupId);


        const logo = await knex('tbl_web_setting').first();
        const appName = logo ? logo.name : null;
        // Fetching the first record from the 'currency' table
        const currency = await knex('currency').first();
        const currencySign = currency ? currency.currency_sign : null;

        // Get the year, month, and day in the required format
        const year = current.getFullYear();
        const month = String(current.getMonth() + 1).padStart(2, '0'); // Months are zero-based, so add 1
        const day = String(current.getDate()).padStart(2, '0');
        // Combine into the desired format
        const formattedDate = `${year}-${month}-${day}`;

        const templateData = {
            baseurl: process.env.BASE_URL,
            group_id: groupId,
            user_name: userName,
            user_email: userEmail,
            paymentMethod: paymentMethod,
            delivery_date: deliverydateval,
            orderss_address: ar.house_no + ', ' + ar.landmark + ', ' + ar.society,
            store_orderss: storeOrders,
            coupon_discount: totalCal.coupon_discount.toFixed(2),
            paid_by_wallet: totalCal.paid_by_wallet.toFixed(2),
            cod_charges: totalCal.cod_charges.toFixed(2),
            final_amount: finalAmount,
            app_name: appName,
            currency_sign: currencySign,
            order_type: "quick",
            order_date: formattedDate,
        };
        const subject = 'Order Successfully Placed'
        // Trigger the email after order is placed
        //  sendMail = await codorderplacedMail(userEmail,templateData,subject,groupId);
        logToFile("ai sensy response: " + JSON.stringify(templateData));

        var currentorders = await knex('orders')
            .where('group_id', groupId)
            .first();

        return currentorders;
    } catch (error) {
        logToFile(`[getQuickOrderCheckoutSdk] CRITICAL ERROR for groupId ${appDetails.group_id}: ${error.stack}`);
        console.error(`[getQuickOrderCheckoutSdk] CRITICAL ERROR:`, error);
        throw error;
    }
};

const getSubordercheckout = async (appDetails) => {
    try {
        logToFile("getSubordercheckout STARTED for groupId: " + appDetails.group_id);
        console.log("[getSubordercheckout] STARTED for appDetails:", JSON.stringify(appDetails).substring(0, 500) + "...");
        const current = new Date();

        // Normalize userId for PostgreSQL (address.user_id, store_approval are TEXT; tbl_user_bank_details.user_id is integer)
        const userIdInt = parseInt(appDetails.user_id, 10);
        const userIdStr = String(appDetails.user_id);

        // Destructure other details from appDetails
        let {
            user_id: userId,
            address_id: addressId,
            delivery_date: deliveryDate,
            time_slot: timeSlot,
            store_id: storeId,
            del_partner_tip: delPartnerTip,
            del_partner_instruction: del_partner_instruction,
            order_instruction: order_instruction,
            payment_method: paymentMethod,
            payment_status: paymentStatus,
            wallet,
            totalwalletamt: totalWalletAmt,
            totalrefwalletamt: totalRefWalletAmt,
            coupon_id: couponId,
            coupon_code: couponCode,
            payment_id: paymentId,
            payment_gateway: paymentGateway,
            si_sub_ref_no: siSubRefNo,
            bank_id: bankId,
            is_subscription: isSubscription,
            payment_type: paymentType,
            //AutoRenewSubCart:AutoRenewSubCart,
            group_id: paymentOrderId,
            platform: platform,
            browser: browser,
        } = appDetails;
        const del_partner_tip = 0;

        // Determine siStatus based on siSubRefNo
        const siStatus = siSubRefNo ? "yes" : "no";
        const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
        const hashSubstring = hash.substring(0, 2); // Adjust substring length as needed
        // Combine all parts to form group_id
        const groupId = (paymentOrderId) ? paymentOrderId : generateRandomLetters(6) + generateRandomDigits(2) + hashSubstring;

        // Parallel fetch: address, user, reserveAmt, storeDetailsAmt, storeOrders (merged storeItemList + ordersDetails) - 5 queries in 1 round-trip
        const [ar, user, reserveAmt, storeDetailsAmt, ordersDetails] = await Promise.all([
            knex('address')
                .select('society', 'city', 'city_id', 'society_id', 'lat', 'lng', 'address_id', 'house_no', 'receiver_email', 'landmark')
                .where('user_id', userIdStr)
                .where('address_id', addressId)
                .first(),
            knex('users')
                .select('user_phone', 'wallet', 'wallet_balance', 'referral_balance', 'country_code', 'name', 'email')
                .where('id', userIdInt)
                .first(),
            knex('orders')
                .innerJoin('subscription_order', 'subscription_order.cart_id', '=', 'orders.cart_id')
                .select('orders.reserve_amount')
                .where('orders.is_subscription', 1)
                .where('orders.user_id', userIdStr)
                .groupBy('orders.order_id'),
            knex('store_orders')
                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                .join('product', 'product_varient.product_id', '=', 'product.product_id')
                .where('store_orders.store_approval', userIdStr)
                .where('store_orders.order_cart_id', 'incart')
                .where('subscription_flag', '1')
                .select(knex.raw('SUM(store_orders.total_mrp) as Totalmrp'), knex.raw('SUM(store_orders.price) as Totalprice'), knex.raw('COUNT(store_orders.store_order_id) as count'))
                .first(),
            knex('store_orders')
                .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
                .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                .join('product', 'product_varient.product_id', '=', 'product.product_id')
                .select('store_orders.*', 'store_products.stock', 'store_products.mrp',
                    'product.hide as product_hide', 'product.is_delete as product_delete')
                .where('store_orders.store_approval', userIdStr)
                .where('subscription_flag', '1')
                .where('store_orders.order_cart_id', "incart")
        ]);

        if (!ar) {
            logToFile("No address found for the provided user ID and address ID");
            throw new Error('No address found for the provided user ID and address ID');
        }
        if (!user) {
            throw new Error('User not found');
        }

        // Wallet calculation when wallet=yes and both wallet amounts are not explicitly provided.
        // If user intentionally sends only one wallet amount (e.g. referral only), do not auto-fill the other.
        if ((wallet || '').toLowerCase() === 'yes') {
            const isWalletAmtMissing = totalWalletAmt === undefined || totalWalletAmt === null || totalWalletAmt === '';
            const isRefWalletAmtMissing = totalRefWalletAmt === undefined || totalRefWalletAmt === null || totalRefWalletAmt === '';

            if (isWalletAmtMissing && isRefWalletAmtMissing) {
                let totalReserveAmt = 0;
                reserveAmt.forEach(item => { totalReserveAmt += parseFloat(item.reserve_amount); });

                let actualWallet = parseFloat(user.wallet_balance || 0);
                let actualRefWallet = parseFloat(user.referral_balance || 0);

                const storeTotalPriceSub = parseFloat(storeDetailsAmt?.totalprice ?? storeDetailsAmt?.Totalprice ?? 0);
                let WalletRefDiscountAmount = ((storeTotalPriceSub * 50) / 100).toFixed(2);

                // If they are empty, we might need to recalculate them based on available balance and caps
                // User snippet shows a specific logic for RefWalletBalance:
                // RefWalletBalance = (wallet.toLowerCase() === 'yes' && RefWalletBalance >= WalletRefDiscountAmount) ? WalletRefDiscountAmount : RefWalletBalance;

                if (isWalletAmtMissing) {
                    // This was the old logic, keeping it for wallet_balance
                    let WalletDiscountAmount = ((storeTotalPriceSub * 50) / 100).toFixed(2);
                    totalWalletAmt = ((actualWallet - totalReserveAmt) >= WalletDiscountAmount) ? WalletDiscountAmount : (actualWallet - totalReserveAmt);
                }

                if (isRefWalletAmtMissing) {
                    totalRefWalletAmt = (actualRefWallet >= WalletRefDiscountAmount) ? WalletRefDiscountAmount : actualRefWallet;
                }
            }
        }

        // Stock validation from merged ordersDetails
        for (const storeItem of ordersDetails) {
            if (storeItem.stock === 0 || storeItem.product_hide == 1 || storeItem.product_delete == 1) {
                logToFile("One or more items in your cart are out of stock. Unable to proceed with the order.");
                throw new Error("One or more items in your cart are out of stock. Unable to proceed with the order.");
            }
        }

        // Get today's date and current time in Dubai timezone
        const dubaiTime = moment.tz("Asia/Dubai");
        const todayDubai = dubaiTime.format("YYYY-MM-DD");
        const isAfter6PM = dubaiTime.hour() >= 17;

        // Condition 1: Check if any order has a sub_delivery_date of today
        ordersDetails.forEach(order => {
            if (order.sub_delivery_date === todayDubai) {
                logToFile("Check if any order has a sub_delivery_date of today");
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

        // User, reserveAmt, storeDetailsAmt already fetched in parallel above
        let userPhone = user.country_code + user.user_phone;
        let userName = user.name;
        let userEmail = (user.email) ? user.email : ar.receiver_email;

        let totalReserveAmt = 0;
        reserveAmt.forEach(item => {
            totalReserveAmt += parseFloat(item.reserve_amount);
        });

        let actualWallet = parseFloat(user.wallet_balance || 0) - totalReserveAmt;
        // Persist balances + wallet_history whenever main or ref amounts apply (not only wallet === 'yes')
        const walletEnabled = parseFloat(totalWalletAmt || 0) > 0 || parseFloat(totalRefWalletAmt || 0) > 0;
        let WalletBalanace = user.wallet_balance - totalWalletAmt;
        let RefWalletBalance = user.referral_balance - totalRefWalletAmt;

        const storeTotalPriceSub = parseFloat(storeDetailsAmt?.totalprice ?? storeDetailsAmt?.Totalprice ?? 0);
        const total = Math.round(storeTotalPriceSub * 100);
        const deduction = Math.round(parseFloat(totalWalletAmt || 0) * 100) + Math.round(parseFloat(totalRefWalletAmt || 0) * 100);
        const TotalpriceAmount = (total - deduction) / 100;

        const WithWalletAmount = parseFloat(TotalpriceAmount) + parseFloat(totalWalletAmt || 0) + parseFloat(totalRefWalletAmt || 0);
        const WalletDiscountAmount = ((storeTotalPriceSub * 50) / 100).toFixed(2);
        const WalletStatus = ((wallet || '').toLowerCase() === 'yes' && actualWallet >= WalletDiscountAmount) ? 'percentage' : 'fixed';

        // For subscription/pay-per-delivery, determine method based on how much is really left after wallet/referral
        // AND only classify as 'Wallet' when wallet/referral was actually used.
        const subWalletPortion = parseFloat(totalWalletAmt || 0) + parseFloat(totalRefWalletAmt || 0);
        const subGrossTotal = parseFloat(storeTotalPriceSub || 0);
        const subNonWalletPortion = Math.max(0, subGrossTotal - subWalletPortion);

        const subWalletUsedFlag = subWalletPortion > 0;

        let paymentMethodNew = '';
        if (subWalletUsedFlag && subNonWalletPortion <= 0) {
            paymentMethodNew = 'Wallet';
        } else {
            paymentMethodNew = paymentMethod;
        }


        if (siStatus == 'yes' || paymentType == 'payperdelivery') {
            const number = groupId;
            const description = "";
            const amount = (TotalpriceAmount) ? TotalpriceAmount : "0";
            const orderJson = { number, description, amount };
            const ordertype = "subscription";
            const payment_status = (paymentMethodNew === 'Wallet' || paymentType.toLowerCase() !== 'payperdelivery') ? 'success' : 'Pending';
            const group_id = groupId;
            const user_id = userId;
            const bank_id = 0;
            const si_sub_ref_no = siSubRefNo;
            const store_id = storeId;
            const payment_method = (subWalletUsedFlag && subNonWalletPortion <= 0) ? 'Wallet' : paymentMethod;
            const payment_gateway = paymentGateway;
            const payment_id = paymentId;
            const coupon_id = couponId;
            const coupon_code = couponCode;
            const discount_amount = 0;
            const delivery_date = deliveryDate;
            const time_slot = timeSlot;
            const device_id = "";
            const totalwalletamt = totalWalletAmt;
            const totalrefwalletamt = totalRefWalletAmt;
            const is_subscription = isSubscription;
            const payment_type = paymentType;
            const storeItemList = ordersDetails;
            const address_id = addressId;
            const custom_data = { address_id, ordertype, payment_status, group_id, user_id, bank_id, si_sub_ref_no, store_id, payment_method, wallet, payment_gateway, payment_id, coupon_id, coupon_code, discount_amount, delivery_date, time_slot, del_partner_tip, del_partner_instruction, order_instruction, device_id, totalwalletamt, totalrefwalletamt, is_subscription, payment_type, platform, browser, storeItemList };

            const mainJsonSaveRequest = {
                merchant_key: "",
                operation: 'purchase',
                methods: (paymentMethodNew == 'Wallet') ? paymentMethodNew : paymentMethod,
                success_url: "",
                cancel_url: "",
                hash: "",
                order: orderJson,
                customer: "",
                billing_address: "",
                custom_data: custom_data
            };

            const maxIdResult = await knex('payment_order_request_details').max('id as maxId').first();
            const nextId = (maxIdResult?.maxId ? parseInt(maxIdResult.maxId, 10) : 0) + 1;

            const insert = await knex('payment_order_request_details').insert({
                id: nextId,
                json_data: JSON.stringify(mainJsonSaveRequest),
                group_id: group_id,
                order_type: ordertype,
                added_on: new Date()
            });

            logToFile("Payperdelivery: " + JSON.stringify(mainJsonSaveRequest));
        }



        if (siStatus == 'yes' && paymentType.toLowerCase() == 'paynow') {

            const BankDetails = await knex('tbl_user_bank_details')
                .where('si_sub_ref_no', siSubRefNo)
                .where('user_id', userIdInt)
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

                if (TotalpriceAmount > 0) {
                    try {
                        const fetch = (await import('node-fetch')).default;

                        const response = await fetch(checkoutUrl, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: jsonData,
                        });

                        const data = await response.json();
                        logToFile("/payment/recurring response body: " + JSON.stringify(data));

                        if (!response.ok) {
                            throw new Error(`HTTP error! Status: ${response.status} - Data: ${JSON.stringify(data)}`);
                        }

                        const status = (data && data.status) ? data.status.toLowerCase() : '';
                        if (status === 'decline' || status === 'pending') {
                            const errorMessage = status === 'decline'
                                ? 'Card issue detected! Status: decline'
                                : `Payment Status issue! Status: ${data.status}`;
                            throw new Error(errorMessage);
                        }

                        const maxSiLogResult = await knex('tbl_si_deduction_log').max('id as maxId').first();
                        const nextSiLogId = (maxSiLogResult?.maxId ? parseInt(maxSiLogResult.maxId, 10) : 0) + 1;
                        await knex('tbl_si_deduction_log').insert({
                            id: nextSiLogId,
                            si_sub_ref_no: siSubRefNo,
                            user_id: userIdStr,
                            amount: String(parseFloat(TotalpriceAmount).toFixed(2)),
                            payment_method: 'SI',
                            payment_status: '0',
                            card_id: groupId,
                        });
                        //return await response.json();
                    } catch (error) {
                        console.error('Error sending payment data:', error);
                        logToFile(`checkoutModel fun getSubordercheckout error: ${error instanceof Error ? error.stack || error.message : JSON.stringify(error)}`);
                        throw error; // Re-throw for handling in the calling code
                    }
                } else {
                    logToFile(`[CHECKOUT] Skipping TotalPay Recurring API for order ${groupId} as amount is ${TotalpriceAmount} (fully paid by wallet/coupons).`);
                }


            } else {
                // No BankDetails found
                logToFile("No bank details found for the given criteria.");
                throw new Error('No bank details found for the given criteria.');
            }
        }


        // Pre-fetch max IDs once for latency (avoid N queries in loop)
        const [maxOrderIdRes, maxSubIdRes] = await Promise.all([
            knex('orders').max('order_id as maxOrderId').first(),
            knex('subscription_order').max('id as maxId').first()
        ]);
        let nextOrderIdCounter = (maxOrderIdRes?.maxOrderId ? parseInt(maxOrderIdRes.maxOrderId, 10) : 0) + 1;
        let nextSubIdCounter = (maxSubIdRes?.maxId ? parseInt(maxSubIdRes.maxId, 10) : 0) + 1;

        // ordersDetails already fetched in parallel above - no extra query
        let price2 = 0;
        let tax_p = 0;
        let tax_price = 0;
        let TotalpriceStore = (storeDetailsAmt?.totalprice ?? storeDetailsAmt?.Totalprice ?? 0);
        for (const productList of ordersDetails) {
            const { varient_id: varientId, qty: orderQty, store_order_id: storeOrderId, sub_total_delivery: subTotalDelivery, repeat_orders: repeatOrders, sub_delivery_date: subDeliveryDate, sub_time_slot: subTimeSlot, price, mrp } = productList;

            // Generate cart_id
            const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
            const cartId = generateRandomLetters(4) + generateRandomDigits(2) + hash.substring(0, 2);

            // Use productList directly - ordersDetails already has price, mrp from merged query (no extra DB call)
            price2 = parseFloat(price || 0);

            const totalPrice = parseFloat(price2);

            let paidByWallet = 0;
            let paidByRefWallet = 0;

            const useSubWalletDeductions =
                (wallet || '').toLowerCase() === 'yes' ||
                parseFloat(totalWalletAmt || 0) > 0 ||
                parseFloat(totalRefWalletAmt || 0) > 0;
            if (useSubWalletDeductions) {
                const price = parseFloat(totalPrice);
                const maxRefWallet = price * 0.5;
                paidByRefWallet = Math.min(parseFloat(totalRefWalletAmt || 0), maxRefWallet);
                const remainingAmount = price - paidByRefWallet;
                paidByWallet = Math.min(parseFloat(totalWalletAmt || 0), remainingAmount);
            }

            const paymentStatus = (paymentType.toLowerCase() === 'payperdelivery') ? 'Pending' : 'success';
            const remPrice = (paymentType.toLowerCase() === 'payperdelivery') ? (totalPrice - paidByWallet - paidByRefWallet) : 0;
            const siPaymentFlag = (paymentType.toLowerCase() === 'payperdelivery') ? 'no' : 'yes';
            const siOrder = (paymentType.toLowerCase() === 'payperdelivery') ? 'yes' : 'no';
            const reserveAmount = (paymentType.toLowerCase() === 'payperdelivery') ? (paidByWallet + paidByRefWallet) : 0;

            const nextOrderId = nextOrderIdCounter++;
            const orderInsertData = {
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
                delivery_date: subDeliveryDate,
                time_slot: subTimeSlot,
                address_id: addressId,
                avg_tax_per: 0,
                total_tax_price: 0,
                total_delivery: parseInt(subTotalDelivery, 10) || 0,
                repeat_orders: repeatOrders,
                is_subscription: isSubscription ? 1 : 0,
                del_partner_tip: 0,
                del_partner_instruction: del_partner_instruction,
                order_instruction: order_instruction,
                group_id: groupId, // Use groupId for group_id or adjust as needed
                cod_charges: 0,
                paid_by_wallet: (paidByWallet) ? parseFloat(paidByWallet).toFixed(2) : 0,
                paid_by_ref_wallet: (paidByRefWallet) ? parseFloat(paidByRefWallet).toFixed(2) : 0,
                payment_method: (parseFloat(TotalpriceAmount) <= 0) ? 'Wallet' : paymentMethod,
                coupon_id: 0,
                coupon_code: 0,
                coupon_discount: 0,
                payment_status: paymentStatus,
                payment_type: paymentType,
                si_sub_ref_no: siSubRefNo,
                bank_id: bankId,
                si_order: siOrder,
                reserve_amount: parseFloat(reserveAmount).toFixed(2),
                pastorecentrder: 'new',
                platform: (platform) ? platform : '',
                browser: (browser) ? browser : '',
                order_status: 'Pending',
            };

            logToFile(`[getSubordercheckout] Inserting into orders table for groupId ${groupId}: ` + JSON.stringify(orderInsertData));
            console.log(`[getSubordercheckout] Inserting into orders table for groupId ${groupId}`);

            await knex('orders').insert(orderInsertData);
            logToFile(`[getSubordercheckout] SUCCESSFULLY INSERTED into orders table for groupId ${groupId}`);
            console.log(`[getSubordercheckout] SUCCESSFULLY INSERTED into orders table for groupId ${groupId}`);

            await knex('store_orders')
                .where('store_order_id', storeOrderId)
                .update({
                    'order_cart_id': cartId,
                });

            // if(AutoRenewSubCart == 'yes'){
            //     await knex('store_orders')
            //     .where('store_order_id',storeOrderId)
            //     .update({
            //     'isautorenew':"yes",
            //     }); 
            // }

            let k = 0;
            const repeatOrderss = repeatOrders.trim().split(',');
            const totalDeliveryWeek = repeatOrderss.length * subTotalDelivery;
            for (let i = 0; i < 1000; i++) {
                const deliveryDates = moment(subDeliveryDate).add(i, 'days').format('YYYY-MM-DD');
                for (let j = 0; j < repeatOrderss.length; j++) {
                    const timestamp = moment(deliveryDates).toDate().getTime();
                    const day = moment(deliveryDates).format('ddd');
                    const days_name = repeatOrderss[j].trim();
                    if (days_name.toLowerCase() == day.toLowerCase()) {

                        const currentSubId = nextSubIdCounter++;
                        const subOrderInsertData = {
                            'id': currentSubId,
                            'store_order_id': storeOrderId,
                            'cart_id': cartId,
                            'user_id': userIdInt,
                            'order_id': nextOrderId,
                            'store_id': storeId,
                            'delivery_date': deliveryDates,
                            'time_slot': subTimeSlot,
                            'created_date': current,
                            'order_status': 'Pending',
                            'si_payment_flag': siPaymentFlag,
                            'group_id': groupId,
                            'subscription_id': String(currentSubId),
                            'platform': (platform) ? platform : '',
                            'browser': (browser) ? browser : '',
                        };

                        logToFile(`[getSubordercheckout] Inserting into subscription_order for groupId ${groupId}: ` + JSON.stringify(subOrderInsertData));
                        console.log(`[getSubordercheckout] Inserting into subscription_order for item ${k + 1}`);
                        await knex('subscription_order').insert(subOrderInsertData);
                        logToFile(`[getSubordercheckout] SUCCESSFULLY INSERTED into subscription_order (id: ${currentSubId}) for groupId ${groupId}`);
                        console.log(`[getSubordercheckout] SUCCESSFULLY INSERTED into subscription_order (id: ${currentSubId})`);
                        k++;
                    }
                }
                if (k == totalDeliveryWeek) {
                    break;
                }

            }

            // Generate invoice automatically after order is placed
            await userModel.generateInvoice({ user_id: userIdInt, cart_id: cartId });
        }

        if (paymentType.toLowerCase() == 'paynow' && walletEnabled) {
            await knex('users')
                .where('id', userIdInt)
                .update({
                    wallet_balance: WalletBalanace.toFixed(2),
                    referral_balance: RefWalletBalance.toFixed(2),
                });

            if (totalWalletAmt > 0) {
                const maxWIdResult = await knex('wallet_history').max('w_id as maxWId').first();
                const nextWId = (maxWIdResult?.maxWId ? parseInt(maxWIdResult.maxWId, 10) : 0) + 1;
                await knex('wallet_history').insert({
                    w_id: nextWId,
                    user_id: userIdInt,
                    amount: String(parseFloat(totalWalletAmt).toFixed(2)),
                    resource: 'order_placed_wallet',
                    type: 'deduction',
                    group_id: groupId,
                    cart_id: "",
                });
            }

            if (totalRefWalletAmt > 0) {
                const nearestExpiry = await knex("wallet_history")
                    .select("expiry_date")
                    .where("user_id", userIdInt)
                    .whereNotNull("expiry_date")
                    .where("expiry_date", ">", knex.fn.now())
                    .orderBy("expiry_date", "asc")
                    .first();

                const maxWIdResultRef = await knex('wallet_history').max('w_id as maxWId').first();
                const nextWIdRef = (maxWIdResultRef?.maxWId != null ? parseInt(maxWIdResultRef.maxWId, 10) : 0) + 1;

                await knex('wallet_history').insert({
                    w_id: nextWIdRef,
                    user_id: userIdInt,
                    amount: String(parseFloat(totalRefWalletAmt).toFixed(2)),
                    resource: 'order_placed_wallet_ref',
                    type: 'deduction',
                    group_id: groupId,
                    cart_id: "",
                    expiry_date: nearestExpiry ? nearestExpiry.expiry_date : null,
                });
            }
        }

        totalCal = await knex('orders')
            .where('group_id', groupId)
            .select(
                knex.raw('COALESCE(SUM(coupon_discount::double precision), 0) as coupon_discount'),
                knex.raw('COALESCE(SUM(rem_price::double precision), 0) as rem_price'),
                knex.raw('COALESCE(SUM(paid_by_wallet::double precision), 0) as paid_by_wallet'),
                knex.raw('COALESCE(SUM(paid_by_ref_wallet::double precision), 0) as paid_by_ref_wallet'),
                knex.raw("COALESCE(SUM(COALESCE(NULLIF(TRIM(COALESCE(cod_charges, '')), '')::double precision, 0)), 0) as cod_charges"),
                knex.raw('COALESCE(SUM(total_products_mrp::double precision), 0) as total_products_mrp')
            )
            .first();
        finalAmount = parseFloat(totalCal.total_products_mrp).toFixed(2);
        // Reuse totalCal for email template below - avoid duplicate query

        //check offer for lucky draw
        const lastofferdate = process.env.LAST_OFFER_DATE;
        const currentDate = new Date().toISOString().split('T')[0];
        // && paymentStatus == 'success'
        if (currentDate <= lastofferdate && finalAmount >= 200 && paymentType.toLowerCase() == 'paynow') {
            const maxLuckydrawResult = await knex('tbl_luckydraw').max('id as maxId').first();
            const nextLuckydrawId = (maxLuckydrawResult?.maxId ? parseInt(maxLuckydrawResult.maxId, 10) : 0) + 1;
            await knex('tbl_luckydraw').insert({
                id: nextLuckydrawId,
                user_id: userIdInt,
                order_id: groupId,
                order_type: 'subscription',
            });

            const usertotalorders = await knex('tbl_luckydraw')
                .distinct('orders.group_id')
                .rightJoin('orders', 'orders.group_id', '=', 'tbl_luckydraw.order_id')
                .where('tbl_luckydraw.user_id', userIdInt)
                .where('tbl_luckydraw.is_delete', 0)
                .where('orders.order_status', '!=', 'Cancelled');

            const getUserup = await knex('users').where('id', userIdInt).first();
            const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
            // Convert OTP code to a time-based string
            const phone_with_country_code = `${getUserup.country_code}${getUserup.user_phone}`;

            const payload = {
                "apiKey": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
                "campaignName": "updatesubscriptionOrderAppleGiveaway",
                "destination": "+" + phone_with_country_code,
                "userName": "Quickart General Trading Co LLC",
                "templateParams": [
                    getUserup.name, groupId, finalAmount, `${usertotalorders.length}`
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

            logToFile("check offer for lucky draw ");
        }

        //SMS Code
        message = "You have successfully subscribed to QuicKart! Your order " + groupId + " will be delivered right on schedule.";
        sendSMSOrder(userPhone, message);

        //Email Code - reuse totalCal from above (no duplicate query)
        // Parallel fetch: storeOrders, logo, currency, sub dates (1 query for min+max)
        const year = current.getFullYear();
        const month = String(current.getMonth() + 1).padStart(2, '0');
        const day = String(current.getDate()).padStart(2, '0');
        const formattedDate = `${year}-${month}-${day}`;

        const [storeOrdersRows, logo, currency, subDateRange] = await Promise.all([
            knex('store_orders')
                .select('store_orders.*', 'orders.group_id', 'orders.time_slot', 'orders.total_delivery')
                .join('orders', 'orders.cart_id', '=', 'store_orders.order_cart_id')
                .where('orders.group_id', groupId),
            knex('tbl_web_setting').first(),
            knex('currency').first(),
            knex('subscription_order')
                .where('group_id', groupId)
                .select(knex.raw('MIN(delivery_date) as first_date'), knex.raw('MAX(delivery_date) as last_date'))
                .first()
        ]);
        const storeOrders = storeOrdersRows;
        const appName = logo ? logo.name : null;
        const currencySign = currency ? currency.currency_sign : null;

        let formattedstartDate = formattedDate;
        let formattedendDate = formattedDate;
        if (subDateRange && subDateRange.first_date) {
            const date1 = new Date(subDateRange.first_date);
            formattedstartDate = `${date1.getFullYear()}-${String(date1.getMonth() + 1).padStart(2, '0')}-${String(date1.getDate()).padStart(2, '0')}`;
        }
        if (subDateRange && subDateRange.last_date) {
            const date2 = new Date(subDateRange.last_date);
            formattedendDate = `${date2.getFullYear()}-${String(date2.getMonth() + 1).padStart(2, '0')}-${String(date2.getDate()).padStart(2, '0')}`;
        }


        const templateData = {
            baseurl: process.env.BASE_URL,
            group_id: groupId,
            user_name: userName,
            user_email: userEmail,
            delivery_date: deliveryDate,
            orderss_address: ar.house_no + ', ' + ar.landmark + ', ' + ar.society,
            store_orderss: storeOrders,
            coupon_discount: totalCal.coupon_discount.toFixed(2),
            paid_by_wallet: totalCal.paid_by_wallet.toFixed(2),
            paid_by_ref_wallet: totalCal.paid_by_ref_wallet.toFixed(2),
            cod_charges: totalCal.cod_charges.toFixed(2),
            final_amount: finalAmount,
            app_name: appName,
            currency_sign: currencySign,
            order_type: "subscription",
            startdelivery_date: formattedstartDate,
            enddelivery_date: formattedendDate,
            order_date: formattedDate,
            paymentMethod: paymentMethod,
        };
        const subject = 'Order Successfully Placed'
        // Trigger the email after order is placed
        //  sendMail = await codorderplacedMail(userEmail, templateData,subject,groupId);
        logToFile("Order Successfully Placed mail: " + JSON.stringify(templateData));


        return "success";
    } catch (error) {
        logToFile(`[getSubordercheckout] CRITICAL ERROR for groupId ${appDetails.group_id}: ${error instanceof Error ? error.stack || error.message : JSON.stringify(error)}`);
        console.error(`[getSubordercheckout] CRITICAL ERROR for groupId ${appDetails.group_id}:`, error);
        throw error;
    }
};


// Generate random letters, digits, and hash substring
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

module.exports = {
    getSubordercheckout,
    getQuickordercheckout,
    getQuickOrderCheckoutSdk
};
