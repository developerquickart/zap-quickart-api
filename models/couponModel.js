const knex = require('../db');

// PostgreSQL: use || for string concat (parameterized, no SQL injection)
const couponImageSelect = (query, baseurl) =>
  query.select(
    '*',
    knex.raw("(COALESCE(?::text, '') || COALESCE(coupon_image, '')) as coupon_image", [baseurl])
  );

const couponlists = async (appDetatils) => {
  const user = appDetatils.user_id;
  const userStr = String(user); // orders.user_id is text
  const cart_id = appDetatils.cart_id;
  const store_id = appDetatils.store_id;
  const baseurl = process.env.BUNNY_NET_IMAGE || '';

  const check = await knex('orders')
    .where('cart_id', cart_id)
    .first();

  let coupon;

  if (check) {
    const p = check.total_price;
    const storeId = check.store_id;

    // Parallel fetch for latency: run both queries concurrently
    // cart_value is integer; p (total_price) is decimal - use ::numeric to avoid "invalid input syntax for type integer"
    const [generalCoupons, userCoupons] = await Promise.all([
      couponImageSelect(knex('coupon'), baseurl)
        .where('store_id', storeId)
        .where('coupon_visibility', 'Show To Customer')
        .whereRaw('cart_value <= ?::numeric', [p])
        .whereIn('user_id', [0, 999])
        .whereRaw('DATE(start_date) <= CURRENT_DATE')
        .whereRaw('DATE(end_date) >= CURRENT_DATE'),
      couponImageSelect(knex('coupon'), baseurl)
        .where('store_id', storeId)
        .where('coupon_visibility', 'Show To Customer')
        .whereRaw('cart_value <= ?::numeric', [p])
        .where('user_id', user)
        .whereRaw('DATE(start_date) <= CURRENT_DATE')
        .whereRaw('DATE(end_date) >= CURRENT_DATE'),
    ]);

    // Deduplicate by coupon_id (user + general should not overlap, but safe)
    const seen = new Set();
    coupon = [...userCoupons, ...generalCoupons].filter((c) => {
      if (seen.has(c.coupon_id)) return false;
      seen.add(c.coupon_id);
      return true;
    });
  } else {
    // Parallel fetch when no order
    const [userCoupons, generalCoupons] = await Promise.all([
      couponImageSelect(knex('coupon'), baseurl)
        .where('store_id', store_id)
        .where('user_id', user)
        .where('coupon_visibility', 'Show To Customer')
        .whereRaw('DATE(start_date) <= CURRENT_DATE')
        .whereRaw('DATE(end_date) >= CURRENT_DATE'),
      couponImageSelect(knex('coupon'), baseurl)
        .where('store_id', store_id)
        .whereIn('user_id', [0, 999])
        .where('coupon_visibility', 'Show To Customer')
        .whereRaw('DATE(start_date) <= CURRENT_DATE')
        .whereRaw('DATE(end_date) >= CURRENT_DATE'),
    ]);

    const seen = new Set();
    coupon = [...userCoupons, ...generalCoupons].filter((c) => {
      if (seen.has(c.coupon_id)) return false;
      seen.add(c.coupon_id);
      return true;
    });
  }

  if (coupon.length === 0) return [];

  // Single batch query instead of N+1: get usage counts for all coupon_ids
  const couponIds = coupon.map((c) => c.coupon_id);
  const usageRows = await knex('orders')
    .select('coupon_id')
    .whereIn('coupon_id', couponIds)
    .where('user_id', userStr)
    .where('order_status', '!=', 'Cancelled');

  const usageByCouponId = new Map();
  for (const row of usageRows) {
    usageByCouponId.set(
      row.coupon_id,
      (usageByCouponId.get(row.coupon_id) || 0) + 1
    );
  }

  const customizedProductData = [];
  for (const coupons of coupon) {
    const usedCount = usageByCouponId.get(coupons.coupon_id) || 0;
    if (usedCount === 0 || coupons.uses_restriction != 1) {
      coupons.user_uses = 0;
      customizedProductData.push(coupons);
    }
  }

  return customizedProductData;
};

const applycoupon = async (appDetatils) => {
  const store_id = appDetatils.store_id;
  const coupon_code = appDetatils.coupon_code;
  const order_type = appDetatils.order_type;
  const user_id = appDetatils.user_id ? String(appDetatils.user_id) : String(appDetatils.device_id);
  const total_delivery = 1;

  let validCoupon;
  if (order_type == "subscription") {
    validCoupon = await knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .where('store_approval', user_id)
      .whereNull('subscription_flag')
      .where('order_cart_id', 'incart');
  } else if (order_type == "quick") {
    validCoupon = await knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .where('store_approval', user_id)
      .whereNull('subscription_flag')
      .where('order_cart_id', 'incart')
  }

  //  return validCoupon
  //comment for jivan
  //   for (const valid of validCoupon) {
  //     if (valid.mrp !== valid.price) {
  //         throw new Error('Coupon cannot be applied on discounted products');
  //     }
  //   }


  const d = new Date();
  const currentDate = d.toISOString().split('T')[0];
  if (order_type == "subscription") {
    validCouponss = await knex('store_orders')
      .select('deal_product.deal_price', 'store_products.mrp')
      .join('deal_product', 'store_orders.varient_id', '=', 'deal_product.varient_id')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .where('store_approval', user_id)
      .where('deal_product.valid_from', '<=', currentDate)
      .where('deal_product.valid_to', '>', currentDate)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag');
  } else if (order_type == "quick") {
    validCouponss = await knex('store_orders')
      .select('deal_product.deal_price', 'store_products.mrp')
      .join('deal_product', 'store_orders.varient_id', '=', 'deal_product.varient_id')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .where('store_approval', user_id)
      .where('deal_product.valid_from', '<=', currentDate)
      .where('deal_product.valid_to', '>', currentDate)
      .whereNull('subscription_flag')
      .where('order_cart_id', 'incart')

  }

  //  return validCouponss
  //comment for jivan
  // for (const validss of validCouponss) {
  //     if (validss.mrp !== validss.price) {
  //         throw new Error('Coupon cannot be applied on discounted products');
  //     }
  // }

  coupon = await knex('coupon')
    .where('coupon_code', coupon_code)
    .first();

  orderss = await knex('orders')
    .where('user_id', user_id)
    .where('coupon_code', coupon_code);

  ordersss = await knex('orders')
    .where('coupon_code', coupon_code)

  coupon_codelists = await knex('coupon')
    .where('coupon_code', coupon_code);

  coupon_codelist = await knex('coupon')
    .where('coupon_code', coupon_code)
    .first();

  if (!coupon_codelist) {
    throw new Error('Coupon code is not valid');
  }

  uses_restriction = (coupon_codelist.uses_restriction) ? coupon_codelist.uses_restriction : 0;
  coupon_visibility = coupon_codelist.coupon_visibility;


  if (coupon_codelists.length > 0) {

    start_date = coupon_codelist.start_date;
    end_date = coupon_codelist.end_date;

    const currentdate = new Date().toISOString().slice(0, 19).replace('T', ' ');
    newstart_date = start_date.toISOString().slice(0, 19).replace('T', ' ');
    newend_date = end_date.toISOString().slice(0, 19).replace('T', ' ');
    //  if(newstart_date <= currentdate && newend_date >= currentdate)
    //   {   


    if (coupon_visibility == 'Show To Customer') {

      // Show Coupon Visibility  

      //  if(uses_restriction > ordersss.length){


      if (orderss.length == 0) {

        if (coupon) {
          if (order_type == "subscription") {
            check = await knex('store_orders')
              .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
              .sum({ total_price: 'store_orders.price' })
              .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
              .where('store_orders.order_cart_id', 'incart')
              .where('store_orders.store_id', store_id)
              .where('store_orders.store_approval', user_id)
              .whereNull('store_orders.subscription_flag')
              .first();
          } else if (order_type == "quick") {
            check = await knex('store_orders')
              .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
              .sum({ total_price: 'store_orders.price' })
              .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
              .where('store_orders.order_cart_id', 'incart')
              .where('store_orders.store_id', store_id)
              .where('store_orders.store_approval', user_id)
              .whereNull('store_orders.subscription_flag')
              .first();
          }

          p = total_delivery * parseFloat(check?.total_price || 0);
          if (p <= 0) {
            throw new Error('Coupon cannot be applied on discounted products');
          }

          mincart = coupon.cart_value;

          // return mincart
          if (mincart <= p) {
            check2 = await knex('orders')
              .where('coupon_id', coupon.coupon_id)
              .where('user_id', user_id)
              .where('order_status', '!=', 'Cancelled')

            if (coupon.uses_restriction > check2.length) {

              mincart = coupon.cart_value;
              am = coupon.amount;
              type = coupon.type;
              if (type == '%' || type == 'Percentage' || type == 'percentage') {
                per = (p * am) / 100;

                discount_amount = p - per;

              }
              else {

                per = am;
                discount_amount = p - am;
              }
              save_amount = p - discount_amount;
              // if(save_amount >= coupon.max_discount_amount){
              //         save_amount = coupon.max_discount_amount;   
              // }

              let cart_amount = Math.round(p * 100) / 100;
              let discounted_amount = Math.round(discount_amount * 100) / 100;
              let save_amounts = Math.round(save_amount * 100) / 100;

              if (cart_amount == 0 && discounted_amount == 0 && save_amounts == 0) {
                throw new Error('All the products in your cart are already discounted, so you are not eligible for an additional coupon discount.');
              }

              const data = {
                coupon_id: coupon.coupon_id,
                coupon_code: coupon.coupon_code,
                cart_amount: Math.round(p * 100) / 100,
                discounted_amount: Math.round(discount_amount * 100) / 100,
                save_amount: Math.round(save_amount * 100) / 100
              };

              return data

            }
          } else {
            throw new Error('Cart value is low.');
            //throw new Error('Cart value is low. Minimum cart value to apply this coupon should be '+mincart+' AED');

          }
        } else {
          throw new Error('Coupon code is not valid');

        }
        // }else
        // {
        //     throw new Error('Already used coupon code');

        // }
        // }else
        // {
        //     throw new Error('Coupon is expired');

        // }


      } else {

        // Hide Coupon Visibility  

        if (uses_restriction > orderss.length) {

          if (coupon) {
            check = await knex('store_orders')
              .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
              .sum({ total_price: 'store_orders.price' })
              .whereRaw('(store_orders.price::numeric / NULLIF(store_orders.qty::numeric, 0)) >= store_products.mrp::numeric')
              .where('store_orders.order_cart_id', 'incart')
              .where('store_orders.store_id', store_id)
              .where('store_orders.store_approval', user_id)
              .whereNull('store_orders.subscription_flag')
              .first();

            p = total_delivery * parseFloat(check?.total_price || 0);
            if (p <= 0) {
              throw new Error('Coupon cannot be applied on discounted products');
            }
            mincart = coupon.cart_value;


            if (mincart <= p) {
              check2 = await knex('orders')
                .where('coupon_id', coupon.coupon_id)
                .where('user_id', user_id)
                .where('order_status', '!=', 'Cancelled');

              if (coupon.uses_restriction > check2.length) {

                mincart = coupon.cart_value;
                am = coupon.amount;
                type = coupon.type;
                if (type == '%' || type == 'Percentage' || type == 'percentage') {
                  per = (p * am) / 100;
                  discount_amount = p - per;
                }
                else {
                  per = am;
                  discount_amount = p - am;
                }
                save_amount = p - discount_amount;
                // if(save_amount >= coupon.max_discount_amount){
                //         save_amount = coupon.max_discount_amount;   
                // }

                let cart_amount = Math.round(p * 100) / 100;
                let discounted_amount = Math.round(discount_amount * 100) / 100;
                let save_amounts = Math.round(save_amount * 100) / 100;


                if (cart_amount == 0 && discounted_amount == 0 && save_amounts == 0) {
                  throw new Error('All the products in your cart are already discounted, so you are not eligible for an additional coupon discount.');
                }
                const data = {
                  coupon_id: coupon.coupon_id,
                  coupon_code: coupon.coupon_code,
                  cart_amount: Math.round(p * 100) / 100,
                  discounted_amount: Math.round(discount_amount * 100) / 100,
                  save_amount: Math.round(save_amount * 100) / 100
                };


                //$message = array('status'=>'1', 'message'=>'Coupon Applied Successfully', 'data'=>$data);
                return data;
              }
            } else {
              // throw new Error('Cart value is low. Minimum cart value to apply this coupon should be '+mincart+' AED');
              throw new Error('Cart value is low.');

            }
          } else {
            throw new Error('Coupon code is not valid');

          }
        } else {
          throw new Error('Already used coupon code');

        }




      }



    } else {
      throw new Error('Coupon is expired');

    }


  } else {
    throw new Error('Coupon code is not valid');

  }


}

module.exports = {
  couponlists,
  applycoupon
};
