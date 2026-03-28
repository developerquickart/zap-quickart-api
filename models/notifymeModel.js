const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const axios = require('axios');



const showNotifyMe = async (appDetails) => {
  const { store_id, is_subscription, user_id } = appDetails;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  
  // Validate and set defaults for pagination - ensure valid integers
  const page = Math.max(1, parseInt(appDetails.page) || 1);
  const perPage = Math.max(1, Math.min(100, parseInt(appDetails.perpage) || 10));

  // Fetch product details with necessary joins and filters
  let topsellingsQuery =  knex('store_products')
    .select(
      'store_products.*',
      knex.raw(`? || product.product_image as product_image`, [baseurl]),
      knex.raw('100-((store_products.price*100.0)/store_products.mrp) as discountper'),
      'tbl_country.country_icon',
      'product_varient.unit as prdunit',
      'product_varient.varient_id',
      'product_varient.quantity',
      'product.product_id',
      'product.product_name',
      'product.thumbnail',
      'product.type',
      knex.raw('product.percentage::numeric as percentage'),
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
    .join('product_notify_me', 'product_notify_me.varient_id', 'store_products.varient_id')
    .innerJoin('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
    .innerJoin('product', 'product_varient.product_id', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id = product.country_id::integer'))
    .whereNotNull('store_products.price')
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('product_notify_me.user_id', user_id)
    .where('product.approved', 1)
     .where(builder => {
                      builder
                        .where('product.is_offer_product', 0) 
                        .whereNull('product.offer_date')
                        .orWhereRaw('(product.offer_date)::date IS DISTINCT FROM CURRENT_DATE')
                    });
    
    const productDetail = await topsellingsQuery.offset((page - 1) * perPage)
        .limit(perPage);

  // Extract variant IDs and product IDs for bulk queries
  const variantIds = productDetail.map(product => product.varient_id);
  const productIds = [...new Set(productDetail.map(product => product.product_id))];

  // Batch fetch additional data - optimized for PostgreSQL
  const [wishList, cartItems, notifyMeList, subscriptionProducts, deals] = await Promise.all([
    variantIds.length > 0 ? knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id) : [],
    variantIds.length > 0 ? knex('store_orders')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', store_id) : [],
    variantIds.length > 0 ? knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id) : [],
    variantIds.length > 0 ? knex('store_orders')
      .select('varient_id')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1)
      .where('order_cart_id', 'incart') : [],
    variantIds.length > 0 ? knex('deal_product')
      .whereIn('varient_id', variantIds)
      .where('store_id', store_id)
      .where('deal_product.valid_from', '<=', knex.fn.now())
      .where('deal_product.valid_to', '>', knex.fn.now()) : []
  ]);

  // Map additional data for easy access
  const dealMap = Object.fromEntries(deals.map(deal => [deal.varient_id, deal.deal_price]));
  const subscriptionMap = Object.fromEntries(subscriptionProducts.map(sub => [sub.varient_id, true]));
  const wishListMap = Object.fromEntries(wishList.map(item => [item.varient_id, true]));
  const cartMap = Object.fromEntries(cartItems.map(item => [item.varient_id, item.qty]));
  const notifyMeMap = Object.fromEntries(notifyMeList.map(item => [item.varient_id, true]));

  // Process product details
  const customizedProductData = await Promise.all(
    productDetail.map(async product => {
      const isFavourite = wishListMap[product.varient_id] ? 'true' : 'false';
      const cartQty = cartMap[product.varient_id] || 0;
      const notifyMe = notifyMeMap[product.varient_id] ? 'true' : 'false';
      const isSubscription = subscriptionMap[product.varient_id] ? 'true' : 'false';
      const dealPrice = dealMap[product.varient_id];
      const price = dealPrice || product.price;

      // Handle percentage as numeric (already cast in query)
      const percentageValue = parseFloat(product.percentage) || 0;
      const sub_price = (product.mrp * percentageValue) / 100;
      const subscription_price = parseFloat((product.mrp - sub_price).toFixed(2));
      const country_icon = product.country_icon ? baseurl + product.country_icon : null;

      const priceval = Number.isInteger(price) ? price + '.001' : price;
      const mrpval = Number.isInteger(product.mrp) ? product.mrp + '.001' : product.mrp;

      // Fetch feature tags if available
      let feature_tags = [];
      if (product.fcat_id) {
        const fcatinput = product.fcat_id.split(',').map(Number);
        feature_tags = await knex('feature_categories')
          .whereIn('id', fcatinput)
          .where('status',1)
          .where('is_deleted', 0)
          .select('id', knex.raw(`? || image as image`, [baseurl]));
      }
      
       const features = await knex('product_features')
                    .select('tbl_feature_value_master.id','tbl_feature_value_master.feature_value')    
                    .join('tbl_feature_value_master','tbl_feature_value_master.id','=','product_features.feature_value_id')    
                    .where('product_id', product.product_id);
      
       // Batch fetch all variants for this product to avoid N+1 queries
       const app = await knex('store_products')
                 .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                 .select('store_products.store_id','store_products.stock','product_varient.varient_id', 'product_varient.description', 'store_products.price', 'store_products.mrp',
                 'product_varient.varient_image','product_varient.unit','product_varient.quantity',knex.raw('100-((store_products.price*100.0)/store_products.mrp) as discountper'))
                 .where('store_products.store_id', appDetails.store_id)
                 .where('product_varient.product_id', product.product_id)
                 .whereNotNull('store_products.price')
                 .where('product_varient.approved',1);

       // Extract variant IDs for this product
       const productVariantIds = app.map(v => v.varient_id);
       
       // Batch fetch all deals, wishlist, cart, notify_me, and images for all variants
       const [variantDeals, variantWishlist, variantCartItems, variantSubCartItems, variantNotifyMe, productImages] = await Promise.all([
         productVariantIds.length > 0 ? knex('deal_product')
           .whereIn('varient_id', productVariantIds)
           .where('store_id', appDetails.store_id)
           .where('deal_product.valid_from', '<=', knex.fn.now())
           .where('deal_product.valid_to', '>', knex.fn.now()) : [],
         appDetails.user_id && productVariantIds.length > 0 ? knex('wishlist')
           .whereIn('varient_id', productVariantIds)
           .where('user_id', user_id) : [],
         appDetails.user_id && productVariantIds.length > 0 ? knex('store_orders')
           .whereIn('varient_id', productVariantIds)
           .where('store_approval', user_id)
           .where('order_cart_id', 'incart')
           .whereNull('subscription_flag')
           .where('store_id', appDetails.store_id) : [],
         appDetails.user_id && productVariantIds.length > 0 ? knex('store_orders')
           .whereIn('varient_id', productVariantIds)
           .where('store_approval', user_id)
           .where('order_cart_id', 'incart')
           .where('subscription_flag', 1)
           .where('store_id', appDetails.store_id) : [],
         appDetails.user_id && productVariantIds.length > 0 ? knex('product_notify_me')
           .whereIn('varient_id', productVariantIds)
           .where('user_id', user_id) : [],
         knex('product_images')
           .select(knex.raw(`? || image as image`, [baseurl]))
           .where('product_id', product.product_id)
           .orderBy('type','DESC')
       ]);

       // Create maps for O(1) lookup
       const variantDealMap = Object.fromEntries(variantDeals.map(d => [d.varient_id, d.deal_price]));
       const variantWishlistMap = Object.fromEntries(variantWishlist.map(w => [w.varient_id, true]));
       const variantCartMap = Object.fromEntries(variantCartItems.map(c => [c.varient_id, c.qty]));
       const variantSubCartMap = Object.fromEntries(variantSubCartItems.map(c => [c.varient_id, c.qty]));
       const variantNotifyMeMap = Object.fromEntries(variantNotifyMe.map(n => [n.varient_id, true]));

       // Fallback to product image if no product_images found
       if (productImages.length === 0) {
         const fallbackImage = await knex('product')
           .select(knex.raw(`? || product_image as image`, [baseurl]))
           .where('product_id', product.product_id)
           .first();
         if (fallbackImage) {
           productImages.push(fallbackImage);
         }
       }

       const customizedVarientData = [];
       let total_cart_qty = 0;
       let total_subcart_qty = 0;

       for (let i = 0; i < app.length; i++) {
         const ProductList = app[i];
         
         // Get deal price or regular price
         const vprice = variantDealMap[ProductList.varient_id] || ProductList.price;

         let isFavourite1 = 'false';
         let notifyMe1 = 'false';
         let cartQty1 = 0;
         let subcartQty1 = 0;

         if (appDetails.user_id) {
           isFavourite1 = variantWishlistMap[ProductList.varient_id] ? 'true' : 'false';
           cartQty1 = variantCartMap[ProductList.varient_id] || 0;
           subcartQty1 = variantSubCartMap[ProductList.varient_id] || 0;
           notifyMe1 = variantNotifyMeMap[ProductList.varient_id] ? 'true' : 'false';
         }
         
         total_cart_qty = total_cart_qty + cartQty1;
         total_subcart_qty = total_subcart_qty + subcartQty1;

         const customizedVarient = {
           stock: ProductList.stock,        
           varient_id: ProductList.varient_id,
           product_id: product.product_id,
           product_name: product.product_name,
           product_image: productImages.length > 0 ? productImages[0].image + "?width=200&height=200&quality=100" : '',
           thumbnail: productImages.length > 0 ? productImages[0].image : '',
           description: ProductList.description,
           price: vprice,
           mrp: ProductList.mrp,
           unit: ProductList.unit,
           quantity: ProductList.quantity,
           type: product.type,
           discountper: ProductList.discountper || 0,
           notify_me: notifyMe1,
           isFavourite: isFavourite1,
           cart_qty: cartQty1,
           subcartQty: subcartQty1,
           country_icon: product.country_icon ? baseurl + product.country_icon : null,
         };
       
         customizedVarientData.push(customizedVarient);  
       }
       const varients = customizedVarientData;

      return {
        p_id: product.p_id,
        varient_id: product.varient_id,
        stock: product.stock,
        store_id: product.store_id,
        price: parseFloat(priceval),
        mrp: parseFloat(mrpval),
        min_ord_qty: product.min_ord_qty,
        max_ord_qty: product.max_ord_qty,
        buyingprice: product.buyingprice,
        product_code: product.product_code,
        partner_id: product.partner_id,
        product_id: product.product_id,
        quantity: product.quantity,
        unit: product.prdunit,
        description: product.description,
        varient_image: product.varient_image,
        ean: product.ean,
        approved: product.approved,
        added_by: product.added_by,
        cat_id: product.cat_id,
        brand_id: product.brand_id,
        product_name: product.product_name,
        product_image: product.product_image + "?width=200&height=200&quality=100",
        type: product.type,
        hide: product.hide,
        percentage: product.percentage,
        isSubscription,
        subscription_price,
        availability: product.availability,
        discountper: product.discountper || 0,
        country_icon,
        avgrating: 0, // Placeholder for ratings
        notify_me: notifyMe,
        isFavourite,
        cart_qty: cartQty,
        total_cart_qty: total_cart_qty,
        total_subcart_qty:total_subcart_qty,
        feature_tags,
        countrating: 0,
        is_customized:product.is_customized,
        features:features,
        varients:varients
      };
    })
  );

  return customizedProductData;
};


const addNotifyMe = async (appDetatils) => {
  // Destructure the necessary fields directly
  const { product_id, varient_id, user_id, platform, fcmtoken, device_id } = appDetatils;
  
  // Check for existing record with same user_id and varient_id
  const existingEntry = await knex('product_notify_me')
    .where({ user_id, varient_id })
    .first();
    
  if (existingEntry) {
    // If a duplicate is found, return a message or handle it appropriately
    throw new Error('Notification already exists');
  }

  // Insert with all available fields (id and created_at will use defaults in PostgreSQL)
  const insertData = {
    user_id,
    product_id,
    varient_id,
    platform: platform || null,
    fcmtoken: fcmtoken || null,
  };
  
  // Add device_id if provided
  if (device_id) {
    insertData.device_id = device_id;
  }

  const insertNotifyMe = await knex('product_notify_me').insert(insertData).returning('id');
  return insertNotifyMe; // Return the result (e.g., insert ID or affected rows)
};


const deleteNotifyMe = async (appDetatils) => {
  // Destructure the necessary fields directly
  const { varient_id, user_id } = appDetatils;
  
  // Delete notification entry
  const deleteNotify = await knex('product_notify_me')
    .where('varient_id', varient_id)
    .where('user_id', user_id)
    .delete();
    
  return deleteNotify; // Return the result (e.g., affected rows count)
};
    

module.exports = {
showNotifyMe,
addNotifyMe,
deleteNotifyMe
};
