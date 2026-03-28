const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library


const sneakyprodlist = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
  const userLat = parseFloat(appDetatils.userLat);
  const userLng = parseFloat(appDetatils.userLng);
  const pageFilter = appDetatils.page || 1;
  const perPage = appDetatils.perpage || 10;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const store_id = appDetatils.store_id;
  const radiusKm = 3.0;

  // CRITICAL OPTIMIZATION: Use bounding box filter first to dramatically reduce dataset before Haversine calculation
  // Approximate 1 degree latitude ≈ 111 km, so for 3km radius: ~0.027 degrees
  // This pre-filters addresses to a small geographic area before expensive distance calculation
  const latRange = radiusKm / 111.0; // Approximate km per degree latitude
  const lngRange = radiusKm / (111.0 * Math.cos(userLat * Math.PI / 180)); // Adjust for longitude

  // OPTIMIZATION: Fix Haversine query with bounding box pre-filter and proper parameterization
  // Cast lat/lng to numeric since they're stored as text in PostgreSQL schema
  const nearbyuser = await knex('address')
    .select(
      'user_id',
      'address_id',
      'lat',
      'lng',
      knex.raw(`6371 * 2 * ASIN(SQRT(
          POWER(SIN(RADIANS((lat::numeric - ?::numeric) / 2)), 2) +
          COS(RADIANS(?::numeric)) * COS(RADIANS(lat::numeric)) *
          POWER(SIN(RADIANS((lng::numeric - ?::numeric) / 2)), 2)
        )) AS distance`, [userLat, userLat, userLng])
    )
    // Bounding box filter - dramatically reduces rows before expensive distance calculation
    .whereRaw('lat::numeric BETWEEN ?::numeric AND ?::numeric', [userLat - latRange, userLat + latRange])
    .whereRaw('lng::numeric BETWEEN ?::numeric AND ?::numeric', [userLng - lngRange, userLng + lngRange])
    // Then apply precise distance filter
    .whereRaw(`6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS((lat::numeric - ?::numeric) / 2)), 2) +
        COS(RADIANS(?::numeric)) * COS(RADIANS(lat::numeric)) *
        POWER(SIN(RADIANS((lng::numeric - ?::numeric) / 2)), 2)
      )) <= ?`, [userLat, userLat, userLng, radiusKm])
    .orderByRaw('distance')
    .limit(100); // Limit results to prevent excessive processing

  if (nearbyuser.length === 0) {
    return [];
  }

  // Extract address IDs
  const addressIds = nearbyuser.map(addr => addr.address_id);

  if (addressIds.length === 0) {
    return [];
  }

  // OPTIMIZATION: Get orders and start building product query in parallel
  const [orderList] = await Promise.all([
    knex('orders')
      .whereIn('address_id', addressIds)
      .select('cart_id')
  ]);

  const ordersArray = orderList.map(order => order.cart_id);

  if (ordersArray.length === 0) {
    return [];
  }

  // const prodList = await knex('store_orders')
  // .whereIn('order_cart_id', ordersArray)
  // .select('cart_id');

  // Build main product query with PostgreSQL-compatible GROUP BY
  const buildProductQuery = () => {
    return knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
      .leftJoin('deal_product', function () {
        this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
          .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
          .andOn('deal_product.valid_from', '<=', knex.raw('CURRENT_TIMESTAMP'))
          .andOn('deal_product.valid_to', '>', knex.raw('CURRENT_TIMESTAMP'));
      })
      .select(
        knex.raw('MAX(store_products.stock) as stock'),
        knex.raw('MAX(product_varient.varient_id) as varient_id'),
        knex.raw('MAX(product_varient.description) as description'),
        'product.product_id',
        knex.raw('MAX(product.product_name) as product_name'),
        knex.raw('MAX(product.product_image) as product_image'),
        knex.raw('MAX(product.thumbnail) as thumbnail'),
        knex.raw('MAX(store_products.price) as price'),
        knex.raw('MAX(store_products.mrp) as mrp'),
        knex.raw('MAX(product_varient.unit) as unit'),
        knex.raw('MAX(product_varient.quantity) as quantity'),
        knex.raw('MAX(product.type) as type'),
        knex.raw('MAX(tbl_country.country_icon) as country_icon'),
        knex.raw('MAX(product.percentage) as percentage'),
        knex.raw('MAX(product.availability) as availability'),
        knex.raw('MAX(product.fcat_id) as fcat_id'),
        knex.raw('MAX(100-((store_products.price*100)/store_products.mrp)) as discountper'),
        knex.raw('MAX(100-((deal_product.deal_price*100)/store_products.mrp)) as discountper1'),
        knex.raw('MAX(product.is_customized) as is_customized'),
        knex.raw('MAX(CASE WHEN deal_product.deal_price IS NOT NULL THEN deal_product.deal_price ELSE store_products.price END) as final_price')
      )
      .groupBy('product.product_id')
      .where('store_products.store_id', store_id)
      .whereIn('store_orders.order_cart_id', ordersArray)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('store_products.stock', '>', 0)
      .where(builder => {
        builder
          .where('product.is_offer_product', 0)
          .whereNull('product.offer_date')
          .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE');
      });
  };

  // OPTIMIZATION: Run count and data queries in parallel
  const productQuery = buildProductQuery();

  // Optimized count query - use subquery for better performance
  const countQuery = knex
    .from(function () {
      this.select('product.product_id')
        .from('store_orders')
        .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .where('store_products.store_id', store_id)
        .whereIn('store_orders.order_cart_id', ordersArray)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('store_products.stock', '>', 0)
        .where(builder => {
          builder
            .where('product.is_offer_product', 0)
            .whereNull('product.offer_date')
            .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE');
        })
        .groupBy('product.product_id')
        .as('grouped_products');
    })
    .count('* as total')
    .first();

  const [countResult, productDetail] = await Promise.all([
    countQuery,
    productQuery.offset((pageFilter - 1) * perPage).limit(perPage)
  ]);

  const totalPages = Math.ceil((parseInt(countResult?.total) || 0) / perPage);


  if (productDetail.length === 0) {
    return [];
  }

  // Extract IDs for batch fetching
  const productVarientIds = productDetail.map(p => p.varient_id);
  const productIds = [...new Set(productDetail.map(p => p.product_id))];

  // OPTIMIZATION: Price and deal info already in productDetail from main query
  // Create price map directly from productDetail (no extra query needed)
  const priceMap = {};
  productDetail.forEach(p => {
    priceMap[p.varient_id] = p.final_price || p.price;
  });

  //for device_id
  //   if(device_id){
  //              check=await knex('recent_search')
  //              .where('device_id',device_id);

  //              checkww=await knex('recent_search')
  //              .where('device_id',device_id)
  //              .first();   

  //              deletesame=await knex('recent_search')
  //              .where('keyword',keyword)
  //              .delete(); 

  //              if(check.length>=10){
  //                  chec=await knex('recent_search')
  //                  .where('id',checkww.id)
  //                  .delete();  
  //              }        
  //              add = await knex('recent_search')
  //              .insert({
  //                device_id:device_id,
  //                  keyword:keyword
  //              });

  //      }

  // var cartQty=0;
  //   // cart qty check 
  // const CartQtyList = await knex('store_orders')
  // .where('varient_id',ProductList.varient_id)
  // .where('store_approval',user_id)
  // .where('order_cart_id','incart')
  // .where('store_id',store_id)
  // .first();
  // cartQty = CartQtyList ? CartQtyList.qty : 0;

  // OPTIMIZATION: Batch fetch ALL user-related data in parallel (for main products)
  // Handle recent_search cleanup in background (non-blocking, fire and forget)
  if (user_id) {
    // Fire and forget - don't block main flow
    knex('recent_search').where('user_id', user_id).then(check => {
      if (check.length >= 10) {
        return knex('recent_search').where('user_id', user_id).first().then(checkww => {
          if (checkww) {
            return knex('recent_search').where('id', checkww.id).delete();
          }
        });
      }
    }).catch(() => { }); // Ignore errors, non-critical
  }

  const [wishList, cartItems, notifyMeList, subscriptionProducts] = await Promise.all([
    user_id && productVarientIds.length > 0 ? knex('wishlist').whereIn('varient_id', productVarientIds).where('user_id', user_id) : Promise.resolve([]),
    user_id && productVarientIds.length > 0 ? knex('store_orders')
      .whereIn('varient_id', productVarientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', store_id) : Promise.resolve([]),
    user_id && productVarientIds.length > 0 ? knex('product_notify_me').whereIn('varient_id', productVarientIds).where('user_id', user_id) : Promise.resolve([]),
    user_id && productVarientIds.length > 0 ? knex('store_orders')
      .select('store_orders.percentage', 'store_orders.varient_id')
      .whereIn('store_orders.varient_id', productVarientIds)
      .where('store_approval', user_id)
      .where('store_orders.subscription_flag', '1')
      .where('store_orders.order_cart_id', "incart") : Promise.resolve([])
  ]);

  // Create maps for quick lookup
  const wishlistMap = {};
  wishList.forEach(item => { wishlistMap[item.varient_id] = true; });

  const cartMap = {};
  cartItems.forEach(item => { cartMap[item.varient_id] = item.qty; });

  const notifyMeMap = {};
  notifyMeList.forEach(item => { notifyMeMap[item.varient_id] = true; });

  const subscriptionMap = {};
  subscriptionProducts.forEach(item => { subscriptionMap[item.varient_id] = item.percentage; });
  // OPTIMIZATION: Batch fetch ALL features, variants, images, and feature tags in parallel
  // Also extract feature category IDs while processing
  const fcatIdSet = new Set();
  productDetail.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const numId = parseInt(id.trim());
        if (!isNaN(numId)) fcatIdSet.add(numId);
      });
    }
  });
  const allFcatIds = Array.from(fcatIdSet);

  const [
    allFeatures,
    allVariants,
    allImages,
    allFeatureTags
  ] = await Promise.all([
    productIds.length > 0 ? knex('product_features')
      .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
      .whereIn('product_features.product_id', productIds) : Promise.resolve([]),
    productIds.length > 0 ? knex('store_products')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .select(
        'store_products.store_id',
        'store_products.stock',
        'product_varient.varient_id',
        'product_varient.description',
        'store_products.price',
        'store_products.mrp',
        'product_varient.varient_image',
        'product_varient.unit',
        'product_varient.quantity',
        'product_varient.product_id',
        knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper')
      )
      .where('store_products.store_id', store_id)
      .whereIn('product_varient.product_id', productIds)
      .whereNotNull('store_products.price')
      .where('product_varient.approved', 1)
      .where('product_varient.is_delete', 0) : Promise.resolve([]),
    productIds.length > 0 ? knex('product_images')
      .select('product_id', knex.raw('? || image as image', [baseurl]), 'type')
      .whereIn('product_id', productIds)
      .orderBy('type', 'DESC') : Promise.resolve([]),
    // Fetch feature tags in parallel with other data
    allFcatIds.length > 0 ? knex('feature_categories')
      .whereIn('id', allFcatIds)
      .where('status', 1)
      .where('is_deleted', 0)
      .select('id', knex.raw('? || image as image', [baseurl])) : Promise.resolve([])
  ]);

  // Build maps for features, variants, and images
  const featuresMap = {};
  allFeatures.forEach(f => {
    if (!featuresMap[f.product_id]) featuresMap[f.product_id] = [];
    featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
  });

  const variantsMap = {};
  allVariants.forEach(v => {
    if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
    variantsMap[v.product_id].push(v);
  });

  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img.image);
  });

  // Feature tags already fetched in parallel above

  const featureTagsMap = {};
  const featureTagsById = {};
  allFeatureTags.forEach(ft => { featureTagsById[ft.id] = ft; });

  productDetail.forEach(product => {
    if (product.fcat_id) {
      const resultArray = product.fcat_id.split(',').map(Number);
      featureTagsMap[product.product_id] = resultArray.map(id => featureTagsById[id]).filter(Boolean);
    } else {
      featureTagsMap[product.product_id] = [];
    }
  });

  // OPTIMIZATION: Batch fetch ALL cart data for ALL variants
  const variantIds = allVariants.map(v => v.varient_id);
  const [
    allWishlistVariants,
    allCartItemsVariants,
    allNotifyMeVariants,
    allSubCartVariants
  ] = await Promise.all([
    user_id && variantIds.length > 0 ? knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id) : Promise.resolve([]),
    user_id && variantIds.length > 0 ? knex('store_orders')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', store_id) : Promise.resolve([]),
    user_id && variantIds.length > 0 ? knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id) : Promise.resolve([]),
    user_id && variantIds.length > 0 ? knex('store_orders')
      .select('varient_id', 'qty')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('subscription_flag', '1')
      .where('store_id', store_id) : Promise.resolve([])
  ]);

  const wishlistMapVariants = {};
  allWishlistVariants.forEach(w => wishlistMapVariants[w.varient_id] = true);

  const cartMapVariants = {};
  allCartItemsVariants.forEach(c => cartMapVariants[c.varient_id] = c.qty);

  const notifyMeMapVariants = {};
  allNotifyMeVariants.forEach(n => notifyMeMapVariants[n.varient_id] = true);

  const subCartMapVariants = {};
  allSubCartVariants.forEach(sc => subCartMapVariants[sc.varient_id] = sc.qty || 0);

  // OPTIMIZATION: Process products efficiently (no individual queries)
  const customizedProductData1 = productDetail.map(ProductList => {
    // Get price from pre-calculated final_price or fallback
    const price = priceMap[ProductList.varient_id] || ProductList.price || 0;

    const isFavourite = wishlistMap[ProductList.varient_id] ? 'true' : 'false';
    const cartQty = cartMap[ProductList.varient_id] || 0;
    const notifyMe = notifyMeMap[ProductList.varient_id] ? 'true' : 'false';
    const isSubscription = subscriptionMap[ProductList.varient_id] ? 'true' : 'false';

    const sub_price = (ProductList.mrp * (parseFloat(ProductList.percentage) || 0)) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

    if (Number.isInteger(price)) {
      var priceval = price + '.001'
    } else {
      var priceval = price
    }
    if (Number.isInteger(ProductList.mrp)) {
      var mrpval = ProductList.mrp + '.001'
    } else {
      var mrpval = ProductList.mrp
    }

    // Get pre-fetched data from maps
    const featureTags = featureTagsMap[ProductList.product_id] || [];
    const features = featuresMap[ProductList.product_id] || [];
    const variants = variantsMap[ProductList.product_id] || [];
    const productImages = imagesMap[ProductList.product_id] || [baseurl + ProductList.product_image];

    // Build variant data using pre-fetched maps
    const customizedVarientData = variants.map(variant => {
      const isFavourite1 = wishlistMapVariants[variant.varient_id] ? 'true' : 'false';
      const cartQty1 = cartMapVariants[variant.varient_id] || 0;
      const notifyMe1 = notifyMeMapVariants[variant.varient_id] ? 'true' : 'false';
      const subcartQty1 = subCartMapVariants[variant.varient_id] || 0;

      return {
        stock: variant.stock,
        varient_id: variant.varient_id,
        product_id: variant.product_id,
        product_name: ProductList.product_name,
        product_image: productImages[0] ? productImages[0] + "?width=200&height=200&quality=100" : '',
        thumbnail: productImages[0] || '',
        description: variant.description,
        price: variant.price,
        mrp: variant.mrp,
        unit: variant.unit,
        quantity: variant.quantity,
        type: ProductList.type,
        discountper: variant.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        subcartQty: subcartQty1,
        country_icon: ProductList.country_icon ? baseurl + ProductList.country_icon : null,
      };
    });

    const total_cart_qty = variants.reduce((sum, v) => sum + (cartMapVariants[v.varient_id] || 0), 0);
    const total_subcart_qty = variants.reduce((sum, v) => sum + (subCartMapVariants[v.varient_id] || 0), 0);

    return {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
      thumbnail: baseurl + ProductList.thumbnail,
      description: ProductList.description,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      discountper: ProductList.discountper,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      total_subcart_qty: total_subcart_qty,
      countrating: 0,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      country_icon: countryicon,
      feature_tags: featureTags,
      features: features,
      varients: customizedVarientData,
      is_customized: ProductList.is_customized,
      totalPages: totalPages,
    };
  });

  return customizedProductData1;
}
module.exports = {
  sneakyprodlist
};
