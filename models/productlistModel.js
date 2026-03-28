// models/orderModel.js
const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const moment = require('moment');
require('moment-timezone');
const uaeTime = moment.tz(new Date(), "Asia/Dubai");
const wordCount = require('word-count');

const trailpackimagedata = async (appDetatils) => {
  const { user_id } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const today = new Date();

  // Sanitize user_id: treat " ", "null", or undefined as null
  let sanitized_user_id = user_id;
  if (!sanitized_user_id || sanitized_user_id === "null" || (typeof sanitized_user_id === 'string' && sanitized_user_id.trim() === "")) {
    sanitized_user_id = null;
  }

  // Ensure user_id is numeric for the bigint column
  const queryUserId = (sanitized_user_id && !isNaN(sanitized_user_id)) ? parseInt(sanitized_user_id) : null;

  const orderlistdata = queryUserId
    ? await knex('orders')
      .where('order_type', 'LIKE', 'trail')
      .where('user_id', queryUserId)
      .groupBy('trail_id')
      .pluck('trail_id')
    : [];

  const checktrail = await knex('tbl_trail_pack_basic')
    .where('tbl_trail_pack_basic.status', 1)
    .where('tbl_trail_pack_basic.start_date', '<=', today)
    .andWhere('tbl_trail_pack_basic.end_date', '>=', today)
    .where('tbl_trail_pack_basic.is_delete', 0)
    .whereNotIn('id', orderlistdata)
    .orderBy('main_order', 'ASC')
    .select('id', 'popup_image', 'image')
    .first();

  //return checktrail;
  if (checktrail) {
    trailimage = checktrail.popup_image;
    //trailpackimage =  baseurl + "/images/trail_pack/trialpack.png"
    return trailpackimage = baseurl + trailimage;

  } else {
    return trailpackimage = null;
  }


};

const getTopSelling = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
  const { store_id, byname, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage } = appDetatils;
  const pageFilter = page; // You can adjust the page number dynamically
  const perPage = perpage;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }

  const minprice = parseFloat(min_price)
  const maxprice = parseFloat(max_price)
  const mindiscount = parseFloat(min_discount)
  const maxdiscount = parseFloat(max_discount)
  const subcatid = sub_cat_id


  // let categoryarray;
  // if (cat_id !== "null") {
  // categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
  // }

  // let categoryList = await knex('categories').where('parent', 121).pluck('cat_id');
  // let categoryList = await knex('categories').where('parent', 1).pluck('cat_id');

  //  const topsellingsQuery = knex('store_products')
  //  .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
  //  .join('product', 'product_varient.product_id', '=', 'product.product_id')
  //  .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
  //  .leftJoin('tbl_country', 'tbl_country.id', '=', 'product.country_id')
  //  .select(
  //    'store_products.stock',
  //    'product_varient.varient_id',
  //    'product_varient.description',
  //    'product.product_id',
  //    'product.product_name',
  //    'product.product_image',
  //    'product.thumbnail',
  //    'store_products.price',
  //    'store_products.mrp',
  //    'product_varient.unit',
  //    'product_varient.quantity',
  //    'product.type',
  //    'product.percentage',
  //    'product.availability',
  //    'product.country_id',
  //    'product.fcat_id',
  //    'tbl_country.country_icon',
  //    knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
  //    knex.raw('100-((deal_product.deal_price*100)/store_products.mrp) as discountper1'),
  //   )
  //  .groupBy(
  //    'store_products.store_id',
  //    'product_varient.varient_id',
  //    'product.product_id'
  //  )
  //  .where('store_products.store_id', store_id)
  //  .where('product.hide', 0)
  //  .where('product.approved', 1)
  //  .whereIn('product.cat_id', categoryList)
  //  .where('product.is_delete', 0);


  const today = new Date(); // current date
  const oneYearAgo = new Date();
  oneYearAgo.setFullYear(today.getFullYear() - 1);
  const oneYearAgoStr = oneYearAgo.toISOString().split('T')[0]; // Format for PostgreSQL

  // Optimize: Use subquery to pre-filter subscription orders and aggregate quantities first
  // This reduces the dataset before joining with other tables
  const productQuantitiesSubquery = knex('subscription_order as sub')
    .join('store_orders as so', 'sub.store_order_id', 'so.store_order_id')
    .join('orders as or', 'or.cart_id', 'so.order_cart_id')
    .select('so.varient_id', knex.raw('SUM(so.qty) as total_quantity'))
    .whereNotIn('sub.order_status', ['Pause', 'Cancelled'])
    .where('or.order_date', '>=', oneYearAgoStr)
    .where('so.store_id', store_id) // Add store_id filter early
    .groupBy('so.varient_id')
    .havingRaw('SUM(so.qty) > 0'); // Only products with sales

  const topsellingsQuery = knex('product as p')
    .join('product_varient as pv', 'pv.product_id', 'p.product_id')
    .join('store_products as sp', function () {
      this.on('sp.varient_id', '=', 'pv.varient_id')
        .andOn('sp.store_id', '=', knex.raw('?', [store_id]));
    })
    .join(knex.raw('(?) as pq', [productQuantitiesSubquery]), 'pq.varient_id', 'pv.varient_id')
    .leftJoin('tbl_country as c', function () {
      this.on(knex.raw('c.id::text'), '=', knex.raw('p.country_id'));
    })
    .join('categories as cat', 'cat.cat_id', 'p.cat_id')
    .join('brands as brand', 'brand.cat_id', 'p.brand_id')
    .select(
      knex.raw('MAX(sp.stock) as stock'),
      'p.product_image',
      knex.raw('100-((MAX(sp.price)*100)/MAX(sp.mrp)) as discountper'),
      knex.raw('100-((MAX(sp.price)*100)/MAX(sp.mrp)) as discountper1'),
      'p.country_id',
      'c.country_icon',
      knex.raw('MAX(pv.unit) as unit'),
      knex.raw('MAX(pv.varient_id) as varient_id'),
      knex.raw('MAX(pv.quantity) as quantity'),
      'p.product_id',
      'cat.cat_id',
      'cat.parent',
      'p.product_name',
      'p.thumbnail',
      knex.raw('MAX(sp.price) as price'),
      knex.raw('MAX(sp.mrp) as mrp'),
      'p.type',
      'p.percentage',
      'p.availability',
      'pq.total_quantity',
      knex.raw('MAX(pv.description) as description'),
      knex.raw('MAX(pv.varient_image) as varient_image'),
      knex.raw('MAX(pv.ean) as ean'),
      'p.approved',
      'p.cat_id',
      'p.brand_id',
      'p.hide',
      'p.added_by',
      'p.fcat_id',
      'p.is_customized'
    )
    .where('p.is_delete', 0)
    .where('p.is_zap', true)
    .where('sp.stock', '>', 0)
    .groupBy(
      'p.product_image',
      'p.country_id',
      'c.country_icon',
      'p.product_id',
      'cat.cat_id',
      'cat.parent',
      'p.product_name',
      'p.thumbnail',
      'p.type',
      'p.percentage',
      'p.availability',
      'p.approved',
      'p.cat_id',
      'p.brand_id',
      'p.hide',
      'p.added_by',
      'p.fcat_id',
      'p.is_customized',
      'pq.total_quantity'
    )
    .where(builder => {
      builder
        .where('p.is_offer_product', 0)
        .whereNull('p.offer_date')
        .orWhereRaw("p.offer_date::date != CURRENT_DATE")
    })
    .orderBy('pq.total_quantity', 'desc');



  //  if (categoryarray)  {
  //   topsellingsQuery.whereIn('product.cat_id', categoryarray);
  //   }


  //   if (sub_cat_id !== "null")  {
  //     topsellingsQuery.where('product.cat_id', subcatid);
  //   }


  if ((minprice === 0 || minprice) && maxprice) {
    topsellingsQuery.havingRaw('MAX(sp.price) BETWEEN ? AND ?', [minprice, maxprice]);
  }

  if (mindiscount && maxdiscount) {
    topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [
      mindiscount,
      maxdiscount,
      mindiscount,
      maxdiscount,
    ]);
  }
  if (sortprice === 'ltoh') {
    topsellingsQuery.orderByRaw('MAX(sp.price) ASC');
  }

  if (sortprice === 'htol') {
    topsellingsQuery.orderByRaw('MAX(sp.price) DESC');
  }

  if (sortname === 'atoz') {
    topsellingsQuery.orderBy('p.product_name', 'ASC');
  }

  if (sortname === 'ztoa') {
    topsellingsQuery.orderBy('p.product_name', 'DESC');
  }

  // Optimize: Run count and data queries in parallel
  const totalCountQuery = topsellingsQuery.clone().clearSelect().clearOrder().countDistinct('p.product_id as total');
  const dataQuery = topsellingsQuery.offset((pageFilter - 1) * perPage).limit(perPage);

  const [totalCountResult, productDetail] = await Promise.all([
    totalCountQuery.first(),
    dataQuery
  ]);

  const totalCount = parseInt(totalCountResult?.total || 0);
  const totalPages = Math.ceil(totalCount / perPage);


  // Pre-fetch all data in batches to avoid N+1 queries - RUN IN PARALLEL for speed
  const productIds = productDetail.map(p => p.product_id);
  const varientIds = productDetail.map(p => p.varient_id);
  const currentDate = new Date();

  // Prepare all batch queries
  const batchQueries = [];

  // Batch fetch all deals
  let dealsPromise = Promise.resolve([]);
  if (varientIds.length > 0) {
    dealsPromise = knex('deal_product')
      .whereIn('varient_id', varientIds)
      .where('store_id', store_id)
      .where('deal_product.valid_from', '<=', currentDate)
      .where('deal_product.valid_to', '>', currentDate)
      .select('varient_id', 'deal_price');
  }

  // Batch fetch all store products prices
  let storeProductsPromise = Promise.resolve([]);
  if (varientIds.length > 0) {
    storeProductsPromise = knex('store_products')
      .whereIn('varient_id', varientIds)
      .where('store_id', store_id)
      .select('varient_id', 'price');
  }

  // Batch fetch user-specific data if user_id exists
  let wishlistsPromise = Promise.resolve([]);
  let cartItemsPromise = Promise.resolve([]);
  let subCartItemsPromise = Promise.resolve([]);
  let subscriptionsPromise = Promise.resolve([]);
  let notifyMePromise = Promise.resolve([]);

  if (user_id && varientIds.length > 0) {
    wishlistsPromise = knex('wishlist')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .select('varient_id');

    cartItemsPromise = knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('store_id', store_id)
      .whereNull('subscription_flag')
      .select('varient_id', 'qty', 'product_feature_id');

    subCartItemsPromise = knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('subscription_flag', 1)
      .where('store_id', store_id)
      .select('varient_id', 'qty');

    subscriptionsPromise = knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1)
      .where('order_cart_id', 'incart')
      .select('varient_id');

    notifyMePromise = knex('product_notify_me')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .select('varient_id');
  }

  // Batch fetch features for all products
  let featuresPromise = Promise.resolve([]);
  if (productIds.length > 0) {
    featuresPromise = knex('product_features')
      .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
      .whereIn('product_id', productIds);
  }

  // Batch fetch feature categories
  const fcatIds = new Set();
  productDetail.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const parsedId = parseInt(id.trim());
        if (!isNaN(parsedId)) fcatIds.add(parsedId);
      });
    }
  });
  let featureCatsPromise = Promise.resolve([]);
  if (fcatIds.size > 0) {
    featureCatsPromise = knex('feature_categories')
      .whereIn('id', Array.from(fcatIds))
      .where('status', 1)
      .where('is_deleted', 0)
      .select('id', knex.raw(`('${baseurl}' || COALESCE(image, '')) as image`));
  }

  // Batch fetch all variants for all products
  let variantsPromise = Promise.resolve([]);
  if (productIds.length > 0) {
    variantsPromise = knex('store_products')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id',
        'product_varient.description', 'store_products.price', 'store_products.mrp',
        'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity',
        'product_varient.product_id',
        knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
      .where('store_products.store_id', store_id)
      .whereIn('product_varient.product_id', productIds)
      .whereNotNull('store_products.price')
      .where('product_varient.approved', 1)
      .where('product_varient.is_delete', 0);
  }

  // Batch fetch product images
  let imagesPromise = Promise.resolve([]);
  let fallbackImagesPromise = Promise.resolve([]);
  if (productIds.length > 0) {
    imagesPromise = knex('product_images')
      .select('product_id', knex.raw(`('${baseurl}' || COALESCE(image, '')) as image`), 'type')
      .whereIn('product_id', productIds)
      .orderBy('type', 'DESC');
  }

  // Execute ALL batch queries in parallel
  const [
    deals,
    storeProducts,
    wishlists,
    cartItems,
    subCartItems,
    subscriptions,
    notifyMeItems,
    allFeatures,
    allFeatureCats,
    allVariants,
    allImages
  ] = await Promise.all([
    dealsPromise,
    storeProductsPromise,
    wishlistsPromise,
    cartItemsPromise,
    subCartItemsPromise,
    subscriptionsPromise,
    notifyMePromise,
    featuresPromise,
    featureCatsPromise,
    variantsPromise,
    imagesPromise
  ]);

  // Build maps from results
  const dealsMap = {};
  deals.forEach(deal => dealsMap[deal.varient_id] = deal.deal_price);

  const storeProductsMap = {};
  storeProducts.forEach(sp => storeProductsMap[sp.varient_id] = sp.price);

  const wishlistMap = {};
  wishlists.forEach(w => wishlistMap[w.varient_id] = true);

  const cartQtyMap = {};
  const cartFeatureMap = {};
  cartItems.forEach(c => {
    cartQtyMap[c.varient_id] = c.qty;
    if (c.product_feature_id) cartFeatureMap[c.varient_id] = c.product_feature_id;
  });

  const subcartQtyMap = {};
  subCartItems.forEach(sc => subcartQtyMap[sc.varient_id] = sc.qty);

  const subscriptionMap = {};
  subscriptions.forEach(s => subscriptionMap[s.varient_id] = true);

  const notifyMeMap = {};
  notifyMeItems.forEach(n => notifyMeMap[n.varient_id] = true);

  const featuresMap = {};
  allFeatures.forEach(f => {
    if (!featuresMap[f.product_id]) featuresMap[f.product_id] = [];
    featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
  });

  const featureCategoriesMap = {};
  allFeatureCats.forEach(fc => featureCategoriesMap[fc.id] = fc);

  const variantsMap = {};
  allVariants.forEach(v => {
    if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
    variantsMap[v.product_id].push(v);
  });

  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  });

  // Fallback to product_image if no product_images - only fetch if needed
  const productsWithoutImages = productIds.filter(id => !imagesMap[id] || imagesMap[id].length === 0);
  if (productsWithoutImages.length > 0) {
    const fallbackImages = await knex('product')
      .select('product_id', knex.raw(`('${baseurl}' || COALESCE(product_image, '')) as image`))
      .whereIn('product_id', productsWithoutImages);
    fallbackImages.forEach(img => {
      if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
      imagesMap[img.product_id].push(img);
    });
  }

  const customizedProductData = [];
  for (let i = 0; i < productDetail.length; i++) {
    const ProductList = productDetail[i];

    // Get price from deals or store products (already fetched)
    let price = dealsMap[ProductList.varient_id] || storeProductsMap[ProductList.varient_id] || 0;


    // Use pre-fetched data instead of queries
    let isSubscription = 'false';
    let isFavourite = 'false';
    let notifyMe = 'false';
    let cartQty = 0;

    if (user_id) {
      isSubscription = subscriptionMap[ProductList.varient_id] ? 'true' : 'false';
      isFavourite = wishlistMap[ProductList.varient_id] ? 'true' : 'false';
      cartQty = cartQtyMap[ProductList.varient_id] || 0;
      notifyMe = notifyMeMap[ProductList.varient_id] ? 'true' : 'false';
    }

    const baseurl = process.env.BUNNY_NET_IMAGE;


    const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));
    if (ProductList.country_icon == null) {
      countryicon = null
    } else {
      countryicon = baseurl + ProductList.country_icon
    }

    if (Number.isInteger(price)) {
      priceval = price + '.001'
    } else {
      priceval = price
    }
    if (Number.isInteger(ProductList.mrp)) {
      mrpval = ProductList.mrp + '.001'
    } else {
      mrpval = ProductList.mrp
    }

    // Use pre-fetched feature categories
    let feature_tags = [];
    if (ProductList.fcat_id != null) {
      const resultArray = ProductList.fcat_id.split(',').map(Number);
      feature_tags = resultArray.map(id => featureCategoriesMap[id]).filter(Boolean);
    }

    // Use pre-fetched features
    const features = featuresMap[ProductList.product_id] || [];

    // Use pre-fetched variants
    const app = variantsMap[ProductList.product_id] || [];

    // Use pre-fetched data for variants
    const customizedVarientData = [];
    let total_cart_qty = 0;
    let total_subcart_qty = 0;

    for (let j = 0; j < app.length; j++) {
      const ProductList1 = app[j];
      // Use pre-fetched price
      const vprice = dealsMap[ProductList1.varient_id] || storeProductsMap[ProductList1.varient_id] || ProductList1.price;

      // Use pre-fetched user data
      let isFavourite1 = 'false';
      let notifyMe1 = 'false';
      let cartQty1 = 0;
      let subcartQty = 0;
      let productFeatureId = 0;

      if (user_id) {
        isFavourite1 = wishlistMap[ProductList1.varient_id] ? 'true' : 'false';
        cartQty1 = cartQtyMap[ProductList1.varient_id] || 0;
        subcartQty = subcartQtyMap[ProductList1.varient_id] || 0;
        notifyMe1 = notifyMeMap[ProductList1.varient_id] ? 'true' : 'false';
        productFeatureId = cartFeatureMap[ProductList1.varient_id] || 0;

        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty;
      }

      // Use pre-fetched images
      const images = imagesMap[ProductList.product_id] || [];

      const customizedVarient = {
        stock: ProductList1.stock,
        varient_id: ProductList1.varient_id,
        product_id: ProductList1.product_id,
        product_name: ProductList.product_name,
        product_image: images.length > 0 ? images[0].image + "?width=200&height=200&quality=100" : '',
        thumbnail: images.length > 0 ? images[0].image : '',
        description: ProductList1.description,
        price: vprice,
        mrp: ProductList1.mrp,
        unit: ProductList1.unit,
        quantity: ProductList1.quantity,
        type: ProductList.type,
        discountper: ProductList.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        total_cart_qty: cartQty1,
        subcartQty: subcartQty,
        total_subcart_qty: subcartQty,
        product_feature_id: productFeatureId,
        country_icon: ProductList.country_icon ? baseurl + ProductList.country_icon : null,
      };

      customizedVarientData.push(customizedVarient);
    }
    const varients = customizedVarientData;

    const customizedProduct = {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image,
      thumbnail: baseurl + ProductList.thumbnail,
      description: ProductList.description,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      // discountper: ProductList.discountper,
      discountper: 0,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      countrating: 0,
      country_icon: countryicon,
      availability: ProductList.availability,
      feature_tags: feature_tags,
      features: features,
      varients: varients,
      is_customized: ProductList.is_customized,
      page: pageFilter,
      perPage: perPage,
      totalPages: totalPages,
      total_subcart_qty: total_subcart_qty,
      // Add or modify properties as needed
    };

    customizedProductData.push(customizedProduct);

  }

  return customizedProductData;
};

const getWhatsNew = async (appDetatils) => {
  await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');
  const { store_id, byname, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage } = appDetatils;
  const catId = 1;
  const pageFilter = page; // You can adjust the page number dynamically
  const perPage = perpage;
  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }
  const minprice = parseFloat(min_price)
  const maxprice = parseFloat(max_price)
  const mindiscount = parseFloat(min_discount)
  const maxdiscount = parseFloat(max_discount)
  const subcatid = sub_cat_id


  let categoryarray;
  if (cat_id !== "null") {
    categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
  }



  const topsellingsQuery = knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .select(
      'store_products.stock',
      'product_varient.varient_id',
      'product_varient.description',
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'store_products.price',
      'store_products.mrp',
      'product_varient.unit',
      'product_varient.quantity',
      'product.type',
      'product.percentage',
      'product.availability',
      'tbl_country.country_icon',
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
      knex.raw('100-((deal_product.deal_price*100)/store_products.mrp) as discountper1'),
    )
    .groupBy(
      'store_products.store_id',
      'product_varient.varient_id',
      'product.product_id'
    )
    .where('store_products.store_id', store_id)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('store_products.stock', '>', 0);
  //  return topsellingsQuery

  //if (cat_id !== "null") {
  //if (categoryarray.length > 0)  {
  if (categoryarray) {
    topsellingsQuery.whereIn('product.cat_id', categoryarray);
  }

  if (sub_cat_id !== "null") {
    topsellingsQuery.where('product.cat_id', subcatid);
  }
  // return topsellingsQuery
  if ((minprice === 0 || minprice) && maxprice) {
    topsellingsQuery.whereBetween('store_products.price', [minprice, maxprice]);
  }

  if (mindiscount && maxdiscount) {
    topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [
      mindiscount,
      maxdiscount,
      mindiscount,
      maxdiscount,
    ]);
  }

  if (sortprice === 'ltoh') {
    topsellingsQuery.orderBy('store_products.price', 'ASC');
  }

  if (sortprice === 'htol') {
    topsellingsQuery.orderBy('store_products.price', 'DESC');
  }

  if (sortname === 'atoz') {
    topsellingsQuery.orderBy('product.product_name', 'ASC');
  }

  if (sortname === 'ztoa') {
    topsellingsQuery.orderBy('product.product_name', 'DESC');
  }


  const productDetail = await topsellingsQuery.offset((pageFilter - 1) * perPage)
    .limit(perPage);


  const customizedProductData = [];
  for (let i = 0; i < productDetail.length; i++) {
    const ProductList = productDetail[i];
    const currentDate = new Date();
    const deal = await knex('deal_product')
      .where('varient_id', ProductList.varient_id)
      .where('store_id', store_id)
      .where('deal_product.valid_from', '<=', currentDate)
      .where('deal_product.valid_to', '>', currentDate)
      .first();

    let price = 0;
    if (deal) {
      price = deal.deal_price;
    } else {
      const sp = await knex('store_products')
        .where('varient_id', ProductList.varient_id)
        .where('store_id', store_id)
        .first();
      price = sp.price;
    }


    if (user_id) {
      // Wishlist check 
      // Wishlist check 
      var isFavourite = '';
      var notifyMe = '';
      var cartQty = 0;
      const wishList = await knex('wishlist')
        .select('*')
        .where('varient_id', ProductList.varient_id)
        .where('user_id', user_id);

      isFavourite = wishList.length > 0 ? 'true' : 'false';

      // cart qty check 
      const CartQtyList = await knex('store_orders')
        .where('varient_id', ProductList.varient_id)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where('store_id', store_id)
        .whereNull('subscription_flag')
        .first();
      cartQty = CartQtyList ? CartQtyList.qty : 0;


      const cnotify_me = await knex('product_notify_me')
        .where('varient_id', ProductList.varient_id)
        .where('user_id', user_id);
      notifyMe = cnotify_me.length > 0 ? 'true' : 'false';

      const subprod = await knex('store_orders')
        .select('store_orders.percentage')
        .where('store_orders.varient_id', ProductList.varient_id)
        .where('store_approval', user_id)
        .where('store_orders.subscription_flag', 1)
        .where('store_orders.order_cart_id', "incart")
        .first();

      if (subprod) {
        isSubscription = 'true'
      } else {
        isSubscription = 'false'

      }

    } else {
      notifyMe = 'false';
      isFavourite = 'false';
      cartQty = 0;
      isSubscription = 'false'
    }



    const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));


    const baseurl = process.env.BUNNY_NET_IMAGE;

    if (ProductList.country_icon == null) {
      countryicon = null
    } else {
      countryicon = baseurl + ProductList.country_icon
    }

    const customizedProduct = {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image,
      thumbnail: ProductList.thumbnail,
      description: ProductList.description,
      price: price,
      mrp: ProductList.mrp,
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      //discountper: ProductList.discountper,
      discountper: 0,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      countrating: 0,
      country_icon: countryicon,
      varients: null
      // Add or modify properties as needed
    };

    customizedProductData.push(customizedProduct);

  }

  return customizedProductData;
};

const getRecentSelling = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
  const { store_id, byname, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage } = appDetatils;
  const pageFilter = page || 1;
  const perPage = perpage || 10;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const minprice = parseFloat(min_price) || 0;
  const maxprice = parseFloat(max_price) || 0;
  const mindiscount = parseFloat(min_discount) || 0;
  const maxdiscount = parseFloat(max_discount) || 0;
  const subcatid = sub_cat_id;

  // Get category array first (needed for query building)
  const categoryarray = cat_id !== "null"
    ? await knex('categories').where('parent', cat_id).pluck('cat_id')
    : null;

  // Build base query conditions (reusable)
  const buildBaseConditions = (query) => {
    query
      .where('store_products.store_id', store_id)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('product.is_zap', true)
      .where('store_products.stock', '>', 0)
      .where(builder => {
        builder
          .where('product.is_offer_product', 0)
          .whereNull('product.offer_date')
          .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE');
      });

    if (categoryarray && categoryarray.length > 0) {
      query.whereIn('product.cat_id', categoryarray);
    }

    if (sub_cat_id !== "null") {
      query.where('product.cat_id', subcatid);
    }

    if ((minprice > 0 || minprice === 0) && maxprice > 0) {
      query.whereBetween('store_products.price', [minprice, maxprice]);
    }
  };

  // Build optimized main query - optimized GROUP BY with minimal aggregates
  const buildMainQuery = () => {
    const query = knex('store_products')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .leftJoin('deal_product', function () {
        this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
          .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
          .andOn('deal_product.valid_from', '<=', knex.raw('CURRENT_TIMESTAMP'))
          .andOn('deal_product.valid_to', '>', knex.raw('CURRENT_TIMESTAMP'));
      })
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
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
        knex.raw('MAX(store_products.p_id) as p_id'),
        knex.raw('MAX(CASE WHEN deal_product.deal_price IS NOT NULL THEN deal_product.deal_price ELSE store_products.price END) as final_price')
      )
      .groupBy('product.product_id');

    buildBaseConditions(query);

    if (mindiscount > 0 && maxdiscount > 0) {
      query.havingRaw('(MAX(100-((store_products.price*100)/store_products.mrp)) BETWEEN ? AND ?) OR (MAX(100-((deal_product.deal_price*100)/store_products.mrp)) BETWEEN ? AND ?)', [
        mindiscount, maxdiscount, mindiscount, maxdiscount
      ]);
    }

    // Apply sorting
    if (sortprice === 'ltoh') {
      query.orderByRaw('MAX(CASE WHEN deal_product.deal_price IS NOT NULL THEN deal_product.deal_price ELSE store_products.price END) ASC');
    } else if (sortprice === 'htol') {
      query.orderByRaw('MAX(CASE WHEN deal_product.deal_price IS NOT NULL THEN deal_product.deal_price ELSE store_products.price END) DESC');
    } else if (sortname === 'atoz') {
      query.orderByRaw('MAX(product.product_name) ASC');
    } else if (sortname === 'ztoa') {
      query.orderByRaw('MAX(product.product_name) DESC');
    } else {
      query.orderByRaw('MAX(store_products.p_id) ASC');
    }

    return query;
  };

  // OPTIMIZATION: Run count and data queries in parallel
  const mainQuery = buildMainQuery();

  // Fast count query - just count distinct product_ids with same filters
  const countQuery = knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('deal_product', function () {
      this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
        .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
        .andOn('deal_product.valid_from', '<=', knex.raw('CURRENT_TIMESTAMP'))
        .andOn('deal_product.valid_to', '>', knex.raw('CURRENT_TIMESTAMP'));
    })
    .where('store_products.store_id', store_id)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('store_products.stock', '>', 0)
    .where(builder => {
      builder
        .where('product.is_offer_product', 0)
        .whereNull('product.offer_date')
        .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE');
    })
    .modify(buildBaseConditions)
    .countDistinct('product.product_id as total')
    .first();

  const [countResult, productDetail] = await Promise.all([
    countQuery,
    mainQuery.offset((pageFilter - 1) * perPage).limit(perPage)
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

  // OPTIMIZATION: Batch fetch ALL data in parallel - combine all independent queries
  const [
    wishList,
    cartItems,
    notifyMeList,
    subscriptionProducts,
    allFeatures,
    allVariants,
    allImages
  ] = await Promise.all([
    // User-related data for main products
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
      .where('store_orders.order_cart_id', "incart") : Promise.resolve([]),
    // Features for all products
    productIds.length > 0 ? knex('product_features')
      .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
      .whereIn('product_features.product_id', productIds) : Promise.resolve([]),
    // Variants for all products
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
    // Images for all products
    productIds.length > 0 ? knex('product_images')
      .select('product_id', knex.raw('? || image as image', [baseurl]), 'type')
      .whereIn('product_id', productIds)
      .orderBy('type', 'DESC') : Promise.resolve([])
  ]);

  // OPTIMIZATION: Create maps efficiently using for loops (faster than reduce)
  const wishlistMap = {};
  wishList.forEach(item => { wishlistMap[item.varient_id] = true; });

  const cartMap = {};
  cartItems.forEach(item => { cartMap[item.varient_id] = item.qty; });

  const notifyMeMap = {};
  notifyMeList.forEach(item => { notifyMeMap[item.varient_id] = true; });

  const subscriptionMap = {};
  subscriptionProducts.forEach(item => { subscriptionMap[item.varient_id] = item.percentage; });

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

  // OPTIMIZATION: Process feature tags and fetch variant data in parallel
  const variantIds = allVariants.map(v => v.varient_id);

  // Extract unique feature category IDs efficiently
  const fcatIdSet = new Set();
  productDetail.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const parsedId = parseInt(id.trim());
        if (!isNaN(parsedId)) fcatIdSet.add(parsedId);
      });
    }
  });
  const allFcatIds = Array.from(fcatIdSet);

  const [
    allFeatureTags,
    allWishlistVariants,
    allCartItemsVariants,
    allNotifyMeVariants,
    allCartFeatures,
    allSubCartVariants
  ] = await Promise.all([
    // Feature tags
    allFcatIds.length > 0 ? knex('feature_categories')
      .whereIn('id', allFcatIds)
      .where('status', 1)
      .where('is_deleted', 0)
      .select('id', knex.raw('? || image as image', [baseurl])) : Promise.resolve([]),
    // Variant-related user data (all in parallel)
    user_id && variantIds.length > 0 ? knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id) : Promise.resolve([]),
    user_id && variantIds.length > 0 ? knex('store_orders')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', store_id) : Promise.resolve([]),
    user_id && variantIds.length > 0 ? knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id) : Promise.resolve([]),
    user_id && variantIds.length > 0 ? knex('store_orders')
      .select('varient_id', 'product_feature_id')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('store_id', store_id) : Promise.resolve([]),
    // Subscription cart quantities
    user_id && variantIds.length > 0 ? knex('store_orders')
      .select('varient_id', 'qty')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('subscription_flag', '1')
      .where('store_id', store_id) : Promise.resolve([])
  ]);

  // Build feature tags map efficiently
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

  // OPTIMIZATION: Build variant maps efficiently
  const wishlistMapVariants = {};
  allWishlistVariants.forEach(w => wishlistMapVariants[w.varient_id] = true);

  const cartMapVariants = {};
  allCartItemsVariants.forEach(c => cartMapVariants[c.varient_id] = c.qty);

  const notifyMeMapVariants = {};
  allNotifyMeVariants.forEach(n => notifyMeMapVariants[n.varient_id] = true);

  const cartFeaturesMap = {};
  allCartFeatures.forEach(cf => cartFeaturesMap[cf.varient_id] = cf.product_feature_id || 0);

  const subCartMapVariants = {};
  allSubCartVariants.forEach(sc => subCartMapVariants[sc.varient_id] = sc.qty || 0);

  // OPTIMIZATION: Process products efficiently (no individual queries)
  const customizedProductData = productDetail.map(ProductList => {
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
      const productFeatureId = cartFeaturesMap[variant.varient_id] || 0;
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
        product_feature_id: productFeatureId,
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
      discountper: 0,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
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
      total_subcart_qty: total_subcart_qty,
    };
  });

  return customizedProductData;
};

const getDealProduct = async (appDetatils) => {
  await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');
  const { store_id, byname, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage } = appDetatils;
  const pageFilter = page; // You can adjust the page number dynamically
  const perPage = perpage;
  const currentDate = new Date();
  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }
  let categoryarray;
  if (cat_id) {
    categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
  }

  const deal_pssss = knex('deal_product')
    .join('store_products', 'deal_product.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'deal_product.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .select([
      'store_products.stock',
      'product_varient.varient_id',
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'product_varient.description',
      'deal_product.deal_price as price',
      'store_products.mrp',
      'product_varient.unit',
      'product_varient.quantity',
      'product.type',
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
      knex.raw('100-((deal_product.deal_price*100)/store_products.mrp) as discountper1'),
      'deal_product.valid_to',
      'deal_product.valid_from',
      'tbl_country.country_icon',
      'product.percentage',
      'product.availability'
    ])
    .groupBy([
      'store_products.store_id',
      'product_varient.varient_id',
      'product.product_id',
    ])
    .where('deal_product.valid_from', '<=', currentDate)
    .where('deal_product.valid_to', '>', currentDate)
    .whereNotNull('store_products.price')
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('deal_product.store_id', store_id);

  if (cat_id) {
    deal_pssss.whereIn('product.cat_id', categoryarray);
  }

  if (sub_cat_id) {
    deal_pssss.where('product.cat_id', sub_cat_id);
  }

  if ((min_price === 0 || min_price) && max_price) {
    deal_pssss.whereBetween('store_products.price', [min_price, max_price]);
  }

  if (min_discount && max_discount) {
    deal_pssss.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [
      min_discount,
      max_discount,
      min_discount,
      max_discount,
    ]);
  }

  if (sortprice === 'ltoh') {
    deal_pssss.orderBy('store_products.price', 'asc');
  }

  if (sortprice === 'htol') {
    deal_pssss.orderBy('store_products.price', 'desc');
  }

  if (sortname === 'atoz') {
    deal_pssss.orderBy('product.product_name', 'asc');
  }

  if (sortname === 'ztoa') {
    deal_pssss.orderBy('product.product_name', 'desc');
  }

  // Execute the query and get the results
  const results = await deal_pssss;

  let productDetails;

  // Check if results exist
  if (results.length > 0) {
    productDetails = await deal_pssss.offset((pageFilter - 1) * perPage).limit(perPage);
  } else {
    productDetails = results;
  }

  const productDetail = productDetails.filter((product, index, self) => {
    return index === self.findIndex((p) => p.product_id === product.product_id);
  });


  const customizedProductData = [];
  for (let i = 0; i < productDetail.length; i++) {
    var isFavourite = '';
    var notifyMe = '';
    var cartQty = 0;
    const ProductList = productDetail[i];

    const deal = await knex('deal_product')
      .where('varient_id', ProductList.varient_id)
      .where('store_id', store_id)
      .where('deal_product.valid_from', '<=', currentDate)
      .where('deal_product.valid_to', '>', currentDate)
      .first();

    let price = 0;
    if (deal) {
      price = deal.deal_price;
    } else {
      const sp = await knex('store_products')
        .where('varient_id', ProductList.varient_id)
        .where('store_id', store_id)
        .first();
      price = sp.price;
    }


    if (user_id) {
      // Wishlist check 
      const wishList = await knex('wishlist')
        .select('*')
        .where('varient_id', ProductList.varient_id)
        .where('user_id', user_id);

      isFavourite = wishList.length > 0 ? 'true' : 'false';

      // cart qty check 
      const CartQtyList = await knex('store_orders')
        .where('varient_id', ProductList.varient_id)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where('store_id', store_id)
        .whereNull('subscription_flag')
        .first();
      cartQty = CartQtyList ? CartQtyList.qty : 0;


      const cnotify_me = await knex('product_notify_me')
        .where('varient_id', ProductList.varient_id)
        .where('user_id', user_id);
      notifyMe = cnotify_me.length > 0 ? 'true' : 'false';

      const subprod = await knex('store_orders')
        .select('store_orders.percentage')
        .where('store_orders.varient_id', ProductList.varient_id)
        .where('store_approval', user_id)
        .where('store_orders.subscription_flag', 1)
        .where('store_orders.order_cart_id', "incart")
        .first();

      if (subprod) {
        isSubscription = 'true'
      } else {
        isSubscription = 'false'

      }

    } else {
      notifyMe = 'false';
      isFavourite = 'false';
      cartQty = 0;
      isSubscription = 'false';
    }

    const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    if (ProductList.country_icon == null) {
      countryicon = null
    } else {
      countryicon = baseurl + ProductList.country_icon
    }

    const customizedProduct = {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: ProductList.product_image,
      thumbnail: ProductList.thumbnail,
      description: ProductList.description,
      price: price,
      mrp: ProductList.mrp,
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      //discountper: ProductList.discountper,
      discountper: 0,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      countrating: 0,
      country_icon: countryicon,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      varients: null
      // Add or modify properties as needed
    };

    customizedProductData.push(customizedProduct);

  }

  return customizedProductData;
};

const getAdditionalCatSearch = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
  const { store_id, byname, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage, keyword } = appDetatils;
  const storeId = store_id;
  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }
  const pageFilter = page; // You can adjust the page number dynamically
  const perPage = perpage;
  //Varient Product variables
  var visFavourite = '';
  var vnotifyMe = '';
  var vcartQty = 0;
  var vavgrating = 0;
  var vcountrating = 0
  var vdiscountper = 0;
  var bynames = (byname.toLowerCase() == 'fresh food') ? "Fresh Picks" : byname;
  var bynames = (byname.toLowerCase() != 'fresh food' && byname.toLowerCase() == '') ? "DIWALI" : bynames;

  //return byname
  const additionalcat = knex('additional_category');
  //if (cat_id) {
  if (cat_id !== "null") {
    additionalcat.where('id', cat_id);
  }
  // if (keyword) {
  // additionalcat.where('title', keyword);
  // }


  if (bynames) {
    // additionalcat.where('title', byname);
    additionalcat.where('title', 'like', `%${bynames}%`)
    //additionalcat.whereLike('title', byname);
  }

  const result = await additionalcat.select('*').where('status', 1).orderBy('id'); // Replace 'id' with the actual column you want to order by

  const results = result;

  // OPTIMIZATION: Collect ALL product IDs from ALL categories first
  const categoryProductMap = {}; // Maps category_id -> array of product_ids
  const allProductIdsSet = new Set();

  results.forEach(item => {
    const product_ids = item.product_id.split(',').map(id => parseInt(id.trim())).filter(id => id);
    categoryProductMap[item.id] = {
      category: item,
      product_ids: product_ids
    };
    product_ids.forEach(id => allProductIdsSet.add(id));
  });

  const allProductIds = Array.from(allProductIdsSet);

  // If no products, return empty structure
  if (allProductIds.length === 0) {
    return results.map(item => ({
      id: item.id,
      title: item.title,
      sub_title: item.sub_title,
      color1: item.color1,
      color2: item.color2,
      product_details: []
    }));
  }

  // OPTIMIZATION: Fetch ALL products in ONE query (with category ordering preserved)
  // Build a query that includes category ordering info
  const productDetail_s = knex('product')
    .select(
      'store_products.stock',
      'product_varient.varient_image',
      'product_varient.quantity',
      'product_varient.unit',
      'store_products.price',
      'store_products.mrp',
      'product_varient.description',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'product_varient.varient_id',
      'product.product_id',
      'product.type',
      'tbl_country.country_icon',
      'product.percentage',
      'product.availability',
      'product.fcat_id',
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
      'product.is_customized',
    )
    .from('product')
    .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
    .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .leftJoin('tbl_country', function () {
      this.on(knex.raw('tbl_country.id::text'), '=', knex.raw('product.country_id'));
    })
    .leftJoin('add_catproduct_order', 'add_catproduct_order.product_id', '=', 'product.product_id')
    .whereIn('product.product_id', allProductIds)
    .andWhere('product.hide', '=', 0)
    .where('product.is_delete', 0)
    .where('product.is_zap', true)
    .where('store_products.stock', '>', 0)
    .where('store_products.store_id', store_id)
    .where(builder => {
      builder
        .where('product.is_offer_product', 0)
        .whereNull('product.offer_date')
        .orWhereRaw("product.offer_date::date != CURRENT_DATE")
    })
    //.orderBy('product.product_name', 'asc')
    .orderBy('add_catproduct_order.orders', 'asc');

  // OPTIMIZATION: Fetch ALL products without pagination first (we'll paginate per category later)
  const productDetails = await productDetail_s;

  // Group products by product_id (get one variant per product for main product data)
  const productMap = {}; // Maps product_id -> product data
  productDetails.forEach(product => {
    if (!productMap[product.product_id]) {
      productMap[product.product_id] = product;
    }
  });

  const productDetail = Object.values(productMap);

  // Pre-fetch all data in batches to avoid N+1 queries - RUN IN PARALLEL for speed
  const productIds = productDetail.map(p => p.product_id);
  const varientIds = productDetails.map(p => p.varient_id); // All variants from all products
  const currentDate = new Date();
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Prepare all batch queries
  let dealsPromise = Promise.resolve([]);
  if (varientIds.length > 0) {
    dealsPromise = knex('deal_product')
      .whereIn('varient_id', varientIds)
      .where('store_id', store_id)
      .where('deal_product.valid_from', '<=', currentDate)
      .where('deal_product.valid_to', '>', currentDate)
      .select('varient_id', 'deal_price');
  }

  let storeProductsPromise = Promise.resolve([]);
  if (varientIds.length > 0) {
    storeProductsPromise = knex('store_products')
      .whereIn('varient_id', varientIds)
      .where('store_id', store_id)
      .select('varient_id', 'price');
  }

  // Batch fetch user-specific data if user_id exists
  let wishlistsPromise = Promise.resolve([]);
  let cartItemsPromise = Promise.resolve([]);
  let subCartItemsPromise = Promise.resolve([]);
  let subscriptionsPromise = Promise.resolve([]);
  let notifyMePromise = Promise.resolve([]);

  if (user_id && varientIds.length > 0) {
    wishlistsPromise = knex('wishlist')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .select('varient_id');

    cartItemsPromise = knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('store_id', store_id)
      .whereNull('subscription_flag')
      .select('varient_id', 'qty', 'product_feature_id');

    subCartItemsPromise = knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('subscription_flag', 1)
      .where('store_id', store_id)
      .select('varient_id', 'qty');

    subscriptionsPromise = knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1)
      .where('order_cart_id', 'incart')
      .select('varient_id');

    notifyMePromise = knex('product_notify_me')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .select('varient_id');
  }

  // Batch fetch features for all products
  let featuresPromise = Promise.resolve([]);
  if (productIds.length > 0) {
    featuresPromise = knex('product_features')
      .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
      .whereIn('product_id', productIds);
  }

  // Batch fetch feature categories
  const fcatIds = new Set();
  productDetail.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const parsedId = parseInt(id.trim());
        if (!isNaN(parsedId)) fcatIds.add(parsedId);
      });
    }
  });
  let featureCatsPromise = Promise.resolve([]);
  if (fcatIds.size > 0) {
    featureCatsPromise = knex('feature_categories')
      .whereIn('id', Array.from(fcatIds))
      .where('status', 1)
      .where('is_deleted', 0)
      .select('id', knex.raw(`('${baseurl}' || COALESCE(image, '')) as image`));
  }

  // Batch fetch all variants for all products
  let variantsPromise = Promise.resolve([]);
  if (productIds.length > 0) {
    variantsPromise = knex('store_products')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id',
        'product_varient.description', 'store_products.price', 'store_products.mrp',
        'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity',
        'product_varient.product_id',
        knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
      .where('store_products.store_id', store_id)
      .whereIn('product_varient.product_id', productIds)
      .whereNotNull('store_products.price')
      .where('product_varient.approved', 1)
      .where('product_varient.is_delete', 0);
  }

  // Batch fetch product images
  let imagesPromise = Promise.resolve([]);
  if (productIds.length > 0) {
    imagesPromise = knex('product_images')
      .select('product_id', knex.raw(`('${baseurl}' || COALESCE(image, '')) as image`), 'type')
      .whereIn('product_id', productIds)
      .orderBy('type', 'DESC');
  }

  // Execute ALL batch queries in parallel
  const [
    deals,
    storeProducts,
    wishlists,
    cartItems,
    subCartItems,
    subscriptions,
    notifyMeItems,
    allFeatures,
    allFeatureCats,
    allVariants,
    allImages
  ] = await Promise.all([
    dealsPromise,
    storeProductsPromise,
    wishlistsPromise,
    cartItemsPromise,
    subCartItemsPromise,
    subscriptionsPromise,
    notifyMePromise,
    featuresPromise,
    featureCatsPromise,
    variantsPromise,
    imagesPromise
  ]);

  // Build maps from results
  const dealsMap = {};
  deals.forEach(deal => dealsMap[deal.varient_id] = deal.deal_price);

  const storeProductsMap = {};
  storeProducts.forEach(sp => storeProductsMap[sp.varient_id] = sp.price);

  const wishlistMap = {};
  wishlists.forEach(w => wishlistMap[w.varient_id] = true);

  const cartQtyMap = {};
  const cartFeatureMap = {};
  cartItems.forEach(c => {
    cartQtyMap[c.varient_id] = c.qty;
    if (c.product_feature_id) cartFeatureMap[c.varient_id] = c.product_feature_id;
  });

  const subcartQtyMap = {};
  subCartItems.forEach(sc => subcartQtyMap[sc.varient_id] = sc.qty);

  const subscriptionMap = {};
  subscriptions.forEach(s => subscriptionMap[s.varient_id] = true);

  const notifyMeMap = {};
  notifyMeItems.forEach(n => notifyMeMap[n.varient_id] = true);

  const featuresMap = {};
  allFeatures.forEach(f => {
    if (!featuresMap[f.product_id]) featuresMap[f.product_id] = [];
    featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
  });

  const featureCategoriesMap = {};
  allFeatureCats.forEach(fc => featureCategoriesMap[fc.id] = fc);

  const variantsMap = {};
  allVariants.forEach(v => {
    if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
    variantsMap[v.product_id].push(v);
  });

  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  });

  // Fallback to product_image if no product_images - only fetch if needed
  const productsWithoutImages = productIds.filter(id => !imagesMap[id] || imagesMap[id].length === 0);
  if (productsWithoutImages.length > 0) {
    const fallbackImages = await knex('product')
      .select('product_id', knex.raw(`('${baseurl}' || COALESCE(product_image, '')) as image`))
      .whereIn('product_id', productsWithoutImages);
    fallbackImages.forEach(img => {
      if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
      imagesMap[img.product_id].push(img);
    });
  }

  const customizedProductData = [];
  for (let i = 0; i < productDetail.length; i++) {
    // Use pre-fetched data instead of queries
    let isFavourite = 'false';
    let notifyMe = 'false';
    let cartQty = 0;
    let isSubscription = 'false';

    const ProductList = productDetail[i];

    // Get price from deals or store products (already fetched)
    const price = dealsMap[ProductList.varient_id] || storeProductsMap[ProductList.varient_id] || 0;

    // Use pre-fetched user data (already batch-fetched above)
    if (user_id) {
      isFavourite = wishlistMap[ProductList.varient_id] ? 'true' : 'false';
      cartQty = cartQtyMap[ProductList.varient_id] || 0;
      notifyMe = notifyMeMap[ProductList.varient_id] ? 'true' : 'false';
      isSubscription = subscriptionMap[ProductList.varient_id] ? 'true' : 'false';
    } else {
      notifyMe = 'false';
      isFavourite = 'false';
      cartQty = 0;
      isSubscription = 'false';
    }

    const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    // Use pre-fetched country icon
    let countryicon = null;
    if (ProductList.country_icon != null) {
      countryicon = baseurl + ProductList.country_icon;
    }

    let priceval;
    if (Number.isInteger(price)) {
      priceval = price + '.001';
    } else {
      priceval = price;
    }

    let mrpval;
    if (Number.isInteger(ProductList.mrp)) {
      mrpval = ProductList.mrp + '.001';
    } else {
      mrpval = ProductList.mrp;
    }

    // Use pre-fetched feature categories
    let feature_tags = [];
    if (ProductList.fcat_id != null) {
      const resultArray = ProductList.fcat_id.split(',').map(Number);
      feature_tags = resultArray.map(id => featureCategoriesMap[id]).filter(Boolean);
    }

    // Use pre-fetched features
    const features = featuresMap[ProductList.product_id] || [];

    // Use pre-fetched variants
    const app = variantsMap[ProductList.product_id] || [];

    // Use pre-fetched data for variants
    const customizedVarientData = [];
    let total_cart_qty = 0;
    let total_subcart_qty = 0;

    for (let j = 0; j < app.length; j++) {
      const ProductList1 = app[j];
      // Use pre-fetched price
      const vprice = dealsMap[ProductList1.varient_id] || storeProductsMap[ProductList1.varient_id] || ProductList1.price;

      // Use pre-fetched user data
      let isFavourite1 = 'false';
      let notifyMe1 = 'false';
      let cartQty1 = 0;
      let subcartQty1 = 0;
      let productFeatureId = 0;

      if (user_id) {
        isFavourite1 = wishlistMap[ProductList1.varient_id] ? 'true' : 'false';
        cartQty1 = cartQtyMap[ProductList1.varient_id] || 0;
        subcartQty1 = subcartQtyMap[ProductList1.varient_id] || 0;
        notifyMe1 = notifyMeMap[ProductList1.varient_id] ? 'true' : 'false';
        productFeatureId = cartFeatureMap[ProductList1.varient_id] || 0;

        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty1;
      }

      // Use pre-fetched images
      const images = imagesMap[ProductList.product_id] || [];

      const customizedVarient = {
        stock: ProductList1.stock,
        varient_id: ProductList1.varient_id,
        product_id: ProductList.product_id,
        product_name: ProductList.product_name,
        product_image: images.length > 0 ? images[0].image + "?width=200&height=200&quality=100" : '',
        thumbnail: images.length > 0 ? images[0].image : '',
        description: ProductList1.description,
        price: vprice,
        mrp: ProductList1.mrp,
        unit: ProductList1.unit,
        quantity: ProductList1.quantity,
        type: ProductList.type,
        discountper: ProductList.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        subcartQty: subcartQty1,
        product_feature_id: productFeatureId,
        country_icon: ProductList.country_icon ? baseurl + ProductList.country_icon : null,
      };

      customizedVarientData.push(customizedVarient);
    }
    const varients = customizedVarientData;

    const customizedProduct = {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image,
      thumbnail: baseurl + ProductList.thumbnail,
      description: ProductList.description,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      // discountper: ProductList.discountper,
      discountper: 0,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      countrating: 0,
      country_icon: countryicon,
      feature_tags: feature_tags,
      features: features,
      varients: varients,
      is_customized: ProductList.is_customized,
      totalPages: 0, // Will be set per category during pagination
      total_subcart_qty
      // Add or modify properties as needed
    };

    customizedProductData.push(customizedProduct);
  }

  // OPTIMIZATION: Group products by category and build response
  const customizedData = [];
  results.forEach(item => {
    const categoryInfo = categoryProductMap[item.id];
    if (!categoryInfo) {
      return;
    }

    // Filter products for this category (order is preserved from query)
    const categoryProducts = customizedProductData.filter(p =>
      categoryInfo.product_ids.includes(p.product_id)
    );

    // Apply pagination per category (preserving order from add_catproduct_order.orders)
    const categoryTotalPages = Math.ceil(categoryProducts.length / perPage);
    const paginatedProducts = categoryProducts.slice(
      (pageFilter - 1) * perPage,
      pageFilter * perPage
    );

    // Update totalPages in each product for this category
    paginatedProducts.forEach(p => {
      p.totalPages = categoryTotalPages;
    });

    // Flatten: Add products directly to the response array
    customizedData.push(...paginatedProducts);
  });

  return customizedData;

};

const appinformation = async (appDetails) => {
  const store_id = appDetails.store_id;

  // Sanitize user_id: treat " ", "null", or undefined as null
  let user_id = appDetails.user_id;
  if (!user_id || user_id === "null" || (typeof user_id === 'string' && user_id.trim() === "")) {
    user_id = null;
  }

  // Fallback to device_id for fetching data, but we won't update the 'users' table if user_id is null
  const effective_fetch_id = user_id || appDetails.device_id;
  const date_ob = new Date();

  // Determine if we have a valid numeric ID for tables like 'users' and 'orders' which use bigint
  const updateUserId = (user_id && !isNaN(user_id)) ? parseInt(user_id) : null;
  const effective_orders_id = (effective_fetch_id && !isNaN(effective_fetch_id)) ? parseInt(effective_fetch_id) : null;

  return await knex.transaction(async (trx) => {
    const [, userDetails, reserveAmt, appVersioning, deliveryFlag, cityData] = await Promise.all([
      // Only update if we have a valid numeric user_id
      updateUserId
        ? trx('users').where('id', updateUserId).update({ device_id: appDetails.device_id })
        : Promise.resolve(null),

      updateUserId
        ? trx('users')
          .select('wallet', 'activate_deactivate_status', 'referral_code', 'referral_balance', 'wallet_balance')
          .where('id', updateUserId)
          .first()
        : Promise.resolve(null),

      effective_orders_id
        ? trx('orders')
          .where('orders.is_subscription', 1)
          .where('orders.user_id', effective_orders_id)
          .select(trx.raw('COALESCE(SUM(CAST(orders.reserve_amount AS numeric)), 0) as "totalReserveAmt"'))
          .first()
        : Promise.resolve({ totalReserveAmt: 0 }),
      trx('app_versioning')
        .where('platform', appDetails.platform)
        .where('app_name', appDetails.app_name)
        .first(),
      trx('app_settings')
        .where('store_id', 7)
        .select('cod_charges', 'wallet_deduction_percentage')
        .first(),
      trx('city')
        .where('status', 1)
        .select(
          trx.raw('STRING_AGG(city_name, \',\') as "cityName"'),
          trx.raw('STRING_AGG(arabic_name, \',\') as "cityNameA"')
        )
        .first()
    ]);

    // Handle cases where no user is found
  const wallet = userDetails ? userDetails.wallet : 0;
  const userStatus = userDetails ? userDetails.activate_deactivate_status : null;
  const referralCode = userDetails ? userDetails.referral_code : null;

  if (userDetails) {
    //   userlist.referral_message = 'Hi! Use my reference code '+userlist.referral_code +' to signup in QuicKart app. Tap link to download app -';    
    userDetails.referral_message = 'Free delivery, no minimum spend, and super fresh products - all on the QuicKart app! \n Use my referral code ' + userDetails.referral_code + ' to get an exclusive 10% off your order. \n ✨ Sign up now and shop the freshness!';
  }

  // Process wallet balance
  //const totalWallet = (wallet - (reserveAmt.totalReserveAmt || 0)).toFixed(2);
  const walletBal = userDetails ? userDetails.wallet_balance : 0;
  const walletRef = userDetails ? userDetails.referral_balance : 0;
  const totalWallet = ((walletBal + walletRef)).toFixed(2);

  // Determine calendar time value
  const currentTime = `${String(uaeTime.hours()).padStart(2, '0')}:${String(uaeTime.minutes()).padStart(2, '0')}`;
  const calendarTValue = currentTime < "12:00" ? 1 : 1;


  // Check app version and status
  const appDbVersion = appDetails.version;
  const appCurVersion = appDetails.app_cur_version;
  let appVersionStatus = 100;
  let appVersionMessage = "Already updated version";

  if (appDbVersion > appCurVersion) {
    appVersionStatus = (appDbVersion - 1 === appCurVersion && appVersioning.forcefully_update === 1) ? 300 : 200;
    appVersionMessage = "New update available.\nPlease download the updated app.";
  }

  app_link = null
  version = null
  platform = null
  forcefully_update = null

  if (appVersioning) {
    app_link = appVersioning.app_link
    version = appVersioning.version
    platform = appVersioning.platform
    forcefully_update = appVersioning.forcefully_update
  }
  // Handle null safety for deliveryFlag and cityData
  const walletDeductionPercentage = deliveryFlag ? deliveryFlag.wallet_deduction_percentage : null;
  const codCharges = deliveryFlag ? deliveryFlag.cod_charges : null;
  const cityNameEnglish = cityData ? (cityData.cityName || '') : '';
  const cityNameArabic = cityData ? (cityData.cityNameA || '') : '';
  const countryList = [cityNameEnglish, cityNameArabic].filter(Boolean).join(',');

  const customizedProduct = {
    status: 1,
    message: "App Name & Logo",
    wallet_deduction_percentage: walletDeductionPercentage,
    userwallet: totalWallet,
    calendar_t_value: calendarTValue,
    app_link: app_link,
    version: version,
    platform: platform,
    forcefully_update: forcefully_update,
    app_version_status: appVersionStatus,
    app_version_message: appVersionMessage,
    popupdata_home: "",
    whatsapp_link: "97142390322",
    store_id: 7,
    userstatus: userStatus,
    share_link: "https://www.quickart.ae/share_app.php",
    referral_code: referralCode,
    referral_message: (userDetails) ? userDetails.referral_message : '',
    codcharges: codCharges,
    country_list: countryList
  };

    return customizedProduct;
  });
};

const UpdateproductDetails = async (appDetails) => {
  const { user_id } = appDetails;
  const currentDate1 = new Date().toISOString().split('T')[0];

  // Fetch Daily and Subscription products in parallel
  const [DailyProduct, SubscriptionProduct] = await Promise.all([
    knex('store_orders')
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .whereNull('subscription_flag'),
    knex('store_orders')
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .where('subscription_flag', 1)
  ]);

  const dailyVarientIds = [...new Set(DailyProduct.map(p => p.varient_id))];
  const subVarientIds = [...new Set(SubscriptionProduct.map(p => p.varient_id))];

  // Batch fetch all supporting data in parallel
  const [
    storeProductsDaily,
    offerProducts,
    storeProductsSub,
    productVarients,
    productsData,
    categoriesData
  ] = await Promise.all([
    dailyVarientIds.length > 0
      ? knex('store_products').whereIn('varient_id', dailyVarientIds)
      : [],
    dailyVarientIds.length > 0
      ? knex('product')
        .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
        .whereIn('product_varient.varient_id', dailyVarientIds)
        .where('product.offer_date', currentDate1)
        .select('product_varient.varient_id', 'product.offer_price as price')
      : [],
    subVarientIds.length > 0
      ? knex('store_products').whereIn('varient_id', subVarientIds)
      : [],
    subVarientIds.length > 0
      ? knex('product_varient').whereIn('varient_id', subVarientIds)
      : [],
    subVarientIds.length > 0
      ? knex('product')
        .whereIn('product_id', knex('product_varient').whereIn('varient_id', subVarientIds).select('product_id'))
      : [],
    knex('categories') // Fetch all categories once for lookup
  ]);

  const storeProductsDailyMap = Object.fromEntries((storeProductsDaily || []).map(sp => [sp.varient_id, sp]));
  const offerProductsMap = Object.fromEntries((offerProducts || []).map(op => [op.varient_id, op]));
  const storeProductsSubMap = Object.fromEntries((storeProductsSub || []).map(sp => [sp.varient_id, sp]));
  const productVarientsMap = Object.fromEntries((productVarients || []).map(pv => [pv.varient_id, pv]));
  const productsMap = Object.fromEntries((productsData || []).map(p => [p.product_id, p]));
  const categoriesMap = Object.fromEntries((categoriesData || []).map(c => [c.cat_id, c]));

  // Compute and batch update daily products
  const dailyUpdates = DailyProduct.map(product => {
    const ProductStore = storeProductsDailyMap[product.varient_id];
    if (!ProductStore) return null;
    const offer_product = offerProductsMap[product.varient_id];
    let price = ProductStore.price;
    let total_mrp = ProductStore.mrp;
    if (offer_product != null) {
      price = offer_product.price;
      total_mrp = offer_product.price;
    }
    const totalPrice = price * product.qty;
    const totalMrp = total_mrp * product.qty;
    return { store_order_id: product.store_order_id, totalPrice, totalMrp };
  }).filter(Boolean);

  // Compute and batch update subscription products
  const subUpdates = SubscriptionProduct.map(products => {
    const ProductStore = storeProductsSubMap[products.varient_id];
    if (!ProductStore) return null;
    const mrppriceTotal = ProductStore.mrp * products.qty;
    const priceTotal = ProductStore.price * products.qty;
    const repeatOrders = products.repeat_orders;
    const subTotalDelivery = products.sub_total_delivery || 1;
    const repeatOrdersDays = repeatOrders ? wordCount(repeatOrders) : 1;

    let cat_id = '';
    let percentage = '';
    const productVarient = productVarientsMap[products.varient_id];
    if (productVarient) {
      const productDetails = productsMap[productVarient.product_id];
      if (productDetails) {
        cat_id = productDetails.cat_id || '';
        percentage = productDetails.percentage || 0;
      }
    }

    let percentageSubCat = 0;
    let percentageCat = 0;
    const categoriesSubDetails = cat_id ? categoriesMap[cat_id] : null;
    if (categoriesSubDetails) {
      percentageSubCat = categoriesSubDetails.discount_per ?? 0;
      const parent = categoriesSubDetails.parent;
      if (parent) {
        const categoriesParentDetails = categoriesMap[parent];
        if (categoriesParentDetails) {
          percentageCat = categoriesParentDetails.discount_per ?? 0;
        }
      }
    }

    let priceAfterDiscount;
    if (percentage > 0) {
      priceAfterDiscount = ((mrppriceTotal - (mrppriceTotal * percentage) / 100) * repeatOrdersDays * subTotalDelivery).toFixed(2);
    } else if (percentageSubCat > 0) {
      priceAfterDiscount = ((mrppriceTotal - (mrppriceTotal * percentageSubCat) / 100) * repeatOrdersDays * subTotalDelivery).toFixed(2);
    } else if (percentageCat > 0) {
      priceAfterDiscount = ((mrppriceTotal - (mrppriceTotal * percentageCat) / 100) * repeatOrdersDays * subTotalDelivery).toFixed(2);
    } else {
      priceAfterDiscount = (priceTotal * repeatOrdersDays * subTotalDelivery).toFixed(2);
    }
    const finalmrppriceTotal = (mrppriceTotal * repeatOrdersDays * subTotalDelivery).toFixed(2);
    return { store_order_id: products.store_order_id, priceAfterDiscount, finalmrppriceTotal };
  }).filter(Boolean);

  // Execute all updates in parallel within a transaction
  await knex.transaction(async (trx) => {
    await Promise.all([
      ...dailyUpdates.map(u =>
        trx('store_orders')
          .where('store_order_id', u.store_order_id)
          .where('order_cart_id', 'incart')
          .whereNull('subscription_flag')
          .update({ price: u.totalPrice, total_mrp: u.totalMrp })
      ),
      ...subUpdates.map(u =>
        trx('store_orders')
          .where('store_order_id', u.store_order_id)
          .where('order_cart_id', 'incart')
          .where('subscription_flag', 1)
          .update({ price: u.priceAfterDiscount, total_mrp: u.finalmrppriceTotal })
      )
    ]);
  });

  // Daily Product Amount & Cart Count
  const sumDaily1 = await knex('store_orders')
    .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .where('store_orders.store_approval', user_id)
    .where('store_orders.order_cart_id', 'incart')
    .whereNull('subscription_flag')
    .select(
      knex.raw('SUM(store_orders.total_mrp) as mrp'),
      knex.raw('SUM(store_orders.price) as sum'),
      knex.raw('COUNT(store_orders.store_order_id) as count'),
      knex.raw('SUM(store_orders.tx_price) as sum_tax'),
      knex.raw('SUM(store_orders.tx_per) as sum_per')
    )
    .first();

  if (sumDaily1.sum != null && sumDaily1.sum < 30) {
    await knex('store_orders')
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('is_offer_product', 1)
      .whereNull('subscription_flag')
      .delete();
  }

  // Run all remaining independent queries in parallel
  const [sumDaily, cartItemsDaily, sumSubscription, cartItemsSubscription, wall, reserveAmt] = await Promise.all([
    knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .select(
        knex.raw('SUM(store_orders.total_mrp) as mrp'),
        knex.raw('SUM(store_orders.price) as sum'),
        knex.raw('COUNT(store_orders.store_order_id) as count'),
        knex.raw('SUM(store_orders.tx_price) as sum_tax'),
        knex.raw('SUM(store_orders.tx_per) as sum_per')
      )
      .first(),
    knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .select(
        'store_orders.percentage',
        'store_orders.product_name',
        'store_orders.varient_image',
        'store_orders.quantity',
        'store_orders.unit',
        'store_orders.total_mrp',
        'store_products.price',
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
        'store_orders.tx_name',
        'product.product_image',
        'product_varient.description',
        'product.type',
        'store_orders.price as ord_price'
      )
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .where('store_products.store_id', 7)
      .whereNull('subscription_flag')
      .orderBy('store_orders.store_order_id', 'ASC'),
    knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .where('subscription_flag', 1)
      .select(
        knex.raw('SUM(store_orders.total_mrp) as mrp'),
        knex.raw('SUM(store_orders.price) as sum'),
        knex.raw('COUNT(store_orders.store_order_id) as count'),
        knex.raw('SUM(store_orders.tx_price) as sum_tax'),
        knex.raw('SUM(store_orders.tx_per) as sum_per')
      )
      .first(),
    knex('store_orders')
      .join('store_products', 'store_orders.varient_id', '=', 'store_products.varient_id')
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .join('product', 'product_varient.product_id', '=', 'product.product_id')
      .select(
        'store_orders.percentage',
        'store_orders.product_name',
        'store_orders.varient_image',
        'store_orders.quantity',
        'store_orders.unit',
        'store_orders.total_mrp',
        'store_products.price',
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
        'store_orders.tx_name',
        'product.product_image',
        'product_varient.description',
        'product.type',
        'store_orders.price as ord_price'
      )
      .where('store_orders.store_approval', user_id)
      .where('store_orders.order_cart_id', 'incart')
      .where('subscription_flag', 1)
      .where('store_products.store_id', 7)
      .orderBy('store_orders.store_order_id', 'ASC'),
    knex('users')
      .select('wallet', 'activate_deactivate_status', 'referral_code')
      .where('id', user_id)
      .first(),
    knex('orders')
      .innerJoin('subscription_order', 'subscription_order.cart_id', '=', 'orders.cart_id')
      .select('orders.reserve_amount')
      .where('orders.is_subscription', 1)
      .where('orders.user_id', user_id)
      .groupBy('orders.order_id')
  ]);

  let dailysum1 = 0;
  let dailymrppp = 0;
  let dailycartCount = 0;
  let dailydiscountOnMrp = 0;

  if (cartItemsDaily.length > 0) {
    dailysum1 = sumDaily.sum != null ? sumDaily.sum.toFixed(2) : '0.00';
    dailymrppp = sumDaily.mrp != null ? sumDaily.mrp.toFixed(2) : '0.00';
    dailydiscountOnMrp = (dailymrppp - dailysum1).toFixed(2);
    dailycartCount = cartItemsDaily.length;
  }

  // sumSubscription, cartItemsSubscription, wall, reserveAmt from Promise.all above
  let subscriptionsum1 = 0;
  let subscriptionmrppp = 0;
  let subscriptioncartCount = 0;
  let subscriptiondiscountOnMrp = 0;

  if (cartItemsSubscription.length > 0) {
    subscriptionsum1 = sumSubscription.sum != null ? sumSubscription.sum.toFixed(2) : '0.00';
    subscriptionmrppp = sumSubscription.mrp != null ? sumSubscription.mrp.toFixed(2) : '0.00';
    subscriptiondiscountOnMrp = (subscriptionmrppp - subscriptionsum1).toFixed(2);
    subscriptioncartCount = cartItemsSubscription.length;
  }

  const wallet = wall ? wall.wallet : 0;
  const userStatus = wall ? wall.activate_deactivate_status : null;
  const referralCode = wall ? wall.referral_code : null;

  let totalReserveAmt = 0;
  reserveAmt.forEach(item => {
    totalReserveAmt += parseFloat(item.reserve_amount);
  });

  const totalWallet = (wallet - totalReserveAmt).toFixed(2);

  const customizedProduct = {
    dailycartCount: dailycartCount,
    dailytotalPrice: dailysum1,
    dailydiscountOnMrp: dailydiscountOnMrp,
    subscriptioncartCount: subscriptioncartCount,
    subscriptiontotalPrice: subscriptionsum1,
    subscriptiondiscountOnMrp: subscriptiondiscountOnMrp,
    userwallet: totalWallet
  };

  return customizedProduct;



};

const getOccasionalCatSearch = async (appDetatils) => {
  // PostgreSQL migration: Removed MySQL-specific sql_mode setting
  const { byname, store_id, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage, keyword } = appDetatils;
  const storeId = store_id;

  // Proper null handling for PostgreSQL
  let user_id = null;
  if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null) {
    user_id = appDetatils.user_id;
  } else if (appDetatils.device_id && appDetatils.device_id !== "null" && appDetatils.device_id !== null) {
    user_id = appDetatils.device_id;
  }

  const pageFilter = page || 1;
  const perPage = appDetatils.perpage || 200;
  const baseurl = process.env.BUNNY_NET_IMAGE || '';
  const bynames = byname ? byname.toLowerCase() : '';
  const currentDate = new Date().toISOString().split('T')[0];
  const currentDateTime = new Date();

  // Build occasional category query with PostgreSQL-compatible syntax
  const additionalcat = knex('occasional_category')
    .where('from_date', '<=', currentDate)
    .andWhere('to_date', '>=', currentDate);

  if (cat_id && cat_id !== "null" && cat_id !== null) {
    additionalcat.where('id', cat_id);
  }

  if (byname && bynames) {
    // PostgreSQL: Use ILIKE for case-insensitive search (better performance than LIKE with LOWER)
    additionalcat.where('title', 'ilike', `%${bynames}%`);
  }

  const results = await additionalcat.select('*').orderBy('id');
  const customizedData = [];

  // Process each occasional category
  for (let i = 0; i < results.length; i++) {
    const item = results[i];
    let categoryProductIds = [];

    // Determine product IDs based on category configuration
    if (item.product_id && item.subcat_id && item.cat_id) {
      const productIds = item.product_id.split(',');
      categoryProductIds = productIds.map(Number).filter(id => !isNaN(id));
    } else if ((!item.product_id || item.product_id === '' || item.product_id === null) && item.subcat_id && item.cat_id) {
      const subcategories = item.subcat_id.split(',').filter(c => c);
      if (subcategories.length > 0) {
        const productIds = await knex('product')
          .from('product')
          .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
          .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
          .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
          .whereIn('product.cat_id', subcategories)
          .andWhere('product.hide', '=', 0)
          .andWhere('product.is_delete', '=', 0)
          .andWhere('store_products.store_id', storeId)
          .pluck('product.product_id');
        categoryProductIds = productIds;
      }
    } else if ((!item.product_id || item.product_id === '' || item.product_id === null) &&
      (!item.subcat_id || item.subcat_id === '' || item.subcat_id === null) &&
      item.cat_id) {
      const parent_cat_id = item.cat_id.split(',').filter(c => c);
      if (parent_cat_id.length > 0) {
        const subcategories = await knex('categories')
          .whereIn('parent', parent_cat_id)
          .where('status', 1)
          .pluck('cat_id');

        if (subcategories.length > 0) {
          const productIds = await knex('product')
            .from('product')
            .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
            .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
            .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
            .whereIn('product.cat_id', subcategories)
            .andWhere('product.hide', '=', 0)
            .andWhere('product.is_delete', '=', 0)
            .andWhere('store_products.store_id', storeId)
            .pluck('product.product_id');
          categoryProductIds = productIds;
        }
      }
    }

    if (categoryProductIds.length === 0) {
      // Skip if no products found
      continue;
    }

    // Build main product query with PostgreSQL-compatible syntax
    const productDetailQuery = knex('product')
      .select(
        'store_products.stock',
        'product_varient.varient_image',
        'product_varient.quantity',
        'product_varient.unit',
        'store_products.price',
        'store_products.mrp',
        'product_varient.description',
        'product.product_name',
        'product.product_image',
        'product.thumbnail',
        'product_varient.varient_id',
        'product.product_id',
        'product.type',
        'tbl_country.country_icon',
        'product.percentage',
        'product.availability',
        'product.fcat_id',
        // PostgreSQL: Add NULLIF to prevent division by zero
        knex.raw("100-((store_products.price*100)/NULLIF(store_products.mrp, 0)) as discountper")
      )
      .from('product')
      .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
      .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
      .leftJoin('add_occproduct_order', 'add_occproduct_order.product_id', '=', 'product.product_id')
      .whereIn('product.product_id', categoryProductIds)
      .andWhere('product.hide', '=', 0)
      .where('product.is_delete', 0)
      .where('product.is_zap', true)
      .where('store_products.stock', '>', 0)
      .where('store_products.store_id', storeId)
      .where(builder => {
        builder
          .where('product.is_offer_product', 0)
          .whereNull('product.offer_date')
          // PostgreSQL: Replace CURDATE() with CURRENT_DATE and DATE() with ::date
          .orWhereRaw("product.offer_date::date != CURRENT_DATE");
      })
      .orderByRaw('add_occproduct_order.orders ASC NULLS LAST');

    // Get total count for pagination
    const totalproducts = await productDetailQuery.clone();
    const totalPages = Math.ceil(totalproducts.length / perPage);

    // Get paginated products
    const productDetails = await productDetailQuery
      .offset((pageFilter - 1) * perPage)
      .limit(perPage);

    // Remove duplicates by product_id
    const productDetail = productDetails.filter((product, index, self) => {
      return index === self.findIndex((p) => p.product_id === product.product_id);
    });

    if (productDetail.length === 0) {
      continue;
    }

    // Performance optimization: Batch fetch user-specific data for all products
    const varientIds = productDetail.map(p => p.varient_id);
    const productIds = [...new Set(productDetail.map(p => p.product_id))];

    // Batch queries for user data (if user_id exists)
    let wishlistMap = new Map();
    let cartQtyMap = new Map();
    let notifyMeMap = new Map();
    let subscriptionMap = new Map();
    let dealMap = new Map();

    if (user_id) {
      // Batch fetch wishlist
      const wishlistItems = await knex('wishlist')
        .select('varient_id')
        .whereIn('varient_id', varientIds)
        .where('user_id', user_id);
      wishlistItems.forEach(item => wishlistMap.set(item.varient_id, true));

      // Batch fetch cart quantities
      const cartItems = await knex('store_orders')
        .select('varient_id', 'qty')
        .whereIn('varient_id', varientIds)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where('store_id', storeId)
        .whereNull('subscription_flag');
      cartItems.forEach(item => cartQtyMap.set(item.varient_id, item.qty || 0));

      // Batch fetch notify me
      const notifyItems = await knex('product_notify_me')
        .select('varient_id')
        .whereIn('varient_id', varientIds)
        .where('user_id', user_id);
      notifyItems.forEach(item => notifyMeMap.set(item.varient_id, true));

      // Batch fetch subscription data
      const subItems = await knex('store_orders')
        .select('varient_id', 'percentage')
        .whereIn('varient_id', varientIds)
        .where('store_approval', user_id)
        .where('subscription_flag', 1)
        .where('order_cart_id', 'incart');
      subItems.forEach(item => subscriptionMap.set(item.varient_id, item.percentage));
    }

    // Batch fetch deals for all variants
    const deals = await knex('deal_product')
      .select('varient_id', 'deal_price')
      .whereIn('varient_id', varientIds)
      .where('store_id', storeId)
      .where('valid_from', '<=', currentDateTime)
      .where('valid_to', '>', currentDateTime);
    deals.forEach(deal => dealMap.set(deal.varient_id, deal.deal_price));

    // Batch fetch store products for all variants
    const storeProductsMap = new Map();
    const storeProducts = await knex('store_products')
      .select('varient_id', 'price', 'mrp')
      .whereIn('varient_id', varientIds)
      .where('store_id', storeId);
    storeProducts.forEach(sp => storeProductsMap.set(sp.varient_id, sp));

    // Batch fetch feature categories
    const fcatIds = productDetail
      .filter(p => p.fcat_id)
      .map(p => p.fcat_id.split(',').map(Number))
      .flat()
      .filter(id => !isNaN(id));

    let featureCategoriesMap = new Map();
    if (fcatIds.length > 0) {
      const featureCategories = await knex('feature_categories')
        .whereIn('id', fcatIds)
        .where('status', 1)
        .where('is_deleted', 0)
        .select('id', knex.raw(`CONCAT('${baseurl}', image) as image`));
      featureCategories.forEach(fc => featureCategoriesMap.set(fc.id, fc.image));
    }

    // Batch fetch product features
    const featuresMap = new Map();
    if (productIds.length > 0) {
      const features = await knex('product_features')
        .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
        .whereIn('product_features.product_id', productIds);
      features.forEach(f => {
        if (!featuresMap.has(f.product_id)) {
          featuresMap.set(f.product_id, []);
        }
        featuresMap.get(f.product_id).push({ id: f.id, feature_value: f.feature_value });
      });
    }

    // Batch fetch product images
    const imagesMap = new Map();
    if (productIds.length > 0) {
      const images = await knex('product_images')
        .select('product_id', knex.raw(`CONCAT('${baseurl}', image) as image`))
        .whereIn('product_id', productIds)
        .orderBy('type', 'DESC');
      images.forEach(img => {
        if (!imagesMap.has(img.product_id)) {
          imagesMap.set(img.product_id, []);
        }
        imagesMap.get(img.product_id).push(img.image);
      });
    }

    const customizedProductData = [];

    // Process each product
    for (let i = 0; i < productDetail.length; i++) {
      const ProductList = productDetail[i];

      // Get price from deal or store products
      let price = 0;
      if (dealMap.has(ProductList.varient_id)) {
        price = dealMap.get(ProductList.varient_id);
      } else if (storeProductsMap.has(ProductList.varient_id)) {
        price = storeProductsMap.get(ProductList.varient_id).price;
      }

      // User-specific data (from batch queries)
      let isFavourite = 'false';
      let notifyMe = 'false';
      let cartQty = 0;
      let isSubscription = 'false';
      let subscriptionPercentage = null;

      if (user_id) {
        isFavourite = wishlistMap.has(ProductList.varient_id) ? 'true' : 'false';
        notifyMe = notifyMeMap.has(ProductList.varient_id) ? 'true' : 'false';
        cartQty = cartQtyMap.get(ProductList.varient_id) || 0;
        if (subscriptionMap.has(ProductList.varient_id)) {
          isSubscription = 'true';
          subscriptionPercentage = subscriptionMap.get(ProductList.varient_id);
        }
      }

      // Calculate subscription price
      const sub_price = ProductList.percentage ? (ProductList.mrp * ProductList.percentage) / 100 : 0;
      const finalsubprice = ProductList.mrp - sub_price;
      const subscription_price = parseFloat(finalsubprice.toFixed(2));

      // Format price and MRP
      let priceval = Number.isInteger(price) ? price + '.001' : price;
      let mrpval = Number.isInteger(ProductList.mrp) ? ProductList.mrp + '.001' : ProductList.mrp;

      // Country icon
      const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

      // Feature tags
      let feature_tags = [];
      if (ProductList.fcat_id) {
        const fcatArray = ProductList.fcat_id.split(',').map(Number).filter(id => !isNaN(id));
        feature_tags = fcatArray.map(id => ({
          id: id,
          image: featureCategoriesMap.get(id) || null
        })).filter(ft => ft.image !== null);
      }

      // Features
      const features = featuresMap.get(ProductList.product_id) || [];

      // Get all variants for this product (batch query)
      const app = await knex('store_products')
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
          knex.raw("100-((store_products.price*100)/NULLIF(store_products.mrp, 0)) as discountper")
        )
        .where('store_products.store_id', storeId)
        .where('product_varient.product_id', ProductList.product_id)
        .whereNotNull('store_products.price')
        .where('product_varient.approved', 1)
        .where('product_varient.is_delete', 0);

      // Batch fetch variant-specific user data
      const variantIds = app.map(a => a.varient_id);
      let variantWishlistMap = new Map();
      let variantCartMap = new Map();
      let variantSubCartMap = new Map();
      let variantNotifyMap = new Map();
      let variantFeatureMap = new Map();

      if (user_id && variantIds.length > 0) {
        const variantWishlist = await knex('wishlist')
          .select('varient_id')
          .whereIn('varient_id', variantIds)
          .where('user_id', user_id);
        variantWishlist.forEach(item => variantWishlistMap.set(item.varient_id, true));

        const variantCart = await knex('store_orders')
          .select('varient_id', 'qty')
          .whereIn('varient_id', variantIds)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .whereNull('subscription_flag')
          .where('store_id', storeId);
        variantCart.forEach(item => variantCartMap.set(item.varient_id, item.qty || 0));

        const variantSubCart = await knex('store_orders')
          .select('varient_id', 'qty')
          .whereIn('varient_id', variantIds)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('subscription_flag', 1)
          .where('store_id', storeId);
        variantSubCart.forEach(item => variantSubCartMap.set(item.varient_id, item.qty || 0));

        const variantNotify = await knex('product_notify_me')
          .select('varient_id')
          .whereIn('varient_id', variantIds)
          .where('user_id', user_id);
        variantNotify.forEach(item => variantNotifyMap.set(item.varient_id, true));

        const variantFeatures = await knex('store_orders')
          .select('varient_id', 'product_feature_id')
          .whereIn('varient_id', variantIds)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart');
        variantFeatures.forEach(item => variantFeatureMap.set(item.varient_id, item.product_feature_id || 0));
      }

      const customizedVarientData = [];
      let total_cart_qty = 0;
      let total_subcart_qty = 0;

      for (let j = 0; j < app.length; j++) {
        const ProductList1 = app[j];
        const vprice = ProductList1.price;

        let isFavourite1 = 'false';
        let notifyMe1 = 'false';
        let cartQty1 = 0;
        let subcartQty1 = 0;
        let productFeatureId = 0;

        if (user_id) {
          isFavourite1 = variantWishlistMap.has(ProductList1.varient_id) ? 'true' : 'false';
          const isFavorite1 = isFavourite1;
          notifyMe1 = variantNotifyMap.has(ProductList1.varient_id) ? 'true' : 'false';
          cartQty1 = variantCartMap.get(ProductList1.varient_id) || 0;
          subcartQty1 = variantSubCartMap.get(ProductList1.varient_id) || 0;
          productFeatureId = variantFeatureMap.get(ProductList1.varient_id) || 0;
        }

        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty1;

        const productImages = imagesMap.get(ProductList.product_id) || [];
        const productImage = productImages.length > 0 ? productImages[0] : (ProductList.product_image ? baseurl + ProductList.product_image : '');

        const customizedVarient = {
          stock: ProductList1.stock,
          varient_id: ProductList1.varient_id,
          product_id: ProductList.product_id,
          product_name: ProductList.product_name,
          product_image: productImages.length > 0 ? productImage + "?width=200&height=200&quality=100" : '',
          thumbnail: productImages.length > 0 ? productImage : '',
          description: ProductList1.description,
          price: vprice,
          mrp: ProductList1.mrp,
          unit: ProductList1.unit,
          quantity: ProductList1.quantity,
          type: ProductList.type,
          discountper: ProductList.discountper || 0,
          notify_me: notifyMe1,
          isFavourite: isFavourite1,
          isFavorite: isFavourite1,
          cart_qty: cartQty1,
          total_cart_qty: cartQty1,
          subcartQty: subcartQty1,
          total_subcart_qty: subcartQty1,
          product_feature_id: productFeatureId,
          country_icon: countryicon,
        };

        customizedVarientData.push(customizedVarient);
      }

      const customizedProduct = {
        stock: ProductList.stock,
        varient_id: ProductList.varient_id,
        product_id: ProductList.product_id,
        product_name: ProductList.product_name,
        product_image: ProductList.product_image ? baseurl + ProductList.product_image : '',
        thumbnail: ProductList.thumbnail ? baseurl + ProductList.thumbnail : '',
        description: ProductList.description,
        price: parseFloat(priceval),
        mrp: parseFloat(mrpval),
        unit: ProductList.unit,
        quantity: ProductList.quantity,
        type: ProductList.type,
        percentage: ProductList.percentage || null,
        isSubscription: isSubscription,
        subscription_price: subscription_price,
        availability: ProductList.availability,
        discountper: 0,
        avgrating: 0,
        notify_me: notifyMe,
        isFavourite: isFavourite,
        isFavorite: isFavourite,
        cart_qty: cartQty,
        total_cart_qty: total_cart_qty,
        total_subcart_qty: total_subcart_qty,
        countrating: 0,
        country_icon: countryicon,
        feature_tags: feature_tags,
        features: features,
        varients: customizedVarientData,
        totalPages: totalPages
      };

      customizedProductData.push(customizedProduct);
    }

    const customizedItem = {
      id: item.id,
      title: item.title,
      sub_title: item.sub_title,
      color1: item.color1,
      color2: item.color2,
      product_details: customizedProductData
    };

    customizedData.push(customizedItem);
  }

  return customizedData;
};

module.exports = {
  getTopSelling,
  getWhatsNew,
  getRecentSelling,
  getDealProduct,
  getAdditionalCatSearch,
  appinformation,
  UpdateproductDetails,
  getOccasionalCatSearch,
  trailpackimagedata
};
