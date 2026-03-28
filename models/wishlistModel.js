const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library

const addtoWishlist = async (appDetatils) => {

  const varient_id = parseInt(appDetatils.varient_id);
  const store_id = parseInt(appDetatils.store_id);

  // Handle user_id - PostgreSQL schema has user_id as integer
  // For guest users with device_id, we'll need to handle this appropriately
  let user_id;
  if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null) {
    user_id = parseInt(appDetatils.user_id);
  } else {
    // For guest users, device_id should be handled differently
    // Since user_id is integer in schema, we'll use device_id as string identifier
    // Note: This assumes device_id can be converted or stored differently
    // If schema doesn't support this, consider making user_id nullable or adding device_id column
    user_id = appDetatils.device_id; // This may need schema adjustment for guest users
  }

  const currentDate = new Date();

  // Optimized single query with all joins
  // Use raw SQL for the deal_product join to handle date comparisons properly
  let p = await knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin(knex.raw('deal_product ON product_varient.varient_id = deal_product.varient_id AND deal_product.store_id = ? AND deal_product.valid_from <= ? AND deal_product.valid_to > ? AND deal_product.status = 1', [store_id, currentDate, currentDate]))
    .where('store_products.varient_id', varient_id)
    .where('store_products.store_id', store_id)
    .select(
      'product.product_name',
      'product.product_image',
      'product_varient.quantity',
      'product_varient.unit',
      'product_varient.description',
      'store_products.price',
      'store_products.mrp',
      'deal_product.deal_price'
    )
    .first();

  if (!p) {
    throw new Error('Product not found');
  }

  // Determine price - use deal_price if available, otherwise regular price
  const price = (p.deal_price != null) ? parseFloat(p.deal_price) : parseFloat(p.price);
  const mrpprice = parseFloat(p.mrp);

  // Check if already in wishlist - optimized query
  const check = await knex('wishlist')
    .where('varient_id', varient_id)
    .where('user_id', user_id)
    .where('store_id', store_id)
    .select('wish_id')
    .first();

  if (check) {
    // Remove from wishlist
    const del = await knex('wishlist')
      .where('varient_id', varient_id)
      .where('user_id', user_id)
      .where('store_id', store_id)
      .delete();

    return 'Removed from Wishlist';
  } else {
    // Insert to wishlist - convert numeric values to strings for text columns
    // Try to use sequence, fallback to max+1 if sequence doesn't exist
    let nextWishId;
    try {
      // Try to get next value from sequence (standard PostgreSQL sequence name)
      const seqResult = await knex.raw("SELECT nextval('wishlist_wish_id_seq') as next_id");
      nextWishId = seqResult.rows[0].next_id;
    } catch (err) {
      // Sequence doesn't exist, use max+1
      const maxWishId = await knex('wishlist')
        .max('wish_id as max_id')
        .first();
      nextWishId = (maxWishId && maxWishId.max_id) ? parseInt(maxWishId.max_id) + 1 : 1;
    }

    await knex('wishlist').insert({
      wish_id: nextWishId,
      store_id: store_id,
      varient_id: varient_id,
      product_name: p.product_name || '',
      varient_image: p.product_image || '',
      quantity: p.quantity ? p.quantity.toString() : null,
      unit: p.unit ? p.unit.toString() : null,
      mrp: mrpprice.toString(),
      description: p.description || null,
      user_id: user_id,
      created_at: currentDate,
      updated_at: currentDate,
      price: price.toString()
    });

    return 'Added to Wishlist';
  }
}


const getWishlist = async (appDetatils) => {
  const store_id = appDetatils.store_id != null ? parseInt(appDetatils.store_id, 10) : NaN;
  // PostgreSQL wishlist.user_id is integer only - no guest/device_id support
  let user_id = null;
  if (appDetatils.user_id != null && appDetatils.user_id !== '' && appDetatils.user_id !== 'null') {
    const parsed = parseInt(appDetatils.user_id, 10);
    if (Number.isInteger(parsed)) user_id = parsed;
  }
  if (!Number.isInteger(user_id) || Number.isNaN(store_id)) return [];

  const currentDate = new Date();
  const baseurl = process.env.BUNNY_NET_IMAGE || '';

  // Single main query - get all wishlist products with optimized joins (no extra price query for latency)
  let prodsssss = await knex('wishlist')
    .join('store_products', 'wishlist.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('CAST(tbl_country.id AS TEXT)'), '=', 'product.country_id')
    .leftJoin(knex.raw('deal_product ON product_varient.varient_id = deal_product.varient_id AND deal_product.store_id = ? AND deal_product.valid_from <= ? AND deal_product.valid_to > ? AND deal_product.status = 1', [store_id, currentDate, currentDate]))
    .select(
      'product.fcat_id',
      'product.is_customized',
      'product.percentage',
      'product.availability',
      'tbl_country.country_icon',
      'store_products.store_id',
      'store_products.stock',
      'store_products.varient_id',
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'product_varient.description',
      knex.raw('COALESCE(deal_product.deal_price, store_products.price) as price'),
      'store_products.mrp',
      'product_varient.varient_image',
      'product_varient.unit',
      'product_varient.quantity',
      'product.type',
      knex.raw('100-((COALESCE(deal_product.deal_price, store_products.price)*100)/store_products.mrp) as discountper')
    )
    .where('wishlist.user_id', user_id)
    .where('wishlist.store_id', store_id)
    .where('product.is_delete', 0)
    .where('store_products.stock', '>', 0)
    .where('product.approved', 1)
    .where(builder => {
      builder
        .where('product.is_offer_product', 0)
        .whereNull('product.offer_date')
        .orWhereRaw("product.offer_date::date != CURRENT_DATE");
    });

  if (prodsssss.length === 0) {
    return [];
  }

  // Extract all IDs for batch queries
  const varientIds = prodsssss.map(p => p.varient_id);
  const productIds = [...new Set(prodsssss.map(p => p.product_id))];
  const fcatIds = [...new Set(prodsssss.filter(p => p.fcat_id).map(p => p.fcat_id).join(',').split(',').map(Number).filter(id => id))];

  // Batch fetch all related data in parallel
  const [
    cartQuantities,
    subCartQuantities,
    wishlistItems,
    notifyMeItems,
    subscriptionItems,
    productImagesMap,
    featuresMap,
    featureTags
  ] = await Promise.all([
    // Cart quantities (store_approval is text in PostgreSQL)
    knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', String(user_id))
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', store_id)
      .select('varient_id', 'qty')
      .then(rows => {
        const map = {};
        rows.forEach(row => map[row.varient_id] = row.qty);
        return map;
      }),

    // Subscription cart quantities (subscription_flag is text in PostgreSQL)
    knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', String(user_id))
      .where('order_cart_id', 'incart')
      .where('subscription_flag', '1')
      .where('store_id', store_id)
      .select('varient_id', 'qty')
      .then(rows => {
        const map = {};
        rows.forEach(row => map[row.varient_id] = row.qty);
        return map;
      }),

    // Wishlist items (for isFavourite check)
    knex('wishlist')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .select('varient_id')
      .then(rows => new Set(rows.map(r => r.varient_id))),

    // Notify me items
    knex('product_notify_me')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .select('varient_id')
      .then(rows => new Set(rows.map(r => r.varient_id))),

    // Subscription items (store_approval and subscription_flag are text in PostgreSQL)
    knex('store_orders')
      .whereIn('varient_id', varientIds)
      .where('store_approval', String(user_id))
      .where('subscription_flag', '1')
      .where('order_cart_id', 'incart')
      .select('varient_id', 'percentage')
      .then(rows => {
        const map = {};
        rows.forEach(row => map[row.varient_id] = row.percentage);
        return map;
      }),

    // Product images - batch fetch all
    knex('product_images')
      .whereIn('product_id', productIds)
      .select('product_id', 'image', 'type')
      .orderBy('type', 'DESC')
      .then(rows => {
        const map = {};
        rows.forEach(row => {
          if (!map[row.product_id]) map[row.product_id] = [];
          map[row.product_id].push(row);
        });
        return map;
      }),

    // Product features - batch fetch all
    knex('product_features')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
      .whereIn('product_id', productIds)
      .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .then(rows => {
        const map = {};
        rows.forEach(row => {
          if (!map[row.product_id]) map[row.product_id] = [];
          map[row.product_id].push({ id: row.id, feature_value: row.feature_value });
        });
        return map;
      }),

    // Feature tags
    fcatIds.length > 0 ? knex('feature_categories')
      .whereIn('id', fcatIds)
      .where('status', 1)
      .where('is_deleted', 0)
      .select('id', knex.raw(`'${baseurl}' || image as image`))
      .then(rows => {
        const map = {};
        rows.forEach(row => map[row.id] = row);
        return map;
      }) : Promise.resolve({})
  ]);

  // Batch fetch all product variants for all products
  const allVarients = await knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
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
      'product_varient.quantity',
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper')
    )
    .where('store_products.store_id', store_id)
    .whereIn('product_varient.product_id', productIds)
    .whereNotNull('store_products.price')
    .where('product_varient.approved', 1)
    .where('product_varient.is_delete', 0);

  // Group variants by product_id
  const varientsByProduct = {};
  allVarients.forEach(v => {
    if (!varientsByProduct[v.product_id]) varientsByProduct[v.product_id] = [];
    varientsByProduct[v.product_id].push(v);
  });

  // Process products using batched data
  const customizedProductData = [];

  for (const ProductList of prodsssss) {
    const price = parseFloat(ProductList.price);
    const mrp = parseFloat(ProductList.mrp);

    // Use batched data instead of individual queries
    const isFavourite = user_id ? (wishlistItems.has(ProductList.varient_id) ? 'true' : 'false') : 'false';
    const notifyMe = user_id ? (notifyMeItems.has(ProductList.varient_id) ? 'true' : 'false') : 'false';
    const cartQty = user_id ? (cartQuantities[ProductList.varient_id] || 0) : 0;
    const isSubscription = user_id ? (subscriptionItems[ProductList.varient_id] ? 'true' : 'false') : 'false';

    const sub_price = (mrp * (ProductList.percentage || 0)) / 100;
    const finalsubprice = mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

    // Format price values
    const priceval = Number.isInteger(price) ? price + '.001' : price;
    const mrpval = Number.isInteger(mrp) ? mrp + '.001' : mrp;

    // Get feature tags from batched data
    let feature_tags = [];
    if (ProductList.fcat_id) {
      const fcatArray = ProductList.fcat_id.split(',').map(Number).filter(id => id);
      feature_tags = fcatArray.map(id => featureTags[id]).filter(Boolean);
    }

    // Get features from batched data
    const features = featuresMap[ProductList.product_id] || [];

    // Get product variants from batched data
    const productVarients = varientsByProduct[ProductList.product_id] || [];
    let total_cart_qty = 0;
    let total_subcart_qty = 0;

    const customizedVarientData = [];
    for (const ProductList1 of productVarients) {
      const vprice = parseFloat(ProductList1.price);
      const isFavourite1 = user_id ? (wishlistItems.has(ProductList1.varient_id) ? 'true' : 'false') : 'false';
      const notifyMe1 = user_id ? (notifyMeItems.has(ProductList1.varient_id) ? 'true' : 'false') : 'false';
      const cartQty1 = user_id ? (cartQuantities[ProductList1.varient_id] || 0) : 0;
      const subcartQty1 = user_id ? (subCartQuantities[ProductList1.varient_id] || 0) : 0;

      total_cart_qty += cartQty1;
      total_subcart_qty += subcartQty1;

      // Get images from batched data
      const images = productImagesMap[ProductList.product_id] || [];
      let imageUrl = '';
      if (images.length > 0) {
        imageUrl = baseurl + images[0].image;
      } else {
        // Fallback to product image
        imageUrl = ProductList.product_image ? baseurl + ProductList.product_image : '';
      }

      const customizedVarient = {
        stock: ProductList1.stock,
        varient_id: ProductList1.varient_id,
        product_id: ProductList1.product_id,
        product_name: ProductList.product_name,
        product_image: imageUrl ? imageUrl + "?width=200&height=200&quality=100" : '',
        thumbnail: imageUrl || '',
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
        subcartQty: subcartQty1,
        country_icon: countryicon
      };

      customizedVarientData.push(customizedVarient);
    }

    const customizedProduct = {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + (ProductList.product_image || '') + "?width=200&height=200&quality=100",
      thumbnail: baseurl + (ProductList.thumbnail || ''),
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
      isFavorite: isFavourite,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      total_subcart_qty: total_subcart_qty,
      countrating: 0,
      percentage: ProductList.percentage || 0,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      country_icon: countryicon,
      feature_tags: feature_tags,
      is_customized: ProductList.is_customized,
      features: features,
      varients: customizedVarientData
    };

    customizedProductData.push(customizedProduct);
  }

  return customizedProductData;

}

module.exports = {
  getWishlist,
  addtoWishlist
};
