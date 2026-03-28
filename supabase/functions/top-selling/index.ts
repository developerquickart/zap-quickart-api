// Supabase Edge Function: top_selling API
// Migrated from Node.js/Express/MySQL to Supabase Edge Function (Deno/PostgreSQL)
// This version uses Supabase client queries directly

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

interface RequestBody {
  store_id: number | string;
  user_id?: number | string | null;
  device_id?: string;
  page: number;
  perpage: number;
  min_price?: number | string;
  max_price?: number | string;
  min_discount?: number | string;
  max_discount?: number | string;
  sortname?: string;
  sortprice?: string;
  cat_id?: number | string | null;
  sub_cat_id?: number | string | null;
  [key: string]: any;
}

async function getTopSelling(supabase: any, appDetails: RequestBody) {
  const {
    store_id,
    user_id,
    device_id,
    page,
    perpage,
    min_price,
    max_price,
    min_discount,
    max_discount,
    sortname,
    sortprice
  } = appDetails;

  const pageFilter = page;
  const perPage = perpage;
  const baseurl = Deno.env.get('BUNNY_NET_IMAGE') || '';

  // Determine user identifier
  let userId: string | number | null = null;
  if (user_id && user_id !== "null" && user_id !== null) {
    userId = user_id;
  } else if (device_id) {
    userId = device_id;
  }

  const minprice = min_price ? parseFloat(String(min_price)) : null;
  const maxprice = max_price ? parseFloat(String(max_price)) : null;
  const mindiscount = min_discount ? parseFloat(String(min_discount)) : null;
  const maxdiscount = max_discount ? parseFloat(String(max_discount)) : null;

  // Calculate date range (1 year ago)
  const today = new Date();
  const oneYearAgo = new Date();
  oneYearAgo.setFullYear(today.getFullYear() - 1);
  const oneYearAgoStr = oneYearAgo.toISOString().split('T')[0];

  // Get top selling products using a more Supabase-friendly approach
  // First, get all subscription orders from last year
  const { data: subscriptionOrders, error: subError } = await supabase
    .from('subscription_order')
    .select(`
      store_order_id,
      order_status,
      store_orders!inner (
        store_order_id,
        varient_id,
        qty,
        order_cart_id,
        orders!inner (
          cart_id,
          order_date
        )
      )
    `)
    .not('order_status', 'in', '(Pause,Cancelled)')
    .gte('store_orders.orders.order_date', oneYearAgoStr);

  if (subError) {
    throw new Error(`Error fetching subscription orders: ${subError.message}`);
  }

  // Aggregate quantities by product
  const productQuantities: { [key: number]: number } = {};
  const productVariants: { [key: number]: any } = {};

  if (subscriptionOrders) {
    for (const subOrder of subscriptionOrders) {
      const storeOrder = subOrder.store_orders;
      if (storeOrder && Array.isArray(storeOrder)) {
        for (const so of storeOrder) {
          const varientId = so.varient_id;
          const qty = so.qty || 0;
          
          // Get product_id from variant
          const { data: variant } = await supabase
            .from('product_varient')
            .select('product_id')
            .eq('varient_id', varientId)
            .single();

          if (variant) {
            const productId = variant.product_id;
            productQuantities[productId] = (productQuantities[productId] || 0) + qty;
            if (!productVariants[productId]) {
              productVariants[productId] = varientId;
            }
          }
        }
      } else if (storeOrder) {
        const varientId = storeOrder.varient_id;
        const qty = storeOrder.qty || 0;
        
        const { data: variant } = await supabase
          .from('product_varient')
          .select('product_id')
          .eq('varient_id', varientId)
          .single();

        if (variant) {
          const productId = variant.product_id;
          productQuantities[productId] = (productQuantities[productId] || 0) + qty;
          if (!productVariants[productId]) {
            productVariants[productId] = varientId;
          }
        }
      }
    }
  }

  // Sort products by total quantity
  const sortedProductIds = Object.entries(productQuantities)
    .sort(([, a], [, b]) => (b as number) - (a as number))
    .map(([productId]) => parseInt(productId));

  // Get product details for top selling products
  let productQuery = supabase
    .from('product')
    .select(`
      product_id,
      product_name,
      product_image,
      thumbnail,
      type,
      percentage,
      availability,
      country_id,
      fcat_id,
      is_customized,
      is_delete,
      is_offer_product,
      offer_date,
      store_products!inner (
        store_id,
        stock,
        price,
        mrp,
        varient_id,
        product_varient!inner (
          varient_id,
          unit,
          quantity,
          description,
          varient_image,
          ean,
          approved,
          is_delete,
          product_id
        )
      ),
      categories (
        cat_id,
        parent
      ),
      tbl_country (
        id,
        country_icon
      )
    `)
    .in('product_id', sortedProductIds)
    .eq('is_delete', 0)
    .eq('store_products.store_id', store_id)
    .gt('store_products.stock', 0)
    .or('is_offer_product.eq.0,offer_date.is.null,offer_date.neq.' + today.toISOString().split('T')[0]);

  // Apply price filter if provided
  if (minprice !== null && maxprice !== null) {
    productQuery = productQuery
      .gte('store_products.price', minprice)
      .lte('store_products.price', maxprice);
  }

  const { data: products, error: productsError } = await productQuery;

  if (productsError) {
    throw new Error(`Error fetching products: ${productsError.message}`);
  }

  if (!products || products.length === 0) {
    return [];
  }

  // Sort products based on sort parameters
  let sortedProducts = [...products];
  if (sortprice === 'ltoh') {
    sortedProducts.sort((a, b) => {
      const priceA = a.store_products?.[0]?.price || 0;
      const priceB = b.store_products?.[0]?.price || 0;
      return priceA - priceB;
    });
  } else if (sortprice === 'htol') {
    sortedProducts.sort((a, b) => {
      const priceA = a.store_products?.[0]?.price || 0;
      const priceB = b.store_products?.[0]?.price || 0;
      return priceB - priceA;
    });
  } else if (sortname === 'atoz') {
    sortedProducts.sort((a, b) => a.product_name.localeCompare(b.product_name));
  } else if (sortname === 'ztoa') {
    sortedProducts.sort((a, b) => b.product_name.localeCompare(a.product_name));
  } else {
    // Sort by total quantity (default)
    sortedProducts.sort((a, b) => {
      const qtyA = productQuantities[a.product_id] || 0;
      const qtyB = productQuantities[b.product_id] || 0;
      return qtyB - qtyA;
    });
  }

  // Apply pagination
  const totalPages = Math.ceil(sortedProducts.length / perPage);
  const startIndex = (pageFilter - 1) * perPage;
  const endIndex = startIndex + perPage;
  const paginatedProducts = sortedProducts.slice(startIndex, endIndex);

  const customizedProductData: any[] = [];
  const currentDate = new Date().toISOString().split('T')[0];

  for (const ProductList of paginatedProducts) {
    const storeProduct = ProductList.store_products?.[0];
    if (!storeProduct) continue;

    const varientId = storeProduct.varient_id;

    // Check for active deal
    const { data: deal } = await supabase
      .from('deal_product')
      .select('deal_price')
      .eq('varient_id', varientId)
      .eq('store_id', store_id)
      .lte('valid_from', currentDate)
      .gt('valid_to', currentDate)
      .maybeSingle();

    let price = 0;
    if (deal && deal.deal_price) {
      price = parseFloat(deal.deal_price);
    } else {
      price = parseFloat(storeProduct.price) || 0;
    }

    // User-specific data
    let isSubscription = 'false';
    let isFavourite = 'false';
    let notifyMe = 'false';
    let cartQty = 0;

    if (userId) {
      // Check subscription status
      const { data: subprod } = await supabase
        .from('store_orders')
        .select('percentage')
        .eq('varient_id', varientId)
        .eq('store_approval', userId)
        .eq('subscription_flag', 1)
        .eq('order_cart_id', 'incart')
        .maybeSingle();

      isSubscription = subprod ? 'true' : 'false';

      // Check wishlist
      const { data: wishList } = await supabase
        .from('wishlist')
        .select('*')
        .eq('varient_id', varientId)
        .eq('user_id', userId);

      isFavourite = wishList && wishList.length > 0 ? 'true' : 'false';

      // Check cart quantity
      const { data: CartQtyList } = await supabase
        .from('store_orders')
        .select('qty')
        .eq('varient_id', varientId)
        .eq('store_approval', userId)
        .eq('order_cart_id', 'incart')
        .eq('store_id', store_id)
        .is('subscription_flag', null)
        .maybeSingle();

      cartQty = CartQtyList?.qty || 0;

      // Check notify me
      const { data: cnotify_me } = await supabase
        .from('product_notify_me')
        .select('*')
        .eq('varient_id', varientId)
        .eq('user_id', userId);

      notifyMe = cnotify_me && cnotify_me.length > 0 ? 'true' : 'false';
    }

    // Calculate subscription price
    const mrp = parseFloat(storeProduct.mrp) || 0;
    const percentage = ProductList.percentage || 0;
    const sub_price = (mrp * percentage) / 100;
    const finalsubprice = mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    // Country icon
    const countryIcon = ProductList.tbl_country?.country_icon || null;
    const countryicon = countryIcon ? baseurl + countryIcon : null;

    // Price formatting
    let priceval = Number.isInteger(price) ? price + 0.001 : price;
    let mrpval = Number.isInteger(mrp) ? mrp + 0.001 : mrp;

    // Feature tags
    let feature_tags: any[] = [];
    if (ProductList.fcat_id) {
      const resultArray = String(ProductList.fcat_id).split(',').map(Number);
      const { data: ftaglist } = await supabase
        .from('feature_categories')
        .select('id, image')
        .in('id', resultArray)
        .eq('status', 1)
        .eq('is_deleted', 0);

      if (ftaglist) {
        feature_tags = ftaglist.map((tag: any) => ({
          id: tag.id,
          image: baseurl + tag.image
        }));
      }
    }

    // Product features
    const { data: featuresData } = await supabase
      .from('product_features')
      .select(`
        feature_value_id,
        tbl_feature_value_master (
          id,
          feature_value
        )
      `)
      .eq('product_id', ProductList.product_id);

    const formattedFeatures = (featuresData || []).map((f: any) => ({
      id: f.tbl_feature_value_master?.id,
      feature_value: f.tbl_feature_value_master?.feature_value
    })).filter((f: any) => f.id);

    // Get all variants for this product
    const { data: variantsData } = await supabase
      .from('store_products')
      .select(`
        store_id,
        stock,
        varient_id,
        price,
        mrp,
        product_varient!inner (
          varient_id,
          description,
          varient_image,
          unit,
          quantity,
          product_id,
          approved,
          is_delete
        )
      `)
      .eq('store_id', store_id)
      .eq('product_varient.product_id', ProductList.product_id)
      .not('price', 'is', null)
      .eq('product_varient.approved', 1)
      .eq('product_varient.is_delete', 0);

    let total_cart_qty = 0;
    let total_subcart_qty = 0;
    const customizedVarientData: any[] = [];

    if (variantsData) {
      for (const variant of variantsData) {
        const ProductList1 = variant;
        const vprice = parseFloat(ProductList1.price) || 0;
        const variantData = Array.isArray(ProductList1.product_varient) 
          ? ProductList1.product_varient[0] 
          : ProductList1.product_varient;

        let isFavourite1 = 'false';
        let notifyMe1 = 'false';
        let cartQty1 = 0;
        let subcartQty = 0;

        if (userId) {
          // Wishlist check for variant
          const { data: wishList1 } = await supabase
            .from('wishlist')
            .select('*')
            .eq('varient_id', ProductList1.varient_id)
            .eq('user_id', userId);

          isFavourite1 = wishList1 && wishList1.length > 0 ? 'true' : 'false';

          // Cart quantity for variant
          const { data: CartQtyList1 } = await supabase
            .from('store_orders')
            .select('qty')
            .eq('varient_id', ProductList1.varient_id)
            .eq('store_approval', userId)
            .eq('order_cart_id', 'incart')
            .is('subscription_flag', null)
            .eq('store_id', store_id)
            .maybeSingle();

          cartQty1 = CartQtyList1?.qty || 0;

          // Subscription cart quantity
          const { data: subCart } = await supabase
            .from('store_orders')
            .select('qty')
            .eq('varient_id', ProductList1.varient_id)
            .eq('store_approval', userId)
            .eq('order_cart_id', 'incart')
            .eq('subscription_flag', 1)
            .eq('store_id', store_id)
            .maybeSingle();

          subcartQty = subCart?.qty || 0;

          // Notify me for variant
          const { data: cnotify_me1 } = await supabase
            .from('product_notify_me')
            .select('*')
            .eq('varient_id', ProductList1.varient_id)
            .eq('user_id', userId)
            .maybeSingle();

          notifyMe1 = cnotify_me1 ? 'true' : 'false';
        }

        // Get product images
        const { data: images } = await supabase
          .from('product_images')
          .select('image')
          .eq('product_id', ProductList.product_id)
          .order('type', { ascending: false });

        let imageUrl = '';
        let thumbnailUrl = '';
        if (images && images.length > 0) {
          imageUrl = baseurl + images[0].image + "?width=200&height=200&quality=100";
          thumbnailUrl = baseurl + images[0].image;
        } else {
          // Fallback to product main image
          imageUrl = baseurl + ProductList.product_image;
          thumbnailUrl = baseurl + ProductList.thumbnail;
        }

        // Get product feature ID from cart
        let productFeatureId = 0;
        if (userId) {
          const { data: CartFeature } = await supabase
            .from('store_orders')
            .select('product_feature_id')
            .eq('varient_id', ProductList1.varient_id)
            .eq('store_approval', userId)
            .eq('order_cart_id', 'incart')
            .maybeSingle();

          productFeatureId = CartFeature?.product_feature_id || 0;
        }

        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty;

        const discountper = mrp > 0 ? 100 - ((vprice * 100) / mrp) : 0;

        const customizedVarient = {
          stock: ProductList1.stock,
          varient_id: ProductList1.varient_id,
          product_id: ProductList.product_id,
          product_name: ProductList.product_name,
          product_image: imageUrl,
          thumbnail: thumbnailUrl,
          description: variantData?.description || '',
          price: vprice,
          mrp: parseFloat(ProductList1.mrp) || 0,
          unit: variantData?.unit || '',
          quantity: variantData?.quantity || 0,
          type: ProductList.type,
          discountper: discountper,
          notify_me: notifyMe1,
          isFavourite: isFavourite1,
          cart_qty: cartQty1,
          subcartQty: subcartQty,
          product_feature_id: productFeatureId,
          country_icon: countryicon
        };

        customizedVarientData.push(customizedVarient);
      }
    }

    const discountper = mrp > 0 ? 100 - ((price * 100) / mrp) : 0;

    const customizedProduct = {
      stock: storeProduct.stock,
      varient_id: varientId,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image,
      thumbnail: baseurl + ProductList.thumbnail,
      description: storeProduct.product_varient?.description || '',
      price: parseFloat(String(priceval)),
      mrp: parseFloat(String(mrpval)),
      unit: storeProduct.product_varient?.unit || '',
      quantity: storeProduct.product_varient?.quantity || 0,
      type: ProductList.type,
      percentage: percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
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
      features: formattedFeatures,
      varients: customizedVarientData,
      is_customized: ProductList.is_customized,
      page: pageFilter,
      perPage: perPage,
      totalPages: totalPages,
      total_subcart_qty: total_subcart_qty
    };

    customizedProductData.push(customizedProduct);
  }

  return customizedProductData;
}

serve(async (req) => {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  try {
    // Initialize Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? '';
    const supabaseKey = Deno.env.get('SUPABASE_ANON_KEY') ?? '';
    const supabase = createClient(supabaseUrl, supabaseKey);

    // Parse request body
    const requestBody: RequestBody = await req.json();

    // Validate required fields
    if (!requestBody.store_id || !requestBody.page || !requestBody.perpage) {
      return new Response(
        JSON.stringify({
          status: 0,
          message: 'Missing required fields: store_id, page, perpage'
        }),
        {
          status: 400,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        }
      );
    }

    // Get top selling products
    const productData = await getTopSelling(supabase, requestBody);

    // Format response
    const response = {
      status: "1",
      message: "Top selling products",
      page: requestBody.page,
      perPage: requestBody.perpage,
      data: productData
    };

    return new Response(
      JSON.stringify(response),
      {
        status: 200,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );

  } catch (error) {
    console.error('Error:', error);
    return new Response(
      JSON.stringify({
        status: 0,
        message: error.message || 'Internal Server Error'
      }),
      {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      }
    );
  }
});
