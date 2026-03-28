import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.84.0';
import { Redis } from 'https://esm.sh/@upstash/redis@1.20.4';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Initialize Upstash Redis client
const redis = new Redis({
  url: Deno.env.get('UPSTASH_REDIS_REST_URL')!,
  token: Deno.env.get('UPSTASH_REDIS_REST_TOKEN')!,
});

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: corsHeaders });

  try {
    const body = await req.json();
    const { store_id, keyword = 'null', user_id, device_id } = body;
    const actual_user_id = (user_id && user_id !== "null") ? String(user_id) : String(device_id);
    
    const perPage = Number(body.perpage) || 20;
    const page = Number(body.page) || 1;
    const offset = (page - 1) * perPage;

    // --- Search History Recording Logic (Must run before cache check) ---
    // Only record "valid" keywords (strict substring match) to avoid saving misspellings.
    if (keyword && keyword !== 'null' && keyword.trim() !== '' && !['All', 'daily', 'subscription'].includes(keyword)) {
      try {
        const supabase = createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!);
        const isLogged = user_id && user_id !== "null" && !isNaN(Number(user_id));
        const db_user_id = isLogged ? Number(user_id) : null;
        const db_device_id = isLogged ? null : device_id;

        const kw = String(keyword).trim();
        const kwPattern = `%${kw}%`;

        // Validate keyword: must exist as a direct substring in product name OR category title OR brand title.
        // (No fuzzy similarity here — this is intentionally strict.)
        const [pCheck, cCheck, bCheck] = await Promise.all([
          supabase.from('product').select('product_id').ilike('product_name', kwPattern).limit(1),
          supabase.from('categories').select('cat_id').ilike('title', kwPattern).limit(1),
          supabase.from('brands').select('cat_id').ilike('title', kwPattern).limit(1),
        ]);

        const isValidKeyword =
          (pCheck.data && pCheck.data.length > 0) ||
          (cCheck.data && cCheck.data.length > 0) ||
          (bCheck.data && bCheck.data.length > 0);

        if (!isValidKeyword) {
          // Skip insertion for misspellings/invalid keywords.
          // Still allow the search results (RPC may return similarity matches), we just don't save the term.
          console.log('ℹ️ Skipping recent_search insert for invalid keyword:', kw);
          throw new Error('skip_recent_search_insert');
        }

        // 1. Delete same keyword for this specific user/device
        let delQuery = supabase.from('recent_search').delete().eq('keyword', keyword);
        if (db_user_id) delQuery = delQuery.eq('user_id', db_user_id);
        else if (db_device_id) delQuery = delQuery.eq('device_id', db_device_id);
        await delQuery;

        // 2. Maintain 10 item history limit
        let limitQuery = supabase.from('recent_search').select('id').order('id', { ascending: true });
        if (db_user_id) limitQuery = limitQuery.eq('user_id', db_user_id);
        else if (db_device_id) limitQuery = limitQuery.eq('device_id', db_device_id);
        const { data: historyItems } = await limitQuery;

        if (historyItems && historyItems.length >= 10) {
          await supabase.from('recent_search').delete().eq('id', historyItems[0].id);
        }

        // 3. Calculate Next ID (MAX+1 logic)
        const { data: maxIdData } = await supabase.from('recent_search').select('id').order('id', { ascending: false }).limit(1);
        const nextId = (maxIdData && maxIdData[0] ? Number(maxIdData[0].id) : 0) + 1;

        // 4. Insert new record
        await supabase.from('recent_search').insert({
          id: nextId,
          keyword: keyword,
          user_id: db_user_id,
          device_id: db_device_id,
          search_date: new Date().toISOString()
        });
      } catch (err) {
        // Silence the deliberate skip error; log other errors.
        if (err?.message !== 'skip_recent_search_insert') {
          console.error('Search History recording failed:', err);
        }
      }
    }

    // --- Global Redis Caching Logic (User ID removed for global hit) ---
    const cacheKey = [
      'searchbystoreproduct',
      `store:${store_id}`,
      `keyword:${keyword}`,
      `subcat:${body.sub_cat_id || 'null'}`,
      `sortprice:${body.sortprice || 'null'}`,
      `sortname:${body.sortname || 'null'}`,
      `page:${page}`,
      `perpage:${perPage}`,
      `minprice:${body.min_price || 'null'}`,
      `maxprice:${body.max_price || 'null'}`
    ].join('|');

    let responseData: any = await redis.get(cacheKey);
    let products: any[] = [];

    if (responseData) {
      console.log('✅ Global Cache HIT in Edge Function');
      products = responseData.data || [];
    } else {
      // --- Cache Miss: Proceed with Data Fetching ---
      let sortBy = 'relevance';
      let sortOrder = 'desc';
      if (body.sortprice === 'ltoh') { sortBy = 'price'; sortOrder = 'asc'; }
      if (body.sortprice === 'htol') { sortBy = 'price'; sortOrder = 'desc'; }
      if (body.sortname === 'atoz') { sortBy = 'name'; sortOrder = 'asc'; }
      if (body.sortname === 'ztoa') { sortBy = 'name'; sortOrder = 'desc'; }

      const supabase = createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!);
      const baseUrl = Deno.env.get('BUNNY_NET_IMAGE') || '';

      const { data: dbProducts, error: dbError } = await supabase.rpc('search_products_v2', {
        p_store_id: Number(store_id),
        p_query: keyword === 'null' ? '' : keyword,
        p_sub_cat_id: (body.sub_cat_id && body.sub_cat_id !== "null") ? Number(body.sub_cat_id) : null,
        p_min_price: (body.min_price && body.min_price !== "null") ? Number(body.min_price) : 0,
        p_max_price: (body.max_price && body.max_price !== "null") ? Number(body.max_price) : 999999,
        p_limit: perPage,
        p_offset: offset,
        p_sort_by: sortBy,
        p_sort_order: sortOrder
      });

      if (dbError) throw new Error(`RPC Error: ${dbError.message}`);

      const rawProducts = dbProducts || [];
      if (rawProducts.length === 0) {
        const emptyRes = { status: "1", message: "Product not found", data: [] };
        return new Response(JSON.stringify(emptyRes), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }

      // Deduplicate RPC results by product_id.
      // The RPC can return multiple rows per product (e.g., one per "main" variant),
      // but the API response should return each product once with a `varients` array.
      const dedupedByProduct = new Map<string, any>();
      for (const p of rawProducts) {
        const pid = String(p?.res_product_id);
        if (!pid || pid === 'undefined' || pid === 'null') continue;

        const existing = dedupedByProduct.get(pid);
        if (!existing) {
          dedupedByProduct.set(pid, p);
          continue;
        }

        // Keep the cheapest representative row for consistent main `price/mrp/varient_id` fields.
        const existingPrice = Number(existing?.res_price ?? Number.POSITIVE_INFINITY);
        const nextPrice = Number(p?.res_price ?? Number.POSITIVE_INFINITY);
        if (Number.isFinite(nextPrice) && nextPrice < existingPrice) {
          dedupedByProduct.set(pid, p);
        }
      }
      const uniqueProducts = Array.from(dedupedByProduct.values());

      // Initial mapping and variant fetching
      const productIds = uniqueProducts.map((p: any) => p.res_product_id);
      const [variantsRes, featuresRes, tagsRes, imagesRes] = await Promise.all([
        supabase.from('product_varient').select('*').in('product_id', productIds).eq('approved', 1).eq('is_delete', 0),
        supabase.from('product_features').select('product_id, feature_value_id, tbl_feature_value_master(id, feature_value)').in('product_id', productIds),
        supabase.from('feature_categories').select('id, image').eq('status', 1).eq('is_deleted', 0),
        // Match Node API: use product_images (ordered by type desc)
        supabase.from('product_images').select('product_id, image, type').in('product_id', productIds).order('type', { ascending: false })
      ]);

      const variantMap = variantsRes.data?.reduce((acc: any, v: any) => { acc[v.product_id] = acc[v.product_id] || []; acc[v.product_id].push(v); return acc; }, {}) || {};
      const featuresMap = featuresRes.data?.reduce((acc: any, f: any) => { acc[f.product_id] = acc[f.product_id] || []; acc[f.product_id].push(f.tbl_feature_value_master); return acc; }, {}) || {};
      const tagsMap = tagsRes.data?.reduce((acc: any, t: any) => { acc[t.id] = t.image; return acc; }, {}) || {};

      const imagesMap: Record<string, any[]> = {};
      (imagesRes.data || []).forEach((img: any) => {
        const pid = String(img.product_id);
        if (!imagesMap[pid]) imagesMap[pid] = [];
        imagesMap[pid].push(img);
      });

      const totalCount = rawProducts[0]?.total_count || 0;
      const totalPages = Math.ceil(Number(totalCount) / perPage);

      // Fetch store-specific pricing/stock per variant (matches Node API behavior)
      const allVariantIds = Array.from(
        new Set(
          (variantsRes.data || [])
            .map((v: any) => v?.varient_id)
            .filter((id: any) => id !== null && id !== undefined)
        )
      );

      const storeProductsMap: Record<string, any> = {};
      if (allVariantIds.length > 0) {
        const { data: storeProductsRows, error: storeProductsErr } = await supabase
          .from('store_products')
          .select('varient_id, price, mrp, stock')
          .eq('store_id', Number(store_id))
          .in('varient_id', allVariantIds);

        if (storeProductsErr) throw new Error(`store_products fetch error: ${storeProductsErr.message}`);

        (storeProductsRows || []).forEach((row: any) => {
          storeProductsMap[String(row.varient_id)] = row;
        });
      }

      products = uniqueProducts.map((p: any) => {
        const sub_price = (Number(p.res_mrp) * Number(p.res_percentage)) / 100;
        const subscription_price = parseFloat((Number(p.res_mrp) - sub_price).toFixed(2));
        
        const p_variants = (variantMap[p.res_product_id] || []).map((v: any) => {
          const sp = storeProductsMap[String(v.varient_id)];
          const v_price = sp?.price !== undefined && sp?.price !== null ? Number(sp.price) : Number(p.res_price);
          const v_mrp = sp?.mrp !== undefined && sp?.mrp !== null ? Number(sp.mrp) : Number(v.base_mrp || p.res_mrp);
          const v_stock = sp?.stock !== undefined && sp?.stock !== null ? Number(sp.stock) : Number(p.res_stock);

          // Match Node API: 100 - ((price*100)/mrp)
          const v_discountper = v_mrp > 0 ? (100 - (v_price * 100) / v_mrp) : 0;

          // Match Node API variant image logic: product_images[0] (type desc) else main product image
          const firstProductImage = imagesMap[String(p.res_product_id)]?.[0]?.image;
          const variantImg200 = firstProductImage
            ? `${baseUrl}${firstProductImage}?width=200&height=200&quality=100`
            : `${baseUrl}${p.res_product_image}?width=200&height=200&quality=100`;
          const variantThumb = firstProductImage
            ? `${baseUrl}${firstProductImage}`
            : `${baseUrl}${p.res_product_image}`;

          return {
            stock: v_stock,
            varient_id: v.varient_id,
            product_id: p.res_product_id,
            product_name: p.res_product_name,
            product_image: variantImg200,
            thumbnail: variantThumb,
            description: v.description,
            price: v_price,
            mrp: v_mrp,
            unit: v.unit,
            quantity: v.quantity,
            type: p.res_type,
            discountper: v_discountper,
            country_icon: p.res_country_icon ? `${baseUrl}${p.res_country_icon}` : null
          };
        });

        return {
          stock: p.res_stock, cat_id: p.res_cat_id, varient_id: p.res_varient_id, product_id: p.res_product_id, brand_id: p.res_brand_id,
          product_name: p.res_product_name, product_image: `${baseUrl}${p.res_product_image}?width=200&height=200&quality=100`,
          thumbnail: p.res_thumbnail ? `${baseUrl}${p.res_thumbnail}` : null, description: p.res_description, price: Number(p.res_price), mrp: Number(p.res_mrp),
          unit: p.res_unit, quantity: p.res_quantity, type: p.res_type, percentage: p.res_percentage, subscription_price: subscription_price, 
          availability: p.res_availability, discountper: p.res_discountper, avgrating: 0, countrating: 0,
          country_icon: p.res_country_icon ? `${baseUrl}${p.res_country_icon}` : null,
          feature_tags: p.res_fcat_id ? p.res_fcat_id.split(',').map((id: string) => ({ id: Number(id), image: tagsMap[Number(id)] ? `${baseUrl}${tagsMap[Number(id)]}` : null })) : [],
          features: featuresMap[p.res_product_id] || [], varients: p_variants, is_customized: p.res_is_customized, perPage: perPage, totalPages: totalPages
        };
      });

      responseData = { status: "1", message: "Product found", data: products };
      // Store Global Result in Redis (TTL: 300s)
      await redis.set(cacheKey, responseData, { ex: 60 });
    }

    // --- User-Specific Decoration (ALWAYS from Redis) ---
    if (actual_user_id && actual_user_id !== "null") {
      const wishlistKey = `wishlist:set|store:${store_id}|user:${actual_user_id}`;
      const cartKey = `cart:hash|store:${store_id}|user:${actual_user_id}`;
      const subcartKey = `subcart:hash|store:${store_id}|user:${actual_user_id}`;

      const [wishlistRaw, cartHash, subcartHash] = await Promise.all([
        redis.smembers(wishlistKey),
        redis.hgetall(cartKey),
        redis.hgetall(subcartKey)
      ]);

      const wishlistSet = new Set(wishlistRaw || []);
      const cart = cartHash || {};
      const subcart = subcartHash || {};

      products = products.map(p => {
        const vid = String(p.varient_id);
        const decoratedVariants = (p.varients || []).map((v: any) => {
          const vvid = String(v.varient_id);
          const v_cart_qty = parseInt(String(cart[vvid] || 0));
          const v_subcartQty = parseInt(String(subcart[vvid] || 0));
          return {
            ...v,
            isFavourite: wishlistSet.has(Number(vvid)) || wishlistSet.has(vvid) ? "true" : "false",
            cart_qty: v_cart_qty,
            total_cart_qty: v_cart_qty,
            subcartQty: v_subcartQty,
            total_subcart_qty: v_subcartQty,
            notify_me: "false"
          };
        });
        const totalVariantCartQty = decoratedVariants.reduce((sum: number, v: any) => sum + (Number(v?.cart_qty) || 0), 0);
        const totalVariantSubcartQty = decoratedVariants.reduce((sum: number, v: any) => sum + (Number(v?.subcartQty) || 0), 0);

        const p_cart_qty = parseInt(String(cart[vid] || 0));
        const p_subcartQty = parseInt(String(subcart[vid] || 0));

        return {
          ...p,
          isFavourite: wishlistSet.has(Number(vid)) || wishlistSet.has(vid) ? "true" : "false",
          isSubscription: p_subcartQty > 0 ? "true" : "false",
          cart_qty: p_cart_qty,
          total_cart_qty: totalVariantCartQty || p_cart_qty,
          subcartQty: p_subcartQty,
          total_subcart_qty: totalVariantSubcartQty || p_subcartQty,
          varients: decoratedVariants,
          notify_me: "false"
        };
      });
    }

    return new Response(JSON.stringify({ ...responseData, data: products }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

  } catch (error) {
    console.error('Edge Function Error:', error);
    return new Response(JSON.stringify({ status: "0", message: error.message, data: [] }), { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }
});