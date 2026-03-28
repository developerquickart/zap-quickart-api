const knex = require('../db'); // Import your Knex instance

const getrecentSearch = async (appDetatils) => {
    const user_id = appDetatils.user_id;
    const device_id = appDetatils.device_id;
    // PostgreSQL: user_id is integer type, ensure proper type handling
    if (user_id && user_id !== "null" && !isNaN(parseInt(user_id))) {
        return await knex('recent_search')
            .where('user_id', parseInt(user_id))
            .where('keyword', '!=', '')
            .orderBy('id', 'DESC')
            .limit(5)
    } else {
        return await knex('recent_search')
            .where('device_id', device_id)
            .orderBy('id', 'DESC')
            .where('keyword', '!=', '')
            .limit(5)
    }

}

/**
 * Robustly records search history with manual ID management (MAX+1)
 * Scoped deletion to avoid global keyword removal for other users
 */
const recordSearchHistory = async (appDetatils) => {
    const { keyword, device_id } = appDetatils;

    // 1. Validation: Skip if keyword is invalid or system reserved
    if (!keyword || keyword === "" || keyword === "null" || keyword === "All" || keyword === "daily" || keyword === "subscription") {
        return;
    }

    // 2. Identify User (Proper type handling)
    let user_id = null;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && !isNaN(parseInt(appDetatils.user_id))) {
        user_id = parseInt(appDetatils.user_id);
    }

    try {
        // 3. Delete existing entry for this specific user/device to move it to the top
        const deleteQuery = knex('recent_search').where('keyword', keyword);
        if (user_id) {
            deleteQuery.where('user_id', user_id);
        } else if (device_id) {
            deleteQuery.where('device_id', device_id);
        } else {
            return; // No identifier
        }
        await deleteQuery.delete();

        // 4. Maintain limit (10 items) - delete oldest if necessary
        const historyQuery = knex('recent_search');
        if (user_id) {
            historyQuery.where('user_id', user_id);
        } else {
            historyQuery.where('device_id', device_id);
        }

        const existingHistory = await historyQuery.orderBy('id', 'ASC');
        if (existingHistory.length >= 10) {
            await knex('recent_search').where('id', existingHistory[0].id).delete();
        }

        // 5. Calculate Next ID (MAX+1 logic as requested)
        const maxIdResult = await knex('recent_search').max('id as maxId').first();
        const nextId = (parseInt(maxIdResult.maxId) || 0) + 1;

        // 6. Insert new record
        await knex('recent_search').insert({
            id: nextId,
            user_id: user_id, // NULL if not logged in
            device_id: user_id ? null : device_id,
            keyword: keyword,
            search_date: new Date()
        });

        console.log(`✅ Search recorded for ${user_id ? 'User ' + user_id : 'Device ' + device_id}: "${keyword}" (ID: ${nextId})`);
    } catch (error) {
        console.error('❌ Error recording search history:', error.message);
    }
}

const gettrenproducts = async (appDetatils) => {
    const store_id = appDetatils.store_id;

    // Determine user_id - PostgreSQL: ensure proper type handling
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
        user_id = appDetatils.user_id;
    } else {
        user_id = appDetatils.device_id;
    }
    const isValidUserId = user_id && user_id !== "null" && (typeof user_id === "number" || (typeof user_id === "string" && !isNaN(parseInt(user_id))));
    const baseurl = process.env.BUNNY_NET_IMAGE;
    const currentDate = new Date();

    // Step 1: Get all trending products in one query
    const trendsearch = await knex('trending_search')
        .join('store_products', 'trending_search.varient_id', '=', 'store_products.varient_id')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
        .join('store', 'store_products.store_id', '=', 'store.id')
        .select('product.fcat_id', 'product.percentage', 'product.availability', 'tbl_country.country_icon', 'store_products.stock', 'store_products.store_id', 'product_varient.varient_id', 'product.product_id', 'product.product_name',
            knex.raw(`? || product.product_image as product_image`, [baseurl]),
            knex.raw(`? || product.thumbnail as thumbnail`, [baseurl]),
            'product_varient.description', 'store_products.price', 'store_products.mrp',
            knex.raw(`? || product_varient.varient_image as varient_image`, [baseurl]),
            knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
            'product_varient.unit', 'product_varient.quantity', 'product.type')
        .groupBy('product.fcat_id', 'product.percentage', 'product.availability', 'tbl_country.country_icon', 'store_products.stock', 'store_products.store_id', 'product_varient.varient_id', 'product.product_id', 'product.product_name', 'product.product_image', 'product.thumbnail', 'product_varient.description', 'store_products.price', 'store_products.mrp', 'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity', 'product.type')
        .where('store_products.store_id', store_id)
        .where('product.is_delete', 0)
        .where('product.hide', 0)
        .where('product.is_zap', true)
        .where('store_products.stock', '>', 0)
        .where('store_products.is_deleted', 0)
        .where(builder => {
            builder
                .where('product.is_offer_product', 0)
                .whereNull('product.offer_date')
                .orWhereRaw('product.offer_date::date != CURRENT_DATE')
        });

    if (!trendsearch || trendsearch.length === 0) {
        return [];
    }

    // Step 2: Extract all IDs for batch queries
    const variantIds = trendsearch.map(p => p.varient_id);
    const productIds = [...new Set(trendsearch.map(p => p.product_id))];
    const allFcatIds = new Set();
    trendsearch.forEach(p => {
        if (p.fcat_id) {
            p.fcat_id.split(',').forEach(id => {
                const numId = parseInt(id.trim());
                if (!isNaN(numId)) allFcatIds.add(numId);
            });
        }
    });
    const fcatIdArray = Array.from(allFcatIds);

    // Step 3: Batch fetch ALL data in parallel (no loops!)
    const [
        deals,
        wishlistItems,
        cartItems,
        notifyMeItems,
        subscriptionItems,
        allFeatures,
        allProductImages,
        allVariants,
        allStoreProductsForVariants,
        allFeatureCategories,
        allCartFeatures
    ] = await Promise.all([
        // Deals for all variants
        variantIds.length > 0 ? knex('deal_product')
            .whereIn('varient_id', variantIds)
            .where('store_id', store_id)
            .where('valid_from', '<=', currentDate)
            .where('valid_to', '>', currentDate) : [],

        // Wishlist for all variants (if user logged in)
        isValidUserId && variantIds.length > 0 ? knex('wishlist')
            .whereIn('varient_id', variantIds)
            .where('user_id', parseInt(user_id)) : [],

        // Cart items for all variants (if user logged in)
        isValidUserId && variantIds.length > 0 ? knex('store_orders')
            .whereIn('varient_id', variantIds)
            .where('store_approval', String(user_id))
            .where('order_cart_id', 'incart')
            .whereNull('subscription_flag')
            .where('store_id', store_id) : [],

        // Notify me for all variants (if user logged in)
        isValidUserId && variantIds.length > 0 ? knex('product_notify_me')
            .whereIn('varient_id', variantIds)
            .where('user_id', parseInt(user_id)) : [],

        // Subscription items for all variants (if user logged in)
        isValidUserId && variantIds.length > 0 ? knex('store_orders')
            .select('varient_id', 'percentage')
            .whereIn('varient_id', variantIds)
            .where('store_approval', String(user_id))
            .where('subscription_flag', '1')
            .where('order_cart_id', 'incart') : [],

        // Features for all products
        productIds.length > 0 ? knex('product_features')
            .select('product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
            .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
            .whereIn('product_id', productIds) : [],

        // Product images for all products
        productIds.length > 0 ? knex('product_images')
            .select('product_id', knex.raw(`? || image as image`, [baseurl]), 'type')
            .whereIn('product_id', productIds)
            .orderBy('type', 'DESC') : [],

        // All variants for all products (product_name from product table; product_varient has no product_name)
        productIds.length > 0 ? knex('store_products')
            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
            .join('product', 'product_varient.product_id', '=', 'product.product_id')
            .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id',
                'product_varient.product_id', 'product_varient.description', 'store_products.price',
                'store_products.mrp', 'product_varient.varient_image', 'product_varient.unit',
                'product_varient.quantity', 'product.product_name',
                knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
            .where('store_products.store_id', store_id)
            .whereIn('product_varient.product_id', productIds)
            .whereNotNull('store_products.price')
            .where('product_varient.approved', 1)
            .where('product_varient.is_delete', 0) : [],

        // Store products for variant price lookup (redundant but needed for variants loop)
        variantIds.length > 0 ? knex('store_products')
            .select('varient_id', 'price')
            .whereIn('varient_id', variantIds)
            .where('store_id', store_id) : [],

        // Feature categories
        fcatIdArray.length > 0 ? knex('feature_categories')
            .whereIn('id', fcatIdArray)
            .where('status', 1)
            .where('is_deleted', 0)
            .select('id', knex.raw(`? || image as image`, [baseurl])) : [],

        // Cart features for variants (if user logged in)
        isValidUserId && variantIds.length > 0 ? knex('store_orders')
            .select('varient_id', 'product_feature_id')
            .whereIn('varient_id', variantIds)
            .where('store_approval', String(user_id))
            .where('order_cart_id', 'incart')
            .whereNull('subscription_flag')
            .where('store_id', store_id) : []
    ]);

    // Step 4: Create lookup maps for O(1) access
    const dealMap = Object.fromEntries(deals.map(d => [d.varient_id, d.deal_price]));
    const wishlistMap = Object.fromEntries(wishlistItems.map(w => [w.varient_id, true]));
    const cartMap = Object.fromEntries(cartItems.map(c => [c.varient_id, c.qty || 0]));
    const notifyMeMap = Object.fromEntries(notifyMeItems.map(n => [n.varient_id, true]));
    const subscriptionMap = Object.fromEntries(subscriptionItems.map(s => [s.varient_id, s.percentage]));
    const featuresMap = {};
    allFeatures.forEach(f => {
        if (!featuresMap[f.product_id]) featuresMap[f.product_id] = [];
        featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
    });
    const imagesMap = {};
    allProductImages.forEach(img => {
        if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
        imagesMap[img.product_id].push(img.image);
    });
    const variantsMap = {};
    allVariants.forEach(v => {
        if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
        variantsMap[v.product_id].push(v);
    });
    const storeProductsMap = Object.fromEntries(allStoreProductsForVariants.map(sp => [sp.varient_id, sp.price]));
    const featureCategoriesMap = Object.fromEntries(allFeatureCategories.map(fc => [fc.id, fc]));
    const cartFeaturesMap = Object.fromEntries(allCartFeatures.map(cf => [cf.varient_id, cf.product_feature_id || 0]));

    // Step 5: Process products (no database queries in loop!)
    const customizedProductData = [];
    for (let i = 0; i < trendsearch.length; i++) {
        const ProductList = trendsearch[i];

        // Get price from deal map or store products map (O(1) lookup)
        const price = dealMap[ProductList.varient_id] || ProductList.price;

        // Get user-specific data from maps (O(1) lookup)
        const isFavourite = isValidUserId && wishlistMap[ProductList.varient_id] ? 'true' : 'false';
        const cartQty = isValidUserId ? (cartMap[ProductList.varient_id] || 0) : 0;
        const subcartQty = isValidUserId ? (subscriptionMap[ProductList.varient_id] ? 1 : 0) : 0;
        const notifyMe = isValidUserId && notifyMeMap[ProductList.varient_id] ? 'true' : 'false';
        const isSubscription = isValidUserId && subscriptionMap[ProductList.varient_id] ? 'true' : 'false';

        // Calculate subscription price
        const sub_price = (ProductList.mrp * (ProductList.percentage || 0)) / 100;
        const finalsubprice = ProductList.mrp - sub_price;
        const subscription_price = parseFloat(finalsubprice.toFixed(2));

        // Country icon
        const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

        // Format price and MRP
        const priceval = Number.isInteger(price) ? price + '.001' : price;
        const mrpval = Number.isInteger(ProductList.mrp) ? ProductList.mrp + '.001' : ProductList.mrp;

        // Get feature tags from map (O(1) lookup)
        let feature_tags = [];
        if (ProductList.fcat_id) {
            const resultArray = ProductList.fcat_id.split(',').map(Number);
            feature_tags = resultArray
                .map(id => featureCategoriesMap[id])
                .filter(Boolean);
        }

        // Get features from map (O(1) lookup)
        const features = featuresMap[ProductList.product_id] || [];

        // Get product images from map (O(1) lookup)
        const productImages = imagesMap[ProductList.product_id] || [];
        const fallbackImage = ProductList.product_image || '';

        // Get variants from map (O(1) lookup)
        const app = variantsMap[ProductList.product_id] || [];

        // Process variants (no database queries!)
        const customizedVarientData = [];

        for (let j = 0; j < app.length; j++) {
            // prod.varient.dummy = 5678;
            const ProductList1 = app[j];

            // Get variant price from deal map or store products map (O(1) lookup)
            const vprice = dealMap[ProductList1.varient_id] || storeProductsMap[ProductList1.varient_id] || ProductList1.price;

            // Get user-specific data from maps (O(1) lookup)
            let isFavourite1 = isValidUserId && wishlistMap[ProductList1.varient_id] ? 'true' : 'false';
            let cartQty1 = isValidUserId ? (cartMap[ProductList1.varient_id] || 0) : 0;
            let subcartQty1 = isValidUserId ? (subscriptionMap[ProductList1.varient_id] ? 1 : 0) : 0;
            let notifyMe1 = isValidUserId && notifyMeMap[ProductList1.varient_id] ? 'true' : 'false';
            let productFeatureId = isValidUserId ? (cartFeaturesMap[ProductList1.varient_id] || 0) : 0;

            // Use product images already fetched
            const variantImages = productImages.length > 0 ? productImages : (fallbackImage ? [fallbackImage] : []);

            const customizedVarient = {
                stock: ProductList1.stock,
                varient_id: ProductList1.varient_id,
                product_id: ProductList1.product_id,
                product_name: ProductList1.product_name || ProductList.product_name,
                product_image: variantImages.length > 0 ? variantImages[0] + "?width=200&height=200&quality=100" : '',
                thumbnail: variantImages.length > 0 ? variantImages[0] : '',
                description: ProductList1.description,
                price: vprice,
                mrp: ProductList1.mrp,
                unit: ProductList1.unit,
                quantity: ProductList1.quantity,
                type: ProductList1.type || ProductList.type,
                discountper: ProductList1.discountper || ProductList.discountper,
                notify_me: notifyMe1,
                isFavourite: isFavourite1,
                isFavorite: isFavourite1,
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
            store_id: store_id,
            varient_id: ProductList.varient_id,
            product_id: ProductList.product_id,
            product_name: ProductList.product_name,
            product_image: ProductList.product_image + "?width=200&height=200&quality=100",
            thumbnail: ProductList.thumbnail,
            description: ProductList.description,
            price: parseFloat(priceval),
            mrp: parseFloat(mrpval),
            varient_image: ProductList.varient_image,
            discountper: ProductList.discountper,
            avgrating: 0,
            notify_me: notifyMe,
            isFavourite: isFavourite,
            isFavorite: isFavourite,
            cart_qty: cartQty,
            total_cart_qty: cartQty,
            subcartQty: subcartQty,
            total_subcart_qty: subcartQty,
            unit: ProductList.unit,
            quantity: ProductList.quantity,
            type: ProductList.type,
            percentage: ProductList.percentage,
            isSubscription: isSubscription,
            subscription_price: subscription_price,
            availability: ProductList.availability,
            feature_tags: feature_tags,
            country_icon: countryicon,
            features: features,
            varients: varients
        };

        customizedProductData.push(customizedProduct);
    }

    return customizedProductData;
}

const gettrendbrands = async (appDetatils) => {
    const store_id = appDetatils.store_id;

    const baseurl = process.env.BUNNY_NET_IMAGE;
    return await knex('brands')
        .select('brands.cat_id', 'brands.title', knex.raw(`? || brands.image as image`, [baseurl]))
        .join('trending_brand', 'trending_brand.category_id', 'brands.cat_id')
        .where('brands.status', 1)
        .orderBy('brands.cat_id', 'asc');

}

const getUniversalSearch = async (appDetatils) => {

    await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');


    const store_id = appDetatils.store_id
    const keyword = appDetatils.keyword
    // const user_id = appDetatils.user_id
    if (appDetatils.user_id != "null") {
        user_id = appDetatils.user_id
    } else {
        user_id = appDetatils.device_id
    }
    const device_id = appDetatils.device_id
    const byname = appDetatils.byname
    const subcatid = appDetatils.sub_cat_id
    const cat_id = appDetatils.cat_id


    const pageFilter = appDetatils.page; // You can adjust the page number dynamically
    const perPage = appDetatils.perpage;

    const minprice = parseFloat(appDetatils.min_price)
    const maxprice = parseFloat(appDetatils.max_price)
    const mindiscount = parseFloat(appDetatils.min_discount)
    const maxdiscount = parseFloat(appDetatils.max_discount)


    let categoryarray;
    if (cat_id !== "null") {
        categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
    }



    const topsellingsQuery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
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
        .where('product.is_delete', 0);


    //if (cat_id !== "null") {
    //if (categoryarray.length > 0)  {
    if (categoryarray) {
        topsellingsQuery.whereIn('product.cat_id', categoryarray);
    }

    if (subcatid !== "null") {
        topsellingsQuery.where('product.cat_id', subcatid);
    }

    if (keyword) {
        // additionalcat.where('title', byname);
        topsellingsQuery.where('product.product_name', 'like', `%${keyword}%`)
        //additionalcat.whereLike('title', byname);
    }

    if (byname) {
        topsellingsQuery.orderBy('product.product_name', 'byname');
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

    //   if (sortprice === 'ltoh') {
    //     topsellingsQuery.orderBy('store_products.price', 'ASC');
    //   }

    //   if (sortprice === 'htol') {
    //     topsellingsQuery.orderBy('store_products.price', 'DESC');
    //   }

    //   if (sortname === 'atoz') {
    //     topsellingsQuery.orderBy('product.product_name', 'ASC');
    //   }

    //   if (sortname === 'ztoa') {
    //     topsellingsQuery.orderBy('product.product_name', 'DESC');
    //   }


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
        // Recent search history logic moved to controller

        if (user_id) {

            check = await knex('recent_search')
                .where('user_id', user_id);

            checkww = await knex('recent_search')
                .where('user_id', user_id)
                .first();

            deletesame = await knex('recent_search')
                .where('keyword', keyword)
                .delete();

            if (check.length >= 10) {
                chec = await knex('recent_search')
                    .where('id', checkww.id)
                    .delete();
            }
            add = await knex('recent_search')
                .insert({
                    user_id: user_id,
                    keyword: keyword
                });





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
                .whereNull('subscription_flag')
                .where('store_id', store_id)
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
        const customizedProduct = {
            stock: ProductList.stock,
            varient_id: ProductList.varient_id,
            product_id: ProductList.product_id,
            product_name: ProductList.product_name,
            product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
            thumbnail: ProductList.thumbnail,
            description: ProductList.description,
            price: price,
            mrp: ProductList.mrp,
            unit: ProductList.unit,
            quantity: ProductList.quantity,
            type: ProductList.type,
            discountper: ProductList.discountper,
            avgrating: 0,
            notify_me: notifyMe,
            isFavourite: isFavourite,
            isFavorite: isFavourite,
            cart_qty: cartQty,
            percentage: ProductList.percentage,
            isSubscription: isSubscription,
            subscription_price: subscription_price,
            availability: ProductList.availability,
            countrating: 0,
            varients: null
            // Add or modify properties as needed
        };

        customizedProductData.push(customizedProduct);

    }

    return customizedProductData;
};

const getSearchbybannerold = async (appDetatils) => {
    await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');
    const store_id = appDetatils.store_id
    const keyword = appDetatils.keyword
    //const user_id = appDetatils.user_id
    if (appDetatils.user_id != "null") {
        user_id = appDetatils.user_id
    } else {
        user_id = appDetatils.device_id
    }
    const device_id = appDetatils.device_id
    const byname = appDetatils.byname
    const subcatid = appDetatils.sub_cat_id
    const cat_id = appDetatils.cat_id
    const sortprice = appDetatils.sortprice
    const sortname = appDetatils.sortname

    const pageFilter = appDetatils.page; // You can adjust the page number dynamically
    const perPage = appDetatils.perpage;

    const minprice = parseFloat(appDetatils.min_price)
    const maxprice = parseFloat(appDetatils.max_price)
    const mindiscount = parseFloat(appDetatils.min_discount)
    const maxdiscount = parseFloat(appDetatils.max_discount)
    // let words = 0;
    //  if(appDetatils.keyword != "All"){
    //   const inputString = keyword;
    //   let words = inputString.split(/\s+/).filter(word => word.length > 3);
    //  }

    let words = [];
    if (appDetatils.keyword !== 'All') {
        words = keyword.split(/\s+/).filter(word => word.length > 3);
    }

    stock = appDetatils.stock;
    if (stock == 'out') {
        stock = "<";
        by = 1;

    } else
        if (stock == 'all' || stock == "null") {
            stock = "!=";
            by = "null";
        }
        else {
            stock = ">";
            by = 0;
        }

    //let categoryarray1 = [];
    let categoryarray;
    if (cat_id !== "null") {
        categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
    }
    //const categoryarray = categoryarray1.push(cat_id); 
    const topsellingsQuery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
        .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
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
            'tbl_country.country_icon',
            'product.percentage',
            'product.availability',
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
        .where('product.is_delete', 0);




    //if (cat_id !== "null") {
    //if (categoryarray.length > 0)  {
    if (categoryarray) {
        topsellingsQuery.whereIn('product.cat_id', categoryarray);
    }

    if (subcatid !== "null") {
        topsellingsQuery.where('product.cat_id', subcatid);
    }



    // if( words.length > 0 && keyword != "All"){

    //   //const patterns = ['Milk', 'bannana', 'Combo'];
    //   const patterns = words
    //     topsellingsQuery.where(builder => {
    //       patterns.forEach(pattern => {
    //         builder.orWhere('product.product_name', 'like', `%${pattern}%`);
    //       });
    //     });
    //      //additionalcat.whereLike('title', byname);
    //  }

    if (stock) {
        topsellingsQuery.where('store_products.stock', stock, by)
    }
    if (byname) {
        topsellingsQuery.orderBy('product.product_name', 'byname');
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

    if (words.length > 0 && keyword !== 'All') {
        topsellingsQuery.where(builder => {
            words.forEach(pattern => {
                builder.orWhere('product.product_name', 'like', `%${pattern}%`);
            });
        });
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

        //for deviceid
        if (device_id) {

            check = await knex('recent_search')
                .where('device_id', device_id);

            checkww = await knex('recent_search')
                .where('device_id', device_id)
                .first();

            deletesame = await knex('recent_search')
                .where('keyword', keyword)
                .delete();

            if (check.length >= 10) {
                chec = await knex('recent_search')
                    .where('id', checkww.id)
                    .delete();
            }


            add = await knex('recent_search')
                .insert({
                    device_id: device_id,
                    keyword: keyword
                });



        }

        if (user_id) {


            check = await knex('recent_search')
                .where('user_id', user_id);

            checkww = await knex('recent_search')
                .where('user_id', user_id)
                .first();

            deletesame = await knex('recent_search')
                .where('keyword', keyword)
                .delete();

            if (check.length >= 10) {
                chec = await knex('recent_search')
                    .where('id', checkww.id)
                    .delete();
            }
            add = await knex('recent_search')
                .insert({
                    user_id: user_id,
                    keyword: keyword
                });





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
                .whereNull('subscription_flag')
                .where('store_id', store_id)
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
            product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
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
            varients: null,
            // Add or modify properties as needed
        };

        customizedProductData.push(customizedProduct);

    }

    return customizedProductData;
}

/** Limits per-log array size for searchbybanner debug (full count always logged). */
const SEARCHBYBANNER_DEBUG_MAX_IDS = 500;
const searchbybannerDebugSlice = (ids) => {
    const arr = [...new Set(ids.map(Number).filter((n) => Number.isFinite(n)))].sort((a, b) => a - b);
    if (arr.length <= SEARCHBYBANNER_DEBUG_MAX_IDS) return { count: arr.length, ids: arr };
    return { count: arr.length, ids_sample: arr.slice(0, SEARCHBYBANNER_DEBUG_MAX_IDS), truncated: true };
};

const getSearchbybanner = async (appDetatils) => {

    const store_id = appDetatils.store_id;
    const keyword = appDetatils.keyword;
    const baseurl = process.env.BUNNY_NET_IMAGE;

    let user_id;
    if (appDetatils.user_id != "null") {
        user_id = appDetatils.user_id;
    } else {
        user_id = appDetatils.device_id;
    }

    const device_id = appDetatils.device_id;
    const byname = appDetatils.byname;
    const subcatid = appDetatils.sub_cat_id;
    const cat_id = appDetatils.cat_id;
    const brand_id = appDetatils.brand_id;
    const sortprice = appDetatils.sortprice;
    const sortname = appDetatils.sortname;
    const banner_id = appDetatils.banner_id;
    // Normalize banner_type to avoid case/whitespace mismatches from clients
    const banner_type = String(appDetatils.banner_type ?? 'store').trim().toLowerCase();
    const debugPrefix = '[searchbybanner-debug]';
    /** Which banner row branch populated product ids before sequencing: variant | subcat | parent | none */
    let primaryBannerProductSource = 'none';

    // Search History Logic (Moved outside the loop)
    /* if (keyword && keyword !== 'All') {
        const handleRecentSearch = async (u_id, d_id) => {
            const searchUser = u_id || d_id;
            if (!searchUser) return;
  
            const check = await knex('recent_search')
                .where(u_id ? 'user_id' : 'device_id', searchUser);
            
            await knex('recent_search')
                .where(u_id ? 'user_id' : 'device_id', searchUser)
                .where('keyword', keyword)
                .delete();
  
            if (check.length >= 10) {
                const firstEntry = await knex('recent_search')
                    .where(u_id ? 'user_id' : 'device_id', searchUser)
                    .orderBy('id', 'asc')
                    .first();
                if (firstEntry) {
                    await knex('recent_search').where('id', firstEntry.id).delete();
                }
            }
  
            await knex('recent_search').insert({
                [u_id ? 'user_id' : 'device_id']: searchUser,
                keyword: keyword
            });
        };
  
        if (user_id || device_id) {
            await handleRecentSearch(appDetatils.user_id !== "null" ? user_id : null, device_id);
        }
    } */

    let banner = null;
    if (banner_type === 'product') {
        banner = await knex('sec_banner')
            .select('banner_id', 'banner_name', knex.raw("CONCAT(?::text, banner_image) as banner_image", [baseurl]), 'parent_cat_id', 'cat_id', 'varient_id')
            .where('banner_id', banner_id)
            .first();
    } else {
        banner = await knex('store_banner')
            .select('banner_id', 'banner_name', knex.raw("CONCAT(?::text, banner_image) as banner_image", [baseurl]), 'parent_cat_id', 'cat_id', 'varient_id')
            .where('banner_id', banner_id)
            .first();
    }
    console.log(`${debugPrefix} request`, {
        store_id,
        banner_id,
        banner_type,
        subcatid,
        cat_id,
        brand_id,
        byname,
        keyword,
        sortprice,
        sortname,
        min_price: appDetatils.min_price,
        max_price: appDetatils.max_price,
        min_discount: appDetatils.min_discount,
        max_discount: appDetatils.max_discount,
        page: appDetatils.page,
        perpage: appDetatils.perpage
    });
    console.log(`${debugPrefix} banner_row`, {
        banner_table: banner_type === 'product' ? 'sec_banner' : 'store_banner',
        banner_exists: !!banner,
        banner_name: banner?.banner_name ?? null,
        raw_varient_id: banner?.varient_id ?? null,
        raw_cat_id: banner?.cat_id ?? null,
        raw_parent_cat_id: banner?.parent_cat_id ?? null
    });

    // If the banner explicitly targets variant IDs, we must:
    // - select products through those variants
    // - and return ONLY those variants (not every variant of the product)
    let bannerVariantIds = null;
    if (banner?.varient_id && String(banner.varient_id).trim() !== '') {
        // varient_id is a text column in PostgreSQL (store_banner/sec_banner),
        // and may come as "1,2", "[1,2]", "1|2", etc.
        // Extract numeric IDs robustly to avoid false-empty parsing.
        const parsed = (String(banner.varient_id).match(/\d+/g) || [])
            .map(v => parseInt(v, 10))
            .filter(v => Number.isFinite(v) && v > 0);
        if (parsed.length > 0) bannerVariantIds = new Set(parsed);
    }
    const parsedCatIds = banner?.cat_id
        ? banner.cat_id.split(',').map((v) => Number(String(v).trim())).filter((v) => Number.isFinite(v) && v > 0)
        : [];
    const parsedParentCatIds = banner?.parent_cat_id
        ? banner.parent_cat_id.split(',').map((v) => Number(String(v).trim())).filter((v) => Number.isFinite(v) && v > 0)
        : [];
    const parsedVariantIdList = bannerVariantIds ? Array.from(bannerVariantIds).sort((a, b) => a - b) : [];
    console.log(`${debugPrefix} parsed_ids`, {
        parsed_variant_ids: parsedVariantIdList,
        parsed_variant_count: parsedVariantIdList.length,
        parsed_cat_ids: parsedCatIds,
        parsed_cat_count: parsedCatIds.length,
        parsed_parent_cat_ids: parsedParentCatIds,
        parsed_parent_cat_count: parsedParentCatIds.length
    });

    const categoryProductIdsSet = new Set();

    if (banner) {
        if (bannerVariantIds && bannerVariantIds.size > 0) {
            primaryBannerProductSource = 'variant';
            // Priority 1: banner.varient_id contains VARIANT IDs, but the main query filters by product_id.
            // Map variant IDs -> product IDs (scoped to this store and only active/approved items).
            const variantIds = Array.from(bannerVariantIds || []);

            if (variantIds.length > 0) {
                const variantRowsInDb = await knex('product_varient')
                    .whereIn('varient_id', variantIds)
                    .where('is_delete', 0)
                    .pluck('varient_id');
                const variantRowsSet = new Set(variantRowsInDb);
                const variantIdsUnknown = variantIds.filter((v) => !variantRowsSet.has(v));

                const variantProductIds = await knex('store_products')
                    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                    .join('product', 'product_varient.product_id', '=', 'product.product_id')
                    .where('store_products.store_id', store_id)
                    .whereIn('product_varient.varient_id', variantIds)
                    .andWhere('product.hide', '=', 0)
                    .andWhere('product.is_delete', '=', 0)
                    .andWhere('product.approved', '=', 1)
                    .andWhere('product_varient.approved', '=', 1)
                    .andWhere('product_varient.is_delete', '=', 0)
                    .andWhere('store_products.stock', '>', 0)
                    .andWhere('store_products.is_deleted', '=', 0)
                    .whereNotNull('store_products.price')
                    .pluck('product.product_id');

                const variantIdsPassingStoreJoin = await knex('store_products')
                    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                    .join('product', 'product_varient.product_id', '=', 'product.product_id')
                    .where('store_products.store_id', store_id)
                    .whereIn('product_varient.varient_id', variantIds)
                    .andWhere('product.hide', '=', 0)
                    .andWhere('product.is_delete', '=', 0)
                    .andWhere('product.approved', '=', 1)
                    .andWhere('product_varient.approved', '=', 1)
                    .andWhere('product_varient.is_delete', '=', 0)
                    .andWhere('store_products.stock', '>', 0)
                    .andWhere('store_products.is_deleted', '=', 0)
                    .whereNotNull('store_products.price')
                    .distinct('product_varient.varient_id')
                    .pluck('product_varient.varient_id');
                const passingSet = new Set(variantIdsPassingStoreJoin);
                const variantIdsFailingStoreRules = variantIds.filter((v) => variantRowsSet.has(v) && !passingSet.has(v));

                variantProductIds.forEach(id => categoryProductIdsSet.add(id));
                console.log(`${debugPrefix} variant_path`, {
                    variant_ids_requested: searchbybannerDebugSlice(variantIds),
                    variant_ids_in_product_varient_not_deleted: searchbybannerDebugSlice(variantRowsInDb),
                    variant_ids_unknown_in_db: searchbybannerDebugSlice(variantIdsUnknown),
                    variant_ids_failing_store_product_rules: searchbybannerDebugSlice(variantIdsFailingStoreRules),
                    mapped_product_ids: searchbybannerDebugSlice(variantProductIds)
                });
            }
        } else if (banner.cat_id && banner.cat_id !== '') {
            // Priority 2: Use products in these specific subcategories,
            // including additional-category mappings via product_cat.
            const subcategories = banner.cat_id
                .split(',')
                .map(v => Number(String(v).trim()))
                .filter(v => Number.isFinite(v) && v > 0);

            if (subcategories.length === 0) {
                // no valid category ids, skip
                console.log(`${debugPrefix} cat_path_skipped_invalid_ids`, {
                    raw_cat_id: banner.cat_id
                });
            } else {
            primaryBannerProductSource = 'subcat';
            const catProducts = await knex('product')
                .join('product_varient', 'product.product_id', 'product_varient.product_id')
                .join('store_products', 'store_products.varient_id', '=', 'product_varient.varient_id')
                .where('store_products.store_id', store_id)
                .whereIn('product.cat_id', subcategories)
                .andWhere('product.hide', '=', 0)
                .andWhere('product.is_delete', '=', 0)
                .andWhere('product.approved', '=', 1)
                .andWhere('product_varient.approved', '=', 1)
                .andWhere('product_varient.is_delete', '=', 0)
                .andWhere('store_products.stock', '>', 0)
                .andWhere('store_products.is_deleted', '=', 0)
                .whereNotNull('store_products.price')
                .pluck('product.product_id');
            catProducts.forEach(id => categoryProductIdsSet.add(id));

            const extraCatProducts = await knex('product')
                .join('product_cat', 'product_cat.product_id', 'product.product_id')
                .join('product_varient', 'product.product_id', 'product_varient.product_id')
                .join('store_products', 'store_products.varient_id', '=', 'product_varient.varient_id')
                .where('store_products.store_id', store_id)
                .whereIn('product_cat.cat_id', subcategories)
                .andWhere('product.hide', '=', 0)
                .andWhere('product.is_delete', '=', 0)
                .andWhere('product.approved', '=', 1)
                .andWhere('product_varient.approved', '=', 1)
                .andWhere('product_varient.is_delete', '=', 0)
                .andWhere('store_products.stock', '>', 0)
                .andWhere('store_products.is_deleted', '=', 0)
                .whereNotNull('store_products.price')
                .pluck('product.product_id');
            extraCatProducts.forEach(id => categoryProductIdsSet.add(id));
            const catProductSet = new Set(catProducts);
            const onlyInExtra = extraCatProducts.filter((id) => !catProductSet.has(id));
            console.log(`${debugPrefix} cat_path`, {
                subcategory_ids: searchbybannerDebugSlice(subcategories),
                main_cat_product_ids: searchbybannerDebugSlice(catProducts),
                product_cat_only_product_ids: searchbybannerDebugSlice(onlyInExtra),
                product_cat_all_product_ids: searchbybannerDebugSlice(extraCatProducts),
                merged_unique_count: new Set([...catProducts, ...extraCatProducts]).size
            });
            }
        } else if (banner.parent_cat_id && banner.parent_cat_id !== '') {
            // Priority 3: Use products in all subcategories under these parent categories,
            // including additional-category mappings via product_cat.
            const parent_cat_ids = banner.parent_cat_id
                .split(',')
                .map((v) => Number(String(v).trim()))
                .filter((v) => Number.isFinite(v) && v > 0);

            if (parent_cat_ids.length === 0) {
                console.log(`${debugPrefix} parent_path_skipped_invalid_ids`, {
                    raw_parent_cat_id: banner.parent_cat_id
                });
            } else {
            primaryBannerProductSource = 'parent';
            // 1) Find all subcategory IDs under these parents
            const subcategories = await knex('categories')
                .whereIn('parent', parent_cat_ids)
                .where('status', 1)
                .pluck('cat_id');
            console.log(`${debugPrefix} parent_path_subcategories`, {
                parent_cat_ids: searchbybannerDebugSlice(parent_cat_ids),
                resolved_subcategory_ids: searchbybannerDebugSlice(subcategories)
            });

            if (subcategories.length > 0) {
                // 2) Products whose main cat_id is one of these subcategories
                const parentCatProducts = await knex('product')
                    .join('product_varient', 'product.product_id', 'product_varient.product_id')
                    .join('store_products', 'store_products.varient_id', '=', 'product_varient.varient_id')
                    .where('store_products.store_id', store_id)
                    .whereIn('product.cat_id', subcategories)
                    .andWhere('product.hide', '=', 0)
                    .andWhere('product.is_delete', '=', 0)
                    .andWhere('product.approved', '=', 1)
                    .andWhere('product_varient.approved', '=', 1)
                    .andWhere('product_varient.is_delete', '=', 0)
                    .andWhere('store_products.stock', '>', 0)
                    .andWhere('store_products.is_deleted', '=', 0)
                    .whereNotNull('store_products.price')
                    .pluck('product.product_id');
                parentCatProducts.forEach(id => categoryProductIdsSet.add(id));

                // 3) Products mapped to these subcategories via additional category mappings
                const extraCatProducts = await knex('product')
                    .join('product_cat', 'product_cat.product_id', 'product.product_id')
                    .join('product_varient', 'product.product_id', 'product_varient.product_id')
                    .join('store_products', 'store_products.varient_id', '=', 'product_varient.varient_id')
                    .where('store_products.store_id', store_id)
                    .whereIn('product_cat.cat_id', subcategories)
                    .andWhere('product.hide', '=', 0)
                    .andWhere('product.is_delete', '=', 0)
                    .andWhere('product.approved', '=', 1)
                    .andWhere('product_varient.approved', '=', 1)
                    .andWhere('product_varient.is_delete', '=', 0)
                    .andWhere('store_products.stock', '>', 0)
                    .andWhere('store_products.is_deleted', '=', 0)
                    .whereNotNull('store_products.price')
                    .pluck('product.product_id');
                extraCatProducts.forEach(id => categoryProductIdsSet.add(id));
                const parentCatProductSet = new Set(parentCatProducts);
                const onlyInExtraParent = extraCatProducts.filter((id) => !parentCatProductSet.has(id));
                console.log(`${debugPrefix} parent_path_products`, {
                    main_cat_product_ids: searchbybannerDebugSlice(parentCatProducts),
                    product_cat_only_product_ids: searchbybannerDebugSlice(onlyInExtraParent),
                    product_cat_all_product_ids: searchbybannerDebugSlice(extraCatProducts),
                    merged_unique_count: new Set([...parentCatProducts, ...extraCatProducts]).size
                });
            } else {
                console.log(`${debugPrefix} parent_path_no_subcategories`, {
                    parent_cat_ids: searchbybannerDebugSlice(parent_cat_ids),
                    hint: 'No active subcategories (categories.parent in parent_cat_ids, status=1)'
                });
            }
            }
        }
    }

    if (banner && primaryBannerProductSource === 'none') {
        console.log(`${debugPrefix} banner_no_variant_cat_parent`, {
            message: 'Banner row has no usable varient_id / cat_id / parent_cat_id for product resolution (sequencing may still add ids)'
        });
    }
    if (!banner) {
        console.log(`${debugPrefix} banner_not_found`, { banner_id, banner_type });
    }

    // Always Include: Products explicitly mapped to this banner in the sequencing table
    const sequencedProductsQuery = knex('parent_category_product_sequences').select('product_id');
    if (banner_type === 'store') {
        sequencedProductsQuery.where('store_banner_id', banner_id);
    } else {
        sequencedProductsQuery.where('sec_banner_id', banner_id);
    }
    const sequencedIds = await sequencedProductsQuery.pluck('product_id');
    const setBeforeSequencing = new Set(categoryProductIdsSet);
    sequencedIds.forEach(id => categoryProductIdsSet.add(id));
    const addedOnlyViaSequencing = sequencedIds.filter((id) => !setBeforeSequencing.has(id));

    const categoryProductIds = Array.from(categoryProductIdsSet);
    console.log(`${debugPrefix} candidate_products`, {
        primary_banner_product_source: primaryBannerProductSource,
        product_ids_before_sequencing: searchbybannerDebugSlice(Array.from(setBeforeSequencing)),
        sequenced_product_ids: searchbybannerDebugSlice(sequencedIds),
        product_ids_added_only_by_sequence_table: searchbybannerDebugSlice(addedOnlyViaSequencing),
        merged_candidate_product_ids: searchbybannerDebugSlice(categoryProductIds),
        merged_candidate_count: categoryProductIds.length
    });

    const pageFilter = appDetatils.page;
    const perPage = appDetatils.perpage;
    const minprice = parseFloat(appDetatils.min_price);
    const maxprice = parseFloat(appDetatils.max_price);
    const mindiscount = parseFloat(appDetatils.min_discount);
    const maxdiscount = parseFloat(appDetatils.max_discount);

    let words = [];
    /* if (appDetatils.keyword !== 'All') {
        words = keyword.split(/\s+/).filter(word => word.length > 3);
    } */

    const currentDate = new Date();
    const topsellingsQuery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text'), '=', 'product.country_id')
        .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
        .leftJoin('parent_category_product_sequences', function () {
            this.on('product.product_id', '=', 'parent_category_product_sequences.product_id');
            if (banner_type === 'store') {
                this.andOn('parent_category_product_sequences.store_banner_id', '=', knex.raw('?', [banner_id]))
                    .andOn('parent_category_product_sequences.store_banner_id', '!=', 999);
            } else if (banner_type === 'product') {
                this.andOn('parent_category_product_sequences.sec_banner_id', '=', knex.raw('?', [banner_id]))
                    .andOn('parent_category_product_sequences.sec_banner_id', '!=', 999);
            }
        })
        .select(
            knex.raw('MAX(store_products.stock) as stock'),
            knex.raw('MAX(product_varient.varient_id) as varient_id'),
            knex.raw('MAX(product_varient.description) as description'),
            'product.product_id',
            'product.product_name',
            'product.product_image',
            'product.thumbnail',
            knex.raw('MAX(store_products.price) as price'),
            knex.raw('MAX(store_products.mrp) as mrp'),
            knex.raw('MAX(product_varient.unit) as unit'),
            knex.raw('MAX(product_varient.quantity) as quantity'),
            'product.type',
            knex.raw('MAX(tbl_country.country_icon) as country_icon'),
            'product.percentage',
            'product.availability',
            'product.brand_id',
            'product.cat_id',
            'product.fcat_id',
            'product.is_customized',
            knex.raw('100-((MAX(store_products.price)*100)/NULLIF(MAX(store_products.mrp), 0)) as discountper'),
            knex.raw('100-((MAX(deal_product.deal_price)*100)/NULLIF(MAX(store_products.mrp), 0)) as discountper1')
        )
        .groupBy(
            'product.product_id',
            'product.product_name',
            'product.product_image',
            'product.thumbnail',
            'product.type',
            'product.percentage',
            'product.availability',
            'product.brand_id',
            'product.cat_id',
            'product.fcat_id',
            'product.is_customized',
            'parent_category_product_sequences.store_banner_sequence',
            'parent_category_product_sequences.sec_banner_sequence'
        )
        .where('store_products.store_id', store_id)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('product.approved', 1)
        .where('product.is_zap', true)
        .where('product_varient.approved', 1)
        .where('product_varient.is_delete', 0)
        .where('store_products.stock', '>', 0)
        .where('store_products.is_deleted', 0)
        .whereNotNull('store_products.price')
        // Exclude ONLY products that are offer products for today.
        // Include:
        // - Non-offer products
        // - Offer products with NULL offer_date
        // - Offer products where offer_date is NOT today (past or future)
        .where(builder => {
            builder
                .where('product.is_offer_product', '!=', 1)
                .orWhereNull('product.offer_date')
                .orWhereRaw('product.offer_date::date != CURRENT_DATE');
        });

    // Apply product-id restriction only when it exists.
    // For variant-targeted banners, variant filter alone is enough and avoids false empty results.
    if (categoryProductIds.length > 0) {
        topsellingsQuery.whereIn('product.product_id', categoryProductIds);
    } else if (!(bannerVariantIds && bannerVariantIds.size > 0)) {
        console.log(`${debugPrefix} early_return_empty`, {
            reason: 'no categoryProductIds and no bannerVariantIds',
            primary_banner_product_source: primaryBannerProductSource,
            merged_candidate_count: categoryProductIds.length,
            sequenced_ids_count: sequencedIds.length,
            parsed_variant_count: parsedVariantIdList.length
        });
        return { 'products': [], 'banner': banner };
    }

    // If the banner targets specific variants, restrict the main selection to ONLY those variants.
    // This ensures the "main" varient_id in the response is banner-consistent.
    if (bannerVariantIds && bannerVariantIds.size > 0) {
        topsellingsQuery.whereIn('product_varient.varient_id', Array.from(bannerVariantIds));
    }
    console.log(`${debugPrefix} query_filters_applied`, {
        has_variant_filter: !!(bannerVariantIds && bannerVariantIds.size > 0),
        variant_filter_count: bannerVariantIds ? bannerVariantIds.size : 0,
        has_category_product_filter: categoryProductIds.length > 0
    });

    const subcatFilterActive = subcatid !== 'null' && subcatid != null && subcatid !== '';
    const priceFilterActive =
        (minprice === 0 || !Number.isNaN(minprice)) && !Number.isNaN(maxprice) && (minprice === 0 || minprice) && maxprice;
    const discountFilterActive =
        !Number.isNaN(mindiscount) &&
        !Number.isNaN(maxdiscount) &&
        mindiscount &&
        maxdiscount;

    if (subcatid !== "null") {
        topsellingsQuery.where('product.cat_id', subcatid);
    }

    if ((minprice === 0 || minprice) && maxprice) {
        topsellingsQuery.whereBetween('store_products.price', [minprice, maxprice]);
    }

    if (mindiscount && maxdiscount) {
        topsellingsQuery.havingRaw('(100-((MAX(store_products.price)*100)/NULLIF(MAX(store_products.mrp), 0)) BETWEEN ? AND ?) OR (100-((MAX(deal_product.deal_price)*100)/NULLIF(MAX(store_products.mrp), 0)) BETWEEN ? AND ?)', [
            mindiscount, maxdiscount, mindiscount, maxdiscount
        ]);
    }

    console.log(`${debugPrefix} main_query_client_narrowing`, {
        sub_cat_id_applied: subcatFilterActive,
        sub_cat_id_value: subcatid,
        price_between_applied: !!priceFilterActive,
        minprice,
        maxprice,
        discount_having_applied: !!discountFilterActive,
        mindiscount,
        maxdiscount,
        static_main_query_excludes:
            'offer product with offer_date = today; hide/delete/unapproved; stock<=0; null store price'
    });

    // Sequencing
    if (banner_type === 'store') {
        topsellingsQuery.orderByRaw('CASE WHEN parent_category_product_sequences.store_banner_sequence != 999 THEN parent_category_product_sequences.store_banner_sequence ELSE 9999 END ASC');
    } else if (banner_type === 'product') {
        topsellingsQuery.orderByRaw('CASE WHEN parent_category_product_sequences.sec_banner_sequence != 999 THEN parent_category_product_sequences.sec_banner_sequence ELSE 9999 END ASC');
    }

    if (sortprice === 'ltoh') topsellingsQuery.orderBy('store_products.price', 'ASC');
    if (sortprice === 'htol') topsellingsQuery.orderBy('store_products.price', 'DESC');
    if (sortname === 'atoz') topsellingsQuery.orderBy('product.product_name', 'ASC');
    if (sortname === 'ztoa') topsellingsQuery.orderBy('product.product_name', 'DESC');

    /* if (words.length > 0 && keyword !== 'All') {
        topsellingsQuery.where(builder => {
            words.forEach(pattern => {
                builder.orWhere('product.product_name', 'like', `%${pattern}%`);
            });
        });
    } */

    const totalproducts = await topsellingsQuery;
    const totalPages = Math.ceil(totalproducts.length / perPage);

    const productDetail = await topsellingsQuery.offset((pageFilter - 1) * perPage).limit(perPage);
    console.log(`${debugPrefix} query_result`, {
        totalproducts_count: totalproducts.length,
        page_products_count: productDetail.length,
        totalPages,
        page: pageFilter,
        perPage
    });

    const includedIdSet = new Set(totalproducts.map((p) => p.product_id));
    const includedSummaries = totalproducts.map((p) => ({
        product_id: p.product_id,
        product_name: p.product_name,
        varient_id: p.varient_id,
        cat_id: p.cat_id,
        price: p.price
    }));
    const includedSampleLimit = 100;
    console.log(`${debugPrefix} included_products`, {
        distinct_product_count: includedIdSet.size,
        total_grouped_rows: totalproducts.length,
        sample_rows: includedSummaries.slice(0, includedSampleLimit),
        sample_truncated: includedSummaries.length > includedSampleLimit
    });

    const filteredOutFromCandidates =
        categoryProductIds.length > 0 ? categoryProductIds.filter((id) => !includedIdSet.has(id)) : [];
    const sequencedNotInFinal = sequencedIds.filter((id) => !includedIdSet.has(id));
    console.log(`${debugPrefix} filtered_out_vs_main_query`, {
        had_where_in_candidate_product_ids: categoryProductIds.length > 0,
        candidate_product_ids_removed_by_main_query_count: filteredOutFromCandidates.length,
        candidate_product_ids_removed_by_main_query: searchbybannerDebugSlice(filteredOutFromCandidates),
        sequenced_ids_not_in_final_result_count: sequencedNotInFinal.length,
        sequenced_ids_not_in_final_result: searchbybannerDebugSlice(sequencedNotInFinal),
        note: 'Candidates can be dropped by client sub_cat/price/discount filters, today-only offer rule, or failing grouped variant/store row constraints'
    });

    if (totalproducts.length === 0) {
        console.log(`${debugPrefix} empty_banner_result_diagnosis`, {
            primary_banner_product_source: primaryBannerProductSource,
            merged_candidate_count: categoryProductIds.length,
            sequenced_ids_count: sequencedIds.length,
            sub_cat_id_applied: subcatFilterActive,
            price_between_applied: !!priceFilterActive,
            discount_having_applied: !!discountFilterActive,
            has_variant_filter: !!(bannerVariantIds && bannerVariantIds.size > 0)
        });
    }

    const pIds = productDetail.map(p => p.product_id);
    const vIds = productDetail.map(p => p.varient_id);

    if (pIds.length === 0) {
        console.log(`${debugPrefix} empty_page_slice`, {
            totalproducts_count: totalproducts.length,
            page: pageFilter,
            perPage,
            message: totalproducts.length > 0 ? 'Page offset beyond results' : 'No products matched main query'
        });
        return { 'products': [], 'banner': banner };
    }

    // Batch fetching metadata
    const [
        allDeals,
        allWishlist,
        allCartItems,
        allNotifyMe,
        allVariants,
        allImages,
        allFeatures,
        allFeatureTagsInfo
    ] = await Promise.all([
        knex('deal_product').whereIn('varient_id', vIds).where('store_id', store_id).where('valid_from', '<=', currentDate).where('valid_to', '>', currentDate),
        user_id ? knex('wishlist').whereIn('varient_id', vIds).where('user_id', user_id) : [],
        user_id ? knex('store_orders').whereIn('varient_id', vIds).where('store_approval', user_id).where('order_cart_id', 'incart') : [],
        user_id ? knex('product_notify_me').whereIn('varient_id', vIds).where('user_id', user_id) : [],
        knex('store_products')
            .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
            .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id', 'product_varient.description', 'store_products.price', 'store_products.mrp',
                'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity', knex.raw('100-((store_products.price*100)/NULLIF(store_products.mrp, 0)) as discountper'), 'product_varient.product_id')
            .where('store_products.store_id', store_id)
            .whereIn('product_varient.product_id', pIds)
            .whereNotNull('store_products.price')
            .where('product_varient.approved', 1)
            .where('product_varient.is_delete', 0)
            .modify(qb => {
                if (bannerVariantIds && bannerVariantIds.size > 0) {
                    qb.whereIn('product_varient.varient_id', Array.from(bannerVariantIds));
                }
            }),
        knex('product_images').select('product_id', 'type', knex.raw("CONCAT(?::text, image) as image", [baseurl])).whereIn('product_id', pIds).orderBy('type', 'DESC'),
        knex('product_features').select('product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
            .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
            .whereIn('product_id', pIds),
        knex('feature_categories').where('status', 1).select('id', knex.raw("CONCAT(?::text, image) as image", [baseurl]))
    ]);

    // Maps for fast lookup
    const dealMap = new Map(allDeals.map(d => [d.varient_id, d]));
    const wishlistMap = new Set(allWishlist.map(w => w.varient_id));
    const cartMap = new Map();
    allCartItems.forEach(c => {
        const key = `${c.varient_id}_${c.subscription_flag || 0}`;
        cartMap.set(key, c);
    });
    const notifyMap = new Set(allNotifyMe.map(n => n.varient_id));

    // variantMap: product_id -> Array of variants
    const variantMap = new Map();
    allVariants.forEach(v => {
        if (!variantMap.has(v.product_id)) variantMap.set(v.product_id, []);
        variantMap.get(v.product_id).push(v);
    });

    const imageMap = new Map();
    allImages.forEach(img => {
        if (!imageMap.has(img.product_id)) imageMap.set(img.product_id, []);
        imageMap.get(img.product_id).push(img);
    });

    const featureMap = new Map();
    allFeatures.forEach(f => {
        if (!featureMap.has(f.product_id)) featureMap.set(f.product_id, []);
        featureMap.get(f.product_id).push(f);
    });

    const featureTagInfoMap = new Map(allFeatureTagsInfo.map(ft => [ft.id, ft]));

    const customizedProductData = productDetail.map(ProductList => {
        const deal = dealMap.get(ProductList.varient_id);
        const price = deal ? deal.deal_price : ProductList.price;

        const isFavourite = wishlistMap.has(ProductList.varient_id) ? 'true' : 'false';
        const regularCartEntry = cartMap.get(`${ProductList.varient_id}_0`);
        const cartQty = regularCartEntry ? regularCartEntry.qty : 0;
        const subCartEntry = cartMap.get(`${ProductList.varient_id}_1`);
        const subcartQty = subCartEntry ? subCartEntry.qty : 0;
        const notifyMe = notifyMap.has(ProductList.varient_id) ? 'true' : 'false';
        const isSubscription = subCartEntry ? 'true' : 'false';

        const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
        const subscription_price = parseFloat((ProductList.mrp - sub_price).toFixed(2));
        const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

        const priceval = Number.isInteger(price) ? price + '.001' : price;
        const mrpval = Number.isInteger(ProductList.mrp) ? ProductList.mrp + '.001' : ProductList.mrp;

        const feature_tags = (ProductList.fcat_id || '').split(',').map(Number).map(id => featureTagInfoMap.get(id)).filter(Boolean);
        const features = featureMap.get(ProductList.product_id) || [];
        const productImages = imageMap.get(ProductList.product_id) || [];
        const fallbackImage = `${baseurl}${ProductList.product_image}`;

        let total_cart_qty = 0;
        let total_subcart_qty = 0;

        const variants = (variantMap.get(ProductList.product_id) || [])
            .filter(v => !bannerVariantIds || bannerVariantIds.size === 0 || bannerVariantIds.has(v.varient_id))
            .map(v => {
            const vWishlist = wishlistMap.has(v.varient_id) ? 'true' : 'false';
            const vNotify = notifyMap.has(v.varient_id) ? 'true' : 'false';
            const vCart = cartMap.get(`${v.varient_id}_0`);
            const vSubCart = cartMap.get(`${v.varient_id}_1`);
            const vCartQty = vCart ? vCart.qty : 0;
            const vSubCartQty = vSubCart ? vSubCart.qty : 0;

            total_cart_qty += vCartQty;
            total_subcart_qty += vSubCartQty;

            return {
                stock: v.stock,
                varient_id: v.varient_id,
                product_id: v.product_id,
                product_name: ProductList.product_name,
                product_image: productImages.length > 0 ? productImages[0].image + "?width=200&height=200&quality=100" : fallbackImage + "?width=200&height=200&quality=100",
                thumbnail: productImages.length > 0 ? productImages[0].image : ProductList.thumbnail,
                description: v.description,
                price: v.price,
                mrp: v.mrp,
                unit: v.unit,
                quantity: v.quantity,
                type: ProductList.type,
                discountper: ProductList.discountper,
                notify_me: vNotify,
                isFavourite: vWishlist,
                cart_qty: vCartQty,
                subcartQty: vSubCartQty,
                product_feature_id: vCart ? vCart.product_feature_id : 0,
                country_icon: countryicon,
            };
        });

        return {
            stock: ProductList.stock,
            cat_id: ProductList.cat_id,
            varient_id: ProductList.varient_id,
            product_id: ProductList.product_id,
            brand_id: ProductList.brand_id,
            product_name: ProductList.product_name,
            product_image: fallbackImage + "?width=200&height=200&quality=100",
            thumbnail: ProductList.thumbnail,
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
            discountper: 0,
            avgrating: 0,
            notify_me: notifyMe,
            isFavourite: isFavourite,
            cart_qty: cartQty,
            total_cart_qty: total_cart_qty,
            subcartQty: subcartQty,
            total_subcart_qty: total_subcart_qty,
            countrating: 0,
            country_icon: countryicon,
            feature_tags: feature_tags,
            features: features,
            varients: variants,
            page: pageFilter,
            is_customized: ProductList.is_customized,
            perPage: perPage,
            totalPages: totalPages
        };
    });

    return { 'products': customizedProductData, 'banner': banner };
};

const getSearchbystore = async (appDetatils) => {
    // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
    const store_id = appDetatils.store_id
    const keyword = appDetatils.keyword

    let user_id = null;
    if (appDetatils.user_id && appDetatils.user_id != "null" && !isNaN(parseInt(appDetatils.user_id))) {
        user_id = parseInt(appDetatils.user_id)
    }
    const device_id = appDetatils.device_id
    const byname = appDetatils.byname
    const subcatid = appDetatils.sub_cat_id
    const cat_id = appDetatils.cat_id
    const sortprice = appDetatils.sortprice
    const sortname = appDetatils.sortname

    const pageFilter = appDetatils.page || 1; // You can adjust the page number dynamically
    const perPage = appDetatils.perpage || 20;

    const minprice = (appDetatils.min_price && appDetatils.min_price !== 'null') ? parseFloat(appDetatils.min_price) : null;
    const maxprice = (appDetatils.max_price && appDetatils.max_price !== 'null') ? parseFloat(appDetatils.max_price) : null;
    const mindiscount = (appDetatils.min_discount && appDetatils.min_discount !== 'null') ? parseFloat(appDetatils.min_discount) : null;
    const maxdiscount = (appDetatils.max_discount && appDetatils.max_discount !== 'null') ? parseFloat(appDetatils.max_discount) : null;

    stock = appDetatils.stock;
    if (stock == 'out') {
        stock = "<";
        by = 1;

    } else
        if (stock == 'all' || stock == "null") {
            stock = "!=";
            by = "null";
        }
        else {
            stock = ">";
            by = 0;
        }

    //let categoryarray1 = [];
    let categoryarray;
    if (cat_id !== "null") {
        categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
    }
    //const categoryarray = categoryarray1.push(cat_id); 
    const topsellingsQuery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', function () {
            this.on(knex.raw('tbl_country.id::text'), '=', knex.raw('product.country_id'));
        })
        .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
        .select(
            knex.raw('MAX(store_products.stock) as stock'),
            knex.raw('MAX(product_varient.varient_id) as varient_id'),
            knex.raw('MAX(product_varient.description) as description'),
            'product.product_id',
            'product.product_name',
            'product.product_image',
            'product.thumbnail',
            knex.raw('MAX(store_products.price) as price'),
            knex.raw('MAX(store_products.mrp) as mrp'),
            knex.raw('MAX(product_varient.unit) as unit'),
            knex.raw('MAX(product_varient.quantity) as quantity'),
            'product.type',
            knex.raw('MAX(tbl_country.country_icon) as country_icon'),
            'product.percentage',
            'product.availability',
            'product.fcat_id',
            'product.is_customized',
            knex.raw('100 - (MAX(store_products.price) * 100 / MAX(store_products.mrp)) as discountper'),
            knex.raw('100 - (MAX(deal_product.deal_price) * 100 / MAX(store_products.mrp)) as discountper1')
        )
        .where('store_products.store_id', store_id)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('store_products.stock', '>', 0)
        .whereRaw(`(
          product.is_offer_product = 0 
          OR product.offer_date IS NULL 
          OR product.offer_date::date != CURRENT_DATE
        )`)
        .groupBy(
            'product.product_id',
            'product.product_name',
            'product.product_image',
            'product.thumbnail',
            'product.type',
            'product.percentage',
            'product.availability',
            'product.fcat_id',
            'product.is_customized'
        );


    if (keyword == 'daily') {
        // List of IDs
        const ids = [122, 2, 3, 16, 23, 149, 21, 55, 56, 57, 58, 59, 72, 94, 150, 9, 20, 132, 37, 48, 50, 99, 101, 102, 147, 180, 47, 49, 91, 100, 146, 148, 179];
        topsellingsQuery.whereIn('product.availability', ['quick', 'all']);
        topsellingsQuery.whereIn('product.cat_id', ids); // Filter by these IDs
        // PostgreSQL: Use CASE statement instead of MySQL FIELD() function
        const caseOrder = ids.map((id, index) => `WHEN ${id} THEN ${index}`).join(' ');
        topsellingsQuery.orderByRaw(`CASE product.cat_id ${caseOrder} ELSE 999 END`); // Maintain the custom order  
    }

    if (keyword == 'subscription') {
        // List of IDs
        const ids = [122, 2, 3, 16, 23, 149, 37, 48, 50, 99, 101, 102, 147, 180, 47, 49, 91, 100, 146, 148, 179, 9, 20, 132, 21, 55, 56, 57, 58, 59, 72, 94, 150];
        topsellingsQuery.whereIn('product.availability', ['subscription', 'all']);
        topsellingsQuery.whereIn('product.cat_id', ids); // Filter by these IDs
        // PostgreSQL: Use CASE statement instead of MySQL FIELD() function
        const caseOrder = ids.map((id, index) => `WHEN ${id} THEN ${index}`).join(' ');
        topsellingsQuery.orderByRaw(`CASE product.cat_id ${caseOrder} ELSE 999 END`); // Maintain the custom order

    }

    //if (cat_id !== "null") {
    //if (categoryarray.length > 0)  {
    if (categoryarray) {
        topsellingsQuery.whereIn('product.cat_id', categoryarray);
    }

    if (subcatid !== "null") {
        topsellingsQuery.where('product.cat_id', subcatid);
    }

    if (keyword && keyword != 'daily' && keyword != 'subscription' && keyword != 'null') {
        // additionalcat.where('title', byname);
        // Use ILIKE for case-insensitive search in PostgreSQL
        topsellingsQuery.whereRaw('LOWER(product.product_name) LIKE LOWER(?)', [`%${keyword}%`])
        //   topsellingsQuery.whereRaw('SOUNDEX(product.product_name) = SOUNDEX(?)', [keyword])
        //     .orWhere('product.product_name', 'like', `%${keyword}%`);
        //  topsellingsQuery.whereRaw("MATCH(product_name) AGAINST(? IN NATURAL LANGUAGE MODE)", [keyword]);
        //   topsellingsQuery.whereRaw('SOUNDEX(product.product_name) = SOUNDEX(?)', [keyword]);
        //additionalcat.whereLike('title', byname);
    }
    if (stock) {
        // topsellingsQuery.where('store_products.stock',stock,by)
    }
    if (byname) {
        // topsellingsQuery.orderBy('product.product_name', 'byname');
    }

    // return topsellingsQuery
    // Apply discount filters first (HAVING clause)
    if (mindiscount && maxdiscount) {
        topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [
            mindiscount,
            maxdiscount,
            mindiscount,
            maxdiscount,
        ]);
    }

    // Apply price filter (use HAVING for aggregated columns after GROUP BY)
    if (minprice !== null && maxprice !== null && !isNaN(minprice) && !isNaN(maxprice)) {
        topsellingsQuery.havingRaw('MAX(store_products.price) BETWEEN ? AND ?', [minprice, maxprice]);
    }

    // Apply sorting (use aggregate functions for price since we're using GROUP BY)
    if (sortprice === 'ltoh') {
        topsellingsQuery.orderByRaw('MAX(store_products.price) ASC');
    }

    if (sortprice === 'htol') {
        topsellingsQuery.orderByRaw('MAX(store_products.price) DESC');
    }

    if (sortname === 'atoz') {
        topsellingsQuery.orderBy('product.product_name', 'ASC');
    }

    if (sortname === 'ztoa') {
        topsellingsQuery.orderBy('product.product_name', 'DESC');
    }

    // Optimize: Run count and data queries in parallel
    // Fix: Use subquery approach - get distinct product_ids with filters, then count
    // This avoids GROUP BY issues and ensures accurate count
    const distinctProductsSubquery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', function () {
            this.on(knex.raw('tbl_country.id::text'), '=', knex.raw('product.country_id'));
        })
        .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
        .select('product.product_id')
        .where('store_products.store_id', store_id)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('store_products.stock', '>', 0)
        .whereRaw(`(
            product.is_offer_product = 0 
            OR product.offer_date IS NULL 
            OR product.offer_date::date != CURRENT_DATE
          )`)
        .groupBy('product.product_id');

    // Apply same filters as main query
    if (keyword && keyword != 'daily' && keyword != 'subscription' && keyword != 'null') {
        distinctProductsSubquery.whereRaw('LOWER(product.product_name) LIKE LOWER(?)', [`%${keyword}%`]);
    }
    if (categoryarray) {
        distinctProductsSubquery.whereIn('product.cat_id', categoryarray);
    }
    if (subcatid !== "null") {
        distinctProductsSubquery.where('product.cat_id', subcatid);
    }
    if (keyword == 'daily') {
        const ids = [122, 2, 3, 16, 23, 149, 21, 55, 56, 57, 58, 59, 72, 94, 150, 9, 20, 132, 37, 48, 50, 99, 101, 102, 147, 180, 47, 49, 91, 100, 146, 148, 179];
        distinctProductsSubquery.whereIn('product.availability', ['quick', 'all']);
        distinctProductsSubquery.whereIn('product.cat_id', ids);
    }
    if (keyword == 'subscription') {
        const ids = [122, 2, 3, 16, 23, 149, 37, 48, 50, 99, 101, 102, 147, 180, 47, 49, 91, 100, 146, 148, 179, 9, 20, 132, 21, 55, 56, 57, 58, 59, 72, 94, 150];
        distinctProductsSubquery.whereIn('product.availability', ['subscription', 'all']);
        distinctProductsSubquery.whereIn('product.cat_id', ids);
    }

    // Apply HAVING filters if needed (for discount and price)
    if (mindiscount && maxdiscount) {
        distinctProductsSubquery.havingRaw('(100 - (MAX(store_products.price) * 100 / MAX(store_products.mrp)) BETWEEN ? AND ?) OR (100 - (MAX(deal_product.deal_price) * 100 / MAX(store_products.mrp)) BETWEEN ? AND ?)', [
            mindiscount, maxdiscount, mindiscount, maxdiscount
        ]);
    }
    if (minprice !== null && maxprice !== null && !isNaN(minprice) && !isNaN(maxprice)) {
        distinctProductsSubquery.havingRaw('MAX(store_products.price) BETWEEN ? AND ?', [minprice, maxprice]);
    }

    // Count the distinct products
    const totalCountQuery = knex.from(distinctProductsSubquery.as('distinct_products')).count('* as total');

    const dataQuery = topsellingsQuery.offset((pageFilter - 1) * perPage).limit(perPage);

    // Debug: Log the SQL queries
    console.log('=== Query Debug ===');
    console.log('Count Query SQL:', totalCountQuery.toSQL().sql);
    console.log('Data Query SQL:', dataQuery.toSQL().sql);
    console.log('Store ID:', store_id);
    console.log('Keyword:', keyword);
    console.log('Page:', pageFilter, 'PerPage:', perPage);

    const [totalCountResult, productDetail] = await Promise.all([
        totalCountQuery.first(),
        dataQuery
    ]);

    console.log('Total Count Result:', totalCountResult);
    console.log('Product Detail Count:', productDetail?.length || 0);
    console.log('First Product:', productDetail?.[0] || 'None');

    // Fix: Handle count result - PostgreSQL returns {total: "123"} as string, or might be undefined
    // If undefined, try to get count from productDetail length as fallback
    let totalCount = 0;
    if (totalCountResult?.total) {
        totalCount = parseInt(totalCountResult.total);
    } else if (totalCountResult?.count) {
        totalCount = parseInt(totalCountResult.count);
    } else {
        // Fallback: count distinct product_ids from actual results
        const uniqueProductIds = new Set(productDetail.map(p => p.product_id));
        totalCount = uniqueProductIds.size;
        console.log('Warning: Count query returned undefined, using fallback count:', totalCount);
    }
    const totalPages = Math.ceil(totalCount / perPage);

    console.log('Total Count:', totalCount, 'Total Pages:', totalPages);

    // Logic moved to searchController and recordSearchHistory for cache consistency

    // Pre-fetch all data in batches to avoid N+1 queries - RUN IN PARALLEL for speed
    const variantIds = productDetail.map(p => p.varient_id);
    const productIds = productDetail.map(p => p.product_id);
    const currentDate = new Date();
    const baseurl = process.env.BUNNY_NET_IMAGE;

    // Prepare all batch queries
    let dealsPromise = Promise.resolve([]);
    if (variantIds.length > 0) {
        dealsPromise = knex('deal_product')
            .whereIn('varient_id', variantIds)
            .where('store_id', store_id)
            .where('deal_product.valid_from', '<=', currentDate)
            .where('deal_product.valid_to', '>', currentDate)
            .select('varient_id', 'deal_price');
    }

    let variantStoreProductsPromise = Promise.resolve([]);
    if (variantIds.length > 0) {
        variantStoreProductsPromise = knex('store_products')
            .whereIn('varient_id', variantIds)
            .where('store_id', store_id)
            .select('varient_id', 'price');
    }

    // Batch fetch user-specific data if user_id exists
    let wishlistsPromise = Promise.resolve([]);
    let cartItemsPromise = Promise.resolve([]);
    let subCartItemsPromise = Promise.resolve([]);
    let notifyMePromise = Promise.resolve([]);

    if (user_id && variantIds.length > 0) {
        wishlistsPromise = knex('wishlist')
            .whereIn('varient_id', variantIds)
            .where('user_id', user_id)
            .select('varient_id');

        cartItemsPromise = knex('store_orders')
            .whereIn('varient_id', variantIds)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .where('store_id', store_id)
            .whereNull('subscription_flag')
            .select('varient_id', 'qty', 'product_feature_id');

        subCartItemsPromise = knex('store_orders')
            .whereIn('varient_id', variantIds)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .where('subscription_flag', 1)
            .where('store_id', store_id)
            .select('varient_id', 'qty');

        notifyMePromise = knex('product_notify_me')
            .whereIn('varient_id', variantIds)
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
    const fcatIdsSet = new Set();
    productDetail.forEach(p => {
        if (p.fcat_id) {
            p.fcat_id.split(',').forEach(id => {
                const numId = parseInt(id.trim());
                if (!isNaN(numId)) fcatIdsSet.add(numId);
            });
        }
    });
    let featureCatsPromise = Promise.resolve([]);
    if (fcatIdsSet.size > 0) {
        featureCatsPromise = knex('feature_categories')
            .whereIn('id', Array.from(fcatIdsSet))
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
        variantStoreProducts,
        wishList,
        cartItems,
        subCartItems,
        notifyMeList,
        allFeatures,
        allFeatureCats,
        allVariants,
        allImages
    ] = await Promise.all([
        dealsPromise,
        variantStoreProductsPromise,
        wishlistsPromise,
        cartItemsPromise,
        subCartItemsPromise,
        notifyMePromise,
        featuresPromise,
        featureCatsPromise,
        variantsPromise,
        imagesPromise
    ]);

    // Build maps from results
    const dealsMap = {};
    deals.forEach(deal => dealsMap[deal.varient_id] = deal.deal_price);

    const variantPriceMap = {};
    variantStoreProducts.forEach(vsp => variantPriceMap[vsp.varient_id] = vsp.price);

    const wishlistMap = {};
    wishList.forEach(w => wishlistMap[w.varient_id] = true);

    const cartQtyMap = {};
    const cartFeatureMap = {};
    cartItems.forEach(c => {
        cartQtyMap[c.varient_id] = c.qty;
        if (c.product_feature_id) cartFeatureMap[c.varient_id] = c.product_feature_id;
    });

    const subcartQtyMap = {};
    subCartItems.forEach(sc => subcartQtyMap[sc.varient_id] = sc.qty);

    const notifyMeMap = {};
    notifyMeList.forEach(n => notifyMeMap[n.varient_id] = true);

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

        // Use pre-fetched deal price
        const price = dealsMap[ProductList.varient_id] || variantPriceMap[ProductList.varient_id] || ProductList.price;

        // Use pre-fetched user data
        let isFavourite = 'false';
        let notifyMe = 'false';
        let cartQty = 0;
        let isSubscription = 'false';

        if (user_id) {
            isFavourite = wishlistMap[ProductList.varient_id] ? 'true' : 'false';
            cartQty = cartQtyMap[ProductList.varient_id] || 0;
            const subcartQtyEntry = subcartQtyMap[ProductList.varient_id];
            isSubscription = subcartQtyEntry ? 'true' : 'false';
            notifyMe = notifyMeMap[ProductList.varient_id] ? 'true' : 'false';
        }

        const subcartQty = (user_id && subcartQtyMap[ProductList.varient_id]) ? subcartQtyMap[ProductList.varient_id] : 0;

        const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
        const finalsubprice = ProductList.mrp - sub_price;
        const subscription_price = parseFloat(finalsubprice.toFixed(2));

        const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

        let priceval = Number.isInteger(price) ? price + '.001' : price;
        let mrpval = Number.isInteger(ProductList.mrp) ? ProductList.mrp + '.001' : ProductList.mrp;

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

        let total_cart_qty = 0;
        let total_subcart_qty = 0;

        // Process variants using pre-fetched maps
        const customizedVarientData = [];

        for (let j = 0; j < app.length; j++) {
            const ProductList1 = app[j];

            // Use pre-fetched price
            const vprice = dealsMap[ProductList1.varient_id] || variantPriceMap[ProductList1.varient_id] || ProductList1.price;

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
            product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
            thumbnail: ProductList.thumbnail,
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
            subcartQty: subcartQty,
            total_subcart_qty: total_subcart_qty,
            countrating: 0,
            percentage: ProductList.percentage,
            isSubscription: isSubscription,
            subscription_price: subscription_price,
            availability: ProductList.availability,
            country_icon: countryicon,
            features: features,
            varients: varients,
            feature_tags: feature_tags,
            is_customized: ProductList.is_customized,
            perPage: perPage,
            totalPages: totalPages
            // Add or modify properties as needed
        };

        customizedProductData.push(customizedProduct);

    }

    return customizedProductData;
};

const getSearchbyBrands = async (appDetatils) => {
    // PostgreSQL: No need for MySQL sql_mode setting
    const store_id = parseInt(appDetatils.store_id);
    const keyword = appDetatils.keyword || '';
    const device_id = appDetatils.device_id;

    // Proper user_id handling with type validation
    let user_id = null;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && !isNaN(parseInt(appDetatils.user_id))) {
        user_id = parseInt(appDetatils.user_id);
    }

    const brand_id = parseInt(appDetatils.brand_id);
    const sortprice = appDetatils.sortprice;
    const sortname = appDetatils.sortname;

    const pageFilter = parseInt(appDetatils.page) || 1;
    const perPage = parseInt(appDetatils.perpage) || 20;

    const minprice = appDetatils.min_price ? parseFloat(appDetatils.min_price) : null;
    const maxprice = appDetatils.max_price ? parseFloat(appDetatils.max_price) : null;
    const mindiscount = appDetatils.min_discount ? parseFloat(appDetatils.min_discount) : null;
    const maxdiscount = appDetatils.max_discount ? parseFloat(appDetatils.max_discount) : null;

    const currentDate = new Date();
    const currentDateOnly = currentDate.toISOString().split('T')[0]; // For date-only comparisons

    // PostgreSQL: Fixed GROUP BY with all selected columns, fixed country_id join with type casting, fixed CURDATE() to CURRENT_DATE
    const topsellingsQuery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
        .leftJoin('deal_product', function () {
            this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
                .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
                .andOn('deal_product.valid_from', '<=', knex.raw('?', [currentDate]))
                .andOn('deal_product.valid_to', '>', knex.raw('?', [currentDate]));
        })
        .select(
            'store_products.stock',
            'product_varient.varient_id',
            'product_varient.description',
            'product.product_id',
            'product.product_name',
            'product.brand_id',
            'product.product_image',
            'product.thumbnail',
            'store_products.price',
            'store_products.mrp',
            'product_varient.unit',
            'product_varient.quantity',
            'product.type',
            'tbl_country.country_icon',
            'product.percentage',
            'product.availability',
            'product.fcat_id',
            knex.raw('CASE WHEN store_products.mrp > 0 THEN 100-((store_products.price*100)/store_products.mrp) ELSE 0 END as discountper'),
            knex.raw('CASE WHEN store_products.mrp > 0 AND deal_product.deal_price IS NOT NULL THEN 100-((deal_product.deal_price*100)/store_products.mrp) ELSE NULL END as discountper1'),
            'product.is_customized',
            'deal_product.deal_price'
        )
        .where('product.brand_id', brand_id)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('store_products.store_id', store_id)
        .where('store_products.stock', '>', 0)
        .where(builder => {
            builder
                .where('product.is_offer_product', 0)
                .whereNull('product.offer_date')
                .orWhereRaw('product.offer_date::date != CURRENT_DATE');
        })
        .groupBy(
            'product.product_id',
            'store_products.stock',
            'product_varient.varient_id',
            'product_varient.description',
            'product.product_name',
            'product.brand_id',
            'product.product_image',
            'product.thumbnail',
            'store_products.price',
            'store_products.mrp',
            'product_varient.unit',
            'product_varient.quantity',
            'product.type',
            'tbl_country.country_icon',
            'product.percentage',
            'product.availability',
            'product.fcat_id',
            'product.is_customized',
            'deal_product.deal_price'
        );

    if (minprice !== null && maxprice !== null) {
        topsellingsQuery.whereBetween('store_products.price', [minprice, maxprice]);
    }

    if (mindiscount !== null && maxdiscount !== null) {
        topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [
            mindiscount,
            maxdiscount,
            mindiscount,
            maxdiscount,
        ]);
    }

    if (sortprice === 'ltoh') {
        topsellingsQuery.orderBy('store_products.price', 'ASC');
    } else if (sortprice === 'htol') {
        topsellingsQuery.orderBy('store_products.price', 'DESC');
    }

    if (sortname === 'atoz') {
        topsellingsQuery.orderBy('product.product_name', 'ASC');
    } else if (sortname === 'ztoa') {
        topsellingsQuery.orderBy('product.product_name', 'DESC');
    }

    // Get total count first (optimized)
    const totalCountQuery = topsellingsQuery.clone().clearSelect().clearOrder().countDistinct('product.product_id as total');
    const totalResult = await totalCountQuery.first();
    const totalCount = totalResult ? parseInt(totalResult.total) : 0;
    const totalPages = Math.ceil(totalCount / perPage);

    // Get paginated results
    const productDetail = await topsellingsQuery
        .offset((pageFilter - 1) * perPage)
        .limit(perPage);

    // Handle recent search (only once, not in loop) - Performance optimization
    // Search history logic moved to controller and recordSearchHistory for consistency

    // Performance optimization: Batch fetch all data needed for products
    const productIds = productDetail.map(p => p.product_id);
    const varientIds = productDetail.map(p => p.varient_id);

    // Batch fetch: All deals for all variants
    const allDeals = productDetail.length > 0 ? await knex('deal_product')
        .whereIn('varient_id', varientIds)
        .where('store_id', store_id)
        .where('valid_from', '<=', currentDate)
        .where('valid_to', '>', currentDate) : [];
    const dealsMap = {};
    allDeals.forEach(deal => {
        if (!dealsMap[deal.varient_id]) {
            dealsMap[deal.varient_id] = deal;
        }
    });

    // Batch fetch: All wishlist items for user
    const wishlistItems = (user_id && varientIds.length > 0) ? await knex('wishlist')
        .whereIn('varient_id', varientIds)
        .where('user_id', user_id)
        .select('varient_id') : [];
    const wishlistMap = new Set(wishlistItems.map(w => w.varient_id));

    // Batch fetch: All cart quantities for user
    const cartItems = (user_id && varientIds.length > 0) ? await knex('store_orders')
        .whereIn('varient_id', varientIds)
        .where('store_approval', user_id.toString())
        .where('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .where('store_id', store_id)
        .select('varient_id', 'qty', 'product_feature_id') : [];
    const cartMap = {};
    cartItems.forEach(item => {
        if (!cartMap[item.varient_id]) {
            cartMap[item.varient_id] = { qty: item.qty, product_feature_id: item.product_feature_id || 0 };
        }
    });

    // Batch fetch: All subscription cart quantities
    const subCartItems = (user_id && varientIds.length > 0) ? await knex('store_orders')
        .whereIn('varient_id', varientIds)
        .where('store_approval', user_id.toString())
        .where('order_cart_id', 'incart')
        .where('subscription_flag', '1')
        .where('store_id', store_id)
        .select('varient_id', 'qty') : [];
    const subCartMap = {};
    subCartItems.forEach(item => {
        subCartMap[item.varient_id] = item.qty;
    });

    // Batch fetch: All notify_me items
    const notifyMeItems = (user_id && varientIds.length > 0) ? await knex('product_notify_me')
        .whereIn('varient_id', varientIds)
        .where('user_id', user_id)
        .select('varient_id') : [];
    const notifyMeMap = new Set(notifyMeItems.map(n => n.varient_id));

    // Batch fetch: All product images
    const allProductImages = productIds.length > 0 ? await knex('product_images')
        .whereIn('product_id', productIds)
        .select('product_id', 'image', 'type')
        .orderBy('type', 'desc') : [];
    const imagesMap = {};
    allProductImages.forEach(img => {
        if (!imagesMap[img.product_id]) {
            imagesMap[img.product_id] = [];
        }
        imagesMap[img.product_id].push(img);
    });

    // Batch fetch: All features for products
    const allFeatures = productIds.length > 0 ? await knex('product_features')
        .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
        .whereIn('product_features.product_id', productIds) : [];
    const featuresMap = {};
    allFeatures.forEach(f => {
        if (!featuresMap[f.product_id]) {
            featuresMap[f.product_id] = [];
        }
        featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
    });

    // Batch fetch: All feature categories
    const fcatIds = [];
    productDetail.forEach(p => {
        if (p.fcat_id) {
            const ids = p.fcat_id.split(',').map(Number).filter(n => !isNaN(n) && n > 0);
            fcatIds.push(...ids);
        }
    });
    const uniqueFcatIds = [...new Set(fcatIds)];
    const allFeatureCats = uniqueFcatIds.length > 0 ? await knex('feature_categories')
        .whereIn('id', uniqueFcatIds)
        .where('status', 1)
        .where('is_deleted', 0)
        .select('id', 'image') : [];
    const featureCatsMap = {};
    allFeatureCats.forEach(fc => {
        featureCatsMap[fc.id] = fc;
    });

    // Batch fetch: All variants for all products
    const allVariants = productIds.length > 0 ? await knex('store_products')
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
            knex.raw('CASE WHEN store_products.mrp > 0 THEN 100-((store_products.price*100)/store_products.mrp) ELSE 0 END as discountper')
        )
        .where('store_products.store_id', store_id)
        .whereIn('product_varient.product_id', productIds)
        .whereNotNull('store_products.price')
        .where('product_varient.approved', 1)
        .where('product_varient.is_delete', 0) : [];
    const variantsMap = {};
    allVariants.forEach(v => {
        if (!variantsMap[v.product_id]) {
            variantsMap[v.product_id] = [];
        }
        variantsMap[v.product_id].push(v);
    });

    const baseurl = process.env.BUNNY_NET_IMAGE || '';
    const customizedProductData = [];

    for (let i = 0; i < productDetail.length; i++) {
        const ProductList = productDetail[i];

        // Get price from deal or store_products (already in ProductList, but check deal first)
        let price = ProductList.price;
        if (dealsMap[ProductList.varient_id]) {
            price = dealsMap[ProductList.varient_id].deal_price;
        }

        // Wishlist check 
        // Wishlist check 
        var isFavourite = '';
        var notifyMe = '';
        var cartQty = 0;
        if (user_id) {
            const wishList = await knex('wishlist')
                .select('*')
                .where('varient_id', ProductList.varient_id)
                .where('user_id', user_id);
            isFavourite = wishList.length > 0 ? 'true' : 'false';
        } else {
            isFavourite = 'false';
        }

        // cart qty check 
        if (user_id) {
            const CartQtyList = await knex('store_orders')
                .where('varient_id', ProductList.varient_id)
                .where('store_approval', user_id)
                .where('order_cart_id', 'incart')
                .whereNull('subscription_flag')
                .where('store_id', store_id)
                .first();
            cartQty = CartQtyList ? CartQtyList.qty : 0;
        } else {
            cartQty = 0;
        }

        if (user_id) {
            const cnotify_me = await knex('product_notify_me')
                .where('varient_id', ProductList.varient_id)
                .where('user_id', user_id);
            notifyMe = cnotify_me.length > 0 ? 'true' : 'false';
        } else {
            notifyMe = 'false';
        }

        let isSubscription = 'false';

        const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
        const finalsubprice = ProductList.mrp - sub_price;
        const subscription_price = parseFloat(finalsubprice.toFixed(2));

        let countryicon;
        if (ProductList.country_icon == null) {
            countryicon = null;
        } else {
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

        let feature_tags;
        if (ProductList.fcat_id != null) {
            const fcatinput = ProductList.fcat_id;
            const resultArray = fcatinput.split(',').map(Number).filter(n => !isNaN(n) && n > 0);
            const ftaglist = await knex('feature_categories')
                .whereIn('id', resultArray)
                .where('status', 1)
                .where('is_deleted', 0)
                .select('id', knex.raw(`? || image as image`, [baseurl]));
            feature_tags = ftaglist;
        } else {
            feature_tags = [];
        }

        // Get features from batch-fetched data (Performance optimization - eliminates N+1 query)
        const features = featuresMap[ProductList.product_id] || [];

        // Get variants from batch-fetched data (Performance optimization - eliminates N+1 query)
        const productVariants = variantsMap[ProductList.product_id] || [];
        let total_cart_qty = 0;
        let total_subcart_qty = 0;

        // Get images from batch-fetched data (Performance optimization - eliminates N+1 query)
        const productImages = imagesMap[ProductList.product_id] || [];
        // Fallback to product_image if no product_images
        let mainImage = '';
        let thumbnail = '';
        if (productImages.length > 0) {
            mainImage = baseurl + productImages[0].image + "?width=200&height=200&quality=100";
            thumbnail = baseurl + productImages[0].image;
        } else if (ProductList.product_image) {
            mainImage = baseurl + ProductList.product_image + "?width=200&height=200&quality=100";
            thumbnail = baseurl + (ProductList.thumbnail || ProductList.product_image);
        }

        // Process variants using batch-fetched data (Performance optimization - eliminates ALL N+1 queries)
        const customizedVarientData = [];
        for (let j = 0; j < productVariants.length; j++) {
            const ProductList1 = productVariants[j];

            // Get price from deal or store_products (already fetched in batch)
            let vprice = ProductList1.price;
            if (dealsMap[ProductList1.varient_id]) {
                vprice = dealsMap[ProductList1.varient_id].deal_price;
            }

            // Use batch-fetched data for variant-level checks (Performance optimization - no N+1 queries)
            const isFavourite1 = (user_id && wishlistMap.has(ProductList1.varient_id)) ? 'true' : 'false';
            const cartQty1 = (user_id && cartMap[ProductList1.varient_id]) ? cartMap[ProductList1.varient_id].qty : 0;
            const subcartQty1 = (user_id && subCartMap[ProductList1.varient_id]) ? subCartMap[ProductList1.varient_id] : 0;
            const notifyMe1 = (user_id && notifyMeMap.has(ProductList1.varient_id)) ? 'true' : 'false';
            const productFeatureId = (user_id && cartMap[ProductList1.varient_id]) ? cartMap[ProductList1.varient_id].product_feature_id : 0;

            // Use same images for all variants of same product (Performance optimization)
            const variantImage = productImages.length > 0
                ? baseurl + productImages[0].image + "?width=200&height=200&quality=100"
                : mainImage;
            const variantThumbnail = productImages.length > 0
                ? baseurl + productImages[0].image
                : thumbnail;

            total_cart_qty = total_cart_qty + cartQty1;
            total_subcart_qty = total_subcart_qty + subcartQty1;

            const customizedVarient = {
                stock: ProductList1.stock,
                varient_id: ProductList1.varient_id,
                product_id: ProductList1.product_id,
                product_name: ProductList.product_name, // Use main product name
                product_image: variantImage,
                thumbnail: variantThumbnail,
                description: ProductList1.description,
                price: vprice,
                mrp: ProductList1.mrp,
                unit: ProductList1.unit,
                quantity: ProductList1.quantity,
                type: ProductList.type, // Use main product type
                discountper: ProductList1.discountper,
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

        const subcartQty = (user_id && subCartMap[ProductList.varient_id]) ? subCartMap[ProductList.varient_id] : 0;

        const customizedProduct = {
            stock: ProductList.stock,
            varient_id: ProductList.varient_id,
            product_id: ProductList.product_id,
            brand_id: ProductList.brand_id,
            product_name: ProductList.product_name,
            product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
            thumbnail: ProductList.thumbnail,
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
            subcartQty: subcartQty,
            total_subcart_qty: total_subcart_qty,
            countrating: 0,
            percentage: ProductList.percentage,
            isSubscription: isSubscription,
            subscription_price: subscription_price,
            availability: ProductList.availability,
            country_icon: countryicon,
            feature_tags: feature_tags,
            varients: null,
            is_customized: ProductList.is_customized,
            totalPages: totalPages,
            features: features,
            varients: varients,
            // Add or modify properties as needed
        };
        customizedProductData.push(customizedProduct);
    }

    return customizedProductData;
};

const getSearchbypopup = async (appDetatils) => {
    // PostgreSQL: Removed MySQL-specific sql_mode setting
    const { bannerid, store_id, min_price, max_price, stock, min_discount, max_discount, min_rating, max_rating, sort, sortname, sortprice, cat_id, sub_cat_id, page, perpage, keyword } = appDetatils;

    // Proper variable declarations
    let user_id;
    let isSubscription = 'false';
    let countryicon = null;
    let priceval = 0;
    let mrpval = 0;
    let feature_tags = [];
    let varients = [];

    // Determine user_id - PostgreSQL: ensure proper type handling
    const isValidUserId = appDetatils.user_id != "null" && appDetatils.user_id && !isNaN(parseInt(appDetatils.user_id));
    if (isValidUserId) {
        user_id = parseInt(appDetatils.user_id);
    } else {
        user_id = appDetatils.device_id;
    }

    const pageFilter = page || 1;
    const perPage = 200;
    const baseurl = process.env.BUNNY_NET_IMAGE;
    const currentDate = new Date();

    // Get popup banner details
    const popupBannerQuery = knex('popup_banner')
        .where('banner_id', bannerid);

    // PostgreSQL: Removed incorrect 'id' column reference (popup_banner doesn't have 'id', only 'banner_id')
    const results = await popupBannerQuery.select('*');
    if (!results || results.length === 0) {
        return [];
    }

    const customizedData = [];

    for (let i = 0; i < results.length; i++) {
        const item = results[i];
        let categoryProductIds = [];

        // Handle product_id, subcat_id, cat_id logic
        if (item.product_id && item.subcat_id && item.cat_id) {
            const productIds = item.product_id.split(',');
            categoryProductIds = productIds.map(id => parseInt(id)).filter(id => !isNaN(id));
        } else if ((!item.product_id || item.product_id === '') && item.subcat_id && item.cat_id) {
            // PostgreSQL: Convert subcat_id string array to integer array
            const subcategories = item.subcat_id.split(',').map(id => parseInt(id)).filter(id => !isNaN(id));
            const productIds = await knex('product')
                .from('product')
                .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
                .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
                .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
                .whereIn('product.cat_id', subcategories)
                .andWhere('product.hide', '=', 0)
                .andWhere('product.is_delete', '=', 0)
                .pluck('product.product_id');
            categoryProductIds = productIds;
        } else if ((!item.product_id || item.product_id === '') && (!item.subcat_id || item.subcat_id === '') && item.cat_id) {
            // PostgreSQL: Convert cat_id string array to integer array
            const parent_cat_id = item.cat_id.split(',').map(id => parseInt(id)).filter(id => !isNaN(id));
            const subcategories = await knex('categories')
                .whereIn('parent', parent_cat_id)
                .where('status', 1)
                .pluck('cat_id');

            const productIds = await knex('product')
                .from('product')
                .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
                .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
                .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
                .whereIn('product.cat_id', subcategories)
                .andWhere('product.hide', '=', 0)
                .andWhere('product.is_delete', '=', 0)
                .pluck('product.product_id');
            categoryProductIds = productIds;
        }

        if (categoryProductIds.length === 0) {
            continue;
        }
        // Main product query with PostgreSQL fixes
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
                knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper')
            )
            .from('product')
            .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
            .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
            .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
            .leftJoin('add_occproduct_order', knex.raw('add_occproduct_order.product_id::text = product.product_id::text'))
            .whereIn('product.product_id', categoryProductIds)
            .andWhere('product.hide', '=', 0)
            .where('product.is_delete', 0)
            .where('store_products.stock', '>', 0)
            .orderByRaw('add_occproduct_order.orders ASC NULLS LAST');

        const productDetails = await productDetail_s
            .offset((pageFilter - 1) * perPage)
            .limit(perPage);

        const productDetail = productDetails.filter((product, index, self) => {
            return index === self.findIndex((p) => p.product_id === product.product_id);
        });

        if (productDetail.length === 0) {
            continue;
        }
        // PERFORMANCE OPTIMIZATION: Batch fetch all data upfront (no N+1 queries)
        const variantIds = productDetail.map(p => p.varient_id);
        const productIds = [...new Set(productDetail.map(p => p.product_id))];
        const allFcatIds = new Set();
        productDetail.forEach(p => {
            if (p.fcat_id) {
                p.fcat_id.split(',').forEach(id => {
                    const numId = parseInt(id);
                    if (!isNaN(numId)) allFcatIds.add(numId);
                });
            }
        });
        const fcatIdArray = Array.from(allFcatIds);

        // Batch fetch ALL data in parallel
        const [
            deals,
            wishlistItems,
            cartItems,
            notifyMeItems,
            subscriptionItems,
            allFeatures,
            allProductImages,
            allVariants,
            allStoreProductsForVariants,
            allFeatureCategories,
            allCartFeatures
        ] = await Promise.all([
            // Deals for all variants
            variantIds.length > 0 ? knex('deal_product')
                .whereIn('varient_id', variantIds)
                .where('store_id', store_id)
                .where('valid_from', '<=', currentDate)
                .where('valid_to', '>', currentDate) : [],

            // Wishlist for all variants (if user logged in)
            isValidUserId && variantIds.length > 0 ? knex('wishlist')
                .whereIn('varient_id', variantIds)
                .where('user_id', user_id) : [],

            // Cart items for all variants (if user logged in)
            isValidUserId && variantIds.length > 0 ? knex('store_orders')
                .whereIn('varient_id', variantIds)
                .where('store_approval', String(user_id))
                .where('order_cart_id', 'incart')
                .whereNull('subscription_flag')
                .where('store_id', store_id) : [],

            // Notify me for all variants (if user logged in)
            isValidUserId && variantIds.length > 0 ? knex('product_notify_me')
                .whereIn('varient_id', variantIds)
                .where('user_id', user_id) : [],

            // Subscription items for all variants (if user logged in)
            isValidUserId && variantIds.length > 0 ? knex('store_orders')
                .select('varient_id', 'percentage')
                .whereIn('varient_id', variantIds)
                .where('store_approval', String(user_id))
                .where('subscription_flag', '1')
                .where('order_cart_id', 'incart') : [],

            // Features for all products
            productIds.length > 0 ? knex('product_features')
                .select('product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
                .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
                .whereIn('product_id', productIds) : [],

            // Product images for all products
            productIds.length > 0 ? knex('product_images')
                .select('product_id', knex.raw(`? || image as image`, [baseurl]), 'type')
                .whereIn('product_id', productIds)
                .orderBy('type', 'DESC') : [],

            // All variants for all products
            productIds.length > 0 ? knex('store_products')
                .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
                .join('product', 'product_varient.product_id', '=', 'product.product_id')
                .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id',
                    'product_varient.product_id', 'product_varient.description', 'store_products.price',
                    'store_products.mrp', 'product_varient.varient_image', 'product_varient.unit',
                    'product_varient.quantity', 'product.product_name', 'product.type',
                    knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
                .where('store_products.store_id', store_id)
                .whereIn('product_varient.product_id', productIds)
                .whereNotNull('store_products.price')
                .where('product_varient.approved', 1)
                .where('product_varient.is_delete', 0) : [],

            // Store products for variant price lookup
            variantIds.length > 0 ? knex('store_products')
                .select('varient_id', 'price')
                .whereIn('varient_id', variantIds)
                .where('store_id', store_id) : [],

            // Feature categories
            fcatIdArray.length > 0 ? knex('feature_categories')
                .whereIn('id', fcatIdArray)
                .where('status', 1)
                .where('is_deleted', 0)
                .select('id', knex.raw(`? || image as image`, [baseurl])) : [],

            // Cart features for variants (if user logged in)
            isValidUserId && variantIds.length > 0 ? knex('store_orders')
                .select('varient_id', 'product_feature_id')
                .whereIn('varient_id', variantIds)
                .where('store_approval', String(user_id))
                .where('order_cart_id', 'incart')
                .whereNull('subscription_flag')
                .where('store_id', store_id) : []
        ]);

        // Create lookup maps for O(1) access
        const dealMap = Object.fromEntries(deals.map(d => [d.varient_id, d.deal_price]));
        const wishlistMap = Object.fromEntries(wishlistItems.map(w => [w.varient_id, true]));
        const cartMap = Object.fromEntries(cartItems.map(c => [c.varient_id, c.qty || 0]));
        const notifyMeMap = Object.fromEntries(notifyMeItems.map(n => [n.varient_id, true]));
        const subscriptionMap = Object.fromEntries(subscriptionItems.map(s => [s.varient_id, s.percentage]));
        const featuresMap = {};
        allFeatures.forEach(f => {
            if (!featuresMap[f.product_id]) featuresMap[f.product_id] = [];
            featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
        });
        const imagesMap = {};
        allProductImages.forEach(img => {
            if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
            imagesMap[img.product_id].push(img.image);
        });
        const variantsMap = {};
        allVariants.forEach(v => {
            if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
            variantsMap[v.product_id].push(v);
        });
        const storeProductsMap = Object.fromEntries(allStoreProductsForVariants.map(sp => [sp.varient_id, sp.price]));
        const featureCategoriesMap = Object.fromEntries(allFeatureCategories.map(fc => [fc.id, fc]));
        const cartFeaturesMap = Object.fromEntries(allCartFeatures.map(cf => [cf.varient_id, cf.product_feature_id || 0]));

        // Process products (no database queries in loop!)
        const customizedProductData = [];
        for (let i = 0; i < productDetail.length; i++) {
            const ProductList = productDetail[i];

            // Get price from deal map or product list (O(1) lookup)
            const price = dealMap[ProductList.varient_id] || ProductList.price;

            // Get user-specific data from maps (O(1) lookup)
            const isFavourite = isValidUserId && wishlistMap[ProductList.varient_id] ? 'true' : 'false';
            const cartQty = isValidUserId ? (cartMap[ProductList.varient_id] || 0) : 0;
            const notifyMe = isValidUserId && notifyMeMap[ProductList.varient_id] ? 'true' : 'false';
            isSubscription = isValidUserId && subscriptionMap[ProductList.varient_id] ? 'true' : 'false';

            // Calculate subscription price
            const percentage = parseFloat(ProductList.percentage) || 0;
            const sub_price = (ProductList.mrp * percentage) / 100;
            const finalsubprice = ProductList.mrp - sub_price;
            const subscription_price = parseFloat(finalsubprice.toFixed(2));

            // Country icon
            countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

            // Format price and MRP
            priceval = Number.isInteger(price) ? price + '.001' : price;
            mrpval = Number.isInteger(ProductList.mrp) ? ProductList.mrp + '.001' : ProductList.mrp;

            // Get feature tags from map (O(1) lookup)
            if (ProductList.fcat_id != null) {
                const resultArray = ProductList.fcat_id.split(',').map(Number).filter(id => !isNaN(id));
                feature_tags = resultArray
                    .map(id => featureCategoriesMap[id])
                    .filter(Boolean);
            } else {
                feature_tags = [];
            }

            // Get features from map (O(1) lookup)
            const features = featuresMap[ProductList.product_id] || [];

            // Get product images from map (O(1) lookup)
            const productImages = imagesMap[ProductList.product_id] || [];
            const fallbackImage = ProductList.product_image || '';

            // Get variants from map (O(1) lookup)
            const app = variantsMap[ProductList.product_id] || [];

            //prod.varient = app;
            const customizedVarientData = [];
            let total_cart_qty = 0;
            let total_subcart_qty = 0;
            for (let i = 0; i < app.length; i++) {
                // prod.varient.dummy = 5678;
                const ProductList1 = app[i];
                const currentDate = new Date();
                // const deal = await knex('deal_product')
                // .where('varient_id', ProductList1.varient_id)
                // .where('store_id', appDetatils.store_id)
                // .where('deal_product.valid_from', '<=', currentDate)
                // .where('deal_product.valid_to', '>', currentDate)
                // .first();


                // if (deal) {
                //   vprice = deal.deal_price;
                // } else {
                // PERFORMANCE: Use pre-fetched map instead of query
                const vprice = dealMap[ProductList1.varient_id] || storeProductsMap[ProductList1.varient_id] || ProductList1.price;

                let isFavourite1 = 'false';
                let notifyMe1 = 'false';
                let cartQty1 = 0;
                let subcartQty1 = 0;
                let productFeatureId = 0;

                if (isValidUserId) {
                    // PERFORMANCE: Use pre-fetched maps instead of queries
                    isFavourite1 = wishlistMap[ProductList1.varient_id] ? 'true' : 'false';
                    notifyMe1 = notifyMeMap[ProductList1.varient_id] ? 'true' : 'false';
                    cartQty1 = cartMap[ProductList1.varient_id] || 0;
                    subcartQty1 = subscriptionMap[ProductList1.varient_id] ? 1 : 0;
                    productFeatureId = cartFeaturesMap[ProductList1.varient_id] || 0;

                }
                const baseurl = process.env.BUNNY_NET_IMAGE;

                const images = await knex('product_images')
                    .select(knex.raw(`? || image as image`, [baseurl]))
                    .where('product_id', ProductList.product_id)
                    .orderBy('type', 'DESC');
                if (images.length < 0) {
                    const images = await knex('product')
                        .select(knex.raw(`? || product_image as image`, [baseurl]))
                        .where('product_id', ProductList.product_id);
                }


                const CartQtyList = await knex('store_orders')
                    .select('product_feature_id') // ðŸ‘ˆ get the column you want
                    .where('varient_id', ProductList1.varient_id)
                    .where('store_approval', user_id)
                    .where('order_cart_id', 'incart')
                    .first();

                // productFeatureId already set above from cartFeaturesMap
                // PERFORMANCE: Use pre-fetched product images (already fetched in batch above)
                const variantImages = productImages.length > 0 ? productImages : (fallbackImage ? [fallbackImage] : []);

                total_cart_qty = total_cart_qty + cartQty1;
                total_subcart_qty = total_subcart_qty + subcartQty1;

                const customizedVarient = {
                    stock: ProductList1.stock,
                    varient_id: ProductList1.varient_id,
                    product_id: ProductList1.product_id,
                    product_name: ProductList1.product_name,
                    product_image: variantImages.length > 0 ? variantImages[0] + "?width=200&height=200&quality=100" : '',
                    thumbnail: variantImages.length > 0 ? variantImages[0] : '',
                    description: ProductList1.description,
                    price: vprice,
                    mrp: ProductList1.mrp,
                    unit: ProductList1.unit,
                    quantity: ProductList1.quantity,
                    type: ProductList1.type,
                    // discountper:0,
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
            varients = customizedVarientData;

            const subcartQty = isValidUserId ? (subscriptionMap[ProductList.varient_id] ? 1 : 0) : 0;

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
                subcartQty: subcartQty,
                total_subcart_qty: total_subcart_qty,
                countrating: 0,
                country_icon: countryicon,
                feature_tags: feature_tags,
                features: features,
                varients: varients,
                // Add or modify properties as needed
            };
            customizedProductData.push(customizedProduct);
        }

        const customizedItem = {
            id: item.banner_id,
            title: item.banner_name,
            sub_title: null,
            color1: null,
            color2: null,
            product_details: customizedProductData
        };

        customizedData.push(customizedItem);
    }

    return customizedData;
};

const getProducts = async (appDetatils) => {
    return await knex('product').distinct('product_name').pluck('product_name').where('is_delete', 0);
};

const searchbyproduct = async (appDetatils) => {
    await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');
    const store_id = appDetatils.store_id
    const keyword = appDetatils.keyword
    // const user_id = appDetatils.user_id
    if (appDetatils.user_id != "null") {
        user_id = appDetatils.user_id
    } else {
        user_id = appDetatils.device_id
    }
    const device_id = appDetatils.device_id
    const byname = appDetatils.byname
    const subcatid = appDetatils.sub_cat_id
    const cat_id = appDetatils.cat_id
    const sortprice = appDetatils.sortprice
    const sortname = appDetatils.sortname

    const pageFilter = appDetatils.page; // You can adjust the page number dynamically
    const perPage = appDetatils.perpage;

    const minprice = parseFloat(appDetatils.min_price)
    const maxprice = parseFloat(appDetatils.max_price)
    const mindiscount = parseFloat(appDetatils.min_discount)
    const maxdiscount = parseFloat(appDetatils.max_discount)

    stock = appDetatils.stock;
    if (stock == 'out') {
        stock = "<";
        by = 1;

    } else
        if (stock == 'all' || stock == "null") {
            stock = "!=";
            by = "null";
        }
        else {
            stock = ">";
            by = 0;
        }

    //let categoryarray1 = [];
    // let categoryarray;
    // if (cat_id !== "null") {
    // categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
    // }
    //const categoryarray = categoryarray1.push(cat_id); 
    const topsellingsQuery = knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .join('product', 'product_varient.product_id', '=', 'product.product_id')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
        .leftJoin('deal_product', 'product_varient.varient_id', '=', 'deal_product.varient_id')
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
            'tbl_country.country_icon',
            'product.percentage',
            'product.availability',
            'product.fcat_id',
            knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
            knex.raw('100-((deal_product.deal_price*100)/store_products.mrp) as discountper1'),
        )
        // .groupBy(
        //   'store_products.store_id',
        //   'product_varient.varient_id',
        //   'product.product_id'
        // )
        .where('store_products.store_id', store_id)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('store_products.stock', '>', 0)
        .where(builder => {
            builder
                .where('product.is_offer_product', 0)
                .whereNull('product.offer_date')
                .orWhereRaw('DATE(product.offer_date) != CURDATE()')
        });

    if (keyword == 'daily') {
        // List of IDs
        const ids = [122, 2, 3, 16, 23, 149, 21, 55, 56, 57, 58, 59, 72, 94, 150, 9, 20, 132, 37, 48, 50, 99, 101, 102, 147, 180, 47, 49, 91, 100, 146, 148, 179];
        topsellingsQuery.whereIn('product.availability', ['quick', 'all']);
        topsellingsQuery.whereIn('product.cat_id', ids) // Filter by these IDs
        topsellingsQuery.orderByRaw(`FIELD(product.cat_id, ${ids.join(',')})`); // Maintain the custom order  
    }

    if (keyword == 'subscription') {
        // List of IDs
        const ids = [122, 2, 3, 16, 23, 149, 37, 48, 50, 99, 101, 102, 147, 180, 47, 49, 91, 100, 146, 148, 179, 9, 20, 132, 21, 55, 56, 57, 58, 59, 72, 94, 150];
        topsellingsQuery.whereIn('product.availability', ['subscription', 'all']);
        topsellingsQuery.whereIn('product.cat_id', ids) // Filter by these IDs
        topsellingsQuery.orderByRaw(`FIELD(product.cat_id, ${ids.join(',')})`); // Maintain the custom order

    }

    //if (cat_id !== "null") {
    //if (categoryarray.length > 0)  {
    // if (categoryarray)  {
    // topsellingsQuery.whereIn('product.cat_id', categoryarray);
    // }

    // if (subcatid !== "null") {
    // topsellingsQuery.where('product.cat_id', subcatid);
    // }

    if (keyword != 'daily' && keyword != 'subscription') {
        // additionalcat.where('title', byname);
        //   const cleanedSearch = keyword.replace(/\s+/g, ' ').normalize('NFC');
        topsellingsQuery.where('product.product_name', 'like', `%${keyword}%`)
        //   topsellingsQuery.whereRaw('SOUNDEX(product.product_name) = SOUNDEX(?)', [keyword])
        // .orWhere('product.product_name', 'like', `%${keyword}%`);
        //  topsellingsQuery.whereRaw("MATCH(product_name) AGAINST(? IN NATURAL LANGUAGE MODE)", [keyword]);
        //   topsellingsQuery.whereRaw('SOUNDEX(product.product_name) = SOUNDEX(?)', [keyword]);
        //additionalcat.whereLike('title', byname);
    }
    if (stock) {
        // topsellingsQuery.where('store_products.stock',stock,by)
    }
    if (byname) {
        // topsellingsQuery.orderBy('product.product_name', 'byname');
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

    const totalproducts = await topsellingsQuery;
    const totalPages = Math.ceil(totalproducts.length / perPage);
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

        //for device_id
        if (device_id) {

            try {
                check = await knex('recent_search')
                    .where('device_id', device_id);

                checkww = await knex('recent_search')
                    .where('device_id', device_id)
                    .first();

                deletesame = await knex('recent_search')
                    .where('keyword', keyword)
                    .delete();

                if (check.length >= 10) {
                    chec = await knex('recent_search')
                        .where('id', checkww.id)
                        .delete();
                }
                add = await knex('recent_search')
                    .insert({
                        device_id: device_id,
                        keyword: keyword
                    });
            } catch (error) {

            } finally {

            }

        }

        if (user_id) {


            try {
                // Check recent searches for the user
                const check = await knex('recent_search')
                    .where('user_id', user_id);

                // Get the most recent search entry for the user
                const checkww = await knex('recent_search')
                    .where('user_id', user_id)
                    .first();

                // Delete entries with the same keyword
                const deletesame = await knex('recent_search')
                    .where('keyword', keyword)
                    .delete();

                // Check if there are 10 or more recent searches
                if (check.length >= 10) {
                    if (checkww) {
                        // Delete the oldest entry (the one with the id from checkww)
                        await knex('recent_search')
                            .where('id', checkww.id)
                            .delete();
                    }
                }
                // Add the new search entry
                if (keyword) {
                    const add = await knex('recent_search')
                        .insert({
                            user_id: user_id,
                            keyword: keyword
                        });
                }

            } catch (error) {

            } finally {

            }

            // Wishlist check 
            // Wishlist check 
            var isFavourite = '';
            var notifyMe = '';
            var cartQty = 0;
            if (user_id) {
                const wishList = await knex('wishlist')
                    .select('*')
                    .where('varient_id', ProductList.varient_id)
                    .where('user_id', user_id);
                isFavourite = wishList.length > 0 ? 'true' : 'false';
            } else {
                isFavourite = 'false';
            }

            // cart qty check 
            if (user_id) {
                const CartQtyList = await knex('store_orders')
                    .where('varient_id', ProductList.varient_id)
                    .where('store_approval', user_id)
                    .where('order_cart_id', 'incart')
                    .whereNull('subscription_flag')
                    .where('store_id', store_id)
                    .first();
                cartQty = CartQtyList ? CartQtyList.qty : 0;
            } else {
                cartQty = 0;
            }

            if (user_id) {
                const cnotify_me = await knex('product_notify_me')
                    .where('varient_id', ProductList.varient_id)
                    .where('user_id', user_id);
                notifyMe = cnotify_me.length > 0 ? 'true' : 'false';
            } else {
                notifyMe = 'false';
            }
            isSubscription = 'false';
        } else {
            notifyMe = 'false';
            isFavourite = 'false';
            cartQty = 0;
            isSubscription = 'false';
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

        if (ProductList.fcat_id != null) {
            fcatinput = ProductList.fcat_id;
            const resultArray = fcatinput.split(',').map(Number);
            const ftaglist = await knex('feature_categories')
                .whereIn('id', resultArray)
                .where('status', 1)
                .where('is_deleted', 0)
                .select('id', knex.raw(`CONCAT('${baseurl}', image) as image`))
            feature_tags = ftaglist;
        } else {
            feature_tags = [];
        }


        //   const customizedProduct = {
        //       product_name: ProductList.product_name,
        //       };

        // customizedProductData.push(ProductList.product_name);  
        // âœ… Avoid duplicate product names (case-insensitive check)
        const productName = (ProductList.product_name || "").trim().toLowerCase();
        const alreadyExists = customizedProductData.some(
            (name) => name.toLowerCase() === productName
        );

        if (!alreadyExists && productName !== "") {
            customizedProductData.push(ProductList.product_name);
        }
    }

    return customizedProductData;
};


module.exports = {
    getUniversalSearch,
    getSearchbystore,
    getrecentSearch,
    gettrenproducts,
    getSearchbybanner,
    getSearchbyBrands,
    gettrendbrands,
    getSearchbypopup,
    getProducts,
    searchbyproduct,
    recordSearchHistory
};
