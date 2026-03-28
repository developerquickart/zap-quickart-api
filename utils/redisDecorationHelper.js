const knex = require('../db');
const {
    generateWishlistCacheKey,
    generateCartHashKey,
    generateSubCartHashKey,
    sMembers,
    sAdd,
    hGetAll,
    hSet,
    redis
} = require('./redisClient');

const ONE_DAY = 3600 * 24;

/**
 * Get decoration data (wishlist, cart, subcart) for a user from Redis.
 * If data is missing in Redis, it syncs from the database.
 * 
 * @param {string|number} user_id_raw - User ID or Device ID
 * @param {number} store_id - Store ID
 * @returns {Promise<Object>} Decoration maps
 */
const getUserDecorationData = async (user_id_raw, store_id) => {
    if (!user_id_raw || user_id_raw === 'null') {
        return { wishlistSet: new Set(), cartHash: {}, subcartHash: {} };
    }

    // Handle user_id parsing - only parse if it's a numeric string
    let user_id = user_id_raw;
    if (typeof user_id_raw === 'string' && /^\d+$/.test(user_id_raw)) {
        user_id = parseInt(user_id_raw, 10);
    }

    const wishlistKey = generateWishlistCacheKey(user_id, store_id);
    const cartKey = generateCartHashKey(user_id, store_id);
    const subcartKey = generateSubCartHashKey(user_id, store_id);

    // Fetch all in parallel for performance
    const [wishlistResult, cartResult, subcartResult] = await Promise.all([
        sMembers(wishlistKey),
        hGetAll(cartKey),
        hGetAll(subcartKey)
    ]);

    let wishlist = wishlistResult;
    let cart = cartResult;
    let subcart = subcartResult;

    const syncPromises = [];
    const isNumericUser = typeof user_id === 'number' || (typeof user_id === 'string' && /^\d+$/.test(user_id));

    // Sync: If the key doesn't have the '_initialized' marker, fetch from DB
    // Wishlist Sync
    if (isNumericUser && (wishlist.length === 0 || !wishlist.includes('_initialized'))) {
        syncPromises.push((async () => {
            const dbWishlist = await knex('wishlist')
                .where('user_id', user_id)
                .where('store_id', store_id)
                .pluck('varient_id');

            const membersToSet = ['_initialized', ...dbWishlist.map(String)];

            // Populate Redis
            await Promise.all(membersToSet.map(vid => sAdd(wishlistKey, vid, ONE_DAY)));
            wishlist = membersToSet;
        })());
    }

    // Cart Sync
    if (isNumericUser && (Object.keys(cart).length === 0 || !cart._initialized)) {
        syncPromises.push((async () => {
            const dbCart = await knex('store_orders')
                .where('store_approval', user_id)
                .where('order_cart_id', 'incart')
                .where('store_id', store_id)
                .whereNull('subscription_flag')
                .select('varient_id', 'qty');

            const cartData = { _initialized: '1' };
            dbCart.forEach(item => {
                cartData[item.varient_id.toString()] = item.qty.toString();
            });

            // Populate Redis
            await Promise.all(Object.entries(cartData).map(([field, val]) =>
                hSet(cartKey, field, val, ONE_DAY)
            ));
            cart = cartData;
        })());
    }

    // Subcart Sync
    if (isNumericUser && (Object.keys(subcart).length === 0 || !subcart._initialized)) {
        syncPromises.push((async () => {
            const dbSubcart = await knex('store_orders')
                .where('store_approval', user_id)
                .where('order_cart_id', 'incart')
                .where('store_id', store_id)
                .where('subscription_flag', 1)
                .select('varient_id', 'qty');

            const subcartData = { _initialized: '1' };
            dbSubcart.forEach(item => {
                subcartData[item.varient_id.toString()] = item.qty.toString();
            });

            // Populate Redis
            await Promise.all(Object.entries(subcartData).map(([field, val]) =>
                hSet(subcartKey, field, val, ONE_DAY)
            ));
            subcart = subcartData;
        })());
    }

    if (syncPromises.length > 0) {
        await Promise.all(syncPromises);
    }

    // Filter out internal '_initialized' marker from returning to the application
    // Explicitly convert to string as Upstash Redis parses numeric strings as numbers
    const filteredWishlist = wishlist
        .filter(item => item !== '_initialized')
        .map(item => String(item));
    const filteredCart = { ...cart };
    delete filteredCart._initialized;
    const filteredSubcart = { ...subcart };
    delete filteredSubcart._initialized;

    return {
        wishlistSet: new Set(filteredWishlist),
        cartHash: filteredCart,
        subcartHash: filteredSubcart
    };
};

module.exports = {
    getUserDecorationData
};
