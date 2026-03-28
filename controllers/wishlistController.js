const productlistModel = require('../models/productlistModel');
const wishlistModel = require('../models/wishlistModel');
const knex = require('../db'); // Import your Knex instance
const { generateWishlistCacheKey, sAdd, sRem } = require('../utils/redisClient');

const add_rem_wishlist = async (req, res) => {

    try {
        const appDetatils = req.body;
        const productDetails = await wishlistModel.addtoWishlist(appDetatils);

        // Handle user_id - PostgreSQL schema has user_id as integer
        let user_id;
        if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null) {
            user_id = parseInt(appDetatils.user_id);
        } else {
            user_id = appDetatils.device_id; // May need schema adjustment for guest users
        }

        // PostgreSQL count query - returns {count: "5"} as string
        const countResult = await knex('wishlist')
            .where('user_id', user_id)
            .where('store_id', parseInt(appDetatils.store_id))
            .count('wish_id as count')
            .first();

        const count = countResult ? parseInt(countResult.count) || 0 : 0;

        const product = await knex('wishlist')
            .where('user_id', user_id)
            .where('varient_id', parseInt(appDetatils.varient_id))
            .select('product_name')
            .first();

        // Sync Redis Cache (ADD/REMOVE)
        const wishlistCacheKey = generateWishlistCacheKey(user_id, appDetatils.store_id);
        if (productDetails === 'Added to Wishlist') {
            await sAdd(wishlistCacheKey, appDetatils.varient_id.toString());
        } else if (productDetails === 'Removed from Wishlist') {
            await sRem(wishlistCacheKey, appDetatils.varient_id.toString());
        }

        var data = {
            "status": "1",
            "count": count,
            "message": productDetails,
            "product_name": (product) ? product.product_name : ''
        };
        res.status(200).json(data);
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'Nothing in Wishlist From This Location9' });
    }

};

const show_wishlist = async (req, res) => {
    try {
        const appDetatils = req.body;
        const store_id = appDetatils.store_id != null ? parseInt(appDetatils.store_id) : NaN;

        // PostgreSQL wishlist.user_id is integer only - require valid logged-in user
        let user_id = null;
        if (appDetatils.user_id != null && appDetatils.user_id !== '' && appDetatils.user_id !== 'null') {
            const parsed = parseInt(appDetatils.user_id, 10);
            if (Number.isInteger(parsed)) user_id = parsed;
        }

        // Fast path: guest or missing store_id - return empty without hitting DB
        if (user_id == null || !Number.isInteger(user_id) || Number.isNaN(store_id)) {
            return res.status(200).json({
                status: '1',
                message: 'Wishlist items',
                count: 0,
                data: []
            });
        }

        // Parallel: get wishlist data and count in one round-trip each (minimal latency)
        const [productDetails, countResult] = await Promise.all([
            wishlistModel.getWishlist({ ...appDetatils, user_id, store_id }),
            knex('wishlist')
                .where('user_id', user_id)
                .where('store_id', store_id)
                .count('wish_id as count')
                .first()
        ]);

        const count = countResult ? parseInt(countResult.count, 10) || 0 : 0;

        res.status(200).json({
            status: '1',
            message: 'Wishlist items',
            count,
            data: productDetails
        });
    } catch (error) {
        console.error('show_wishlist error:', error);
        res.status(500).json({
            message: 'Nothing in Wishlist From This Location',
            data: [],
            count: 0,
            status: '0'
        });
    }
};

module.exports = {
    show_wishlist,
    add_rem_wishlist
};
