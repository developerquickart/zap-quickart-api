const knex = require('./db');
const searchModel = require('./models/searchModel');
const { getUserDecorationData } = require('./utils/redisDecorationHelper');

const decoratePopupBannerData = async (results, user_id, store_id) => {
    if (!user_id || user_id === 'null' || !results || !Array.isArray(results) || results.length === 0) {
        return results;
    }

    console.log("Fetching decoration data from Redis...");
    const { wishlistSet, cartHash, subcartHash } = await getUserDecorationData(user_id, store_id);
    console.log("Decoration data fetched.");

    return results.map(banner => {
        if (!banner.product_details || !Array.isArray(banner.product_details)) {
            return banner;
        }

        const decoratedProductDetails = banner.product_details.map(product => {
            const vid = product.varient_id ? product.varient_id.toString() : null;
            let decoratedProduct = { ...product };

            if (vid) {
                const cart_qty = parseInt(cartHash[vid] || 0);
                const subcartQty = parseInt(subcartHash[vid] || 0);
                decoratedProduct.isFavourite = wishlistSet.has(vid) ? 'true' : 'false';
                decoratedProduct.cart_qty = cart_qty;
                decoratedProduct.total_cart_qty = cart_qty;
                decoratedProduct.subcartQty = subcartQty;
                decoratedProduct.total_subcart_qty = subcartQty;
            }

            if (product.varients && Array.isArray(product.varients)) {
                decoratedProduct.varients = product.varients.map(v => {
                    const vvid = v.varient_id ? v.varient_id.toString() : null;
                    if (vvid) {
                        const v_cart_qty = parseInt(cartHash[vvid] || 0);
                        const v_subcartQty = parseInt(subcartHash[vvid] || 0);
                        return {
                            ...v,
                            isFavourite: wishlistSet.has(vvid) ? 'true' : 'false',
                            cart_qty: v_cart_qty,
                            total_cart_qty: v_cart_qty,
                            subcartQty: v_subcartQty,
                            total_subcart_qty: v_subcartQty
                        };
                    }
                    return v;
                });
            }

            return decoratedProduct;
        });

        return {
            ...banner,
            product_details: decoratedProductDetails
        };
    });
};

async function debugCall() {
    console.log("Starting isolated debug call with decoration...");
    const appDetails = {
        bannerid: 4,
        store_id: 7,
        user_id: "1",
        device_id: "test_device"
    };

    try {
        console.log("Calling getSearchbypopup...");
        const res = await searchModel.getSearchbypopup(appDetails);
        console.log("getSearchbypopup done. length:", res.length);

        console.log("Calling decoratePopupBannerData...");
        const decorated = await decoratePopupBannerData(res, "1", 7);
        console.log("Decoration successful. Banner count:", decorated.length);

        console.log("Flattening results...");
        const flattened = decorated.flatMap(banner => banner.product_details || []);
        console.log("Flattening successful. Total products:", flattened.length);

        if (flattened.length > 0) {
            console.log("First product name:", flattened[0].product_name);
        }
    } catch (e) {
        console.error("Caught error:");
        console.error(e);
    } finally {
        process.exit(0);
    }
}

debugCall();
