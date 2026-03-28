// controllers/productlistController.js
const productlistModel = require('../models/productlistModel');
const {
  getCache,
  setCache,
  generateAdditionalCatSearchCacheKey,
  generateOccasionalCatSearchCacheKey,
  generateTopSellingCacheKey
} = require('../utils/redisClient');
const { getUserDecorationData } = require('../utils/redisDecorationHelper');

/**
 * Decorate product list with user-specific Redis data (wishlist, cart, subcart).
 * Returns a new array to avoid mutating cached source data.
 * 
 * @param {Array} products - Product list
 * @param {string|number} user_id - User identifier
 * @param {number} store_id - Store ID
 * @returns {Promise<Array>} Decorated product list
 */
const decorateProductList = async (products, user_id, store_id) => {
  if (!user_id || user_id === 'null' || !products || !Array.isArray(products) || products.length === 0) {
    return products;
  }

  const { wishlistSet, cartHash, subcartHash } = await getUserDecorationData(user_id, store_id);

  return products.map(product => {
    const vid = product.varient_id ? product.varient_id.toString() : null;
    let decoratedProduct = { ...product };
    let variantsTotalCartQty = 0;
    let variantsTotalSubcartQty = 0;

    if (vid) {
      const cart_qty = parseInt(cartHash[vid] || 0);
      const subcartQty = parseInt(subcartHash[vid] || 0);
      decoratedProduct.isFavourite = wishlistSet.has(vid) ? 'true' : 'false';
      decoratedProduct.isFavorite = decoratedProduct.isFavourite;
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
          variantsTotalCartQty += v_cart_qty;
          variantsTotalSubcartQty += v_subcartQty;
          return {
            ...v,
            isFavourite: wishlistSet.has(vvid) ? 'true' : 'false',
            isFavorite: wishlistSet.has(vvid) ? 'true' : 'false',
            cart_qty: v_cart_qty,
            total_cart_qty: v_cart_qty,
            subcartQty: v_subcartQty,
            total_subcart_qty: v_subcartQty
          };
        }
        return v;
      });

      decoratedProduct.total_cart_qty = variantsTotalCartQty;
      decoratedProduct.total_subcart_qty = variantsTotalSubcartQty;
    }

    return decoratedProduct;
  });
};

const top_selling = async (req, res) => {
  try {
    const appDetatils = req.body;
    const cacheKey = generateTopSellingCacheKey(appDetatils);

    // 1. Try to get base products from cache
    let data = await getCache(cacheKey);
    let getTopSelling;

    if (data) {
      getTopSelling = data.data;
    } else {
      // 2. Cache miss: fetch CLEAN base list from DB (no user-specific decoration)
      const cleanDetails = { ...appDetatils, user_id: null, device_id: null };
      getTopSelling = await productlistModel.getTopSelling(cleanDetails);
      data = {
        "status": "1",
        "message": "Top selling products",
        "page": appDetatils.page,
        "perPage": appDetatils.perpage,
        "data": getTopSelling,
      };
      // Store in cache for 300 seconds
      await setCache(cacheKey, data, 300);
    }

    // 3. Decoration: ALWAYS add user-specific data (even on cache hit)
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedData = await decorateProductList(getTopSelling, user_id, appDetatils.store_id);

    res.status(200).json({
      ...data,
      data: decoratedData
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};


const whatsnew = async (req, res) => {
  try {
    const appDetatils = req.body;
    const getWhatsNew = await productlistModel.getWhatsNew(appDetatils);

    // Decoration: ALWAYS add user-specific data from Redis
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedData = await decorateProductList(getWhatsNew, user_id, appDetatils.store_id);

    res.status(200).json({
      "status": "1",
      "message": "What New products",
      "data": decoratedData,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};


const recentselling = async (req, res) => {
  try {
    const appDetatils = req.body;
    const getRecentSelling = await productlistModel.getRecentSelling(appDetatils);

    // Decoration: Add user-specific data
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedData = await decorateProductList(getRecentSelling, user_id, appDetatils.store_id);

    res.status(200).json({
      "status": "1",
      "message": "Recent selling products",
      "data": decoratedData,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const dealproduct = async (req, res) => {
  try {
    const appDetatils = req.body;
    const getDealProduct = await productlistModel.getDealProduct(appDetatils);

    // Decoration: ALWAYS add user-specific data from Redis
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedData = await decorateProductList(getDealProduct, user_id, appDetatils.store_id);

    res.status(200).json({
      "status": "1",
      "message": "Deal Products",
      "data": decoratedData,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const additionalcat_search = async (req, res) => {
  try {
    const appDetatils = req.body;
    const cacheKey = generateAdditionalCatSearchCacheKey(appDetatils);

    // 1. Try to get base products from cache
    let data = await getCache(cacheKey);
    let getAdditionalCatSearch;

    if (data) {
      getAdditionalCatSearch = data.data;
    } else {
      // 2. Cache miss: fetch CLEAN base list from DB
      const cleanDetails = { ...appDetatils, user_id: null, device_id: null };
      getAdditionalCatSearch = await productlistModel.getAdditionalCatSearch(cleanDetails);
      data = {
        "status": "1",
        "message": "Additional Category Search",
        "data": getAdditionalCatSearch,
      };
      // Store in cache for 300 seconds
      await setCache(cacheKey, data, 300);
    }

    // 3. Decoration: ALWAYS add user-specific data (even on cache hit)
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedData = await decorateProductList(getAdditionalCatSearch, user_id, appDetatils.store_id);

    res.status(200).json({
      ...data,
      data: decoratedData
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: error.message });
  }
};

const occasionalcat_search = async (req, res) => {
  try {
    const appDetatils = req.body;
    const cacheKey = generateOccasionalCatSearchCacheKey(appDetatils);

    // 1. Try to get base products from cache
    let data = await getCache(cacheKey);
    let getOccasionalCatSearch;

    if (data) {
      getOccasionalCatSearch = data.data;
    } else {
      // 2. Cache miss: fetch CLEAN base list from DB
      const cleanDetails = { ...appDetatils, user_id: null, device_id: null };
      getOccasionalCatSearch = await productlistModel.getOccasionalCatSearch(cleanDetails);
      data = {
        "status": "1",
        "message": "Occasional Category Search",
        "data": getOccasionalCatSearch,
      };
      // Store in cache for 300 seconds
      await setCache(cacheKey, data, 300);
    }

    // 3. Decoration: ALWAYS add user-specific data (even on cache hit)
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }

    // Flatten product_details from all categories into a single array
    const flattenedProducts = getOccasionalCatSearch.flatMap(cat => cat.product_details || []);

    const decoratedData = await decorateProductList(flattenedProducts, user_id, appDetatils.store_id);

    res.status(200).json({
      ...data,
      data: decoratedData
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: error.message });
  }
};

module.exports = {
  top_selling,
  whatsnew,
  recentselling,
  dealproduct,
  additionalcat_search,
  occasionalcat_search
};

