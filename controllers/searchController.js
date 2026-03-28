const productlistModel = require('../models/productlistModel');
const searchModel = require('../models/searchModel');
const knex = require('../db'); // Import your Knex instance
const { getUserDecorationData } = require('../utils/redisDecorationHelper');

/**
 * Decorate product list with user-specific Redis data (wishlist, cart, subcart).
 */
const decorateProductList = async (products, user_id, store_id) => {
  if (!user_id || user_id === 'null' || !products || !Array.isArray(products) || products.length === 0) {
    return products;
  }

  const { wishlistSet, cartHash, subcartHash } = await getUserDecorationData(user_id, store_id);

  return products.map(product => {
    if (!product) return product;
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


const searchbybanner = async (req, res) => {

  try {
    const appDetatils = req.body;
    const getprdlist = await searchModel.getSearchbybanner(appDetatils);

    var data = {
      "status": "1",
      "message": "Product found",
      "page": appDetatils.page,
      "perPage": appDetatils.perpage,
      "data": getprdlist.products,
      "banner_detail": getprdlist.banner,
    };

    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    //res.status(500).json({ status: 0, message: 'Internal Server Error' });
    res.status(500).json({ status: 0, message: error.message });
  }
};

const trensearchproducts = async (req, res) => {

  try {
    const appDetatils = req.body;
    const trenproducts = await searchModel.gettrenproducts(appDetatils);

    // Decoration
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedProducts = await decorateProductList(trenproducts, user_id, appDetatils.store_id);

    var data = {
      "status": "1",
      "message": "Product found",
      "data": decoratedProducts,
    };

    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const recentSearch = async (req, res) => {

  try {
    const appDetatils = req.body;
    const recentSearch = await searchModel.getrecentSearch(appDetatils);

    var data = {
      "status": "1",
      "message": "Recent Search found",
      "data": recentSearch,
    };

    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Products not found' });
  }
};

const trendingrecentsearch = async (req, res) => {

  try {
    const appDetatils = req.body;
    const recentSearch = await searchModel.getrecentSearch(appDetatils);
    const trendbrands = await searchModel.gettrendbrands(appDetatils);
    const trendproducts = await searchModel.gettrenproducts(appDetatils);

    // Decoration
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedTrendProducts = await decorateProductList(trendproducts, user_id, appDetatils.store_id);

    var data = {
      "status": "1",
      "message": "Products found",
      "recent_search": recentSearch,
      "trend_brands": trendbrands,
      "trend_products": decoratedTrendProducts,
    };

    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Products not found' });
  }
};

const searchbystore = async (req, res) => {
  try {
    const appDetatils = req.body;
    console.log('=== SearchByStore Request ===');
    console.log('Request body:', JSON.stringify(appDetatils, null, 2));

    // Import Redis cache utilities
    const { generateSearchCacheKey, getCache, setCache } = require('../utils/redisClient');

    // Generate cache key based on request parameters
    const cacheKey = generateSearchCacheKey(appDetatils);

    // Record search history early - ensuring it runs even on cache hits
    // Use fire-and-forget (async without await) to avoid blocking response, 
    // OR await it for consistency. I'll await it to ensure stability.
    await searchModel.recordSearchHistory(appDetatils);

    // Try to get cached data first
    let cachedData = await getCache(cacheKey);
    if (cachedData) {
      console.log('✅ Returning cached data');
      return res.status(200).json(cachedData);
    }

    // Cache miss - fetch from database
    console.log('❌ Cache miss - fetching from database');
    const getSearchbystore = await searchModel.getSearchbystore(appDetatils);
    console.log('Results count:', getSearchbystore?.length || 0);
    console.log('First result:', getSearchbystore?.[0] || 'No results');

    var data = {
      "status": "1",
      "message": "Product found",
      "data": getSearchbystore,
    };

    // Cache the response
    // TTL: 5 minutes (300 seconds) for search results
    // For more dynamic data, use shorter TTL (e.g., 60 seconds)
    // For less dynamic data, use longer TTL (e.g., 600 seconds = 10 minutes)
    const cacheTTL = 300; // 5 minutes
    await setCache(cacheKey, data, cacheTTL);

    res.status(200).json(data);
  } catch (error) {
    console.error('=== SearchByStore Error ===');
    console.error('Error:', error);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    //res.status(500).json({ status: 0, message: 'Internal Server Error' });
    var data = {
      "message": "Internal Server Error",
      "data": [],
    };
    res.status(500).json({ status: 0, message: error.message });
  }
};

const universal_search = async (req, res) => {

  try {
    const appDetatils = req.body;
    await searchModel.recordSearchHistory(appDetatils);
    const getUniversalSearch = await searchModel.getUniversalSearch(appDetatils);

    // Decoration
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedProducts = await decorateProductList(getUniversalSearch, user_id, appDetatils.store_id);

    var data = {
      "status": "1",
      "message": "Universal Search",
      "data": decoratedProducts,
    };

    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const searchbybrands = async (req, res) => {
  try {
    const appDetatils = req.body;
    await searchModel.recordSearchHistory(appDetatils);
    const getSearchbystore = await searchModel.getSearchbyBrands(appDetatils);

    var data = {
      "status": "1",
      "message": "Product found",
      "data": getSearchbystore,
    };

    res.status(200).json(data);
  } catch (error) {
    var data = {
      "message": "Internal Server Error",
      "data": [],
    };
    res.status(500).json({ status: 0, message: error.message });
  }
};


/**
 * Decorate popup banner results with user-specific Redis data (wishlist, cart, subcart).
 * 
 * @param {Array} results - Banner list with nested products
 * @param {string|number} user_id - User identifier
 * @param {number} store_id - Store ID
 * @returns {Promise<Array>} Decorated banner list
 */
const decoratePopupBannerData = async (results, user_id, store_id) => {
  if (!user_id || user_id === 'null' || !results || !Array.isArray(results) || results.length === 0) {
    return results;
  }

  const { wishlistSet, cartHash, subcartHash } = await getUserDecorationData(user_id, store_id);

  return results.map(banner => {
    if (!banner.product_details || !Array.isArray(banner.product_details)) {
      return banner;
    }

    const decoratedProductDetails = banner.product_details.map(product => {
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

    return {
      ...banner,
      product_details: decoratedProductDetails
    };
  });
};

const searchbypopupbanner = async (req, res) => {
  try {
    const appDetatils = req.body;
    const getSearchbypopup = await searchModel.getSearchbypopup(appDetatils);

    // Decoration: ALWAYS add user-specific data from Redis (wishlist, cart, subcart)
    let user_id;
    if (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null && appDetatils.user_id !== "") {
      user_id = appDetatils.user_id;
    } else {
      user_id = appDetatils.device_id;
    }
    const decoratedBanners = await decoratePopupBannerData(getSearchbypopup, user_id, appDetatils.store_id);

    // Flatten response: Move product_details directly to data and remove banner metadata
    const flattenedProducts = decoratedBanners.flatMap(banner => banner.product_details || []);

    var data = {
      "status": "1",
      "message": "Product found",
      "data": flattenedProducts,
    };
    res.status(200).json(data);
  } catch (error) {
    console.error('Error in searchbypopupbanner:', error);
    //console.error(error);
    //res.status(500).json({ status: 0, message: 'Internal Server Error' });
    var data = {
      "message": "Internal Server Error",
      "data": [],
    };
    res.status(500).json({ status: 0, message: error.message });
  }
};

const searchbyproduct = async (req, res) => {
  try {
    const appDetatils = req.body;
    await searchModel.recordSearchHistory(appDetatils);
    const products = await searchModel.searchbyproduct(appDetatils);
    var data = {
      "status": "1",
      "message": "Products found",
      "data": products,
    };
    res.status(200).json(data);
  } catch (error) {
    //console.error(error);
    //res.status(500).json({ status: 0, message: 'Internal Server Error' });
    var data = {
      "message": "Internal Server Error",
      "data": [],
    };
    res.status(500).json({ status: 0, message: error.message });
  }
};

const getProducts = async (req, res) => {
  try {
    const appDetatils = req.body;
    const products = await searchModel.getProducts(appDetatils);
    var data = {
      "status": "1",
      "message": "Products found",
      "data": products,
    };
    res.status(200).json(data);
  } catch (error) {
    //console.error(error);
    //res.status(500).json({ status: 0, message: 'Internal Server Error' });
    var data = {
      "message": "Internal Server Error",
      "data": [],
    };
    res.status(500).json({ status: 0, message: error.message });
  }
};

module.exports = {
  searchbystore,
  universal_search,
  recentSearch,
  trensearchproducts,
  searchbybanner,
  searchbybrands,
  trendingrecentsearch,
  searchbypopupbanner,
  searchbyproduct,
  getProducts
};
