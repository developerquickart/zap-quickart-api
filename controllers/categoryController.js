// controllers/categoryController.js
const userModel = require('../models/userModel');
const homeappModel = require('../models/homeappModel');
const productlistModel = require('../models/productlistModel');
const knex = require('../db'); // Import your Knex instance
const { getCache, setCache, deleteCache, generateOneApiCacheKey, generateOneApi1CacheKey, generateOneApi2CacheKey, generateOneApi3CacheKey, generateOneApiGlobalCacheKey, generateOneApiUserCacheKey, generateWishlistCacheKey, generateCartHashKey, generateSubCartHashKey, sAdd, sMembers, hGetAll, hSet } = require('../utils/redisClient');

const ONE_HOUR = 900;
const TEN_MINUTES = 600;

const oneapi = async (req, res) => {
  try {
    const appDetatils = req.body;
    const globalCacheKey = generateOneApiGlobalCacheKey(appDetatils);
    const userCacheKey = generateOneApiUserCacheKey(appDetatils);

    const baseurl = process.env.BUNNY_NET_IMAGE;
    const imgurl = process.env.BASE_URL;

    // Helper: Fetch Global Data (Shared across all users)
    const fetchGlobalData = async () => {
      const cachedGlobal = await getCache(globalCacheKey);
      if (cachedGlobal) return cachedGlobal;

      // Use a clean details object for global fetching (no user markers)
      const globalDetails = { ...appDetatils, user_id: null, device_id: null };

      const [
        bannerList, topCat, secondBanner, specialOfferBanner,
        sneakyOfferBanner, popupBanner, recentSelling, topSelling,
        additionalCategory, brandList, occasionalCategory, featurecategory,
        trailpackimage, bgimage
      ] = await Promise.all([
        homeappModel.getBanner(globalDetails),
        homeappModel.getTopCat(globalDetails),
        homeappModel.getsecondBanner(globalDetails),
        homeappModel.specialOfferBanner(globalDetails),
        homeappModel.sneakyOfferBanner(globalDetails),
        homeappModel.popupBanner(globalDetails),
        homeappModel.getrecentSelling(globalDetails),
        homeappModel.gettopSelling(globalDetails),
        homeappModel.getadditionalCategory(globalDetails),
        homeappModel.getBrand(globalDetails),
        homeappModel.getoccasionalCategory(globalDetails),
        homeappModel.getfeaturecat(globalDetails),
        homeappModel.trailpackimagedata(globalDetails),
        knex('app_settings')
          .where('store_id', 7)
          .select(
            'bg_image_color',
            knex.raw(
              `CASE
                WHEN home_bg_image ILIKE 'http%' THEN home_bg_image
                ELSE COALESCE(?, '') || REGEXP_REPLACE(COALESCE(home_bg_image, ''), '^/+', '')
              END as home_bg_image`,
              [baseurl]
            )
          )
          .first()
      ]);

      const globalData = {
        banner: bannerList, top_cat: topCat, second_banner: secondBanner,
        special_offer_banner: specialOfferBanner, sneaky_banner: sneakyOfferBanner,
        popup_banner: popupBanner, recentselling: recentSelling, topselling: topSelling,
        additional_category: additionalCategory, brand: brandList,
        occasionalCategory: occasionalCategory, featurecategory: featurecategory,
        trailpackimage: trailpackimage, oneapi_bg_first_image: bgimage
      };

      await setCache(globalCacheKey, globalData, ONE_HOUR);
      return globalData;
    };

    // Helper: Fetch User Data (Specific to user, including markers)
    const fetchUserData = async () => {
      const user_id = (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null) ? appDetatils.user_id : appDetatils.device_id;

      if (!user_id || user_id === "null") {
        return {
          orderlist: [],
          activesub_ordlist: [],
          trailpackimage: null,
          userMarkers: {
            wishlistIds: [],
            cartMap: {},
            subCartMap: {},
            notifyMeIds: []
          }
        };
      }

      const wishlistCacheKey = generateWishlistCacheKey(user_id, appDetatils.store_id);
      const cartHashKey = generateCartHashKey(user_id, appDetatils.store_id);
      const subCartHashKey = generateSubCartHashKey(user_id, appDetatils.store_id);

      // 1. Fetch Basic User Data (Orders, Notify Me) and Markers from Redis in parallel
      let [userDataFromCache, cartMap, subCartMap, wishlistIds] = await Promise.all([
        getCache(userCacheKey),
        hGetAll(cartHashKey),
        hGetAll(subCartHashKey),
        sMembers(wishlistCacheKey)
      ]);

      let userData = userDataFromCache;

      // 2. Fallback for Basic User Data (Sequential but only if cache miss)
      if (!userData) {
        const [orderlist, activesub_ordlist, notifyMe, trailpackimage] = await Promise.all([
          homeappModel.getorderList(appDetatils),
          homeappModel.getactivesubord(appDetatils),
          user_id ? knex('product_notify_me').where('user_id', user_id).select('varient_id') : [],
          homeappModel.trailpackimagedata(appDetatils)
        ]);

        userData = {
          orderlist, activesub_ordlist,
          trailpackimage,
          userMarkers: {
            notifyMeIds: notifyMe.map(n => n.varient_id)
          }
        };
        setCache(userCacheKey, userData, TEN_MINUTES); // Background
      } else if (userData.trailpackimage === undefined) {
        // Cache was populated before we added trailpackimage: fetch and attach (then optionally backfill cache)
        const trailpackimage = await homeappModel.trailpackimagedata(appDetatils);
        userData.trailpackimage = trailpackimage;
        setCache(userCacheKey, userData, TEN_MINUTES); // Background backfill
      }

      // 3. Marker Synchronization & Lazy-Loading (Parallel Fallback if needed)
      const needsCartInit = user_id && Object.keys(cartMap).length === 0;
      const needsSubCartInit = user_id && Object.keys(subCartMap).length === 0;
      const needsWishlistInit = user_id && wishlistIds.length === 0;

      if (needsCartInit || needsSubCartInit || needsWishlistInit) {
        const syncPromises = [];

        if (needsCartInit) {
          syncPromises.push(
            knex('store_orders')
              .where('store_approval', user_id).where('order_cart_id', 'incart').whereNull('subscription_flag').select('varient_id', 'qty')
              .then(rows => {
                cartMap = rows.length > 0 ? rows.reduce((acc, c) => ({ ...acc, [c.varient_id]: c.qty }), { _initialized: 1 }) : { _initialized: 1 };
                // Background update Redis
                Promise.all(Object.entries(cartMap).map(([field, val]) => hSet(cartHashKey, field, val, ONE_HOUR * 24)));
              })
          );
        }

        if (needsSubCartInit) {
          syncPromises.push(
            knex('store_orders')
              .where('store_approval', user_id).where('order_cart_id', 'incart').where('subscription_flag', 1).select('varient_id', 'qty')
              .then(rows => {
                subCartMap = rows.length > 0 ? rows.reduce((acc, c) => ({ ...acc, [c.varient_id]: c.qty }), { _initialized: 1 }) : { _initialized: 1 };
                // Background update Redis
                Promise.all(Object.entries(subCartMap).map(([field, val]) => hSet(subCartHashKey, field, val, ONE_HOUR * 24)));
              })
          );
        }

        if (needsWishlistInit) {
          const dbUserId = (typeof user_id === 'string' && !isNaN(user_id)) ? parseInt(user_id) : user_id;
          syncPromises.push(
            knex('wishlist').where('user_id', dbUserId).select('varient_id')
              .then(rows => {
                wishlistIds = rows.map(w => w.varient_id.toString());
                if (wishlistIds.length > 0) {
                  // Background update Redis
                  Promise.all(wishlistIds.map(id => sAdd(wishlistCacheKey, id, ONE_HOUR * 24)));
                }
              })
          );
        }

        // Await the needed DB hits in parallel for this request
        await Promise.all(syncPromises);
      }

      // Filter out internal '_initialized' marker from returning to the application
      const filteredWishlist = wishlistIds.filter(item => item !== '_initialized');
      const filteredCart = { ...cartMap };
      delete filteredCart._initialized;
      const filteredSubcart = { ...subCartMap };
      delete filteredSubcart._initialized;

      // Inject markers into userData for decoration
      userData.userMarkers.wishlistIds = filteredWishlist;
      userData.userMarkers.cartMap = filteredCart;
      userData.userMarkers.subCartMap = filteredSubcart;

      return userData;
    };

    // Run BOTH tiers in parallel
    const [globalData, userData] = await Promise.all([
      fetchGlobalData(),
      fetchUserData()
    ]);

    // Decoration Step: Apply user markers to global product lists
    const decorate = (products) => {
      if (!products || !Array.isArray(products)) return products;
      const { wishlistIds, cartMap, subCartMap, notifyMeIds } = userData.userMarkers;
      const decorateVariantList = (list) => {
        if (!list || !Array.isArray(list)) return list;
        return list.map(v => {
          const v_cart_qty = parseInt(cartMap[v.varient_id] || 0, 10);
          const v_subcartQty = parseInt(subCartMap[v.varient_id] || 0, 10);
          return {
            ...v,
            isFavourite: wishlistIds.some(id => id.toString() === v.varient_id.toString()) ? "true" : "false",
            cart_qty: v_cart_qty,
            total_cart_qty: v_cart_qty,
            subcartQty: v_subcartQty,
            total_subcart_qty: v_subcartQty,
            is_notify_me: notifyMeIds.includes(v.varient_id) ? 1 : 0
          };
        });
      };
      return products.map(p => {
        // If product has variants, decorate them too (`variant` or `varients` spelling)
        let variantsTotalCartQty = 0;
        let variantsTotalSubcartQty = 0;
        if (p.variant && Array.isArray(p.variant)) {
          p.variant = decorateVariantList(p.variant);
          variantsTotalCartQty += p.variant.reduce((sum, v) => sum + (Number(v?.cart_qty) || 0), 0);
          variantsTotalSubcartQty += p.variant.reduce((sum, v) => sum + (Number(v?.subcartQty) || 0), 0);
        }
        if (p.varients && Array.isArray(p.varients)) {
          p.varients = decorateVariantList(p.varients);
          variantsTotalCartQty += p.varients.reduce((sum, v) => sum + (Number(v?.cart_qty) || 0), 0);
          variantsTotalSubcartQty += p.varients.reduce((sum, v) => sum + (Number(v?.subcartQty) || 0), 0);
        }
        // Decorate the main product if it has a varient_id
        if (p.varient_id) {
          const p_cart_qty = parseInt(cartMap[p.varient_id] || 0);
          const p_subcartQty = parseInt(subCartMap[p.varient_id] || 0);
          return {
            ...p,
            isFavourite: wishlistIds.some(id => id.toString() === p.varient_id.toString()) ? "true" : "false",
            cart_qty: p_cart_qty,
            total_cart_qty: variantsTotalCartQty || p_cart_qty,
            subcartQty: p_subcartQty,
            total_subcart_qty: variantsTotalSubcartQty || p_subcartQty,
            is_notify_me: notifyMeIds.includes(p.varient_id) ? 1 : 0
          };
        }
        return p;
      });
    };

    // Construct final response (decorate trailpackimage from user cache when present)
    const finalData = {
      status: "1",
      message: "Homepage data",
      ...globalData,
      trailpackimage: userData.trailpackimage !== undefined ? userData.trailpackimage : globalData.trailpackimage,
      additional_category: globalData.additional_category ? globalData.additional_category.map(cat => ({
        ...cat,
        product_details: decorate(cat.product_details)
      })) : [],
      orderlist: userData.orderlist,
      activesub_ordlist: userData.activesub_ordlist,
      recentselling: decorate(globalData.recentselling),
      topselling: decorate(globalData.topselling),
      occasionalCategory: globalData.occasionalCategory ? globalData.occasionalCategory.map(cat => ({
        ...cat,
        product_details: decorate(cat.product_details)
      })) : []
    };

    res.status(200).json(finalData);
  } catch (error) {
    // console.error('OneAPI Split Error:', error);
    res.status(500).json({ status: 0, message: error.message });
  }
};

const oneapi1 = async (req, res) => {

  try {
    const appDetatils = req.body;
    const cacheKey = generateOneApi1CacheKey(appDetatils);

    // Try to get from cache first
    const cachedData = await getCache(cacheKey);
    if (cachedData) {
      return res.status(200).json(cachedData);
    }

    const bannerList = await homeappModel.getBanner(appDetatils);
    const topCat = await homeappModel.getTopCat(appDetatils);
    const featurecategory = await homeappModel.getfeaturecat(appDetatils);
    const secondBanner = await homeappModel.getsecondBanner(appDetatils);
    const activesub_ordlist = await homeappModel.getactivesubord(appDetatils);

    var data = {
      "status": "1",
      "message": "Homepage data",
      "banner": bannerList,
      "top_cat": topCat,
      "featurecategory": featurecategory,
      "second_banner": secondBanner,
      "activesub_ordlist": activesub_ordlist
    };

    // Store in cache for 3000 seconds
    await setCache(cacheKey, data, 3000);

    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const oneapi2 = async (req, res) => {

  try {
    const appDetatils = req.body;
    const cacheKey = generateOneApi2CacheKey(appDetatils);

    // Try to get from cache first
    const cachedData = await getCache(cacheKey);
    if (cachedData) {
      return res.status(200).json(cachedData);
    }

    const specialOfferBanner = await homeappModel.specialOfferBanner(appDetatils);
    const topSelling = await homeappModel.gettopSelling(appDetatils);
    const orderlist = await homeappModel.getorderList(appDetatils);
    const occasionalCategory = await homeappModel.getoccasionalCategory(appDetatils);

    var data = {
      "status": "1",
      "message": "Homepage data",
      "orderlist": orderlist,
      "special_offer_banner": specialOfferBanner,
      "occasionalCategory": occasionalCategory,
      "topselling": topSelling,
    };

    // Store in cache for 3000 seconds
    await setCache(cacheKey, data, 3000);

    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const oneapi3 = async (req, res) => {

  try {
    const appDetatils = req.body;
    const cacheKey = generateOneApi3CacheKey(appDetatils);

    // Try to get from cache first
    const cachedData = await getCache(cacheKey);
    if (cachedData) {
      return res.status(200).json(cachedData);
    }

    const sneakyOfferBanner = await homeappModel.sneakyOfferBanner(appDetatils);
    const trailpackimage = await homeappModel.trailpackimagedata(appDetatils);
    const recentSelling = await homeappModel.getrecentSelling(appDetatils);
    const brandList = await homeappModel.getBrand(appDetatils);
    const popupBanner = await homeappModel.popupBanner(appDetatils);


    var data = {
      "status": "1",
      "message": "Homepage data",
      "popupdata": "",
      "popup_banner": popupBanner,
      "sneaky_banner": sneakyOfferBanner,
      "recentselling": recentSelling,
      "brand": brandList,
      "trailpackimage": trailpackimage
    };

    // Store in cache for 3000 seconds
    await setCache(cacheKey, data, 3000);

    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const oneapiadditionalcategory = async (req, res) => {

  try {
    const appDetatils = req.body;

    const additionalCategory = await homeappModel.getadditionalCategory(appDetatils);

    var data = {
      "status": "1",
      "message": "Homepage data",
      "additional_category": additionalCategory,
    };
    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

//ADDITIONAL CATEGORY META API 1//
const getAdditionalCategoryMeta = async (req, res) => {
  try {
    const appDetatils = req.body;
    const metaList = await homeappModel.getAdditionalCategoryMeta(appDetatils);
    res.status(200).json({
      status: '1',
      message: 'Additional category meta',
      data: metaList
    });
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};
///META API 1 ENDS

//OCCASIONAL CATEGORY META API 2//
const getOccasionalCategoryMeta = async (req, res) => {
  try {
    const appDetatils = req.body;
    const metaList = await homeappModel.getOccasionalCategoryMeta(appDetatils);
    res.status(200).json({
      status: '1',
      message: 'Occasional category meta',
      data: metaList
    });
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};
///META API 2 ENDS



const featurecategory = async (req, res) => {
  try {
    // Same store_id resolution as oneapi home (body.store_id); GET also supports ?store_id= and x-store-id
    const raw =
      req.query.store_id ??
      req.headers['x-store-id'] ??
      (req.body && req.body.store_id);
    const store_id =
      raw != null && raw !== '' ? raw : undefined;
    const featurecategorydata = await homeappModel.getfeaturecategory(
      store_id != null ? { store_id } : {}
    );

    var data = {
      "status": "1",
      "message": "Feature Category",
      "data": featurecategorydata || [],  // Ensure array fallback
    };
    res.status(200).json(data);
  } catch (error) {
    // console.error('Feature Category API Error:', error);
    res.status(500).json({
      status: "0",
      message: 'Failed to fetch feature categories',
      error: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
};

//FEATURED CATEGORY DETAIL API 3//
const featurecategoryDetail = async (req, res) => {
  try {
    const appDetatils = req.body;
    const featurecategorydata = await homeappModel.getfeaturecategoryDetail(appDetatils);

    var data = {
      "status": "1",
      "message": "Feature Category",
      "data": featurecategorydata,

    };
    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    res.status(500).json({ message: 'data not found' });
  }

};

///META API 3 ENDS


// SUBCATEGORY DETAIL META API 4//
const subcategoryDetail = async (req, res) => {
  try {
    const appDetatils = req.body;
    const subcategory = await homeappModel.getSubcategoryDetail(appDetatils);
    var data = {
      "status": "1",
      "message": "Products found",
      "data": subcategory,
    };
    res.status(200).json(data);

  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Products not found' });
  }
};
///META API 4 ENDS

const cat_product = async (req, res) => {
  try {
    const appDetatils = req.body;

    const startTime = Date.now();
    const product = await homeappModel.getcatProduct(appDetatils);
    const totalTime = Date.now() - startTime;
    // console.log(`✅ CatProduct completed in ${totalTime}ms`);
    // console.log(`📊 Products returned: ${product?.length || 0}`);

    var data = {
      "status": "1",
      "message": "Products found",
      "data": product,
    };
    res.status(200).json(data);

  } catch (error) {
    console.error('=== CatProduct Error ===');
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    res.status(500).json({ status: 0, message: 'Products not found' });
  }
};

const featurecat_prod = async (req, res) => {
  try {
    const appDetatils = req.body;
    const product = await homeappModel.getfetcatProd(appDetatils);
    var data = {
      "status": "1",
      "message": "Products List",
      "data": product
    };
    res.status(200).json(data);

  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Products not found' });
  }
};

const product_det = async (req, res) => {
  try {
    const appDetatils = req.body;

    // Fast path: get cat_id first (single indexed query), then run details + similar in parallel
    const catIdRow = await knex('product')
      .where('product_id', parseInt(appDetatils.product_id))
      .select('cat_id')
      .first();

    const [productDetails, similarProds] = await Promise.all([
      homeappModel.prodDetails(appDetatils),
      homeappModel.similarProds(appDetatils, catIdRow?.cat_id)
    ]);

    var data = {
      "status": "1",
      "message": "Products Detail",
      "detail": productDetails,
      "similar_product": similarProds
    };
    res.status(200).json(data);

  } catch (error) {
    // console.error(error);
    if (error.message === 'Product not Found') {
      res.status(200).json({ status: 0, message: 'Product not Found' });
    } else {
      res.status(500).json({ status: 0, message: error.message });
    }
  }
};

const catee = async (req, res) => {

  try {
    const appDetatils = req.body;
    const categoryList = await homeappModel.getcategoryList(appDetatils);

    var data = {
      "status": "1",
      "message": "data found",
      "data": categoryList,

    };
    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    res.status(500).json({ status: 0, message: 'Internal Server Error' });
  }
};

const brand_list = async (req, res) => {
  try {
    const brandList = await homeappModel.getBrandlist();

    const data = {
      "status": "1",
      "message": "data found",
      "data": brandList,
    };
    res.status(200).json(data);
  } catch (error) {
    // console.error('Error in brand_list API:', error);
    res.status(500).json({
      status: "0",
      message: 'Internal Server Error',
      error: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
};

const aboutus = async (req, res) => {
  try {

    const aboutData = await homeappModel.getaboutData();

    var data = {
      "status": "1",
      "message": "About us",
      "data": aboutData,

    };
    res.status(200).json(data);
  } catch (error) {



    // console.error(error);
    res.status(500).json({ message: 'data not found' });


  }


};

const terms = async (req, res) => {
  try {

    const aboutData = await homeappModel.gettermsData();

    var data = {
      "status": "1",
      "message": "Terms & Condition",
      "data": aboutData,

    };
    res.status(200).json(data);
  } catch (error) {



    // console.error(error);
    res.status(500).json({ message: 'data not found' });


  }


};

const subcatee = async (req, res) => {
  try {
    const appDetatils = req.body;
    const categoryList = await homeappModel.getsubcategoryList(appDetatils);

    var data = {
      "status": "1",
      "message": "data found",
      "data": categoryList,

    };
    res.status(200).json(data);
  } catch (error) {
    if (error.message === 'Products not found') {

      var data = {
        "message": "Products not found",
        "data": [],

      };
      res.status(400).json(data);
      //res.status(400).json({ message: 'Products not found' });
    }


    else {
      // console.error(error);
      // res.status(500).json({ message: 'Internal Server Error' });
      res.status(500).json({ status: 0, message: error.message });
    }

  }


};

const appinfo = async (req, res) => {

  try {
    const appDetatils = req.body;
    const baseurl = process.env.BUNNY_NET_IMAGE;

    // Run sequentially to avoid exhausting the DB connection pool (1 connection at a time).
    // Parallel would use 3 connections per request and cause "Timeout acquiring a connection" under load.
    const appdata = await productlistModel.appinformation(appDetatils);
    const trailpackimage = await productlistModel.trailpackimagedata(appDetatils);
    const bgimage = await knex('app_settings')
      .where('store_id', 7)
      .select(
        'bg_image_color',
        knex.raw(
          `CASE
            WHEN home_bg_image ILIKE 'http%' THEN home_bg_image
            ELSE COALESCE(?, '') || REGEXP_REPLACE(COALESCE(home_bg_image, ''), '^/+', '')
          END as home_bg_image`,
          [baseurl]
        )
      )
      .first();

    var data = {
      "status": "1",
      "message": "data found",
      "data": appdata,
      "oneapi_bg_first_image": bgimage,
      "trailpackimage": trailpackimage
      //"data":appdata

    };
    res.status(200).json(data);
  } catch (error) {
    console.error('[app_info API] Error:', error.message);
    console.error('[app_info API] Stack:', error.stack);
    if (req.body && Object.keys(req.body).length) {
      console.error('[app_info API] Request body:', JSON.stringify(req.body));
    }
    res.status(500).json({ status: 0, message: error.message });
  }

};

const updateproductdetails = async (req, res) => {
  try {
    const appDetatils = req.body;
    const appdata = await productlistModel.UpdateproductDetails(appDetatils);
    var data = {
      "status": "1",
      "message": "data found",
      "data": appdata
    };
    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    // res.status(500).json({ status: 0, message: 'Internal Server Error' });
    res.status(500).json({ status: 0, message: error.message });
  }
};

const getoffer = async (req, res) => {
  try {
    const appDetatils = req.body;
    const appdata = await homeappModel.getOffer(appDetatils);
    var data = {
      "status": "1",
      "message": "data found",
      "data": appdata
    };
    res.status(200).json(data);
  } catch (error) {
    // console.error(error);
    // res.status(500).json({ status: 0, message: 'Internal Server Error' });
    res.status(500).json({ status: 0, message: error.message });
  }
};

const refreshOneApiGlobal = async (req, res) => {
  try {
    // Hardcoded parameters for store 7 and subscription 1, user 2
    const appDetails = {
      store_id: 7,
      is_subscription: 1,
      user_id: 2
    };

    const globalCacheKey = generateOneApiGlobalCacheKey(appDetails);

    // 1. Delete the Redis key
    await deleteCache(globalCacheKey);

    // 2. Trigger cache warming by calling oneapi logic but with a mock response object
    // to avoid sending the full body to the cron service.
    let cacheStatus = 'Failed to warm';

    // We create a mock response object to capture the result without sending it
    const mockRes = {
      status: function (code) {
        return {
          json: function (data) {
            if (code === 200 || data.status == 1) {
              cacheStatus = 'Success';
            }
          }
        };
      }
    };

    // Ensure req.body has the right params
    req.body = appDetails;

    // Execute oneapi logic silently
    await oneapi(req, mockRes);

    res.status(200).json({
      status: 1,
      cache_deleted: true,
      cache_warming: cacheStatus,
      message: "OneAPI global cache for store 7 / sub 1 refreshed using user 2"
    });

  } catch (error) {
    console.error('Refresh OneAPI Global Error:', error.message);
    res.status(500).json({ status: 0, message: error.message });
  }
};

const clearAllCache = async (req, res) => {
  try {
    const { redis } = require('../utils/redisClient');
    // Fetch all keys related to oneapi
    const keys = await redis.keys('oneapi*');

    if (keys.length > 0) {
      // Upstash REST API requires exact keys to delete
      for (const key of keys) {
        await redis.del(key);
      }
    }

    res.status(200).json({
      status: 1,
      message: `Successfully cleared ${keys.length} global cache keys from Redis.`
    });
  } catch (error) {
    console.error('Clear All Cache Error:', error.message);
    res.status(500).json({ status: 0, message: error.message });
  }
};

module.exports = {
  oneapi,
  refreshOneApiGlobal,
  oneapi1,
  oneapi2,
  oneapi3,
  oneapiadditionalcategory,
  catee,
  product_det,
  subcatee,
  cat_product,
  brand_list,
  aboutus,
  terms,
  appinfo,
  updateproductdetails,
  featurecat_prod,
  featurecategory,
  getoffer,
  getAdditionalCategoryMeta,
  getOccasionalCategoryMeta,
  featurecategoryDetail,
  subcategoryDetail,
  clearAllCache
};
