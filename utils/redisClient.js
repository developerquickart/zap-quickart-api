const { Redis } = require('@upstash/redis');

// Initialize Upstash Redis client
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

/**
 * Generate cache key for searchbystoreproduct API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateSearchCacheKey = (params) => {
  const {
    store_id,
    keyword,
    user_id,
    device_id,
    sub_cat_id,
    cat_id,
    sortprice,
    sortname,
    page,
    perpage,
    min_price,
    max_price,
    min_discount,
    max_discount,
    stock
  } = params;

  // Create a unique key based on all search parameters
  const keyParts = [
    'searchbystoreproduct',
    `store:${store_id}`,
    `keyword:${keyword || 'null'}`,
    `user:${user_id || device_id || 'null'}`,
    `subcat:${sub_cat_id || 'null'}`,
    `cat:${cat_id || 'null'}`,
    `sortprice:${sortprice || 'null'}`,
    `sortname:${sortname || 'null'}`,
    `page:${page || 1}`,
    `perpage:${perpage || 20}`,
    `minprice:${min_price || 'null'}`,
    `maxprice:${max_price || 'null'}`,
    `mindiscount:${min_discount || 'null'}`,
    `maxdiscount:${max_discount || 'null'}`,
    `stock:${stock || 'null'}`
  ];

  return keyParts.join('|');
};

/**
 * Generate cache key for cat_product API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateCatProductCacheKey = (params) => {
  const {
    cat_id,
    sub_cat_id,
    store_id,
    byname,
    sort,
    sortprice,
    sortname,
    user_id,
    device_id,
    min_price,
    max_price,
    min_discount,
    max_discount,
    page,
    perpage
  } = params;

  // Create a unique key based on all search parameters
  const keyParts = [
    'catproduct',
    `store:${store_id}`,
    `cat:${cat_id || 'null'}`,
    `subcat:${sub_cat_id || 'null'}`,
    `byname:${byname || 'null'}`,
    `sort:${sort || 'null'}`,
    `sortprice:${sortprice || 'null'}`,
    `sortname:${sortname || 'null'}`,
    `user:${user_id || device_id || 'null'}`,
    `page:${page || 1}`,
    `perpage:${perpage || 20}`,
    `minprice:${min_price || 'null'}`,
    `maxprice:${max_price || 'null'}`,
    `mindiscount:${min_discount || 'null'}`,
    `maxdiscount:${max_discount || 'null'}`
  ];

  return keyParts.join('|');
};

/**
 * Generate cache key for notificationlist API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateNotificationListCacheKey = (params) => {
  const { user_id } = params;
  return `notificationlist|user:${user_id}`;
};

/**
 * Generate cache key for oneapi API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOneApiCacheKey = (params) => {
  const { store_id, user_id, device_id, is_subscription } = params;
  return `oneapi|store:${store_id}|user:${user_id || device_id || 'null'}|sub:${is_subscription || 0}`;
};

/**
 * Generate cache key for oneapi1 API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOneApi1CacheKey = (params) => {
  const { store_id, user_id, device_id, is_subscription } = params;
  return `oneapi1|store:${store_id}|user:${user_id || device_id || 'null'}|sub:${is_subscription || 0}`;
};

/**
 * Generate cache key for oneapi2 API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOneApi2CacheKey = (params) => {
  const { store_id, user_id, device_id, is_subscription } = params;
  return `oneapi2|store:${store_id}|user:${user_id || device_id || 'null'}|sub:${is_subscription || 0}`;
};

/**
 * Generate cache key for oneapi3 API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOneApi3CacheKey = (params) => {
  const { store_id, user_id, device_id, is_subscription } = params;
  return `oneapi3|store:${store_id}|user:${user_id || device_id || 'null'}|sub:${is_subscription || 0}`;
};

/**
 * Generate cache key for additionalcat_search API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateAdditionalCatSearchCacheKey = (params) => {
  const { store_id, byname, cat_id, page, perpage, keyword } = params;
  return `additionalcat_search|store:${store_id}|byname:${byname || 'null'}|cat:${cat_id || 'null'}|page:${page || 1}|perpage:${perpage || 20}|kw:${keyword || 'null'}`;
};

/**
 * Generate cache key for occasionalcat_search API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOccasionalCatSearchCacheKey = (params) => {
  const { store_id, byname, cat_id, page, perpage, keyword } = params;
  return `occasionalcat_search|store:${store_id}|byname:${byname || 'null'}|cat:${cat_id || 'null'}|page:${page || 1}|perpage:${perpage || 200}|kw:${keyword || 'null'}`;
};

/**
 * Generate cache key for top_selling API
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateTopSellingCacheKey = (params) => {
  const {
    store_id, byname, min_price, max_price, stock, min_discount,
    max_discount, min_rating, max_rating, sort, sortname,
    sortprice, cat_id, sub_cat_id, page, perpage
  } = params;

  const keyParts = [
    'topselling',
    `store:${store_id}`,
    `byname:${byname || 'null'}`,
    `minprice:${min_price || 'null'}`,
    `maxprice:${max_price || 'null'}`,
    `stock:${stock || 'null'}`,
    `mindiscount:${min_discount || 'null'}`,
    `maxdiscount:${max_discount || 'null'}`,
    `minrating:${min_rating || 'null'}`,
    `maxrating:${max_rating || 'null'}`,
    `sort:${sort || 'null'}`,
    `sortname:${sortname || 'null'}`,
    `sortprice:${sortprice || 'null'}`,
    `cat:${cat_id || 'null'}`,
    `subcat:${sub_cat_id || 'null'}`,
    `page:${page || 1}`,
    `perpage:${perpage || 20}`
  ];

  return keyParts.join('|');
};

/**
 * Generate global cache key for oneapi
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOneApiGlobalCacheKey = (params) => {
  const { store_id, is_subscription } = params;
  return `oneapi:global|store:${store_id}|sub:${is_subscription || 0}`;
};

/**
 * Generate user-specific cache key for oneapi
 * @param {Object} params - Request parameters
 * @returns {string} Cache key
 */
const generateOneApiUserCacheKey = (params) => {
  const { store_id, user_id, device_id, is_subscription } = params;
  return `oneapi:user|store:${store_id}|user:${user_id || device_id || 'null'}|sub:${is_subscription || 0}`;
};

/**
 * Generate wishlist cache key (Redis Set)
 * @param {number|string} user_id - Real user ID or device ID
 * @param {number} store_id - Store ID
 * @returns {string} Cache key
 */
const generateWishlistCacheKey = (user_id, store_id) => {
  return `wishlist:set|store:${store_id}|user:${user_id || 'null'}`;
};

/**
 * Generate regular cart hash key (Redis Hash)
 * @param {number|string} user_id - Real user ID or device ID
 * @param {number} store_id - Store ID
 * @returns {string} Cache key
 */
const generateCartHashKey = (user_id, store_id) => {
  return `cart:hash|store:${store_id}|user:${user_id || 'null'}`;
};

/**
 * Generate subscription cart hash key (Redis Hash)
 * @param {number|string} user_id - Real user ID or device ID
 * @param {number} store_id - Store ID
 * @returns {string} Cache key
 */
const generateSubCartHashKey = (user_id, store_id) => {
  return `subcart:hash|store:${store_id}|user:${user_id || 'null'}`;
};

/**
 * Get cached data
 * @param {string} key - Cache key
 * @returns {Promise<Object|null>} Cached data or null
 */
const getCache = async (key) => {
  try {
    const cached = await redis.get(key);
    if (cached) {
      console.log(`✅ Cache HIT: ${key.substring(0, 80)}...`);
      // Upstash Redis client might already parse JSON if it's an object
      return typeof cached === 'string' ? JSON.parse(cached) : cached;
    }
    console.log(`❌ Cache MISS: ${key.substring(0, 80)}...`);
    return null;
  } catch (error) {
    console.error('Redis GET error:', error.message);
    return null; // Return null on error to allow fallback to database
  }
};

/**
 * Set cache data
 * @param {string} key - Cache key
 * @param {Object} data - Data to cache
 * @param {number} ttl - Time to live in seconds (default: 300 = 5 minutes)
 * @returns {Promise<void>}
 */
const setCache = async (key, data, ttl = 900) => {
  try {
    await redis.setex(key, ttl, JSON.stringify(data));
    console.log(`💾 Cache SET: ${key.substring(0, 80)}... (TTL: ${ttl}s)`);
  } catch (error) {
    console.error('Redis SET error:', error.message);
    // Don't throw - caching is optional, shouldn't break the API
  }
};

/**
 * Delete cache by pattern (useful for cache invalidation)
 * @param {string} pattern - Pattern to match (e.g., 'searchbystoreproduct|store:7|*')
 * @returns {Promise<void>}
 */
const deleteCachePattern = async (pattern) => {
  try {
    // Note: Upstash REST API doesn't support KEYS command directly
    // You'll need to track keys or use a different approach
    // For now, we'll use a simple delete if you know the exact key
    console.log(`🗑️ Cache DELETE pattern: ${pattern}`);
  } catch (error) {
    console.error('Redis DELETE error:', error.message);
  }
};

/**
 * Delete specific cache key
 * @param {string} key - Cache key to delete
 * @returns {Promise<void>}
 */
const deleteCache = async (key) => {
  try {
    await redis.del(key);
    console.log(`🗑️ Cache DELETE: ${key.substring(0, 80)}...`);
  } catch (error) {
    console.error('Redis DELETE error:', error.message);
  }
};

/**
 * Clear all search cache for a specific store
 * @param {number} store_id - Store ID
 * @returns {Promise<void>}
 */
const clearStoreSearchCache = async (store_id) => {
  try {
    // This is a simplified version - Upstash REST API limitations
    // In production, consider maintaining a key index or using Upstash's SCAN
    console.log(`🗑️ Clearing cache for store: ${store_id}`);
    // Note: Full pattern deletion requires SCAN which isn't available in REST API
    // Consider maintaining a Set of keys per store for efficient invalidation
  } catch (error) {
    console.error('Redis CLEAR error:', error.message);
  }
};

/**
 * Clear notification cache for a specific user
 * @param {number} user_id - User ID
 * @returns {Promise<void>}
 */
const clearUserNotificationCache = async (user_id) => {
  try {
    const cacheKey = generateNotificationListCacheKey({ user_id });
    await deleteCache(cacheKey);
  } catch (error) {
    console.error('Redis CLEAR notification cache error:', error.message);
  }
};

/**
 * Redis SET Helpers
 */

const sAdd = async (key, member, ttl = 3600) => {
  try {
    await redis.sadd(key, member);
    await redis.expire(key, ttl);
    console.log(`➕ SADD: ${key} -> ${member}`);
  } catch (error) {
    console.error('Redis SADD error:', error.message);
  }
};

const sRem = async (key, member) => {
  try {
    await redis.srem(key, member);
    console.log(`➖ SREM: ${key} -> ${member}`);
  } catch (error) {
    console.error('Redis SREM error:', error.message);
  }
};

const sIsMember = async (key, member) => {
  try {
    return await redis.sismember(key, member);
  } catch (error) {
    console.error('Redis SISMEMBER error:', error.message);
    return false;
  }
};

const sMembers = async (key) => {
  try {
    return await redis.smembers(key);
  } catch (error) {
    console.error('Redis SMEMBERS error:', error.message);
    return [];
  }
};

/**
 * Redis HASH Helpers
 */

const hSet = async (key, field, value, ttl = 3600) => {
  try {
    await redis.hset(key, { [field]: value });
    await redis.expire(key, ttl);
    const logKey = typeof key === 'string' ? key.substring(0, 80) : 'unknown';
    console.log(`📝 HSET: ${logKey} -> [${field}]: ${value}`);
  } catch (error) {
    console.error('Redis HSET error:', error.message);
  }
};

const hGetAll = async (key) => {
  try {
    const data = await redis.hgetall(key);
    return data || {};
  } catch (error) {
    console.error('Redis HGETALL error:', error.message);
    return {};
  }
};

const hDel = async (key, field) => {
  try {
    await redis.hdel(key, field);
    const logKey = typeof key === 'string' ? key.substring(0, 80) : 'unknown';
    console.log(`🗑️ HDEL: ${logKey} -> [${field}]`);
  } catch (error) {
    console.error('Redis HDEL error:', error.message);
  }
};

module.exports = {
  redis,
  generateSearchCacheKey,
  generateCatProductCacheKey,
  generateNotificationListCacheKey,
  generateOneApiCacheKey,
  generateOneApi1CacheKey,
  generateOneApi2CacheKey,
  generateOneApi3CacheKey,
  generateAdditionalCatSearchCacheKey,
  generateOccasionalCatSearchCacheKey,
  generateTopSellingCacheKey,
  generateOneApiGlobalCacheKey,
  generateOneApiUserCacheKey,
  generateWishlistCacheKey,
  generateCartHashKey,
  generateSubCartHashKey,
  getCache,
  setCache,
  deleteCache,
  deleteCachePattern,
  clearStoreSearchCache,
  clearUserNotificationCache,
  sAdd,
  sRem,
  sIsMember,
  sMembers,
  hSet,
  hGetAll,
  hDel
};
