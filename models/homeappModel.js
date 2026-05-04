// models/homeappModel.js
const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const { format } = require('date-fns');
const moment = require('moment');
const logToFile = require("../utils/logger");

/**
 * Shared with oneapi home (`getfeaturecat`): feature rows that have in-stock variants at this store.
 * @param {string|number|undefined} store_id - Same as req.body.store_id on oneapi; defaults to 7.
 * @param {{ limit?: number }} [opts] - Homepage passes limit 18; standalone API returns all matching rows.
 */
const selectFeatureCategoriesForStore = async (store_id, opts = {}) => {
  const { limit } = opts;
  const baseurl = process.env.BUNNY_NET_IMAGE || '';
  const storeId =
    store_id != null && store_id !== ''
      ? store_id
      : 7;

  let q = knex('feature_categories')
    .select(
      'feature_categories.id',
      'feature_categories.title',
      knex.raw(`? || feature_categories.image as image`, [baseurl])
    )
    .innerJoin('product', function () {
      this.on(knex.raw(`product.fcat_id IS NOT NULL AND product.fcat_id != '' AND feature_categories.id::text = ANY(string_to_array(TRIM(product.fcat_id), ','))`));
    })
    .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
    .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .where('feature_categories.is_deleted', 0)
    .where('feature_categories.status', 1)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('product.is_zap', true)
    .where('store_products.store_id', storeId)
    .where('store_products.stock', '>', 0)
    .where('store_products.is_deleted', 0)
    .groupBy(
      'feature_categories.id',
      'feature_categories.title',
      'feature_categories.image',
      knex.raw('feature_categories."order"')
    )
    .orderBy(knex.raw('feature_categories."order"'), 'ASC');

  if (limit != null && Number.isFinite(Number(limit))) {
    q = q.limit(Number(limit));
  }

  return q;
};

const getfeaturecategory = async (appDetatils = {}) => {
  try {
    const result = await selectFeatureCategoriesForStore(appDetatils.store_id, {});
    return result || [];
  } catch (error) {
    // console.error('getfeaturecategory error:', error);
    // logToFile.error('getfeaturecategory error:', error);
    return [];
  }
};




//featurecategoryDetail META API MODEL FUNCTION 1//
const getfeaturecategoryDetail = async (appDetatils) => {
  const baseurl = process.env.BUNNY_NET_IMAGE;

  return await knex('feature_categories')
    .select('id', 'feature_categories.title', knex.raw(`CONCAT('${baseurl}', feature_categories.image) as image`), 'meta_title', 'meta_description')
    .where('is_deleted', 0)
    .where('status', 1)
    .where('id', appDetatils.fcat_id)
    .first();

};
//FUNCTION 1 ENDS//


const getorderList = async (appDetatils) => {
  const { store_id } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const user_id = (appDetatils.user_id && appDetatils.user_id !== "null" && appDetatils.user_id !== null) ? appDetatils.user_id : appDetatils.device_id;

  if (!user_id || user_id === "null") {
    return [];
  }

  // Fetch all orders
  const orders = await knex('orders')
    .select('orders.cart_id', 'orders.group_id', 'orders.is_subscription', 'orders.order_date')
    .where('orders.user_id', user_id)
    .whereNotNull('orders.payment_method')
    .whereNot('orders.order_status', null)
    .orderBy('orders.order_id', 'desc')
    .groupBy('orders.group_id', 'orders.cart_id', 'orders.is_subscription', 'orders.order_date', 'orders.order_id')
    .limit(8);

  if (orders.length === 0) {
    return []; // Return an empty array if no orders are found
  }

  // Extract all group_ids
  const groupIds = orders.map(order => order.group_id);
  const orderData = await knex('orders')
    .whereIn('group_id', groupIds)
    .select('cart_id', 'group_id'); // Fetch cart_id and group_id




  if (orderData.length === 0) {
    return Promise.all(
      orders.map(async (order) => {
        const ordtotalprice = await knex('orders')
          .whereIn('group_id', [order.group_id]) // Ensure array format for whereIn
          .sum('total_price as total');

        return {
          cart_id: order.cart_id,
          group_id: order.group_id,
          order_date: order.order_date,
          ordtotalprice: ordtotalprice[0]?.total || 0, // Handle possible undefined
          type: order.is_subscription === 1 ? 'Subscription' : 'Quick',
          is_subscription: order.is_subscription,
          prodList: [] // Empty product list if no data
        };
      })
    );
  }

  // Fetch all store orders in one query
  const cartIds = orderData.map(data => data.cart_id);
  const storeOrders = await knex('store_orders')
    .whereIn('order_cart_id', cartIds)
    .where('store_id', store_id)
    .select('store_order_id', 'order_cart_id', 'product_name', knex.raw(`? || varient_image as varient_image`, [baseurl]));

  // Group store orders by cart_id for easy lookup
  const storeOrderMap = {};
  storeOrders.forEach(order => {
    if (!storeOrderMap[order.order_cart_id]) {
      storeOrderMap[order.order_cart_id] = [];
    }
    storeOrderMap[order.order_cart_id].push(order);
  });

  // Fetch all group-level totals in one query
  const groupTotals = await knex('orders')
    .whereIn('group_id', groupIds)
    .groupBy('group_id')
    .select('group_id', knex.raw('SUM(total_price) as total'));

  const groupTotalMap = {};
  groupTotals.forEach(gt => {
    groupTotalMap[gt.group_id] = gt.total;
  });

  // Create the final customized product data
  const customizedProductData = orders.map((order) => {
    const typeval = order.is_subscription === 1 ? 'Subscription' : 'Quick';

    // Get the list of cart_ids for the current order
    const orderCartIds = orderData
      .filter(data => data.group_id === order.group_id)
      .map(data => data.cart_id);

    // Get the product list for the current order's cart_ids
    const prodList = orderCartIds.flatMap(cart_id => storeOrderMap[cart_id] || []);

    return {
      cart_id: order.cart_id,
      group_id: order.group_id,
      order_date: order.order_date,
      ordtotalprice: groupTotalMap[order.group_id] || 0,
      type: typeval,
      is_subscription: order.is_subscription,
      prodList: prodList
    };
  });

  return customizedProductData;
};

const getfeaturecat = async (appDetatils = {}) => {
  const { store_id } = appDetatils;

  try {
    const result = await selectFeatureCategoriesForStore(store_id, { limit: 18 });
    return result || [];
  } catch (error) {
    // console.error('getfeaturecat error:', error);
    // logToFile.error('getfeaturecat error:', error);
    return [];
  }
};

const trailpackimagedata = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  if (!user_id || user_id === "null") {
    return null;
  }

  //const today = new Date()
  const today = new Date().toISOString().split('T')[0];
  const orderlistdata = await knex('orders')
    .whereRaw('order_type ILIKE ?', ['trail'])
    .whereNotIn('order_status', ['Payment_failed', 'Cancelled'])
    .where('user_id', user_id)
    .groupBy('trail_id')
    .pluck('trail_id');

  const checktrail = await knex('tbl_trail_pack_basic')
    .where('tbl_trail_pack_basic.status', 1)
    .where('tbl_trail_pack_basic.start_date', '<=', today)
    .andWhere('tbl_trail_pack_basic.end_date', '>=', today)
    .where('tbl_trail_pack_basic.is_delete', 0)
    .whereNotIn('id', orderlistdata)
    .orderBy('main_order', 'ASC')
    .select('id', 'popup_image', 'image')
    .first();

  if (checktrail) {
    trailimage = checktrail.popup_image;
    //trailpackimage =  baseurl + "/images/trail_pack/trialpack.png"
    return trailpackimage = baseurl + trailimage;

  } else {
    return trailpackimage = null;
  }


};

const getBanner = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Detailed logging to debug banner behaviour in OneAPI
  console.log('=== [getBanner] START ===');
  console.log('[getBanner] Input:', JSON.stringify({ store_id, user_id, is_subscription }, null, 2));

  // 1) Fetch ALL active banners for this store (for diagnostics),
  //    regardless of whether they have valid products.
  const allBanners = await knex('store_banner')
    .select(
      'banner_id',
      'banner_name',
      'varient_id',
      'cat_id',
      'parent_cat_id'
    )
    .where('store_id', store_id)
    .where('is_delete', '!=', 1)
    .where('status', 1);

  console.log(
    '[getBanner] All active banners for store:',
    allBanners.map(b => ({ banner_id: b.banner_id, banner_name: b.banner_name }))
  );

  // 2) Actual query used by OneAPI – only banners that have at least one valid product
  const banners = await knex('store_banner')
    .select(
      'banner_id',
      'banner_name',
      // Include mapping fields only for logging/debug purposes
      'varient_id',
      'cat_id',
      'parent_cat_id',
      knex.raw(`? || banner_image as banner_image`, [baseurl])
    )
    .where('store_id', store_id)
    .where('is_delete', '!=', 1)
    .where('status', 1)
    .where(function () {
      this.whereExists(function () {
        this.select('*').from('product')
          .join('product_varient', 'product.product_id', 'product_varient.product_id')
          .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
          .where('store_products.store_id', store_id)
          .where('product.hide', 0)
          .where('product.is_delete', 0)
          .where('product.is_zap', true)
          .where('store_products.stock', '>', 0)
          .where('store_products.is_deleted', 0)
          // Exclude today's offer products from banner validity check:
          // include products when:
          // - is_offer_product = 0, OR
          // - offer_date is NULL, OR
          // - offer_date is not today
          .whereRaw('(product.is_offer_product = 0 OR product.offer_date IS NULL OR product.offer_date::date != CURRENT_DATE)')
          .where(function () {
            // Link products to banner either directly via cat/parent_cat/varient,
            // or via additional category mappings in product_cat.
            this.whereRaw("product_varient.varient_id::text = ANY(string_to_array(store_banner.varient_id, ','))")
              .orWhereRaw("product.cat_id::text = ANY(string_to_array(store_banner.cat_id, ','))")
              .orWhereRaw("product.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(store_banner.parent_cat_id, ',')))")
              .orWhereExists(function () {
                this.select('*')
                  .from('product_cat')
                  .whereRaw('product_cat.product_id = product.product_id')
                  .andWhereRaw("product_cat.cat_id::text = ANY(string_to_array(store_banner.cat_id, ','))");
              })
              .orWhereExists(function () {
                this.select('*')
                  .from('product_cat')
                  .whereRaw('product_cat.product_id = product.product_id')
                  .andWhereRaw("product_cat.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(store_banner.parent_cat_id, ',')))");
              });
          });
      });
    })
    .orderBy('store_banner.sequence', 'asc');

  console.log(`[getBanner] Banners returned after DB filter: ${banners.length}`);

  const returnedIds = new Set(banners.map(b => b.banner_id));
  const skippedBanners = allBanners.filter(b => !returnedIds.has(b.banner_id));
  if (skippedBanners.length) {
    console.log(
      '[getBanner] Banners SKIPPED by validity filter (not returned at all):',
      skippedBanners.map(b => ({ banner_id: b.banner_id, banner_name: b.banner_name }))
    );
  }

  // For each banner, log how many active products are linked and a few product names.
  // Also log (capped) invalid mapped products with reasons for invalidity.
  for (const banner of banners) {
    try {
      // If no mapping fields at all, log that as the reason.
      if (!banner.varient_id && !banner.cat_id && !banner.parent_cat_id) {
        console.log(
          `[getBanner] banner_id=${banner.banner_id} name="${banner.banner_name}" -> 0 linked products (reason: no varient_id/cat_id/parent_cat_id configured on banner)`
        );
        continue;
      }

      const productsQuery = knex('product')
        .join('product_varient', 'product.product_id', 'product_varient.product_id')
        .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
        .where('store_products.store_id', store_id)
        .where('product.hide', 0)
        .where('product.is_delete', 0)
        .where('product.is_zap', true)
        .where('store_products.stock', '>', 0)
        .where('store_products.is_deleted', 0)
        .whereRaw('(product.is_offer_product = 0 OR product.offer_date IS NULL OR product.offer_date::date != CURRENT_DATE)')
        .where(function () {
          // Use the outer "banner" via closure
          if (banner.varient_id) {
            this.orWhereRaw(
              "product_varient.varient_id::text = ANY(string_to_array(?, ','))",
              [banner.varient_id]
            );
          }
          if (banner.cat_id) {
            this.orWhereRaw(
              "product.cat_id::text = ANY(string_to_array(?, ','))",
              [banner.cat_id]
            );
          }
          if (banner.parent_cat_id) {
            this.orWhereRaw(
              "product.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(?, ',')))",
              [banner.parent_cat_id]
            );
          }
          // Additional-category mappings through product_cat
          if (banner.cat_id) {
            this.orWhereExists(function () {
              this.select('*')
                .from('product_cat')
                .whereRaw('product_cat.product_id = product.product_id')
                .andWhereRaw(
                  "product_cat.cat_id::text = ANY(string_to_array(?, ','))",
                  [banner.cat_id]
                );
            });
          }
          if (banner.parent_cat_id) {
            this.orWhereExists(function () {
              this.select('*')
                .from('product_cat')
                .whereRaw('product_cat.product_id = product.product_id')
                .andWhereRaw(
                  "product_cat.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(?, ',')))",
                  [banner.parent_cat_id]
                );
            });
          }
        });

      const products = await productsQuery
        .select(
          'product.product_id',
          'product.product_name'
        )
        .limit(50); // cap to avoid huge log lines

      const totalProducts = products.length;
      const validProductIds = new Set(products.map(p => p.product_id));
      const productNames = products.map(p => p.product_name).filter(Boolean);
      const sampleNames = productNames.slice(0, 10);

      let reason = '';
      if (totalProducts === 0) {
        reason = 'reason: mappings exist but no active (in‑stock, non‑deleted, non‑offer‑today) products match';
      }

      console.log(
        `[getBanner] banner_id=${banner.banner_id} name="${banner.banner_name}" -> linked products: ${totalProducts}` +
        (sampleNames.length ? ` | sample products: ${sampleNames.join(', ')}` : '') +
        (reason ? ` | ${reason}` : '')
      );

      // Now log invalid mapped products (up to 20) with reasons.
      const candidateQuery = knex('product')
        .join('product_varient', 'product.product_id', 'product_varient.product_id')
        .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
        .where('store_products.store_id', store_id)
        .where(function () {
          if (banner.varient_id) {
            this.orWhereRaw(
              "product_varient.varient_id::text = ANY(string_to_array(?, ','))",
              [banner.varient_id]
            );
          }
          if (banner.cat_id) {
            this.orWhereRaw(
              "product.cat_id::text = ANY(string_to_array(?, ','))",
              [banner.cat_id]
            );
          }
          if (banner.parent_cat_id) {
            this.orWhereRaw(
              "product.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(?, ',')))",
              [banner.parent_cat_id]
            );
          }
          if (banner.cat_id) {
            this.orWhereExists(function () {
              this.select('*')
                .from('product_cat')
                .whereRaw('product_cat.product_id = product.product_id')
                .andWhereRaw(
                  "product_cat.cat_id::text = ANY(string_to_array(?, ','))",
                  [banner.cat_id]
                );
            });
          }
          if (banner.parent_cat_id) {
            this.orWhereExists(function () {
              this.select('*')
                .from('product_cat')
                .whereRaw('product_cat.product_id = product.product_id')
                .andWhereRaw(
                  "product_cat.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(?, ',')))",
                  [banner.parent_cat_id]
                );
            });
          }
        });

      const candidateProducts = await candidateQuery
        .select(
          'product.product_id',
          'product.product_name',
          'product.hide',
          'product.is_delete',
          'product.is_offer_product',
          'product.offer_date',
          'store_products.stock',
          'store_products.is_deleted'
        )
        .limit(100); // safety cap

      const invalidLogs = [];
      const today = new Date().toISOString().slice(0, 10);

      for (const p of candidateProducts) {
        if (validProductIds.has(p.product_id)) continue; // already counted as valid

        const reasons = [];
        if (p.hide) reasons.push('product.hide = 1');
        if (p.is_delete) reasons.push('product.is_delete = 1');
        if (p.stock <= 0) reasons.push('stock <= 0');
        if (p.is_deleted) reasons.push('store_products.is_deleted = 1');
        if (p.is_offer_product && p.offer_date) {
          const offerDate = new Date(p.offer_date).toISOString().slice(0, 10);
          if (offerDate === today) {
            reasons.push('today offer product excluded (is_offer_product = 1 and offer_date = today)');
          }
        }
        if (reasons.length === 0) {
          reasons.push('fails other banner product filters');
        }

        invalidLogs.push(
          `product_id=${p.product_id} name="${p.product_name}" reasons=[${reasons.join(', ')}]`
        );

        if (invalidLogs.length >= 20) break; // cap per banner
      }

      if (invalidLogs.length) {
        console.log(
          `[getBanner] banner_id=${banner.banner_id} invalid mapped products (showing up to 20): ` +
          invalidLogs.join(' | ')
        );
      }
    } catch (e) {
      console.error(
        `[getBanner] Error while logging products for banner_id=${banner.banner_id}:`,
        e.message
      );
    }
  }

  // EXTRA DIAGNOSTICS:
  // For banners that did NOT pass the main filter (never returned),
  // log whether they have any mapped products at all and why they are invalid.
  for (const banner of skippedBanners) {
    try {
      if (!banner.varient_id && !banner.cat_id && !banner.parent_cat_id) {
        console.log(
          `[getBanner][SKIPPED] banner_id=${banner.banner_id} name="${banner.banner_name}" -> 0 linked products (reason: no varient_id/cat_id/parent_cat_id configured on banner)`
        );
        continue;
      }

      const candidateQuery = knex('product')
        .join('product_varient', 'product.product_id', 'product_varient.product_id')
        .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
        .where('store_products.store_id', store_id)
        .where(function () {
          if (banner.varient_id) {
            this.orWhereRaw(
              "product_varient.varient_id::text = ANY(string_to_array(?, ','))",
              [banner.varient_id]
            );
          }
          if (banner.cat_id) {
            this.orWhereRaw(
              "product.cat_id::text = ANY(string_to_array(?, ','))",
              [banner.cat_id]
            );
          }
          if (banner.parent_cat_id) {
            this.orWhereRaw(
              "product.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(?, ',')))",
              [banner.parent_cat_id]
            );
          }
          if (banner.cat_id) {
            this.orWhereExists(function () {
              this.select('*')
                .from('product_cat')
                .whereRaw('product_cat.product_id = product.product_id')
                .andWhereRaw(
                  "product_cat.cat_id::text = ANY(string_to_array(?, ','))",
                  [banner.cat_id]
                );
            });
          }
          if (banner.parent_cat_id) {
            this.orWhereExists(function () {
              this.select('*')
                .from('product_cat')
                .whereRaw('product_cat.product_id = product.product_id')
                .andWhereRaw(
                  "product_cat.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(?, ',')))",
                  [banner.parent_cat_id]
                );
            });
          }
        });

      const candidateProducts = await candidateQuery
        .select(
          'product.product_id',
          'product.product_name',
          'product.hide',
          'product.is_delete',
          'product.is_offer_product',
          'product.offer_date',
          'store_products.stock',
          'store_products.is_deleted'
        )
        .limit(100);

      if (!candidateProducts.length) {
        console.log(
          `[getBanner][SKIPPED] banner_id=${banner.banner_id} name="${banner.banner_name}" -> 0 mapped products at all (reason: no product/variant/category matches banner mappings)`
        );
        continue;
      }

      const today = new Date().toISOString().slice(0, 10);
      const invalidLogs = [];

      for (const p of candidateProducts) {
        const reasons = [];
        if (p.hide) reasons.push('product.hide = 1');
        if (p.is_delete) reasons.push('product.is_delete = 1');
        if (p.stock <= 0) reasons.push('stock <= 0');
        if (p.is_deleted) reasons.push('store_products.is_deleted = 1');
        if (p.is_offer_product && p.offer_date) {
          const offerDate = new Date(p.offer_date).toISOString().slice(0, 10);
          if (offerDate === today) {
            reasons.push('today offer product excluded (is_offer_product = 1 and offer_date = today)');
          }
        }
        if (reasons.length === 0) {
          reasons.push('fails other banner product filters');
        }

        invalidLogs.push(
          `product_id=${p.product_id} name="${p.product_name}" reasons=[${reasons.join(', ')}]`
        );

        if (invalidLogs.length >= 20) break;
      }

      console.log(
        `[getBanner][SKIPPED] banner_id=${banner.banner_id} name="${banner.banner_name}" -> mapped products (all invalid under main filters): count=${candidateProducts.length}, showing up to 20:` +
        (invalidLogs.length ? ` ${invalidLogs.join(' | ')}` : ' <no details>')
      );
    } catch (e) {
      console.error(
        `[getBanner][SKIPPED] Error while analysing skipped banner_id=${banner.banner_id}:`,
        e.message
      );
    }
  }

  // Map only safe fields back to API response
  const responseBanners = banners.map(banner => ({
    banner_id: banner.banner_id,
    banner_name: banner.banner_name,
    banner_image: `${banner.banner_image}?width=700&height=700&quality=100`
  }));

  console.log('[getBanner] Final banners payload summary:', responseBanners.map(b => ({
    banner_id: b.banner_id,
    banner_name: b.banner_name
  })));
  console.log('=== [getBanner] END ===');

  return responseBanners;

};

const getBrand = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const brand = await knex('brands')
    .select('cat_id', 'title', knex.raw(`? || image as image`, [baseurl]))
    .where('status', 1)
    .orderBy('cat_id', 'asc')
    .limit(8);
  return brand;

};

const getBrandlist = async () => {
  const baseurl = process.env.BUNNY_NET_IMAGE || '';

  // Optimized PostgreSQL query using EXISTS subquery for better performance
  // This avoids unnecessary JOIN processing and GROUP BY overhead
  return await knex('brands')
    .select(
      'brands.cat_id',
      'brands.title',
      knex.raw("(? || COALESCE(brands.image, '')) as image", [baseurl])
    )
    .where('brands.status', 1)
    .whereExists(function () {
      this.select('*')
        .from('product')
        .whereRaw('product.brand_id = brands.cat_id');
    })
    .orderBy('brands.cat_id', 'asc');
};

const getaboutData = async () => {
  const about = await knex('aboutuspage')
    .first();
  return about;

};

const gettermsData = async () => {
  const terms = await knex('termspage')
    .first();
  return terms;

};

const getTopCat = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  //   return await knex('categories')
  //   .select('title','cat_id','description',knex.raw(`CONCAT('${baseurl}', image) as image`) )
  //   .where('level',0)
  //   .orderBy('sequence_list','ASC')
  //   .limit(18);
  //  logToFile(`in one api getTopCat fun`);

  const mainCats = await knex('categories')
    .join('categories as cat', 'categories.cat_id', 'cat.parent')
    .join('product', function () {
      this.on('cat.cat_id', '=', 'product.cat_id')
        .orOnExists(function () {
          this.select('*')
            .from('product_cat')
            .whereRaw('product_cat.product_id = product.product_id')
            .andWhereRaw('product_cat.cat_id = cat.cat_id');
        });
    })
    .join('product_varient', 'product.product_id', 'product_varient.product_id')
    .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .select(
      'categories.title',
      'categories.cat_id',
      knex.raw(`? || categories.image as image`, [baseurl]),
      'categories.description'
    )
    .groupBy('categories.cat_id', 'categories.title', 'categories.image', 'categories.description')
    .where('categories.level', 0)
    .where('categories.is_delete', 0)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('product.approved', 1)
    .where('product.is_zap', true)
    .where('store_products.stock', '>', 0)
    .whereNotNull('store_products.price')
    .where('store_products.store_id', store_id)
    .orderBy('categories.sequence_list', 'ASC')
    .limit(18);

  // Batch fetch ALL subcategories in ONE query instead of 18 separate queries
  const catIds = mainCats.map(cat => cat.cat_id);
  const allSubcats = await knex('categories')
    .select(
      'categories.title',
      'categories.cat_id',
      'categories.parent'
    )
    .join('product', function () {
      this.on('categories.cat_id', '=', 'product.cat_id')
        .orOnExists(function () {
          this.select('*')
            .from('product_cat')
            .whereRaw('product_cat.product_id = product.product_id')
            .andWhereRaw('product_cat.cat_id = categories.cat_id');
        });
    })
    .join('product_varient', 'product.product_id', 'product_varient.product_id')
    .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .whereIn('categories.parent', catIds)
    .where('store_products.store_id', store_id)
    .where('categories.status', 1)
    .where('categories.is_delete', 0)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('product.approved', 1)
    .where('product.is_zap', true)
    .where('store_products.stock', '>', 0)
    .whereNotNull('store_products.price')
    .where(builder => {
      builder
        .where('product.is_offer_product', 0)
        .whereNull('product.offer_date')
        .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE')
    })
    .groupBy('categories.title', 'categories.cat_id', 'categories.parent', 'categories.order')
    .orderBy('categories.order', 'asc');

  // Group subcategories by parent
  const subcatMap = {};
  allSubcats.forEach(subcat => {
    if (!subcatMap[subcat.parent]) {
      subcatMap[subcat.parent] = subcat;
    }
  });

  // Map subcategories to main categories
  const enrichedCats = mainCats.map(cat => ({
    ...cat,
    subcategory_title: subcatMap[cat.cat_id]?.title || null,
    subcategory_id: subcatMap[cat.cat_id]?.cat_id || null
  }));

  return enrichedCats;

  //   return await knex('categories')
  //       .join('categories as cat', 'categories.cat_id', 'cat.parent')
  //       .join('product', 'cat.cat_id', 'product.cat_id')
  //       .join('product_varient', 'product.product_id', 'product_varient.product_id')
  //       .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
  //       .select('categories.title', 'categories.cat_id', knex.raw(`CONCAT('${baseurl}', categories.image) as image`), 'categories.description')
  //       .groupBy('categories.cat_id')
  //       .where('categories.level', 0)
  //       .where('categories.is_delete', 0)
  //       .orderBy('categories.sequence_list', 'ASC')
  //       .limit(18);

};

const getTopCat1 = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  //   return await knex('categories')
  //   .select('title','cat_id','description',knex.raw(`CONCAT('${baseurl}', image) as image`) )
  //   .where('level',0)
  //   .orderBy('sequence_list','ASC')
  //   .limit(18);

  return await knex('categories')
    .join('categories as cat', 'categories.cat_id', 'cat.parent')
    .join('product', 'cat.cat_id', 'product.cat_id')
    .join('product_varient', 'product.product_id', 'product_varient.product_id')
    .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .select('categories.title', 'categories.cat_id', knex.raw(`CONCAT('${baseurl}', categories.image) as image`), 'categories.description')
    .groupBy('categories.cat_id')
    .where('categories.level', 0)
    .where('categories.is_delete', 0)
    .orderBy('categories.sequence_list', 'ASC')
    .limit(18);

};
const getWhatsNew = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;
  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }
  const storeId = store_id;
  const categoryId = 37;


  const productDetails = await knex('store_products')
    .select(
      'store_products.store_id',
      'product.cat_id',
      'store_products.stock',
      'product_varient.varient_id',
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'product_varient.description',
      'store_products.price',
      'store_products.mrp',
      'product_varient.varient_image',
      'product_varient.unit',
      'product_varient.quantity',
      'product.type',
      'product.country_id',
      'tbl_country.country_icon',
      'product.percentage',
      'product.availability',
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper')
    )
    .innerJoin('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .innerJoin('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .whereNotNull('store_products.price')
    .where('store_products.store_id', storeId)
    .where('product.is_subscription', 1)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where(builder => {
      builder
        .where('product.is_offer_product', 0)
        .whereNull('product.offer_date')
        .orWhereRaw('DATE(product.offer_date) != CURDATE()')
    })
    //.where('product.cat_id', categoryId)
    .orderBy('product.product_name', 'desc')
    .limit(8);
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const customizedProductData = [];
  for (let i = 0; i < productDetails.length; i++) {
    const ProductList = productDetails[i];

    var cartQty = 0;
    if (user_id) {
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

      try {
        const cnotify_me = await knex('product_notify_me')
          .where('varient_id', ProductList.varient_id)
          .where('user_id', user_id);

        notifyMe = cnotify_me.length > 0 ? 'true' : 'false';

        if (cnotify_me.length === 0) {
          notifyMe = cnotify_me.length > 0 ? 'true' : 'false';
        } else {
          notifyMe = 'false';
        }
      } catch (error) {
        notifyMe = 'false';
      }

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

    if (ProductList.country_icon == null) {
      countryicon = null
    } else {
      countryicon = baseurl + ProductList.country_icon
    }

    const customizedProduct = {
      store_id: ProductList.store_id,
      // is_subscription:ProductList.is_subscription,
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
      thumbnail: baseurl + ProductList.thumbnail,
      price: ProductList.price,
      mrp: ProductList.mrp,
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      // discountper: ProductList.discountper, 
      discountper: 0,
      country_icon: countryicon,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      cart_qty: cartQty,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite
      // Add or modify properties as needed
    };
    customizedProductData.push(customizedProduct);
  }

  return customizedProductData;
};

const getdealProduct = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;
  const storeId = store_id;
  const currentDate = new Date().toISOString().split('T')[0];

  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }
  const deal_pssss = await knex('deal_product')
    .join('store_products', 'deal_product.varient_id', '=', 'store_products.varient_id')
    .join('product_varient', 'deal_product.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .select(
      'store_products.stock',
      'deal_product.deal_price as price',
      'product_varient.quantity',
      'product_varient.unit',
      'store_products.mrp',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'product_varient.varient_id',
      'product.product_id',
      'deal_product.valid_to',
      'deal_product.valid_from',
      'product.type',
      'tbl_country.country_icon',
      'product.percentage',
      'product.availability',
    )
    .groupBy(
      'store_products.store_id',
      'store_products.stock',
      'deal_product.deal_price',
      'product_varient.varient_image',
      'product_varient.quantity',
      'product_varient.unit',
      'store_products.mrp',
      'product_varient.description',
      'product.product_name',
      'product.product_image',
      'product_varient.varient_id',
      'product.product_id',
      'deal_product.valid_to',
      'deal_product.valid_from',
      'product.type'
    )
    .where('deal_product.valid_from', '<=', currentDate)
    .where('deal_product.valid_to', '>', currentDate)
    .whereNotNull('store_products.price')
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('deal_product.store_id', storeId)
    // .where('product.is_subscription', is_subscription)
    .orderBy('product.product_name', 'asc')
    .limit(8);
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const productDetailss = deal_pssss.filter((product, index, self) => {
    return index === self.findIndex((p) => p.product_id === product.product_id);
  });
  const customizedProductData = [];
  for (let i = 0; i < productDetailss.length; i++) {
    const ProductList = productDetailss[i];

    var cartQty = 0;
    if (user_id && ProductList) {
      const CartQtyList = await knex('store_orders')
        .where('varient_id', ProductList.varient_id)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .where('store_id', store_id)
        .first();
      cartQty = CartQtyList ? CartQtyList.qty : 0;

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
      isSubscription = 'false'
    }

    const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    if (ProductList.country_icon == null) {
      countryicon = null
    } else {
      countryicon = baseurl + ProductList.country_icon
    }

    const customizedProduct = {
      stock: ProductList.stock,
      price: ProductList.price,
      quantity: ProductList.quantity,
      unit: ProductList.unit,
      mrp: ProductList.mrp,
      product_name: ProductList.product_name,
      product_image: baseurl + ProductList.product_image + "?width=200&height=200&quality=100",
      thumbnail: ProductList.thumbnail,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      valid_to: ProductList.valid_to,
      valid_from: ProductList.valid_from,
      type: ProductList.type,
      cart_qty: cartQty,
      country_icon: countryicon,
      percentage: ProductList.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: ProductList.availability,
      // Add or modify properties as needed
    };
    customizedProductData.push(customizedProduct);
  }



  return customizedProductData;
};

const getsecondBanner = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  return await knex('sec_banner')
    .where('sec_banner.store_id', store_id)
    .where('sec_banner.is_delete', 0)
    .where('sec_banner.status', 1)
    .where(function () {
      this.whereExists(function () {
        this.select('*').from('product')
          .join('product_varient', 'product.product_id', 'product_varient.product_id')
          .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
          .where('store_products.store_id', store_id)
          .where('product.hide', 0)
          .where('product.is_delete', 0)
          .where('product.is_zap', true)
          .where('store_products.stock', '>', 0)
          .where('store_products.is_deleted', 0)
          // Exclude today's offer products from second-banner validity check
          .whereRaw('(product.is_offer_product = 0 OR product.offer_date IS NULL OR product.offer_date::date != CURRENT_DATE)')
          .where(function () {
            // Link products to second banner either directly via cat/parent_cat/varient/brand,
            // or via additional category mappings in product_cat.
            this.whereRaw("product_varient.varient_id::text = ANY(string_to_array(sec_banner.varient_id, ','))")
              .orWhereRaw("product.cat_id::text = ANY(string_to_array(sec_banner.cat_id, ','))")
              .orWhereRaw("product.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(sec_banner.parent_cat_id, ',')))")
              .orWhereRaw("product.brand_id::text = sec_banner.brand_id::text")
              .orWhereExists(function () {
                this.select('*')
                  .from('product_cat')
                  .whereRaw('product_cat.product_id = product.product_id')
                  .andWhereRaw("product_cat.cat_id::text = ANY(string_to_array(sec_banner.cat_id, ','))");
              })
              .orWhereExists(function () {
                this.select('*')
                  .from('product_cat')
                  .whereRaw('product_cat.product_id = product.product_id')
                  .andWhereRaw("product_cat.cat_id::text IN (SELECT cat_id::text FROM categories WHERE parent::text = ANY(string_to_array(sec_banner.parent_cat_id, ',')))");
              });
          });
      });
    })
    .select('sec_banner.banner_id', 'sec_banner.brand_id', 'sec_banner.banner_name', knex.raw(`? || banner_image as banner_image`, [baseurl]))
    .orderBy('sec_banner.sequence', 'ASC')
    .limit(6);
};

const sneakyOfferBanner = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  return await knex('sneaky_banner')
    .select('banner_id', 'banner_name', knex.raw(`? || banner_image as banner_image`, [baseurl]))
    .where('store_id', store_id)
    .orderBy('banner_id', 'DESC')
    .first();
};

const popupBanner = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const weburl = process.env.WEB_URL;
  const popupbanner = await knex('popup_banner')
    .select('banner_id', 'banner_name', 'type', 'trail_id', 'search_by',
      knex.raw(`? || banner_image as banner_image`, [baseurl]))
    .where('store_id', store_id)
    .orderBy('banner_id', 'DESC')
    .first();

  // return popupbanner.type;

  // If no popupbanner found, return empty array
  if (!popupbanner) {
    return null;
  }

  // If trail_id exists
  if (popupbanner.type == 'trial') {
    const today = new Date().toISOString().split('T')[0];
    const checktraillist = await knex('tbl_trail_pack_basic')
      .where('status', 1)
      .where('start_date', '<=', today)
      .where('end_date', '>=', today)
      .where('is_delete', 0)

    if (!checktraillist) {
      return null;
    }

    const orderlist = await knex('orders')
      .whereRaw('order_type ILIKE ?', ['trail'])
      .where('user_id', user_id)
      .groupBy('trail_id')
      .pluck('trail_id');

    const storeproduct = await knex('tbl_trail_pack_deatils')
      .join('store_products', knex.raw('tbl_trail_pack_deatils.varient_id = store_products.varient_id::text'))
      .where('store_products.stock', 0)
      .pluck('tbl_trail_pack_deatils.varient_id');

    const trialidlist = await knex('tbl_trail_pack_deatils')
      .whereIn('varient_id', storeproduct)
      .pluck('tbl_trail_pack_deatils.trail_id');

    const uniqueValues = [...new Set(trialidlist)];



    if (orderlist) {
      const checktrail = await knex('tbl_trail_pack_basic')
        .where('status', 1)
        .where('start_date', '<=', today)
        .where('end_date', '>=', today)
        .where('is_delete', 0)
        .whereNotIn('id', uniqueValues)
        .whereNotIn('id', orderlist)
        .orderBy('main_order', 'ASC')
        .select('id', 'popup_image', 'image', 'title')
        .first();

      if (checktrail) {
        trailimage = checktrail.popup_image;
        trailpackimage = baseurl + trailimage;
        checktrailid = checktrail.id;
        checktrailtitle = checktrail.title;


      } else {
        // trailpackimage =  null;
        // checktrailid =null;
        // checktrailtitle = null;

        return null;
      }

    } else {
      //   trailpackimage =  null;
      //   checktrailid =null;
      //   checktrailtitle = null;

      return null;
    }

    // const popuplist = await knex('popup_banner')
    //     .select('banner_id', 'banner_name', 'type', 'trail_id', 'search_by',
    //         knex.raw(`CONCAT('${baseurl}', banner_image) as banner_image`))
    //     .where('store_id', store_id)
    //     .whereIn('trail_id', trailIdarray)
    //     .orderBy('banner_id', 'DESC')
    //     .first();


    const popuplist = {
      banner_id: checktrailid,
      banner_name: checktrailtitle,
      type: "trial",
      trail_id: checktrailid,
      search_by: null,
      banner_image: trailpackimage
    };





    // popuplist.push(popuplist);


    return popuplist ? popuplist : null;
  } else if (popupbanner.type == 'offers') {
    const currentDate = new Date().toISOString().split('T')[0];
    const popuplist = await knex('popup_banner')
      .select('banner_id', 'banner_name', 'type', 'trail_id', 'search_by', knex.raw(`? || banner_image as banner_image`, [baseurl]))
      .where('store_id', store_id)
      .where('type', 'offers')
      .whereRaw('DATE(start_date) <= ?', [currentDate])
      .whereRaw('DATE(end_date) >= ?', [currentDate])
      .where(function () {
        this.whereExists(function () {
          this.select('*').from('product')
            .join('product_varient', 'product.product_id', 'product_varient.product_id')
            .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
            .where('store_products.store_id', store_id)
            .where('product.hide', 0)
            .where('product.is_delete', 0)
            .where('product.is_zap', true)
            .where('store_products.stock', '>', 0)
            .where('store_products.is_deleted', 0)
            .where(function () {
              this.whereRaw("product.product_id::text = ANY(string_to_array(popup_banner.product_id, ','))")
                .orWhereRaw("product.cat_id::text = ANY(string_to_array(popup_banner.cat_id, ','))")
                .orWhereRaw("product.cat_id::text = ANY(string_to_array(popup_banner.subcat_id, ','))");
            });
        });
      })
      .first();

    if (popuplist) {
      popuplist.offer_url = process.env.POPUP_OFFER_URL + 'offers';
    }

    return popuplist ? popuplist : null;
  } else {
    // Recheck in case trail_id is not present
    const popuplist = await knex('popup_banner')
      .select('banner_id', 'banner_name', 'type', 'trail_id', 'search_by',
        knex.raw(`? || banner_image as banner_image`, [baseurl]))
      .where('store_id', store_id)
      .orderBy('banner_id', 'DESC')
      .first();

    return popuplist ? popuplist : null;
  }
};

const specialOfferBanner = async (appDetatils) => {
  const { store_id, user_id, is_subscription } = appDetatils;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  return await knex('special_offers_banner')
    .select('banner_id', 'banner_name', knex.raw(`? || banner_image as banner_image`, [baseurl]))
    .where('store_id', store_id)
    .orderBy('banner_id', 'DESC')
    .first();
};

const similarProds = async (appDetatils, catId) => {
  const cat_id = catId;
  const store_id = appDetatils.store_id;
  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const currentDate = new Date();

  // Optimized: Single query with LEFT JOIN for deals
  const prod = await knex('store_products')
    .select(
      'store_products.store_id',
      'product.cat_id',
      'store_products.stock',
      'product_varient.varient_id',
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'product_varient.description',
      'store_products.price',
      'store_products.mrp',
      'product_varient.varient_image',
      'product_varient.unit',
      'product_varient.quantity',
      'product.type',
      'product.country_id',
      'tbl_country.country_icon',
      'product.percentage',
      'product.availability',
      'product.fcat_id',
      knex.raw('ROUND((100-((store_products.price*100)/store_products.mrp))::numeric, 2) as discountper'),
      knex.raw('COALESCE(deal_product.deal_price, store_products.price) as final_price')
    )
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .leftJoin('deal_product', function () {
      this.on('deal_product.varient_id', '=', 'product_varient.varient_id')
        .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
        .andOn('deal_product.valid_from', '<=', knex.raw('?', [currentDate]))
        .andOn('deal_product.valid_to', '>', knex.raw('?', [currentDate]));
    })
    .where('product.cat_id', parseInt(cat_id))
    .where('store_products.store_id', parseInt(store_id))
    .whereNotNull('store_products.price')
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('store_products.stock', '>', 0)
    .where('product.approved', 1)
    .where(builder => {
      builder
        .where('product.is_offer_product', 0)
        .whereNull('product.offer_date')
        .orWhereRaw("product.offer_date::date != CURRENT_DATE");
    })
    .orderByRaw('RANDOM()')
    .limit(5);

  if (prod.length === 0) {
    return [];
  }

  // Optimized: Batch fetch all related data in parallel
  const varientIds = prod.map(p => p.varient_id);
  const productIds = prod.map(p => p.product_id);
  const uniqueProductIds = [...new Set(productIds)];

  // Parallel batch queries (include product fallback images in same round)
  const [
    wishlistData,
    cartData,
    notifyData,
    subscriptionData,
    tagsData,
    imagesData,
    fallbackProductImages,
    featureTagsData
  ] = await Promise.all([
    // Wishlist check
    user_id ? knex('wishlist')
      .select('varient_id')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .then(rows => new Set(rows.map(r => r.varient_id))) : Promise.resolve(new Set()),

    // Cart quantities
    user_id ? knex('store_orders')
      .select('varient_id', 'qty')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', store_id)
      .then(rows => {
        const map = new Map();
        rows.forEach(r => map.set(r.varient_id, r.qty));
        return map;
      }) : Promise.resolve(new Map()),

    // Notify me
    user_id ? knex('product_notify_me')
      .select('varient_id')
      .whereIn('varient_id', varientIds)
      .where('user_id', user_id)
      .then(rows => new Set(rows.map(r => r.varient_id))) : Promise.resolve(new Set()),

    // Subscription check
    user_id ? knex('store_orders')
      .select('varient_id')
      .whereIn('varient_id', varientIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1)
      .where('order_cart_id', 'incart')
      .then(rows => new Set(rows.map(r => r.varient_id))) : Promise.resolve(new Set()),

    // Tags - batch fetch
    knex('tags')
      .select('product_id', 'tag')
      .whereIn('product_id', uniqueProductIds)
      .then(rows => {
        const map = new Map();
        uniqueProductIds.forEach(pid => map.set(pid, []));
        rows.forEach(r => {
          if (!map.has(r.product_id)) map.set(r.product_id, []);
          map.get(r.product_id).push(r);
        });
        return map;
      }),

    // Product images - batch fetch
    knex('product_images')
      .select('product_id', knex.raw(`('${baseurl}' || image) as image`), 'type')
      .whereIn('product_id', uniqueProductIds)
      .orderBy('type', 'DESC')
      .then(rows => {
        const map = new Map();
        uniqueProductIds.forEach(pid => map.set(pid, []));
        rows.forEach(r => {
          if (!map.has(r.product_id)) map.set(r.product_id, []);
          map.get(r.product_id).push(r);
        });
        return map;
      }),
    // Fallback product images (same round - no extra latency)
    knex('product')
      .select('product_id', knex.raw(`('${baseurl}' || product_image) as image`))
      .whereIn('product_id', uniqueProductIds),

    // Feature categories - batch fetch
    knex('feature_categories')
      .select('id', knex.raw(`('${baseurl}' || image) as image`))
      .where('status', 1)
      .where('is_deleted', 0)
      .then(rows => {
        const map = new Map();
        rows.forEach(r => map.set(r.id, r));
        return map;
      })
  ]);

  // Merge fallback images (already fetched in same round)
  fallbackProductImages.forEach(img => {
    const imgs = imagesData.get(img.product_id);
    if (!imgs || imgs.length === 0) {
      imagesData.set(img.product_id, [img]);
    }
  });

  // Build response
  const customizedProductData = prod.map(ProductList => {
    const price = ProductList.final_price || ProductList.price;
    const isFavourite = user_id && wishlistData.has(ProductList.varient_id) ? 'true' : 'false';
    const cartQty = user_id ? (cartData.get(ProductList.varient_id) || 0) : 0;
    const notifyMe = user_id && notifyData.has(ProductList.varient_id) ? 'true' : 'false';
    const isSubscription = user_id && subscriptionData.has(ProductList.varient_id) ? 'true' : 'false';

    const tags = tagsData.get(ProductList.product_id) || [];
    const imageslist = imagesData.get(ProductList.product_id) || [];

    let feature_tags = [];
    if (ProductList.fcat_id != null) {
      const resultArray = ProductList.fcat_id.split(',').map(Number);
      feature_tags = resultArray
        .filter(id => featureTagsData.has(id))
        .map(id => featureTagsData.get(id));
    }

    const sub_price = (ProductList.mrp * ProductList.percentage) / 100;
    const finalsubprice = ProductList.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));
    const countryicon = ProductList.country_icon ? baseurl + ProductList.country_icon : null;

    return {
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: ProductList.product_name,
      product_image: `${baseurl}${ProductList.product_image}?width=200&height=200&quality=100`,
      country_icon: countryicon,
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
      discountper: parseFloat(ProductList.discountper || 0),
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      countrating: 0,
      tags: tags,
      feature_tags: feature_tags,
      images: imageslist,
      varients: null
    };
  });

  return customizedProductData;
};


const prodDetails = async (appDetatils) => {
  const user_id = appDetatils.user_id != "null" ? appDetatils.user_id : appDetatils.device_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const currentDate = new Date();
  const currentDate1 = new Date().toISOString().split('T')[0];
  const storeId = parseInt(appDetatils.store_id);
  const productId = parseInt(appDetatils.product_id);
  if (isNaN(storeId) || isNaN(productId)) {
    throw new Error("Product not Found");
  }

  // Single query: product + deal + offer
  let prod = await knex('store_products')
    .select(
      'store_products.store_id',
      'store_products.stock',
      'product_varient.varient_id',
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      'store_products.price',
      'store_products.mrp',
      'product_varient.unit',
      'product_varient.quantity',
      'product.type',
      'product.available_days',
      'tbl_country.country_icon',
      'tbl_country.country_name as country_of_origin',
      'product.shelf_life',
      'product.percentage',
      'product.availability',
      'product.fcat_id',
      'product.is_customized',
      'product.hide',
      'product.is_delete',
      knex.raw('ROUND((100-((store_products.price*100)/store_products.mrp))::numeric, 2) as discountper'),
      knex.raw('COALESCE(deal_product.deal_price, offer_product.offer_price, store_products.price) as final_price'),
      knex.raw('CASE WHEN offer_product.product_id IS NOT NULL THEN true ELSE false END as is_offer_product')
    )
    .innerJoin('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
    .innerJoin('product', 'product_varient.product_id', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .leftJoin('deal_product', function () {
      this.on('deal_product.varient_id', '=', 'product_varient.varient_id')
        .andOn('deal_product.store_id', '=', knex.raw('?', [storeId]))
        .andOn('deal_product.valid_from', '<=', knex.raw('?', [currentDate]))
        .andOn('deal_product.valid_to', '>', knex.raw('?', [currentDate]));
    })
    .leftJoin('product as offer_product', function () {
      this.on('offer_product.product_id', '=', 'product.product_id')
        .andOn('offer_product.offer_date', '=', knex.raw('?::date', [currentDate1]));
    })
    .where('store_products.store_id', storeId)
    .where('product.product_id', productId)
    .where('product.approved', 1)
    .whereNotNull('store_products.price')
    .first();

  if (!prod) {
    throw new Error("Product not Found");
  }

  if (prod.hide === 1 || prod.is_delete === 1) {
    throw new Error("Product not Found");
  }

  // Set price and offer flag
  prod.price = prod.final_price;
  prod.is_offer_product = prod.is_offer_product || false;

  // Format images
  prod.product_image = baseurl + prod.product_image + "?width=200&height=200&quality=100";
  const countryicon = prod.country_icon ? baseurl + prod.country_icon : null;
  prod.country_icon = countryicon;

  // Optimized: Fetch all user-related data in parallel
  const [
    wishlistCheck,
    cartQtyData,
    notifyCheck,
    subscriptionCheck,
    ratingData,
    tagsData,
    featureTagsData,
    imagesData,
    variantsData,
    featuresData
  ] = await Promise.all([
    // Wishlist check
    user_id ? knex('wishlist')
      .select('varient_id')
      .where('varient_id', prod.varient_id)
      .where('user_id', user_id)
      .first()
      .then(row => !!row) : Promise.resolve(false),

    // Cart: single query for incart (regular + subscription + product_feature_id)
    user_id ? knex('store_orders')
      .select('qty', 'subscription_flag', 'product_feature_id')
      .where('varient_id', prod.varient_id)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('store_id', storeId)
      .then(rows => {
        let cartQty = 0, subcartQty = 0, productFeatureId = 0;
        rows.forEach(r => {
          if (r.subscription_flag == null || r.subscription_flag === 0) {
            cartQty = r.qty || 0;
            productFeatureId = r.product_feature_id || 0;
          } else {
            subcartQty = r.qty || 0;
          }
        });
        return { cartQty, subcartQty, productFeatureId };
      }) : Promise.resolve({ cartQty: 0, subcartQty: 0, productFeatureId: 0 }),

    // Notify me
    user_id ? knex('product_notify_me')
      .where('varient_id', prod.varient_id)
      .where('user_id', user_id)
      .first()
      .then(row => !!row) : Promise.resolve(false),

    // Subscription check
    user_id ? knex('store_orders')
      .select('percentage')
      .where('varient_id', prod.varient_id)
      .where('store_approval', user_id)
      .where('subscription_flag', 1)
      .where('order_cart_id', 'incart')
      .first()
      .then(row => !!row) : Promise.resolve(false),

    // Rating: single query with AVG/COUNT in DB
    knex('product_rating')
      .where('varient_id', prod.varient_id)
      .where('store_id', storeId)
      .select(
        knex.raw('COALESCE(AVG((rating)::numeric), 0) as avgrating'),
        knex.raw('COUNT(*)::int as countrating')
      )
      .first()
      .then(r => ({
        avgrating: parseFloat(Number(r?.avgrating || 0).toFixed(2)),
        countrating: parseInt(r?.countrating || 0, 10)
      })),

    // Tags
    knex('tags')
      .where('product_id', prod.product_id),

    // Feature categories
    prod.fcat_id ? (() => {
      const resultArray = prod.fcat_id.split(',').map(Number);
      return knex('feature_categories')
        .whereIn('id', resultArray)
        .where('status', 1)
        .where('is_deleted', 0)
        .select('id', knex.raw(`('${baseurl}' || image) as image`));
    })() : Promise.resolve([]),

    // Product images: fetch both in parallel, use images or fallback
    Promise.all([
      knex('product_images')
        .select(knex.raw(`('${baseurl}' || image) as image`))
        .where('product_id', prod.product_id)
        .orderBy('type', 'DESC'),
      knex('product')
        .select(knex.raw(`('${baseurl}' || product_image) as image`))
        .where('product_id', prod.product_id)
    ]).then(([images, fallback]) => (images.length > 0 ? images : fallback)),

    // Variants with deals - optimized single query
    knex('store_products')
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
        knex.raw('ROUND((100-((store_products.price*100)/store_products.mrp))::numeric, 2) as discountper'),
        knex.raw('COALESCE(deal_product.deal_price, store_products.price) as final_price')
      )
      .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
      .leftJoin('deal_product', function () {
        this.on('deal_product.varient_id', '=', 'product_varient.varient_id')
          .andOn('deal_product.store_id', '=', knex.raw('?', [storeId]))
          .andOn('deal_product.valid_from', '<=', knex.raw('?', [currentDate]))
          .andOn('deal_product.valid_to', '>', knex.raw('?', [currentDate]));
      })
      .where('store_products.store_id', storeId)
      .where('product_varient.product_id', productId)
      .whereNotNull('store_products.price')
      .where('product_varient.approved', 1),

    // Features - single query
    knex('product_features')
      .select('tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
      .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
      .where('product_id', productId)
  ]);

  // Set user-related flags
  prod.isFavourite = wishlistCheck ? 'true' : 'false';
  prod.cartQty = cartQtyData.cartQty;
  prod.notifyMe = notifyCheck ? 'true' : 'false';
  prod.isSubscription = subscriptionCheck ? 'true' : 'false';

  // Set rating data
  prod.avgrating = ratingData.avgrating;
  prod.countrating = ratingData.countrating;

  // Set tags and feature tags
  prod.tags = tagsData;
  prod.feature_tags = featureTagsData;

  // Set images
  prod.images = imagesData;

  // Set features
  prod.features = featuresData;

  // Calculate subscription price
  const sub_price = (prod.mrp * prod.percentage) / 100;
  const finalsubprice = prod.mrp - sub_price;
  prod.subscription_price = parseFloat(finalsubprice.toFixed(2));
  prod.isAutoRenew = "no";

  // Optimized: Batch fetch all variant-related user data
  const variantIds = variantsData.map(v => v.varient_id);
  let variantUserData = {};

  if (user_id && variantIds.length > 0) {
    const [
      variantWishlist,
      variantStoreOrders,
      variantNotify
    ] = await Promise.all([
      knex('wishlist')
        .select('varient_id')
        .whereIn('varient_id', variantIds)
        .where('user_id', user_id)
        .then(rows => new Set(rows.map(r => r.varient_id))),
      // Single store_orders query: cart qty, subcart qty, product_feature_id
      knex('store_orders')
        .select('varient_id', 'qty', 'subscription_flag', 'product_feature_id')
        .whereIn('varient_id', variantIds)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .where('store_id', storeId)
        .then(rows => {
          const cart = new Map();
          const subCart = new Map();
          const featureIds = new Map();
          rows.forEach(r => {
            if (r.subscription_flag == null || r.subscription_flag === 0) {
              cart.set(r.varient_id, r.qty);
              featureIds.set(r.varient_id, r.product_feature_id || 0);
            } else {
              subCart.set(r.varient_id, r.qty);
            }
          });
          return { cart, subCart, featureIds };
        }),
      knex('product_notify_me')
        .select('varient_id')
        .whereIn('varient_id', variantIds)
        .where('user_id', user_id)
        .then(rows => new Set(rows.map(r => r.varient_id)))
    ]);

    variantUserData = {
      wishlist: variantWishlist,
      cart: variantStoreOrders.cart,
      subCart: variantStoreOrders.subCart,
      notify: variantNotify,
      featureIds: variantStoreOrders.featureIds
    };
  }

  // Build variants array
  const customizedProductData = [];
  let total_cart_qty = 0;
  let total_subcart_qty = 0;

  variantsData.forEach(ProductList => {
    const price = ProductList.final_price || ProductList.price;
    const isFavourite = user_id && variantUserData.wishlist && variantUserData.wishlist.has(ProductList.varient_id) ? 'true' : 'false';
    const cartQty = user_id && variantUserData.cart ? (variantUserData.cart.get(ProductList.varient_id) || 0) : 0;
    const subcartQty = user_id && variantUserData.subCart ? (variantUserData.subCart.get(ProductList.varient_id) || 0) : 0;
    const notifyMe = user_id && variantUserData.notify && variantUserData.notify.has(ProductList.varient_id) ? 'true' : 'false';
    const productFeatureId = user_id && variantUserData.featureIds ? (variantUserData.featureIds.get(ProductList.varient_id) || 0) : 0;

    total_cart_qty += cartQty;
    total_subcart_qty += subcartQty;

    customizedProductData.push({
      stock: ProductList.stock,
      varient_id: ProductList.varient_id,
      product_id: ProductList.product_id,
      product_name: prod.product_name,
      product_image: (imagesData[0]) ? imagesData[0].image + "?width=200&height=200&quality=100" : "",
      thumbnail: (imagesData[0]) ? imagesData[0].image : "",
      description: ProductList.description,
      price: price,
      mrp: ProductList.mrp,
      unit: ProductList.unit,
      quantity: ProductList.quantity,
      type: ProductList.type,
      discountper: parseFloat(ProductList.discountper),
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      countryicon: countryicon,
      subcartQty: subcartQty,
      product_feature_id: productFeatureId
    });
  });

  prod.total_cart_qty = total_cart_qty;
  prod.total_subcart_qty = total_subcart_qty;
  prod.varients = customizedProductData;
  prod.discountper = parseFloat(prod.discountper);

  return prod;
};

const getcatProduct2 = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
  const {
    cat_id,
    sub_cat_id: subcatid,
    store_id,
    byname: filter1,
    sort: issort,
    sortprice,
    sortname,
    user_id: rawUserId,
    min_price: minPrice,
    max_price: maxPrice,
    min_discount: minDiscount,
    max_discount: maxDiscount,
    page: pageFilter,
    perpage: perPage
  } = appDetatils;

  const user_id = rawUserId !== "null" ? rawUserId : appDetatils.device_id;
  const minprice = parseFloat(minPrice);
  const maxprice = parseFloat(maxPrice);
  const mindiscount = parseFloat(minDiscount);
  const maxdiscount = parseFloat(maxDiscount);

  const page = parseInt(pageFilter) || 1;
  const per_page = parseInt(perPage) || 20;
  const offset = (page - 1) * per_page;

  // Optimize: Build base query with efficient joins
  // Note: Removed currentDate1 as it's not used in the query
  let topsellingsQuery = knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('deal_product', function () {
      this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
        .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
        .andOn('deal_product.valid_from', '<=', knex.raw('CURRENT_TIMESTAMP'))
        .andOn('deal_product.valid_to', '>', knex.raw('CURRENT_TIMESTAMP'));
    })
    .leftJoin('tbl_country', function () {
      this.on(knex.raw('tbl_country.id::text'), '=', knex.raw('product.country_id'));
    })
    .leftJoin('parent_category_product_sequences', function () {
      this.on('product.product_id', '=', 'parent_category_product_sequences.product_id')
        .andOn('parent_category_product_sequences.cat_id', '=', knex.raw('?', [cat_id === 'null' ? null : cat_id]))
        .andOn('parent_category_product_sequences.sub_cat_id', '=', knex.raw('?', [subcatid === 'null' ? null : subcatid]));
    })
    .select(
      knex.raw('MAX(parent_category_product_sequences.sequence) as manual_sequence'),
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
    .where('product.is_zap', true)
    .where('store_products.stock', '>', 0)
    .where('product.approved', 1)
    .whereNotNull('store_products.price')
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




  // Fetch category array and product_cat product IDs for filtering
  let categoryarray = null;
  if (cat_id !== "null") {
    categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
    categoryarray.push(parseInt(cat_id, 10));

    const productIdsFromCatParent = await knex('product_cat')
      .whereIn('cat_id', categoryarray)
      .pluck('product_id');

    topsellingsQuery.andWhere(function () {
      this.whereIn('product.cat_id', categoryarray);
      if (productIdsFromCatParent.length > 0) {
        this.orWhereIn('product.product_id', productIdsFromCatParent);
      }
    });
  }

  if (subcatid !== "null") {
    const productIdsFromProductCat = await knex('product_cat')
      .where('cat_id', subcatid)
      .pluck('product_id');

    topsellingsQuery.andWhere(function () {
      this.where('product.cat_id', subcatid);
      if (cat_id !== "null") {
        this.orWhere('product.cat_id', cat_id);
      }
      if (productIdsFromProductCat.length > 0) {
        this.orWhereIn('product.product_id', productIdsFromProductCat);
      }
    });
  }


  // Priority Sequencing Order
  topsellingsQuery.orderByRaw('CASE WHEN MAX(parent_category_product_sequences.sequence) IS NOT NULL THEN MAX(parent_category_product_sequences.sequence) ELSE 9999 END ASC');

  // Apply discount filters
  if (mindiscount && maxdiscount) {
    topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [mindiscount, maxdiscount, mindiscount, maxdiscount]);
  }

  // Apply price filter (use HAVING for aggregated columns after GROUP BY)
  if (minprice && maxprice) {
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

  // Optimize: Run count and data queries in parallel for maximum speed
  const totalCountQuery = topsellingsQuery.clone().clearSelect().clearOrder().countDistinct('product.product_id as total');
  const dataQuery = topsellingsQuery.offset(offset).limit(per_page);

  const queryStartTime = Date.now();
  const [totalCountResult, productDetails] = await Promise.all([
    totalCountQuery.first(),
    dataQuery
  ]);
  const queryTime = Date.now() - queryStartTime;
  // console.log(`📊 Main query completed in ${queryTime}ms - Products: ${productDetails?.length || 0}`);

  const totalCount = parseInt(totalCountResult?.total || 0);
  const totalPages = Math.ceil(totalCount / per_page);

  // Collect all IDs for batch queries
  const variantIds = productDetails.map(p => p.varient_id);
  const productIds = productDetails.map(p => p.product_id);
  const currentDate = new Date();
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Optimize: Prepare all batch queries
  // Note: Deals are already joined in main query, but we fetch separately for accuracy
  // We can skip this if the join is sufficient, but keeping for consistency
  let dealsPromise = Promise.resolve([]);
  if (variantIds.length > 0) {
    dealsPromise = knex('deal_product')
      .whereIn('varient_id', variantIds)
      .where('store_id', store_id)
      .whereRaw('deal_product.valid_from <= CURRENT_TIMESTAMP')
      .whereRaw('deal_product.valid_to > CURRENT_TIMESTAMP')
      .select('varient_id', 'deal_price');
  }

  let wishlistsPromise = Promise.resolve([]);
  let cartItemsPromise = Promise.resolve([]);
  let subCartItemsPromise = Promise.resolve([]);
  let subscriptionsPromise = Promise.resolve([]);
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

    subscriptionsPromise = knex('store_orders')
      .select('varient_id')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1);

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
  productDetails.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const parsed = parseInt(id, 10);
        if (!isNaN(parsed)) fcatIdsSet.add(parsed);
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

  // Optimize: Execute ALL batch queries in parallel (first batch)
  const batch1StartTime = Date.now();
  const [
    deals,
    wishList,
    cartItems,
    subCartItems,
    subscriptions,
    notifyMeList,
    allFeatures,
    allFeatureCats,
    allVariants,
    allImages
  ] = await Promise.all([
    dealsPromise,
    wishlistsPromise,
    cartItemsPromise,
    subCartItemsPromise,
    subscriptionsPromise,
    notifyMePromise,
    featuresPromise,
    featureCatsPromise,
    variantsPromise,
    imagesPromise
  ]);
  const batch1Time = Date.now() - batch1StartTime;
  // console.log(`📊 Batch 1 queries completed in ${batch1Time}ms`);

  // Build imagesMap first to check which products need fallback
  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  });

  // Collect all variant IDs from fetched variants for second batch
  const allVariantIds = allVariants.map(v => v.varient_id);

  // Prepare second batch queries (variant prices and fallback images)
  const productsWithoutImages = productIds.filter(id => !imagesMap[id] || imagesMap[id].length === 0);

  const secondBatchPromises = [];
  if (allVariantIds.length > 0) {
    secondBatchPromises.push(
      knex('store_products')
        .whereIn('varient_id', allVariantIds)
        .where('store_id', store_id)
        .select('varient_id', 'price')
    );
  } else {
    secondBatchPromises.push(Promise.resolve([]));
  }

  if (productsWithoutImages.length > 0) {
    secondBatchPromises.push(
      knex('product')
        .select('product_id', knex.raw(`('${baseurl}' || COALESCE(product_image, '')) as image`))
        .whereIn('product_id', productsWithoutImages)
    );
  } else {
    secondBatchPromises.push(Promise.resolve([]));
  }

  // Execute second batch in parallel
  const batch2StartTime = Date.now();
  const [variantStoreProducts, fallbackImages] = await Promise.all(secondBatchPromises);
  const batch2Time = Date.now() - batch2StartTime;
  // console.log(`📊 Batch 2 queries completed in ${batch2Time}ms`);

  // Optimize: Build maps using Map for faster lookups (O(1) vs O(n) for objects)
  const mapBuildStartTime = Date.now();

  const dealsMap = new Map();
  for (let i = 0; i < deals.length; i++) {
    dealsMap.set(deals[i].varient_id, deals[i].deal_price);
  }

  const wishlistMap = new Map();
  for (let i = 0; i < wishList.length; i++) {
    wishlistMap.set(wishList[i].varient_id, true);
  }

  const cartQtyMap = new Map();
  const cartFeatureMap = new Map();
  for (let i = 0; i < cartItems.length; i++) {
    const c = cartItems[i];
    cartQtyMap.set(c.varient_id, c.qty);
    if (c.product_feature_id) cartFeatureMap.set(c.varient_id, c.product_feature_id);
  }

  const subcartQtyMap = new Map();
  for (let i = 0; i < subCartItems.length; i++) {
    subcartQtyMap.set(subCartItems[i].varient_id, subCartItems[i].qty);
  }

  const subscriptionMap = new Map();
  for (let i = 0; i < subscriptions.length; i++) {
    subscriptionMap.set(subscriptions[i].varient_id, true);
  }

  const notifyMeMap = new Map();
  for (let i = 0; i < notifyMeList.length; i++) {
    notifyMeMap.set(notifyMeList[i].varient_id, true);
  }

  const featuresMap = new Map();
  for (let i = 0; i < allFeatures.length; i++) {
    const f = allFeatures[i];
    if (!featuresMap.has(f.product_id)) featuresMap.set(f.product_id, []);
    featuresMap.get(f.product_id).push({ id: f.id, feature_value: f.feature_value });
  }

  const featureCategoriesMap = new Map();
  for (let i = 0; i < allFeatureCats.length; i++) {
    featureCategoriesMap.set(allFeatureCats[i].id, allFeatureCats[i]);
  }

  const variantsMap = new Map();
  for (let i = 0; i < allVariants.length; i++) {
    const v = allVariants[i];
    if (!variantsMap.has(v.product_id)) variantsMap.set(v.product_id, []);
    variantsMap.get(v.product_id).push(v);
  }

  // Add fallback images to imagesMap
  for (let i = 0; i < fallbackImages.length; i++) {
    const img = fallbackImages[i];
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  }

  const variantPriceMap = new Map();
  for (let i = 0; i < variantStoreProducts.length; i++) {
    variantPriceMap.set(variantStoreProducts[i].varient_id, variantStoreProducts[i].price);
  }

  const mapBuildTime = Date.now() - mapBuildStartTime;
  // console.log(`📊 Map building completed in ${mapBuildTime}ms`);

  // Process product details using pre-fetched maps (optimized with Map lookups)
  const processStartTime = Date.now();
  const imageUrlSuffix = "?width=200&height=200&quality=100";
  const customizedProductData = productDetails.map((product) => {
    // Use pre-fetched maps with Map.get() for O(1) lookups
    const isFavourite = wishlistMap.has(product.varient_id) ? 'true' : 'false';
    const cartQty = cartQtyMap.get(product.varient_id) || 0;
    const notifyMe = notifyMeMap.has(product.varient_id) ? 'true' : 'false';
    const isSubscription = subscriptionMap.has(product.varient_id) ? 'true' : 'false';

    // Optimize: Pre-compute subscription price
    const percentageMultiplier = product.percentage / 100;
    const subscription_price = parseFloat((product.mrp * (1 - percentageMultiplier)).toFixed(2));
    const countryicon = product.country_icon ? baseurl + product.country_icon : null;

    // Use pre-fetched deal price with Map
    const price = dealsMap.get(product.varient_id) || product.price;

    // Optimize: Avoid string concatenation for numbers
    const priceval = Number.isInteger(price) ? price + 0.001 : price;
    const mrpval = Number.isInteger(product.mrp) ? product.mrp + 0.001 : product.mrp;

    // Use pre-fetched feature categories (optimize string operations)
    let feature_tags = [];
    if (product.fcat_id != null && product.fcat_id !== '') {
      const fcatIds = product.fcat_id.split(',');
      for (let i = 0; i < fcatIds.length; i++) {
        const id = parseInt(fcatIds[i], 10);
        const fcat = featureCategoriesMap.get(id);
        if (fcat) feature_tags.push(fcat);
      }
    }

    // Use pre-fetched features with Map
    const features = featuresMap.get(product.product_id) || [];

    // Use pre-fetched variants with Map
    const app = variantsMap.get(product.product_id) || [];

    // Process variants using pre-fetched maps (optimized loop)
    const customizedVarientData = [];
    let total_cart_qty = 0;
    let total_subcart_qty = 0;

    // Pre-fetch images once
    const images = imagesMap[product.product_id] || [];
    const firstImage = images.length > 0 ? images[0].image : '';
    const productImageUrl = firstImage ? firstImage + imageUrlSuffix : '';

    const variantCount = app.length;
    for (let i = 0; i < variantCount; i++) {
      const ProductList = app[i];

      // Use pre-fetched price with Map lookups
      const vprice = dealsMap.get(ProductList.varient_id) || variantPriceMap.get(ProductList.varient_id) || ProductList.price;

      // Use pre-fetched user data with Map (optimize condition check)
      let isFavourite1 = 'false';
      let notifyMe1 = 'false';
      let cartQty1 = 0;
      let subcartQty1 = 0;
      let productFeatureId = 0;

      if (user_id) {
        isFavourite1 = wishlistMap.has(ProductList.varient_id) ? 'true' : 'false';
        cartQty1 = cartQtyMap.get(ProductList.varient_id) || 0;
        subcartQty1 = subcartQtyMap.get(ProductList.varient_id) || 0;
        notifyMe1 = notifyMeMap.has(ProductList.varient_id) ? 'true' : 'false';
        productFeatureId = cartFeatureMap.get(ProductList.varient_id) || 0;

        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty1;
      }

      customizedVarientData.push({
        stock: ProductList.stock,
        varient_id: ProductList.varient_id,
        product_id: product.product_id,
        product_name: product.product_name,
        product_image: productImageUrl,
        thumbnail: firstImage,
        description: ProductList.description,
        price: vprice,
        mrp: ProductList.mrp,
        unit: ProductList.unit,
        quantity: ProductList.quantity,
        type: product.type,
        discountper: ProductList.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        subcartQty: subcartQty1,
        product_feature_id: productFeatureId,
        country_icon: countryicon,
      });
    }
    const varients = customizedVarientData;

    // Optimize: Pre-compute product image URL
    const mainProductImage = baseurl + product.product_image + imageUrlSuffix;

    return {
      stock: product.stock,
      varient_id: product.varient_id,
      product_id: product.product_id,
      product_name: product.product_name,
      product_image: mainProductImage,
      thumbnail: product.thumbnail,
      description: product.description,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      unit: product.unit,
      quantity: product.quantity,
      type: product.type,
      percentage: product.percentage,
      isSubscription,
      subscription_price,
      availability: product.availability,
      discountper: product.discountper || 0,
      avgrating: 0, // Placeholder for ratings
      notify_me: notifyMe,
      isFavourite,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      countrating: 0, // Placeholder for country ratings
      country_icon: countryicon,
      feature_tags: feature_tags,
      features: features,
      varients: varients,
      is_customized: product.is_customized,
      totalPages: totalPages,
      total_subcart_qty: total_subcart_qty,
    };
  });

  const processTime = Date.now() - processStartTime;
  // console.log(`📊 Data processing completed in ${processTime}ms`);

  return customizedProductData;
};
const getcatProduct = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL
  const {
    cat_id,
    sub_cat_id: subcatid,
    store_id,
    byname: filter1,
    sort: issort,
    sortprice,
    sortname,
    user_id: rawUserId,
    min_price: minPrice,
    max_price: maxPrice,
    min_discount: minDiscount,
    max_discount: maxDiscount,
    page: pageFilter,
    perpage: perPage
  } = appDetatils;

  const user_id = rawUserId !== "null" ? rawUserId : appDetatils.device_id;
  const minprice = parseFloat(minPrice);
  const maxprice = parseFloat(maxPrice);
  const mindiscount = parseFloat(minDiscount);
  const maxdiscount = parseFloat(maxDiscount);

  const page = parseInt(pageFilter) || 1;
  const per_page = parseInt(perPage) || 100;
  const offset = (page - 1) * per_page;

  // Optimize: Build base query with efficient joins
  // Note: Removed currentDate1 as it's not used in the query
  let topsellingsQuery = knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('deal_product', function () {
      this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
        .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
        .andOn('deal_product.valid_from', '<=', knex.raw('CURRENT_TIMESTAMP'))
        .andOn('deal_product.valid_to', '>', knex.raw('CURRENT_TIMESTAMP'));
    })
    .leftJoin('tbl_country', function () {
      this.on(knex.raw('tbl_country.id::text'), '=', knex.raw('product.country_id'));
    })
    .leftJoin('parent_category_product_sequences', function () {
      this.on('product.product_id', '=', 'parent_category_product_sequences.product_id')
        .andOn('parent_category_product_sequences.cat_id', '=', knex.raw('?', [cat_id === 'null' ? null : cat_id]))
        .andOn('parent_category_product_sequences.sub_cat_id', '=', knex.raw('?', [subcatid === 'null' ? null : subcatid]));
    })
    .select(
      knex.raw('MAX(parent_category_product_sequences.sequence) as manual_sequence'),
      knex.raw('MAX(store_products.stock) as stock'),
      knex.raw('MAX(product_varient.varient_id) as varient_id'),
      knex.raw('MAX(product_varient.description) as description'),
      'product.product_id',
      'product.product_name',
      'product.product_image',
      'product.thumbnail',
      knex.raw('MAX(store_products.price) as price'),
      knex.raw('MAX(store_products.mrp) as mrp'),
      // MAX(unit) is wrong for text ('KG' > 'G' lexicographically). Align with MAX(varient_id): same variant row.
      knex.raw("(array_agg(product_varient.unit ORDER BY product_varient.varient_id DESC))[1] as unit"),
      knex.raw("(array_agg(product_varient.quantity ORDER BY product_varient.varient_id DESC))[1] as quantity"),
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
    .where('product.is_zap', true)
    .where('product_varient.approved', 1)
    .where('product_varient.is_delete', 0)
    .where('store_products.stock', '>', 0)
    .where('product.approved', 1)
    .whereNotNull('store_products.price')
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




  // Fetch category array and product_cat product IDs for filtering
  let categoryarray = null;
  if (cat_id !== "null") {
    categoryarray = await knex('categories').where('parent', cat_id).pluck('cat_id');
    categoryarray.push(parseInt(cat_id, 10));

    const productIdsFromCatParent = await knex('product_cat')
      .whereIn('cat_id', categoryarray)
      .pluck('product_id');

    topsellingsQuery.andWhere(function () {
      this.whereIn('product.cat_id', categoryarray);
      if (productIdsFromCatParent.length > 0) {
        this.orWhereIn('product.product_id', productIdsFromCatParent);
      }
    });
  }

  if (subcatid !== "null") {
    const productIdsFromProductCat = await knex('product_cat')
      .where('cat_id', subcatid)
      .pluck('product_id');

    topsellingsQuery.andWhere(function () {
      this.where('product.cat_id', subcatid);
      if (cat_id !== "null") {
        this.orWhere('product.cat_id', cat_id);
      }
      if (productIdsFromProductCat.length > 0) {
        this.orWhereIn('product.product_id', productIdsFromProductCat);
      }
    });
  }


  // Priority Sequencing Order
  topsellingsQuery.orderByRaw('CASE WHEN MAX(parent_category_product_sequences.sequence) IS NOT NULL THEN MAX(parent_category_product_sequences.sequence) ELSE 9999 END ASC');

  // Apply discount filters
  if (mindiscount && maxdiscount) {
    topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [mindiscount, maxdiscount, mindiscount, maxdiscount]);
  }

  // Apply price filter (use HAVING for aggregated columns after GROUP BY)
  if (minprice && maxprice) {
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

  // Optimize: Run count and data queries in parallel for maximum speed
  const totalCountQuery = topsellingsQuery.clone().clearSelect().clearOrder().countDistinct('product.product_id as total');
  const dataQuery = topsellingsQuery.offset(offset).limit(per_page);

  const queryStartTime = Date.now();
  const [totalCountResult, productDetails] = await Promise.all([
    totalCountQuery.first(),
    dataQuery
  ]);
  const queryTime = Date.now() - queryStartTime;
  // console.log(`📊 Main query completed in ${queryTime}ms - Products: ${productDetails?.length || 0}`);

  const totalCount = parseInt(totalCountResult?.total || 0);
  const totalPages = Math.ceil(totalCount / per_page);

  // Collect all IDs for batch queries
  const variantIds = productDetails.map(p => p.varient_id);
  const productIds = productDetails.map(p => p.product_id);
  const currentDate = new Date();
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Optimize: Prepare all batch queries
  // Note: Deals are already joined in main query, but we fetch separately for accuracy
  // We can skip this if the join is sufficient, but keeping for consistency
  let dealsPromise = Promise.resolve([]);
  if (variantIds.length > 0) {
    dealsPromise = knex('deal_product')
      .whereIn('varient_id', variantIds)
      .where('store_id', store_id)
      .whereRaw('deal_product.valid_from <= CURRENT_TIMESTAMP')
      .whereRaw('deal_product.valid_to > CURRENT_TIMESTAMP')
      .select('varient_id', 'deal_price');
  }

  let wishlistsPromise = Promise.resolve([]);
  let cartItemsPromise = Promise.resolve([]);
  let subCartItemsPromise = Promise.resolve([]);
  let subscriptionsPromise = Promise.resolve([]);
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

    subscriptionsPromise = knex('store_orders')
      .select('varient_id')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1);

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
  productDetails.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const parsed = parseInt(id, 10);
        if (!isNaN(parsed)) fcatIdsSet.add(parsed);
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
      .where('store_products.stock', '>', 0)
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

  // Optimize: Execute ALL batch queries in parallel (first batch)
  const batch1StartTime = Date.now();
  const [
    deals,
    wishList,
    cartItems,
    subCartItems,
    subscriptions,
    notifyMeList,
    allFeatures,
    allFeatureCats,
    allVariants,
    allImages
  ] = await Promise.all([
    dealsPromise,
    wishlistsPromise,
    cartItemsPromise,
    subCartItemsPromise,
    subscriptionsPromise,
    notifyMePromise,
    featuresPromise,
    featureCatsPromise,
    variantsPromise,
    imagesPromise
  ]);
  const batch1Time = Date.now() - batch1StartTime;
  // console.log(`📊 Batch 1 queries completed in ${batch1Time}ms`);

  // Build imagesMap first to check which products need fallback
  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  });

  // Collect all variant IDs from fetched variants for second batch
  const allVariantIds = allVariants.map(v => v.varient_id);

  // Prepare second batch queries (variant prices and fallback images)
  const productsWithoutImages = productIds.filter(id => !imagesMap[id] || imagesMap[id].length === 0);

  const secondBatchPromises = [];
  if (allVariantIds.length > 0) {
    secondBatchPromises.push(
      knex('store_products')
        .whereIn('varient_id', allVariantIds)
        .where('store_id', store_id)
        .select('varient_id', 'price')
    );
  } else {
    secondBatchPromises.push(Promise.resolve([]));
  }

  if (productsWithoutImages.length > 0) {
    secondBatchPromises.push(
      knex('product')
        .select('product_id', knex.raw(`('${baseurl}' || COALESCE(product_image, '')) as image`))
        .whereIn('product_id', productsWithoutImages)
    );
  } else {
    secondBatchPromises.push(Promise.resolve([]));
  }

  // Execute second batch in parallel
  const batch2StartTime = Date.now();
  const [variantStoreProducts, fallbackImages] = await Promise.all(secondBatchPromises);
  const batch2Time = Date.now() - batch2StartTime;
  // console.log(`📊 Batch 2 queries completed in ${batch2Time}ms`);

  // Optimize: Build maps using Map for faster lookups (O(1) vs O(n) for objects)
  const mapBuildStartTime = Date.now();

  const dealsMap = new Map();
  for (let i = 0; i < deals.length; i++) {
    dealsMap.set(deals[i].varient_id, deals[i].deal_price);
  }

  const wishlistMap = new Map();
  for (let i = 0; i < wishList.length; i++) {
    wishlistMap.set(wishList[i].varient_id, true);
  }

  const cartQtyMap = new Map();
  const cartFeatureMap = new Map();
  for (let i = 0; i < cartItems.length; i++) {
    const c = cartItems[i];
    cartQtyMap.set(c.varient_id, c.qty);
    if (c.product_feature_id) cartFeatureMap.set(c.varient_id, c.product_feature_id);
  }

  const subcartQtyMap = new Map();
  for (let i = 0; i < subCartItems.length; i++) {
    subcartQtyMap.set(subCartItems[i].varient_id, subCartItems[i].qty);
  }

  const subscriptionMap = new Map();
  for (let i = 0; i < subscriptions.length; i++) {
    subscriptionMap.set(subscriptions[i].varient_id, true);
  }

  const notifyMeMap = new Map();
  for (let i = 0; i < notifyMeList.length; i++) {
    notifyMeMap.set(notifyMeList[i].varient_id, true);
  }

  const featuresMap = new Map();
  for (let i = 0; i < allFeatures.length; i++) {
    const f = allFeatures[i];
    if (!featuresMap.has(f.product_id)) featuresMap.set(f.product_id, []);
    featuresMap.get(f.product_id).push({ id: f.id, feature_value: f.feature_value });
  }

  const featureCategoriesMap = new Map();
  for (let i = 0; i < allFeatureCats.length; i++) {
    featureCategoriesMap.set(allFeatureCats[i].id, allFeatureCats[i]);
  }

  const variantsMap = new Map();
  for (let i = 0; i < allVariants.length; i++) {
    const v = allVariants[i];
    if (!variantsMap.has(v.product_id)) variantsMap.set(v.product_id, []);
    variantsMap.get(v.product_id).push(v);
  }

  // Add fallback images to imagesMap
  for (let i = 0; i < fallbackImages.length; i++) {
    const img = fallbackImages[i];
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  }

  const variantPriceMap = new Map();
  for (let i = 0; i < variantStoreProducts.length; i++) {
    variantPriceMap.set(variantStoreProducts[i].varient_id, variantStoreProducts[i].price);
  }

  const mapBuildTime = Date.now() - mapBuildStartTime;
  // console.log(`📊 Map building completed in ${mapBuildTime}ms`);

  // Process product details using pre-fetched maps (optimized with Map lookups)
  const processStartTime = Date.now();
  const imageUrlSuffix = "?width=200&height=200&quality=100";
  const customizedProductData = productDetails.map((product) => {
    // Pre-compute percentage for subscription math
    const percentageMultiplier = product.percentage / 100;
    const countryicon = product.country_icon ? baseurl + product.country_icon : null;

    // Use pre-fetched feature categories (optimize string operations)
    let feature_tags = [];
    if (product.fcat_id != null && product.fcat_id !== '') {
      const fcatIds = product.fcat_id.split(',');
      for (let i = 0; i < fcatIds.length; i++) {
        const id = parseInt(fcatIds[i], 10);
        const fcat = featureCategoriesMap.get(id);
        if (fcat) feature_tags.push(fcat);
      }
    }

    // Use pre-fetched features with Map
    const features = featuresMap.get(product.product_id) || [];

    // Use pre-fetched variants with Map
    const app = variantsMap.get(product.product_id) || [];

    // Process variants using pre-fetched maps (optimized loop)
    const customizedVarientData = [];
    let total_cart_qty = 0;
    let total_subcart_qty = 0;

    // Pre-fetch images once
    const images = imagesMap[product.product_id] || [];
    const firstImage = images.length > 0 ? images[0].image : '';
    const productImageUrl = firstImage ? firstImage + imageUrlSuffix : '';

    const variantCount = app.length;
    for (let i = 0; i < variantCount; i++) {
      const ProductList = app[i];
      if (!(Number(ProductList.stock) > 0)) {
        continue;
      }

      // Use pre-fetched price with Map lookups
      const vprice = dealsMap.get(ProductList.varient_id) || variantPriceMap.get(ProductList.varient_id) || ProductList.price;

      // Use pre-fetched user data with Map (optimize condition check)
      let isFavourite1 = 'false';
      let notifyMe1 = 'false';
      let cartQty1 = 0;
      let subcartQty1 = 0;
      let productFeatureId = 0;

      if (user_id) {
        isFavourite1 = wishlistMap.has(ProductList.varient_id) ? 'true' : 'false';
        cartQty1 = cartQtyMap.get(ProductList.varient_id) || 0;
        subcartQty1 = subcartQtyMap.get(ProductList.varient_id) || 0;
        notifyMe1 = notifyMeMap.has(ProductList.varient_id) ? 'true' : 'false';
        productFeatureId = cartFeatureMap.get(ProductList.varient_id) || 0;

        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty1;
      }

      customizedVarientData.push({
        stock: ProductList.stock,
        varient_id: ProductList.varient_id,
        product_id: product.product_id,
        product_name: product.product_name,
        product_image: productImageUrl,
        thumbnail: firstImage,
        description: ProductList.description,
        price: vprice,
        mrp: ProductList.mrp,
        unit: ProductList.unit,
        quantity: ProductList.quantity,
        type: product.type,
        discountper: ProductList.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        subcartQty: subcartQty1,
        product_feature_id: productFeatureId,
        country_icon: countryicon,
      });
    }
    customizedVarientData.sort((a, b) => {
      const aPrice = Number(a.price);
      const bPrice = Number(b.price);
      if (aPrice !== bPrice) return aPrice - bPrice;
      return Number(a.varient_id) - Number(b.varient_id);
    });
    const varients = customizedVarientData;
    const mainVariant = varients[0];
    if (!mainVariant) {
      return null;
    }

    // Optimize: Pre-compute product image URL
    const mainProductImage = baseurl + product.product_image + imageUrlSuffix;

    return {
      stock: mainVariant.stock,
      varient_id: mainVariant.varient_id,
      product_id: product.product_id,
      product_name: product.product_name,
      product_image: mainProductImage,
      thumbnail: mainVariant.thumbnail,
      description: mainVariant.description,
      price: parseFloat(mainVariant.price),
      mrp: parseFloat(mainVariant.mrp),
      unit: mainVariant.unit,
      quantity: mainVariant.quantity,
      type: product.type,
      percentage: product.percentage,
      isSubscription: mainVariant.varient_id && subscriptionMap.has(mainVariant.varient_id) ? 'true' : 'false',
      subscription_price: parseFloat((mainVariant.mrp * (1 - percentageMultiplier)).toFixed(2)),
      availability: product.availability,
      discountper: product.discountper || 0,
      avgrating: 0, // Placeholder for ratings
      notify_me: mainVariant.notify_me,
      isFavourite: mainVariant.isFavourite,
      cart_qty: mainVariant.cart_qty,
      total_cart_qty: total_cart_qty,
      countrating: 0, // Placeholder for country ratings
      country_icon: countryicon,
      feature_tags: feature_tags,
      features: features,
      varients: varients,
      is_customized: product.is_customized,
      totalPages: totalPages,
      total_subcart_qty: total_subcart_qty,
    };
  });

  const processTime = Date.now() - processStartTime;
  // console.log(`📊 Data processing completed in ${processTime}ms`);

  return customizedProductData.filter(Boolean);
};

const getfetcatProd = async (appDetatils) => {
  // PostgreSQL: no MySQL sql_mode - use strict GROUP BY with aggregates
  const {
    fcat_id,
    store_id,
    byname: filter1,
    sort: issort,
    sortprice,
    sortname,
    user_id: rawUserId,
    min_price: minPrice,
    max_price: maxPrice,
    min_discount: minDiscount,
    max_discount: maxDiscount,
    page: pageFilter,
    perpage: perPage
  } = appDetatils;

  const user_id = rawUserId !== "null" ? rawUserId : appDetatils.device_id;
  const minprice = parseFloat(minPrice) || 0;
  const maxprice = parseFloat(maxPrice) || 0;
  const mindiscount = parseFloat(minDiscount) || 0;
  const maxdiscount = parseFloat(maxDiscount) || 0;

  // Validate and set defaults for pagination (ensure valid integers)
  const page = parseInt(pageFilter, 10) || 1;
  const itemsPerPage = parseInt(perPage, 10) || 20;
  const offset = Math.max(0, (page - 1) * itemsPerPage);
  const limit = Math.max(1, itemsPerPage); // Ensure at least 1

  const baseurl = process.env.BUNNY_NET_IMAGE || '';

  // Build base query - PostgreSQL: explicit type cast for tbl_country, GROUP BY with MAX aggregates
  let topsellingsQuery = knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .join('product', 'product_varient.product_id', '=', 'product.product_id')
    .leftJoin('deal_product', function () {
      this.on('product_varient.varient_id', '=', 'deal_product.varient_id')
        .andOn('deal_product.store_id', '=', knex.raw('?', [store_id]))
        .andOn('deal_product.valid_from', '<=', knex.raw('CURRENT_TIMESTAMP'))
        .andOn('deal_product.valid_to', '>', knex.raw('CURRENT_TIMESTAMP'));
    })
    .leftJoin('tbl_country', function () {
      this.on(knex.raw('tbl_country.id::text'), '=', knex.raw('product.country_id'));
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
      'product.fcat_id',
      'product.is_customized',
      knex.raw('100 - (MAX(store_products.price) * 100 / MAX(store_products.mrp)) as discountper'),
      knex.raw('100 - (MAX(deal_product.deal_price) * 100 / MAX(store_products.mrp)) as discountper1')
    )
    .where('store_products.store_id', store_id)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('product.is_zap', true)
    .where('store_products.stock', '>', 0)
    .where('product.approved', 1)
    .whereRaw("product.fcat_id LIKE ?", [`%${fcat_id}%`])
    .whereNotNull('store_products.price')
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

  // Apply price filter (HAVING for aggregated columns)
  if (minprice && maxprice) {
    topsellingsQuery.havingRaw('MAX(store_products.price) BETWEEN ? AND ?', [minprice, maxprice]);
  }

  if (mindiscount && maxdiscount) {
    topsellingsQuery.havingRaw('(discountper BETWEEN ? AND ?) OR (discountper1 BETWEEN ? AND ?)', [mindiscount, maxdiscount, mindiscount, maxdiscount]);
  }

  // Apply sorting (aggregates for price)
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

  // Run count and data in parallel for lowest latency
  const totalCountQuery = topsellingsQuery.clone().clearSelect().clearOrder().countDistinct('product.product_id as total');
  const dataQuery = topsellingsQuery.offset(offset).limit(limit);

  const [totalCountResult, productDetails] = await Promise.all([
    totalCountQuery.first(),
    dataQuery
  ]);

  const totalCount = parseInt(totalCountResult?.total || 0, 10);
  const totalPages = limit > 0 ? Math.ceil(totalCount / limit) : 1;

  if (!productDetails || productDetails.length === 0) {
    return [];
  }

  const variantIds = productDetails.map(p => p.varient_id);
  const productIds = productDetails.map(p => p.product_id);
  const currentDate = new Date();

  // Batch 1: all related data in parallel (minimize round-trips)
  let dealsPromise = knex('deal_product')
    .whereIn('varient_id', variantIds)
    .where('store_id', store_id)
    .whereRaw('deal_product.valid_from <= CURRENT_TIMESTAMP')
    .whereRaw('deal_product.valid_to > CURRENT_TIMESTAMP')
    .select('varient_id', 'deal_price');

  let wishlistsPromise = Promise.resolve([]);
  let cartItemsPromise = Promise.resolve([]);
  let subCartItemsPromise = Promise.resolve([]);
  let subscriptionsPromise = Promise.resolve([]);
  let notifyMePromise = Promise.resolve([]);
  if (user_id && variantIds.length > 0) {
    wishlistsPromise = knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id).select('varient_id');
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
    subscriptionsPromise = knex('store_orders')
      .select('varient_id')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('subscription_flag', 1);
    notifyMePromise = knex('product_notify_me')
      .whereIn('varient_id', variantIds)
      .where('user_id', user_id)
      .select('varient_id');
  }

  const fcatIdsSet = new Set();
  productDetails.forEach(p => {
    if (p.fcat_id) {
      p.fcat_id.split(',').forEach(id => {
        const parsed = parseInt(id, 10);
        if (!isNaN(parsed)) fcatIdsSet.add(parsed);
      });
    }
  });
  let featureCatsPromise = Promise.resolve([]);
  if (fcatIdsSet.size > 0) {
    featureCatsPromise = knex('feature_categories')
      .whereIn('id', Array.from(fcatIdsSet))
      .where('status', 1)
      .where('is_deleted', 0)
      .select('id', knex.raw("(? || COALESCE(image, '')) as image", [baseurl]));
  }

  let featuresPromise = knex('product_features')
    .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
    .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
    .whereIn('product_id', productIds);

  let variantsPromise = knex('store_products')
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

  let imagesPromise = knex('product_images')
    .select('product_id', knex.raw("(? || COALESCE(image, '')) as image", [baseurl]), 'type')
    .whereIn('product_id', productIds)
    .orderBy('type', 'DESC');

  const [
    deals,
    wishList,
    cartItems,
    subCartItems,
    subscriptions,
    notifyMeList,
    allFeatureCats,
    allFeatures,
    allVariants,
    allImages
  ] = await Promise.all([
    dealsPromise,
    wishlistsPromise,
    cartItemsPromise,
    subCartItemsPromise,
    subscriptionsPromise,
    notifyMePromise,
    featureCatsPromise,
    featuresPromise,
    variantsPromise,
    imagesPromise
  ]);

  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  });
  const allVariantIds = allVariants.map(v => v.varient_id);
  const productsWithoutImages = productIds.filter(id => !imagesMap[id] || imagesMap[id].length === 0);

  const secondBatchPromises = [
    allVariantIds.length > 0
      ? knex('store_products').whereIn('varient_id', allVariantIds).where('store_id', store_id).select('varient_id', 'price')
      : Promise.resolve([]),
    productsWithoutImages.length > 0
      ? knex('product').select('product_id', knex.raw("(? || COALESCE(product_image, '')) as image", [baseurl])).whereIn('product_id', productsWithoutImages)
      : Promise.resolve([])
  ];

  const [variantStoreProducts, fallbackImages] = await Promise.all(secondBatchPromises);
  fallbackImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img);
  });

  // Build maps for O(1) lookups (lowest latency in processing)
  const dealsMap = new Map();
  deals.forEach(d => dealsMap.set(d.varient_id, d.deal_price));
  const wishlistMap = new Map();
  wishList.forEach(w => wishlistMap.set(w.varient_id, true));
  const cartQtyMap = new Map();
  const cartFeatureMap = new Map();
  cartItems.forEach(c => {
    cartQtyMap.set(c.varient_id, c.qty);
    if (c.product_feature_id) cartFeatureMap.set(c.varient_id, c.product_feature_id);
  });
  const subcartQtyMap = new Map();
  subCartItems.forEach(s => subcartQtyMap.set(s.varient_id, s.qty));
  const subscriptionMap = new Map();
  subscriptions.forEach(s => subscriptionMap.set(s.varient_id, true));
  const notifyMeMap = new Map();
  notifyMeList.forEach(n => notifyMeMap.set(n.varient_id, true));
  const featureCategoriesMap = new Map();
  allFeatureCats.forEach(fc => featureCategoriesMap.set(fc.id, fc));
  const featuresMap = new Map();
  allFeatures.forEach(f => {
    if (!featuresMap.has(f.product_id)) featuresMap.set(f.product_id, []);
    featuresMap.get(f.product_id).push({ id: f.id, feature_value: f.feature_value });
  });
  const variantsMap = new Map();
  allVariants.forEach(v => {
    if (!variantsMap.has(v.product_id)) variantsMap.set(v.product_id, []);
    variantsMap.get(v.product_id).push(v);
  });
  const variantPriceMap = new Map();
  variantStoreProducts.forEach(v => variantPriceMap.set(v.varient_id, v.price));

  const imageUrlSuffix = '?width=200&height=200&quality=100';

  // Single pass over products, no async, no extra queries
  const customizedProductData = productDetails.map((product) => {
    const isFavourite = wishlistMap.has(product.varient_id) ? 'true' : 'false';
    const cartQty = cartQtyMap.get(product.varient_id) || 0;
    const notifyMe = notifyMeMap.has(product.varient_id) ? 'true' : 'false';
    const isSubscription = subscriptionMap.has(product.varient_id) ? 'true' : 'false';
    const percentageMultiplier = (product.percentage || 0) / 100;
    const subscription_price = parseFloat((product.mrp * (1 - percentageMultiplier)).toFixed(2));
    const countryicon = product.country_icon ? baseurl + product.country_icon : null;
    const price = dealsMap.get(product.varient_id) || product.price;
    const priceval = Number.isInteger(price) ? price + 0.001 : price;
    const mrpval = Number.isInteger(product.mrp) ? product.mrp + 0.001 : product.mrp;

    let feature_tags = [];
    if (product.fcat_id) {
      product.fcat_id.split(',').forEach(idStr => {
        const id = parseInt(idStr, 10);
        const fcat = featureCategoriesMap.get(id);
        if (fcat) feature_tags.push(fcat);
      });
    }
    const features = featuresMap.get(product.product_id) || [];
    const app = variantsMap.get(product.product_id) || [];
    const images = imagesMap[product.product_id] || [];
    const firstImage = images.length > 0 ? images[0].image : '';
    const productImageUrl = firstImage ? firstImage + imageUrlSuffix : '';

    let total_cart_qty = 0;
    let total_subcart_qty = 0;
    const customizedVarientData = [];
    for (let i = 0; i < app.length; i++) {
      const ProductList = app[i];
      const vprice = dealsMap.get(ProductList.varient_id) || variantPriceMap.get(ProductList.varient_id) || ProductList.price;
      let isFavourite1 = 'false';
      let notifyMe1 = 'false';
      let cartQty1 = 0;
      let subcartQty1 = 0;
      let productFeatureId = 0;
      if (user_id) {
        isFavourite1 = wishlistMap.has(ProductList.varient_id) ? 'true' : 'false';
        cartQty1 = cartQtyMap.get(ProductList.varient_id) || 0;
        subcartQty1 = subcartQtyMap.get(ProductList.varient_id) || 0;
        notifyMe1 = notifyMeMap.has(ProductList.varient_id) ? 'true' : 'false';
        productFeatureId = cartFeatureMap.get(ProductList.varient_id) || 0;
        total_cart_qty += cartQty1;
        total_subcart_qty += subcartQty1;
      }
      customizedVarientData.push({
        stock: ProductList.stock,
        varient_id: ProductList.varient_id,
        product_id: product.product_id,
        product_name: product.product_name,
        product_image: productImageUrl,
        thumbnail: firstImage,
        description: ProductList.description,
        price: vprice,
        mrp: ProductList.mrp,
        unit: ProductList.unit,
        quantity: ProductList.quantity,
        type: product.type,
        discountper: ProductList.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        subcartQty: subcartQty1,
        product_feature_id: productFeatureId,
        country_icon: countryicon,
      });
    }

    const mainProductImage = (product.product_image ? baseurl + product.product_image : '') + '?width=200&height=200&quality=70';

    return {
      stock: product.stock,
      varient_id: product.varient_id,
      product_id: product.product_id,
      product_name: product.product_name,
      product_image: mainProductImage,
      thumbnail: product.thumbnail,
      description: product.description,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      unit: product.unit,
      quantity: product.quantity,
      type: product.type,
      percentage: product.percentage,
      isSubscription,
      subscription_price,
      availability: product.availability,
      discountper: product.discountper || 0,
      avgrating: 0,
      notify_me: notifyMe,
      isFavourite,
      cart_qty: cartQty,
      countrating: 0,
      country_icon: countryicon,
      feature_tags,
      totalPages,
      is_customized: product.is_customized,
      features,
      total_cart_qty,
      total_subcart_qty,
      varients: customizedVarientData,
    };
  });

  return customizedProductData;
};

const getrecentSelling = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;
  const storeId = store_id;
  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Fetch products in bulk
  const productDetails = await knex('store_products')
    .select(
      'store_products.store_id',
      'store_products.stock',
      'product_varient.varient_id',
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
      'product.is_customized',
    )
    .innerJoin('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
    .innerJoin('product', 'product_varient.product_id', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .whereRaw('store_products.store_id = ?::integer', [storeId])
    .whereRaw('product.hide = 0')
    .whereRaw('product.is_delete = 0')
    .where('product.is_zap', true)
    .whereRaw('store_products.stock > 0')
    .whereNotNull('store_products.price')
    .where(builder => {
      builder
        .whereRaw('product.is_offer_product = 0')
        .whereNull('product.offer_date')
        .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE')
    })
    .orderBy('store_products.p_id', 'asc')
    .limit(8);

  if (productDetails.length === 0) {
    return [];
  }

  const productVarientIds = productDetails.map(product => product.varient_id);
  const productIds = [...new Set(productDetails.map(p => p.product_id))]; // Unique product IDs

  // Batch fetch ALL user-related data in parallel (for main products)
  const [wishList, cartItems, notifyMeList, subscriptionProducts] = await Promise.all([
    user_id ? knex('wishlist').whereIn('varient_id', productVarientIds).where('user_id', user_id) : [],
    user_id ? knex('store_orders')
      .whereIn('varient_id', productVarientIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', storeId) : [],
    user_id ? knex('product_notify_me').whereIn('varient_id', productVarientIds).where('user_id', user_id) : [],
    user_id ? knex('store_orders')
      .select('store_orders.percentage', 'store_orders.varient_id')
      .whereIn('store_orders.varient_id', productVarientIds)
      .where('store_approval', user_id)
      .where('store_orders.subscription_flag', 1)
      .where('store_orders.order_cart_id', "incart") : []
  ]);

  // Mapping the fetched data to make it easy to access
  const wishlistMap = wishList.reduce((acc, item) => ({ ...acc, [item.varient_id]: true }), {});
  const cartMap = cartItems.reduce((acc, item) => ({ ...acc, [item.varient_id]: item.qty }), {});
  const notifyMeMap = notifyMeList.reduce((acc, item) => ({ ...acc, [item.varient_id]: true }), {});
  const subscriptionMap = subscriptionProducts.reduce((acc, item) => ({ ...acc, [item.varient_id]: item.percentage }), {});

  // Batch fetch ALL features for ALL products at once (OUTSIDE the map)
  const allFeatures = await knex('product_features')
    .select('product_features.product_id', 'tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
    .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.feature_value_id')
    .whereIn('product_features.product_id', productIds);

  const featuresMap = {};
  allFeatures.forEach(f => {
    if (!featuresMap[f.product_id]) featuresMap[f.product_id] = [];
    featuresMap[f.product_id].push({ id: f.id, feature_value: f.feature_value });
  });

  // Batch fetch ALL variants for ALL products
  const allVariants = await knex('store_products')
    .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
    .select(
      'store_products.store_id',
      'store_products.stock',
      'product_varient.varient_id',
      'product_varient.description',
      'store_products.price',
      'store_products.mrp',
      'product_varient.varient_image',
      'product_varient.unit',
      'product_varient.quantity',
      'product_varient.product_id',
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper')
    )
    .where('store_products.store_id', storeId)
    .whereIn('product_varient.product_id', productIds)
    .whereNotNull('store_products.price')
    .where('product_varient.approved', 1)
    .where('product_varient.is_delete', 0);

  const variantsMap = {};
  allVariants.forEach(v => {
    if (!variantsMap[v.product_id]) variantsMap[v.product_id] = [];
    variantsMap[v.product_id].push(v);
  });

  // Batch fetch ALL product images
  const allImages = await knex('product_images')
    .select('product_id', knex.raw(`? || image as image`, [baseurl]), 'type')
    .whereIn('product_id', productIds)
    .orderBy('type', 'DESC');

  const imagesMap = {};
  allImages.forEach(img => {
    if (!imagesMap[img.product_id]) imagesMap[img.product_id] = [];
    imagesMap[img.product_id].push(img.image);
  });

  // Batch fetch ALL feature tags
  const allFcatIds = productDetails
    .filter(p => p.fcat_id)
    .map(p => p.fcat_id.split(',').map(Number))
    .flat()
    .filter((id, index, self) => self.indexOf(id) === index); // Remove duplicates

  const allFeatureTags = allFcatIds.length > 0 ? await knex('feature_categories')
    .whereIn('id', allFcatIds)
    .where('status', 1)
    .where('is_deleted', 0)
    .select('id', knex.raw(`? || image as image`, [baseurl])) : [];

  const featureTagsMap = {};
  productDetails.forEach(product => {
    if (product.fcat_id) {
      const resultArray = product.fcat_id.split(',').map(Number);
      featureTagsMap[product.product_id] = allFeatureTags.filter(ft => resultArray.includes(ft.id));
    } else {
      featureTagsMap[product.product_id] = [];
    }
  });

  // Batch fetch ALL cart data for ALL variants
  const variantIds = allVariants.map(v => v.varient_id);
  const [allWishlistVariants, allCartItemsVariants, allSubCartItemsVariants, allNotifyMeVariants, allCartFeatures] = await Promise.all([
    user_id && variantIds.length > 0 ? knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id) : [],
    user_id && variantIds.length > 0 ? knex('store_orders')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .whereNull('subscription_flag')
      .where('store_id', storeId) : [],
    user_id && variantIds.length > 0 ? knex('store_orders')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('subscription_flag', 1)
      .where('store_id', storeId) : [],
    user_id && variantIds.length > 0 ? knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id) : [],
    user_id && variantIds.length > 0 ? knex('store_orders')
      .select('varient_id', 'product_feature_id')
      .whereIn('varient_id', variantIds)
      .where('store_approval', user_id)
      .where('order_cart_id', 'incart')
      .where('store_id', storeId) : []
  ]);

  const wishlistMapVariants = {};
  allWishlistVariants.forEach(w => wishlistMapVariants[w.varient_id] = true);
  const cartMapVariants = {};
  allCartItemsVariants.forEach(c => cartMapVariants[c.varient_id] = c.qty);
  const subCartMapVariants = {};
  allSubCartItemsVariants.forEach(c => subCartMapVariants[c.varient_id] = c.qty);
  const notifyMeMapVariants = {};
  allNotifyMeVariants.forEach(n => notifyMeMapVariants[n.varient_id] = true);
  const cartFeaturesMap = {};
  allCartFeatures.forEach(cf => cartFeaturesMap[cf.varient_id] = cf.product_feature_id || 0);

  // NOW process products (no more individual queries inside the loop)
  const customizedProductData = productDetails.map(product => {
    const featureTags = featureTagsMap[product.product_id] || [];
    const isFavourite = wishlistMap[product.varient_id] ? 'true' : 'false';
    const cartQty = cartMap[product.varient_id] || 0;
    const notifyMe = notifyMeMap[product.varient_id] ? 'true' : 'false';
    const isSubscription = subscriptionMap[product.varient_id] ? 'true' : 'false';

    const sub_price = (product.mrp * product.percentage) / 100;
    const finalsubprice = product.mrp - sub_price;
    const subscription_price = parseFloat(finalsubprice.toFixed(2));

    if (Number.isInteger(product.price)) {
      var priceval = product.price + '.001'
    } else {
      var priceval = product.price
    }
    if (Number.isInteger(product.mrp)) {
      var mrpval = product.mrp + '.001'
    } else {
      var mrpval = product.mrp
    }

    // Get pre-fetched data from maps
    const features = featuresMap[product.product_id] || [];
    const variants = variantsMap[product.product_id] || [];
    const productImages = imagesMap[product.product_id] || [baseurl + product.product_image];

    // Build variant data using pre-fetched maps
    const customizedVarientData = variants.map(variant => {
      const isFavourite1 = wishlistMapVariants[variant.varient_id] ? 'true' : 'false';
      const cartQty1 = cartMapVariants[variant.varient_id] || 0;
      const subcartQty1 = subCartMapVariants[variant.varient_id] || 0;
      const notifyMe1 = notifyMeMapVariants[variant.varient_id] ? 'true' : 'false';
      const productFeatureId = cartFeaturesMap[variant.varient_id] || 0;

      return {
        stock: variant.stock,
        varient_id: variant.varient_id,
        product_id: variant.product_id,
        product_name: product.product_name,
        product_image: productImages[0] + "?width=200&height=200&quality=100",
        thumbnail: productImages[0],
        description: variant.description,
        price: variant.price,
        mrp: variant.mrp,
        unit: variant.unit,
        quantity: variant.quantity,
        type: product.type,
        discountper: variant.discountper,
        notify_me: notifyMe1,
        isFavourite: isFavourite1,
        cart_qty: cartQty1,
        total_cart_qty: cartQty1,
        subcartQty: subcartQty1,
        total_subcart_qty: subcartQty1,
        product_feature_id: productFeatureId,
        country_icon: product.country_icon ? baseurl + product.country_icon : null,
      };
    });

    const total_cart_qty = variants.reduce((sum, v) => sum + (cartMapVariants[v.varient_id] || 0), 0);
    const total_subcart_qty = variants.reduce((sum, v) => sum + (subCartMapVariants[v.varient_id] || 0), 0);

    return {
      store_id: product.store_id,
      stock: product.stock,
      varient_id: product.varient_id,
      product_id: product.product_id,
      product_name: product.product_name,
      product_image: baseurl + product.product_image + "?width=200&height=200&quality=100",
      thumbnail: baseurl + product.thumbnail,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      unit: product.unit,
      quantity: product.quantity,
      type: product.type,
      discountper: 0,
      country_icon: product.country_icon ? baseurl + product.country_icon : null,
      cart_qty: cartQty,
      total_cart_qty: total_cart_qty,
      avgrating: 0,
      notify_me: notifyMe,
      percentage: product.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: product.availability,
      isFavourite: isFavourite,
      feature_tags: featureTags,
      is_customized: product.is_customized,
      features: features,
      varients: customizedVarientData,
      total_subcart_qty: total_subcart_qty,
    };
  });

  return customizedProductData;
};

const gettopSellingold = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;
  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const pageFilter = appDetatils.page;
  const perPage = appDetatils.perpage;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  let categoryList = await knex('categories').where('parent', 121).pluck('cat_id');

  // Fetch all products in one go
  const productDetail = await knex('store_products')
    .select(
      'store_products.*',
      knex.raw(`CONCAT('${baseurl}', product_image) as product_image`),
      knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
      'tbl_country.country_icon',
      'product_varient.unit as prdunit',
      'product_varient.varient_id',
      'product_varient.quantity',
      'product.product_id',
      'product.product_name',
      'product.thumbnail',
      'product.type',
      'product.percentage',
      'product.availability',
      'product_varient.description',
      'product_varient.varient_image',
      'product_varient.ean',
      'product_varient.approved',
      'product.cat_id',
      'product.brand_id',
      'product.hide',
      'product.added_by'
    )
    .innerJoin('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
    .innerJoin('product', 'product_varient.product_id', 'product.product_id')
    .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
    .where('store_products.store_id', store_id)
    .whereNotNull('store_products.price')
    .where('product.hide', 0)
    .whereIn('product.cat_id', categoryList)
    .where('product.is_delete', 0)
    .where('product.approved', 1)
    .limit(8);

  // Extract variant IDs for bulk queries
  const variantIds = productDetail.map(product => product.varient_id);

  // Batch fetch wishlist, cart, notify me, and subscription data in parallel
  const [wishList, cartItems, notifyMeList, subscriptionProducts, deals] = await Promise.all([
    knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id),
    knex('store_orders').whereIn('varient_id', variantIds).where('store_approval', user_id).where('order_cart_id', 'incart').whereNull('subscription_flag').where('store_id', store_id),
    knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id),
    knex('store_orders').select('varient_id').whereIn('varient_id', variantIds).where('store_approval', user_id).where('subscription_flag', 1).where('order_cart_id', 'incart'),
    knex('deal_product').whereIn('varient_id', variantIds).where('store_id', store_id).where('deal_product.valid_from', '<=', new Date()).where('deal_product.valid_to', '>', new Date())
  ]);

  // Preprocess deal prices
  const dealMap = {};
  deals.forEach(deal => {
    dealMap[deal.varient_id] = deal.deal_price;
  });

  // Preprocess subscription data
  const subscriptionMap = {};
  subscriptionProducts.forEach(sub => {
    subscriptionMap[sub.varient_id] = true;
  });

  // Preprocess wishlist data
  const wishListMap = {};
  wishList.forEach(item => {
    wishListMap[item.varient_id] = true;
  });

  // Preprocess cart items
  const cartMap = {};
  cartItems.forEach(item => {
    cartMap[item.varient_id] = item.qty;
  });

  // Preprocess notify me data
  const notifyMeMap = {};
  notifyMeList.forEach(item => {
    notifyMeMap[item.varient_id] = true;
  });

  // Process products and construct response
  const customizedProductData = productDetail.map(product => {
    const isFavourite = wishListMap[product.varient_id] ? 'true' : 'false';
    const cartQty = cartMap[product.varient_id] || 0;
    const notifyMe = notifyMeMap[product.varient_id] ? 'true' : 'false';
    const isSubscription = subscriptionMap[product.varient_id] ? 'true' : 'false';
    const dealPrice = dealMap[product.varient_id];
    const price = dealPrice || product.price;

    const sub_price = (product.mrp * product.percentage) / 100;
    const subscription_price = parseFloat((product.mrp - sub_price).toFixed(2));

    const country_icon = product.country_icon ? baseurl + product.country_icon : null;

    if (Number.isInteger(price)) {
      priceval = price + '.001'
    } else {
      priceval = price
    }
    if (Number.isInteger(product.mrp)) {
      mrpval = product.mrp + '.001'
    } else {
      mrpval = product.mrp
    }

    return {
      p_id: product.p_id,
      varient_id: product.varient_id,
      stock: product.stock,
      store_id: product.store_id,
      price: parseFloat(priceval),
      mrp: parseFloat(mrpval),
      min_ord_qty: product.min_ord_qty,
      max_ord_qty: product.max_ord_qty,
      buyingprice: product.buyingprice,
      product_code: product.product_code,
      partner_id: product.partner_id,
      product_id: product.product_id,
      quantity: product.quantity,
      unit: product.prdunit,
      description: product.description,
      varient_image: product.varient_image,
      ean: product.ean,
      approved: product.approved,
      added_by: product.added_by,
      cat_id: product.cat_id,
      brand_id: product.brand_id,
      product_name: product.product_name,
      product_image: product.product_image + "?width=200&height=200&quality=100",
      type: product.type,
      hide: product.hide,
      percentage: product.percentage,
      isSubscription: isSubscription,
      subscription_price: subscription_price,
      availability: product.availability,
      discountper: product.discountper || 0,
      country_icon: country_icon,
      avgrating: 0, // Placeholder for ratings
      notify_me: notifyMe,
      isFavourite: isFavourite,
      cart_qty: cartQty,
      countrating: 0
    };
  });

  return customizedProductData;
};

const gettopSelling = async (appDetails) => {
  try {
    const { store_id, is_subscription } = appDetails;
    const user_id = appDetails.user_id !== "null" ? appDetails.user_id : appDetails.device_id;
    const baseurl = process.env.BUNNY_NET_IMAGE;

    // Fetch category list
    // const categoryList = await knex('categories').where('parent', 121).pluck('cat_id');
    let categoryList = await knex('categories').where('parent', 1).pluck('cat_id');
    // Fetch product details
    // const productDetail = await knex('store_products')
    //   .select(
    //     'store_products.*',
    //     knex.raw(`CONCAT('${baseurl}', product_image) as product_image`),
    //     knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
    //     'tbl_country.country_icon',
    //     'product_varient.unit as prdunit',
    //     'product_varient.varient_id',
    //     'product_varient.quantity',
    //     'product.product_id',
    //     'product.product_name',
    //     'product.thumbnail',
    //     'product.type',
    //     'product.percentage',
    //     'product.availability',
    //     'product_varient.description',
    //     'product_varient.varient_image',
    //     'product_varient.ean',
    //     'product_varient.approved',
    //     'product.cat_id',
    //     'product.brand_id',
    //     'product.hide',
    //     'product.added_by',
    //     'product.fcat_id'
    //   )
    //   .innerJoin('product_varient', 'store_products.varient_id', 'product_varient.varient_id')
    //   .innerJoin('product', 'product_varient.product_id', 'product.product_id')
    //   .leftJoin('tbl_country', 'tbl_country.id', '=', 'product.country_id')
    //   .where('store_products.store_id', store_id)
    //   .whereNotNull('store_products.price')
    //   .where('product.hide', 0)
    //   .whereIn('product.cat_id', categoryList)
    //   .where('product.is_delete', 0)
    //   .where('product.approved', 1)
    //   .limit(8);

    const today = new Date(); // current date
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(today.getFullYear() - 1);
    // Format to 'YYYY-MM-DD'
    const oneYearAgoDate = oneYearAgo.toISOString().slice(0, 10);

    const productDetail = await knex('subscription_order as sub')
      .join('store_orders as so', 'sub.store_order_id', 'so.store_order_id')
      .join('store_products as sp', 'sp.varient_id', 'so.varient_id')
      .join('product_varient as pv', 'pv.varient_id', 'so.varient_id')
      .join('orders as or', 'or.cart_id', 'so.order_cart_id')
      .join('product as p', 'pv.product_id', 'p.product_id')
      .leftJoin('tbl_country as c', knex.raw('c.id::text = p.country_id'))
      .join('categories as cat', 'cat.cat_id', 'p.cat_id')
      .join('brands as brand', 'brand.cat_id', 'p.brand_id')
      .select(
        'sp.*',
        knex.raw(`? || p.product_image as product_image`, [baseurl]),
        knex.raw('100-((sp.price*100)/sp.mrp) as discountper'),
        'c.country_icon',
        'pv.unit as prdunit',
        'pv.varient_id',
        'pv.quantity',
        'p.product_id',
        'cat.cat_id',
        'cat.parent',
        'p.product_name',
        'p.thumbnail',
        'p.type',
        'p.percentage',
        'p.availability',
        knex.raw('SUM(so.qty) as total_quantity'),
        'pv.description',
        'pv.varient_image',
        'pv.ean',
        'p.approved',
        'p.cat_id',
        'p.brand_id',
        'p.hide',
        'p.added_by',
        'p.fcat_id',
        'p.is_customized',
      )
      .whereNotIn('sub.order_status', ['Pause', 'Cancelled'])
      .where('or.order_date', '>=', oneYearAgoDate) // make sure `oneYearAgo` is a valid Date object or formatted string
      .where('p.hide', 0)
      .where('p.is_delete', 0)
      .where('p.approved', 1)
      .where('p.is_zap', true)
      .where('sp.stock', '>', 0)
      .where(builder => {
        builder
          .where('p.is_offer_product', 0)
          .whereNull('p.offer_date')
          .orWhereRaw('DATE(p.offer_date) != CURRENT_DATE')
      })
      .groupBy('p.product_name', 'pv.varient_id', 'sp.p_id', 'sp.store_id', 'sp.stock', 'sp.price', 'sp.mrp', 'sp.min_ord_qty', 'sp.max_ord_qty', 'sp.buyingprice', 'c.country_icon', 'pv.unit', 'pv.quantity', 'cat.cat_id', 'cat.parent', 'p.thumbnail', 'p.type', 'p.percentage', 'p.availability', 'pv.description', 'pv.varient_image', 'pv.ean', 'p.approved', 'p.cat_id', 'p.brand_id', 'p.hide', 'p.added_by', 'p.fcat_id', 'p.is_customized', 'p.product_image', 'p.product_id')
      .orderBy('total_quantity', 'desc')
      .limit(10);


    const variantIds = productDetail.map(product => product.varient_id);

    // Fetch associated data in parallel
    const [wishList, cartItems, notifyMeList, subscriptionProducts, deals] = await Promise.all([
      knex('wishlist').whereIn('varient_id', variantIds).where('user_id', user_id),
      knex('store_orders')
        .whereIn('varient_id', variantIds)
        .where('store_approval', user_id)
        .where('order_cart_id', 'incart')
        .whereNull('subscription_flag')
        .where('store_id', store_id),
      knex('product_notify_me').whereIn('varient_id', variantIds).where('user_id', user_id),
      knex('store_orders')
        .select('varient_id')
        .whereIn('varient_id', variantIds)
        .where('store_approval', user_id)
        .where('subscription_flag', 1)
        .where('order_cart_id', 'incart'),
      knex('deal_product')
        .whereIn('varient_id', variantIds)
        .where('store_id', store_id)
        .where('deal_product.valid_from', '<=', new Date())
        .where('deal_product.valid_to', '>', new Date())
    ]);

    const dealMap = Object.fromEntries(deals.map(deal => [deal.varient_id, deal.deal_price]));
    const subscriptionMap = Object.fromEntries(subscriptionProducts.map(sub => [sub.varient_id, true]));
    const wishListMap = Object.fromEntries(wishList.map(item => [item.varient_id, true]));
    const cartMap = Object.fromEntries(cartItems.map(item => [item.varient_id, item.qty]));
    const notifyMeMap = Object.fromEntries(notifyMeList.map(item => [item.varient_id, true]));

    // Process product details with asynchronous logic for feature tags
    const customizedProductData = await Promise.all(productDetail.map(async product => {
      let featureTags = [];
      if (product.fcat_id) {
        const resultArray = product.fcat_id.split(',').map(Number);
        featureTags = await knex('feature_categories')
          .whereIn('id', resultArray)
          .where('status', 1)
          .where('is_deleted', 0)
          .select('id', knex.raw(`? || image as image`, [baseurl]));
      }

      const price = dealMap[product.varient_id] || product.price;
      const subscriptionPrice = parseFloat((product.mrp - (product.mrp * product.percentage) / 100).toFixed(2));

      let total_cart_qty = 0;
      let total_subcart_qty = 0;
      const features = await knex('product_features')
        .select('tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features.	feature_value_id')
        .where('product_id', product.product_id);

      let app = await knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id', 'product_varient.description', 'store_products.price', 'store_products.mrp',
          'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity', knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
        .where('store_products.store_id', appDetails.store_id)
        .where('product_varient.product_id', product.product_id)
        .whereNotNull('store_products.price')
        .where('product_varient.approved', 1)
        .where('product_varient.is_delete', 0);

      //prod.varient = app;
      const customizedVarientData = [];
      for (let i = 0; i < app.length; i++) {
        // prod.varient.dummy = 5678;
        const ProductList = app[i];
        const currentDate = new Date();
        // const deal = await knex('deal_product')
        // .where('varient_id', ProductList.varient_id)
        // .where('store_id', appDetails.store_id)
        // .where('deal_product.valid_from', '<=', currentDate)
        // .where('deal_product.valid_to', '>', currentDate)
        // .first();


        // if (deal) {
        //   vprice = deal.deal_price;
        // } else {
        const sp = await knex('store_products')
          .where('varient_id', ProductList.varient_id)
          .where('store_id', appDetails.store_id)
          .first();
        var vprice = sp.price;
        // }

        if (user_id) {
          // Wishlist check 
          var isFavourite = '';
          var notifyMe = '';
          var cartQty = 0;
          var subcartQty = 0;
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
            .where('store_id', appDetails.store_id)
            .first();
          cartQty = CartQtyList ? CartQtyList.qty : 0;

          const subCart1 = await knex('store_orders')
            .where('varient_id', ProductList.varient_id)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .where('subscription_flag', 1)
            .where('store_id', appDetails.store_id)
            .first();
          subcartQty = subCart1 ? subCart1.qty : 0;

          const cnotify_me = await knex('product_notify_me')
            .where('varient_id', ProductList.varient_id)
            .where('user_id', user_id)
            .first();

          notifyMe = (cnotify_me) ? 'true' : 'false';

        } else {
          notifyMe = 'false';
          isFavourite = 'false';
          cartQty = 0;
          subcartQty = 0;
        }
        const baseurl = process.env.BUNNY_NET_IMAGE;

        const images = await knex('product_images')
          .select(knex.raw(`CONCAT('${baseurl}', image) as image`))
          .where('product_id', product.product_id)
          .orderBy('type', 'DESC');
        if (images.length < 0) {
          const images = await knex('product')
            .select(knex.raw(`CONCAT('${baseurl}', product_image) as image`))
            .where('product_id', product.product_id);
        }
        const CartQtyList = await knex('store_orders')
          .select('product_feature_id') // 👈 get the column you want
          .where('varient_id', ProductList.varient_id)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('store_id', appDetails.store_id)
          .first();

        const productFeatureId = CartQtyList ? CartQtyList.product_feature_id : 0;

        total_cart_qty = total_cart_qty + cartQty;
        total_subcart_qty = total_subcart_qty + subcartQty;

        const firstImage = images.length > 0 ? images[0].image : '';

        const customizedVarient = {
          stock: ProductList.stock,
          varient_id: ProductList.varient_id,
          product_id: ProductList.product_id,
          product_name: ProductList.product_name,
          product_image: firstImage ? firstImage + "?width=200&height=200&quality=100" : '',
          thumbnail: firstImage,
          description: ProductList.description,
          price: vprice,
          mrp: ProductList.mrp,
          unit: ProductList.unit,
          quantity: ProductList.quantity,
          type: ProductList.type,
          discountper: ProductList.discountper,
          // discountper:0,
          notify_me: notifyMe,
          isFavourite: isFavourite,
          cart_qty: cartQty,
          total_cart_qty: cartQty,
          subcartQty: subcartQty,
          total_subcart_qty: subcartQty,
          product_feature_id: productFeatureId,
          country_icon: product.country_icon ? `${baseurl}${product.country_icon}` : null,
        };

        customizedVarientData.push(customizedVarient);
      }
      varients = customizedVarientData;

      return {
        total_quantity: product.total_quantity,
        p_id: product.p_id,
        varient_id: product.varient_id,
        stock: product.stock,
        store_id: product.store_id,
        price: parseFloat(price),
        mrp: parseFloat(product.mrp),
        min_ord_qty: product.min_ord_qty,
        max_ord_qty: product.max_ord_qty,
        buyingprice: product.buyingprice,
        product_code: product.product_code,
        partner_id: product.partner_id,
        product_id: product.product_id,
        quantity: product.quantity,
        unit: product.prdunit,
        description: product.description,
        varient_image: product.varient_image,
        ean: product.ean,
        approved: product.approved,
        added_by: product.added_by,
        cat_id: product.cat_id,
        brand_id: product.brand_id,
        product_name: product.product_name,
        product_image: `${product.product_image}?width=200&height=200&quality=100`,
        type: product.type,
        hide: product.hide,
        percentage: product.percentage,
        isSubscription: subscriptionMap[product.varient_id] ? 'true' : 'false',
        subscription_price: subscriptionPrice,
        availability: product.availability,
        discountper: product.discountper || 0,
        country_icon: product.country_icon ? `${baseurl}${product.country_icon}` : null,
        avgrating: 0,
        notify_me: notifyMeMap[product.varient_id] ? 'true' : 'false',
        isFavourite: wishListMap[product.varient_id] ? 'true' : 'false',
        cart_qty: cartMap[product.varient_id] || 0,
        total_cart_qty: total_cart_qty,
        countrating: 0,
        feature_tags: featureTags,
        is_customized: product.is_customized,
        features: features,
        varients: varients,
        total_subcart_qty: total_subcart_qty,
      };
    }));

    return customizedProductData;
  } catch (error) {
    // console.error("Error fetching top-selling products:", error);
    throw error;
  }
};

const getsubscriptonDetails = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;


  if (appDetatils.user_id != "null") {
    user_id = appDetatils.user_id
  } else {
    user_id = appDetatils.device_id
  }
  const subdetails = await knex('users')
    .where('id', user_id)
    .where('noti_popup', 1);

  if (subdetails.length > 0) {
    return subscripton_details = {
      'is_subscription': 1,
      'message': ''
    };
  } else {
    return subscripton_details = {
      'is_subscription': 1,
      'message': ''
    };
  }

};

const getadditionalCategory = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;
  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Fetch all categories
  const results = await knex('additional_category').whereNot('id', 12).where('status', 1).orderBy('main_order', 'asc');

  if (results.length === 0) {
    return [];
  }

  // Prepare a list to return
  const customizedData = [];

  // Fetch all wishlist, cart, notify me, and subscription data for user in one go
  const [wishList, cartItems, notifyMeList, subscriptionProducts] = await Promise.all([
    knex('wishlist').where('user_id', user_id),
    knex('store_orders').where('store_approval', user_id).where('store_id', store_id).where('order_cart_id', 'incart').whereNull('subscription_flag'),
    knex('product_notify_me').where('user_id', user_id),
    knex('store_orders').select('varient_id').where('store_approval', user_id).where('subscription_flag', 1)
  ]);

  // Process each category
  for (const item of results) {
    const categoryProductIds = item.product_id.split(',');

    // Fetch product details for current category
    const productDetail = await knex('product')
      .select(
        'store_products.store_id',
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
        knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'),
        'product.is_customized',
      )
      .from('product')
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
      .leftJoin('add_catproduct_order', 'add_catproduct_order.product_id', '=', 'product.product_id')
      .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
      .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .whereIn('product.product_id', categoryProductIds)
      .andWhere('product.hide', '=', 0)
      .andWhere('product.is_delete', '=', 0)
      .where('store_products.stock', '>', 0)
      .where(builder => {
        builder
          .where('product.is_offer_product', 0)
          .whereNull('product.offer_date')
          .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE')
      })
      .orderBy('add_catproduct_order.orders', 'asc');

    // Deduplicate products
    const productDetails = productDetail.filter((product, index, self) =>
      index === self.findIndex((p) => p.product_id === product.product_id)
    ).slice(0, 6);

    // Process product details and add additional info (wishlist, cart qty, subscription, etc.)
    //const customizedProductData = productDetails.map(product => {
    const customizedProductData = await Promise.all(productDetails.map(async product => {
      let featureTags = [];
      if (product.fcat_id) {
        const resultArray = product.fcat_id.split(',').map(Number);
        featureTags = await knex('feature_categories')
          .whereIn('id', resultArray)
          .where('status', 1)
          .where('is_deleted', 0)
          .select('id', knex.raw(`? || image as image`, [baseurl]));
      }
      const isFavourite = wishList.some(w => w.varient_id === product.varient_id) ? 'true' : 'false';
      const cartItem = cartItems.find(c => c.varient_id === product.varient_id);
      const cartQty = cartItem ? cartItem.qty : 0;
      const notifyMe = notifyMeList.some(n => n.varient_id === product.varient_id) ? 'true' : 'false';
      const isSubscription = subscriptionProducts.some(s => s.varient_id === product.varient_id) ? 'true' : 'false'; // Check if it's in subscription

      const sub_price = (product.mrp * product.percentage) / 100;
      const finalsubprice = product.mrp - sub_price;
      const subscription_price = parseFloat(finalsubprice.toFixed(2));
      if (Number.isInteger(product.price)) {
        var priceval = product.price + '.001'
      } else {
        var priceval = product.price
      }
      if (Number.isInteger(product.mrp)) {
        var mrpval = product.mrp + '.001'
      } else {
        var mrpval = product.mrp
      }

      let total_cart_qty = 0;
      let total_subcart_qty = 0;
      const features = await knex('product_features')
        .select('tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features. feature_value_id')
        .where('product_id', product.product_id);

      let app = await knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id', 'product_varient.description', 'store_products.price', 'store_products.mrp',
          'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity', knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
        .where('store_products.store_id', appDetatils.store_id)
        .where('product_varient.product_id', product.product_id)
        .whereNotNull('store_products.price')
        .where('product_varient.approved', 1)

      //prod.varient = app;
      const customizedVarientData = [];
      for (let i = 0; i < app.length; i++) {
        // prod.varient.dummy = 5678;
        const ProductList = app[i];
        const currentDate = new Date();
        // const deal = await knex('deal_product')
        // .where('varient_id', ProductList.varient_id)
        // .where('store_id', appDetatils.store_id)
        // .where('deal_product.valid_from', '<=', currentDate)
        // .where('deal_product.valid_to', '>', currentDate)
        // .first();


        // if (deal) {
        //   vprice = deal.deal_price;
        // } else {
        const sp = await knex('store_products')
          .where('varient_id', ProductList.varient_id)
          .where('store_id', appDetatils.store_id)
          .first();
        var vprice = sp.price;
        // }

        if (user_id) {
          // Wishlist check 
          var isFavourite1 = '';
          var notifyMe1 = '';
          var cartQty1 = 0;
          var subcartQty1 = 0;
          const wishList = await knex('wishlist')
            .select('*')
            .where('varient_id', ProductList.varient_id)
            .where('user_id', user_id);

          isFavourite1 = wishList.length > 0 ? 'true' : 'false';

          // cart qty check 
          const CartQtyList = await knex('store_orders')
            .where('varient_id', ProductList.varient_id)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .whereNull('subscription_flag')
            .where('store_id', appDetatils.store_id)
            .first();
          cartQty1 = CartQtyList ? CartQtyList.qty : 0;

          // Subscription cart qty
          const subCart = await knex('store_orders')
            .where('varient_id', ProductList.varient_id)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .where('subscription_flag', 1) // subscription order
            .where('store_id', appDetatils.store_id)
            .first();
          subcartQty1 = subCart ? subCart.qty : 0;

          const cnotify_me = await knex('product_notify_me')
            .where('varient_id', ProductList.varient_id)
            .where('user_id', user_id)
            .first();

          notifyMe1 = (cnotify_me) ? 'true' : 'false';

        } else {
          notifyMe1 = 'false';
          isFavourite1 = 'false';
          cartQty1 = 0;
          subcartQty1 = 0;
        }
        const baseurl = process.env.BUNNY_NET_IMAGE;

        const images = await knex('product_images')
          .select(knex.raw(`CONCAT('${baseurl}', image) as image`))
          .where('product_id', product.product_id)
          .orderBy('type', 'DESC');
        if (images.length < 0) {
          const images = await knex('product')
            .select(knex.raw(`CONCAT('${baseurl}', product_image) as image`))
            .where('product_id', product.product_id);
        }
        const CartQtyList = await knex('store_orders')
          .select('product_feature_id') // 👈 get the column you want
          .where('varient_id', ProductList.varient_id)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('store_id', appDetatils.store_id)
          .first();

        const productFeatureId = CartQtyList ? CartQtyList.product_feature_id : 0;

        total_cart_qty = total_cart_qty + cartQty1;
        total_subcart_qty = total_subcart_qty + subcartQty1;

        const customizedVarient = {
          stock: ProductList.stock,
          varient_id: ProductList.varient_id,
          product_id: ProductList.product_id,
          product_name: ProductList.product_name,
          product_image: images.length > 0 ? images[0].image + "?width=200&height=200&quality=100" : '',
          thumbnail: images.length > 0 ? images[0].image : '',
          description: ProductList.description,
          price: vprice,
          mrp: ProductList.mrp,
          unit: ProductList.unit,
          quantity: ProductList.quantity,
          type: ProductList.type,
          discountper: ProductList.discountper,
          // discountper:0,
          notify_me: notifyMe1,
          isFavourite: isFavourite1,
          cart_qty: cartQty1,
          total_cart_qty: cartQty1,
          subcartQty: subcartQty1,
          total_subcart_qty: subcartQty1,
          product_feature_id: productFeatureId,
          country_icon: product.country_icon ? baseurl + product.country_icon : null,
        };

        customizedVarientData.push(customizedVarient);
      }
      varients = customizedVarientData;

      return {
        store_id: product.store_id,
        stock: product.stock,
        varient_id: product.varient_id,
        product_id: product.product_id,
        product_name: product.product_name,
        product_image: baseurl + product.product_image + "?width=200&height=200&quality=100",
        thumbnail: product.thumbnail,
        price: parseFloat(priceval),
        mrp: parseFloat(mrpval),
        unit: product.unit,
        quantity: product.quantity,
        type: product.type,
        discountper: product.discountper || 0,
        country_icon: product.country_icon ? baseurl + product.country_icon : null,
        cart_qty: cartQty,
        total_cart_qty: total_cart_qty,
        avgrating: 0, // Placeholder for ratings
        notify_me: notifyMe,
        isFavourite: isFavourite,
        percentage: product.percentage,
        isSubscription: isSubscription,
        subscription_price: subscription_price,
        availability: product.availability,
        feature_tags: featureTags,
        is_customized: product.is_customized,
        features: features,
        varients: varients,
        total_subcart_qty: total_subcart_qty,
      };
    }));

    // Add category with its specific product details
    customizedData.push({
      id: item.id,
      title: item.title,
      sub_title: item.sub_title,
      color1: item.color1,
      color2: item.color2,
      product_details: customizedProductData
    });
  }

  return customizedData;
};

//additionalCategory META API MODEL FUNCTION 4//
const getAdditionalCategoryMeta = async (appDetatils) => {
  const { store_id, byname } = appDetatils;

  var bynames = (byname.toLowerCase() == 'fresh food') ? "Fresh Picks" : byname;
  var bynames = (byname.toLowerCase() != 'fresh food' && byname.toLowerCase() == '') ? "DIWALI" : bynames;

  return await knex('additional_category')
    .select('id', 'title', 'meta_title', 'meta_description')
    .where('status', 1)
    .where('title', 'like', `%${bynames}%`)
    .first();
};
//FUNCTION 4 ENDS//

//occasionalCategory META API MODEL FUNCTION 3//
const getOccasionalCategoryMeta = async (appDetatils) => {
  const { store_id, byname } = appDetatils;

  var bynames = byname.toLowerCase();

  return await knex('occasional_category')
    .select('id', 'title', 'meta_title', 'meta_description')
    .where('status', 1)
    .where('title', 'like', `%${bynames}%`)
    .first();
};
//FUNCTION 3 ENDS//

const getcategoryList = async (appDetatils) => {
  const startTime = Date.now();
  const { store_id } = appDetatils;
  const storeId = store_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Step 1: Parallel aggregates first, then fetch parents (avoid expensive DISTINCT discovery query)
  const step1Start = Date.now();

  // 1A) Run aggregates WITHOUT pre-filtering parentIds (this replaces the slow DISTINCT discovery query)
  const step1aStart = Date.now();
  const [stfromAgg, subcatCountAgg] = await Promise.all([
    // MIN price per parent (stfrom)
    knex('categories as child')
      .join('product', function () {
        this.on('child.cat_id', '=', 'product.cat_id')
          .orOnExists(function () {
            this.select('*')
              .from('product_cat')
              .whereRaw('product_cat.product_id = product.product_id')
              .andWhereRaw('product_cat.cat_id = child.cat_id');
          });
      })
      .join('product_varient', 'product.product_id', 'product_varient.product_id')
      .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .select('child.parent as cat_id')
      .min({ stfrom: 'store_products.price' })
      .where('child.level', 1)
      .where('child.is_delete', 0)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('product.approved', 1)
      .where('store_products.stock', '>', 0)
      .whereNotNull('store_products.price')
      .where('store_products.store_id', storeId)
      .groupBy('child.parent'),
    // Count subcategories per parent (subcat_count)
    knex('categories as child')
      .join('product', function () {
        this.on('child.cat_id', '=', 'product.cat_id')
          .orOnExists(function () {
            this.select('*')
              .from('product_cat')
              .whereRaw('product_cat.product_id = product.product_id')
              .andWhereRaw('product_cat.cat_id = child.cat_id');
          });
      })
      .join('product_varient', 'product.product_id', 'product_varient.product_id')
      .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .select('child.parent as cat_id')
      .countDistinct({ subcat_count: 'child.cat_id' })
      .where('child.level', 1)
      .where('child.is_delete', 0)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('product.approved', 1)
      .where('store_products.stock', '>', 0)
      .whereNotNull('store_products.price')
      .where('store_products.store_id', storeId)
      .groupBy('child.parent')
  ]);
  const step1aTime = Date.now() - step1aStart;

  // Parent ids come from the aggregates (parents that actually have store products)
  const parentIds = new Array(subcatCountAgg.length);
  for (let i = 0; i < subcatCountAgg.length; i++) parentIds[i] = subcatCountAgg[i].cat_id;

  if (parentIds.length === 0) {
    const step1Time = Date.now() - step1Start;
    // console.log(`📊 Category Step 1 completed in ${step1Time}ms (no categories)`);
    return [];
  }

  // 1B) Fetch parent category rows (small result set)
  const step1bStart = Date.now();
  const parentCategories = await knex('categories')
    .select('order_list', 'title', 'cat_id', 'image', 'description')
    .whereIn('cat_id', parentIds)
    .where('level', 0)
    .where('is_delete', 0)
    .orderBy('cat_id', 'asc');
  const step1bTime = Date.now() - step1bStart;

  // Build maps for fast merge
  const stfromMap = new Map();
  for (let i = 0; i < stfromAgg.length; i++) {
    const row = stfromAgg[i];
    stfromMap.set(row.cat_id, row.stfrom);
  }
  const subcatCountMap = new Map();
  for (let i = 0; i < subcatCountAgg.length; i++) {
    const row = subcatCountAgg[i];
    // pg may return count as string
    subcatCountMap.set(row.cat_id, Number(row.subcat_count) || 0);
  }

  const uniqueCats = new Array(parentCategories.length);
  for (let i = 0; i < parentCategories.length; i++) {
    const c = parentCategories[i];
    uniqueCats[i] = {
      order_list: c.order_list,
      title: c.title,
      cat_id: c.cat_id,
      image: c.image,
      store_id: storeId,
      description: c.description,
      stfrom: stfromMap.get(c.cat_id) ?? null,
      subcat_count: subcatCountMap.get(c.cat_id) ?? 0
    };
  }

  const step1Time = Date.now() - step1Start;
  // console.log(
  //   `📊 Category Step 1 completed in ${step1Time}ms (aggs: ${step1aTime}ms, parents: ${step1bTime}ms) - Found: ${uniqueCats.length} categories`
  // );

  // Step 2 & 3: Run batch queries in PARALLEL for maximum speed
  const batchStart = Date.now();
  const [allSubCategories, allFirstSubcats] = await Promise.all([
    // Batch fetch all subcategories
    knex('categories')
      .join('product', function () {
        this.on('categories.cat_id', '=', 'product.cat_id')
          .orOnExists(function () {
            this.select('*')
              .from('product_cat')
              .whereRaw('product_cat.product_id = product.product_id')
              .andWhereRaw('product_cat.cat_id = categories.cat_id');
          });
      })
      .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
      .join('store_products', 'product_varient.varient_id', '=', 'store_products.varient_id')
      .select(
        'categories.title',
        'categories.cat_id',
        'categories.image',
        'categories.parent',
        'store_products.store_id',
        'categories.description',
        knex.raw('MIN(store_products.price) as stfrom')
      )
      .groupBy(
        'categories.title',
        'categories.cat_id',
        'categories.image',
        'categories.parent',
        'store_products.store_id',
        'categories.description'
      )
      .whereIn('categories.parent', parentIds)
      .where('categories.level', 1)
      .where('categories.is_delete', 0)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('product.approved', 1)
      .where('store_products.stock', '>', 0)
      .whereNotNull('store_products.price')
      .where('store_products.store_id', storeId)
      .orderBy('categories.order', 'asc'),

    // Batch fetch all first subcategories
    knex('categories')
      .select(
        'categories.title',
        'categories.cat_id',
        'categories.parent'
      )
      .join('product', function () {
        this.on('categories.cat_id', '=', 'product.cat_id')
          .orOnExists(function () {
            this.select('*')
              .from('product_cat')
              .whereRaw('product_cat.product_id = product.product_id')
              .andWhereRaw('product_cat.cat_id = categories.cat_id');
          });
      })
      .join('product_varient', 'product.product_id', 'product_varient.product_id')
      .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .where('store_products.store_id', storeId)
      .whereIn('categories.parent', parentIds)
      .where('categories.status', 1)
      .where('product.hide', 0)
      .where('product.is_delete', 0)
      .where('product.approved', 1)
      .where('store_products.stock', '>', 0)
      .whereNotNull('store_products.price')
      .whereRaw(`(
      product.is_offer_product = 0 
      OR product.offer_date IS NULL 
      OR product.offer_date::date != CURRENT_DATE
    )`)
      .where('categories.is_delete', 0)
      .groupBy('categories.title', 'categories.cat_id', 'categories.parent')
      .orderBy('categories.parent', 'asc')
      .orderBy('categories.order', 'asc')
  ]);
  const batchTime = Date.now() - batchStart;
  // console.log(`📊 Category Steps 2 & 3 (parallel batch) completed in ${batchTime}ms - Subcategories: ${allSubCategories.length}, First subcategories: ${allFirstSubcats.length}`);
  const mapStart = Date.now();
  // Group subcategories by parent_id (remove duplicates)
  const subCategoriesMap = new Map();
  const seenSubcats = new Set();
  for (let i = 0; i < allSubCategories.length; i++) {
    const sub = allSubCategories[i];
    const key = `${sub.parent}_${sub.cat_id}`;
    if (!seenSubcats.has(key)) {
      seenSubcats.add(key);
      if (!subCategoriesMap.has(sub.parent)) {
        subCategoriesMap.set(sub.parent, []);
      }
      subCategoriesMap.get(sub.parent).push(sub);
    }
  }

  // Map first subcategory by parent_id (get first one per parent)
  const firstSubcatMap = new Map();
  for (let i = 0; i < allFirstSubcats.length; i++) {
    const sub = allFirstSubcats[i];
    if (!firstSubcatMap.has(sub.parent)) {
      firstSubcatMap.set(sub.parent, sub);
    }
  }
  const mapTime = Date.now() - mapStart;
  // console.log(`📊 Category map building completed in ${mapTime}ms`);

  // Step 5: Process categories using pre-fetched maps (no async queries)
  const processStart = Date.now();
  const customizedCategoryData = [];
  for (let j = 0; j < uniqueCats.length; j++) {
    const cat = uniqueCats[j];

    // ✅ Use pre-fetched data from maps (no database query)
    const subCategorys = subCategoriesMap.get(cat.cat_id) || [];
    const firstSubcat = firstSubcatMap.get(cat.cat_id) || null;

    const customizedcategory = {
      order_list: cat.order_list,
      title: cat.title,
      cat_id: cat.cat_id,
      image: baseurl + (cat.image || ''),
      store_id: cat.store_id,
      description: cat.description,
      stfrom: cat.stfrom,
      subcat_count: cat.subcat_count,
      subcategory: subCategorys,
      subcategory_title: firstSubcat?.title || null,
      subcategory_id: firstSubcat?.cat_id || null
    };
    customizedCategoryData.push(customizedcategory);
  }
  const processTime = Date.now() - processStart;
  // console.log(`📊 Category processing completed in ${processTime}ms`);

  const totalTime = Date.now() - startTime;
  // console.log(`✅ Category API completed in ${totalTime}ms - Categories: ${customizedCategoryData.length}`);

  return customizedCategoryData;
};

const getsubcategoryList = async (appDetatils) => {
  // Removed MySQL-specific SQL mode setting - not needed for PostgreSQL

  const startTime = Date.now();
  const storeId = appDetatils.store_id;
  const catId = appDetatils.cat_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;

  // Fetch categories with pagination
  const page = appDetatils.page || 1;
  const perPage = appDetatils.perpage || 10;
  const offset = (page - 1) * perPage;

  // Optimize: Use two-step approach for better performance with PostgreSQL
  // Step 1: Get distinct category IDs that have products in the store (fast query)
  const step1Start = Date.now();
  const categoryIdsWithProducts = await knex('categories')
    .distinct('categories.cat_id')
    .leftJoin('product', 'categories.cat_id', 'product.cat_id')
    .leftJoin('product_varient', 'product.product_id', 'product_varient.product_id')
    .leftJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .where('store_products.store_id', storeId)
    .where('categories.parent', catId)
    .where('categories.status', 1)
    .where('categories.is_delete', 0)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('store_products.is_deleted', 0)
    .where('store_products.stock', '>', 0)
    .whereRaw(`(
      product.is_offer_product = 0 
      OR product.offer_date IS NULL 
      OR product.offer_date::date != CURRENT_DATE
    )`)
    .pluck('categories.cat_id');
  const step1Time = Date.now() - step1Start;
  // console.log(`📊 Subcategory Step 1 (get category IDs) completed in ${step1Time}ms - Found: ${categoryIdsWithProducts.length} categories`);

  // Step 1b: Find additional subcategory IDs via product_cat table
  const additionalCatIds = await knex('product_cat')
    .distinct('product_cat.cat_id')
    .join('categories', 'product_cat.cat_id', 'categories.cat_id')
    .join('product', 'product_cat.product_id', 'product.product_id')
    .join('product_varient', 'product_cat.product_id', 'product_varient.product_id')
    .join('store_products', 'product_varient.varient_id', 'store_products.varient_id')
    .where('categories.parent', catId)
    .where('categories.status', 1)
    .where('categories.is_delete', 0)
    .where('store_products.store_id', storeId)
    .where('product.hide', 0)
    .where('product.is_delete', 0)
    .where('store_products.is_deleted', 0)
    .where('store_products.stock', '>', 0)
    .whereRaw(`(
      product.is_offer_product = 0 
      OR product.offer_date IS NULL 
      OR product.offer_date::date != CURRENT_DATE
    )`)
    .pluck('product_cat.cat_id');

  // Merge both sets of subcategory IDs and deduplicate
  const allCategoryIds = [...new Set([...categoryIdsWithProducts, ...additionalCatIds])];

  if (allCategoryIds.length === 0) {
    throw new Error('Products not found');
  }

  // Step 2: Fetch category details efficiently (very fast query with WHERE IN)
  const step2Start = Date.now();
  const cats = await knex('categories')
    .select(
      'categories.status',
      'categories.order_list',
      'categories.order',
      'categories.title',
      'categories.cat_id',
      knex.raw(`(CASE WHEN categories.image IS NOT NULL AND categories.image != '' THEN '${baseurl}' || categories.image ELSE NULL END) as image`),
      knex.raw(`(CASE WHEN categories.cat_appimage IS NOT NULL AND categories.cat_appimage != '' THEN '${baseurl}' || categories.cat_appimage ELSE NULL END) as cat_bannerimage`),
      knex.raw(`(CASE WHEN categories.cat_bannerimage IS NOT NULL AND categories.cat_bannerimage != '' THEN '${baseurl}' || categories.cat_bannerimage ELSE NULL END) as cat_webbannerimage`),
      'categories.description'
    )
    .whereIn('categories.cat_id', allCategoryIds)
    .orderBy('categories.order', 'asc')
    .limit(perPage)
    .offset(offset);
  const step2Time = Date.now() - step2Start;
  // console.log(`📊 Subcategory Step 2 (get category details) completed in ${step2Time}ms - Returned: ${cats.length} categories`);

  const totalTime = Date.now() - startTime;
  // console.log(`✅ Subcategory API completed in ${totalTime}ms`);

  return cats.map(cat => ({
    status: cat.status,
    order_list: cat.order_list,
    order: cat.order,
    title: cat.title,
    cat_id: cat.cat_id,
    image: cat.image,
    banner: cat.cat_bannerimage,
    web_banner: cat.cat_webbannerimage,
    description: cat.description,
  }));
};

//subcategoryDetail META API MODEL FUNCTION 2//
const getSubcategoryDetail = async (appDetatils) => {
  //await knex.raw('SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,"ONLY_FULL_GROUP_BY",""))');

  const storeId = appDetatils.store_id;
  const catId = appDetatils.subcat_id;

  const baseurl = process.env.BUNNY_NET_IMAGE;

  const cat = await knex('categories')
    .select(
      'categories.status',
      'categories.order',
      'categories.title',
      'categories.cat_id',
      'categories.meta_title',
      'categories.meta_description',
      knex.raw(`CONCAT('${baseurl}', categories.image) as image`),
      knex.raw(`CONCAT('${baseurl}', categories.cat_appimage) as cat_bannerimage`),
      knex.raw(`CONCAT('${baseurl}', categories.cat_bannerimage) as cat_webbannerimage`),
      'categories.description'
    )
    .where('categories.cat_id', catId)
    .where('categories.is_delete', 0)
    .first();
  if (!cat) {
    throw new Error('Subcategory not found');
  }

  return cat;
};
//FUNCTION 2 ENDS//


const getoccasionalCategory = async (appDetatils) => {
  const { store_id, is_subscription } = appDetatils;
  const user_id = appDetatils.user_id !== "null" ? appDetatils.user_id : appDetatils.device_id;
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const currentDate = new Date().toISOString().split('T')[0];

  const todayDay = moment().format('ddd').toLowerCase();
  // Fetch all categories
  //   const results = await knex('occasional_category')
  //   .orderBy('main_order', 'asc')
  //   .where('hide', 0)
  //   .where('from_date', '<=', currentDate)
  //   .andWhere('to_date', '>=', currentDate);

  const results = await knex('occasional_category')
    .orderBy('main_order', 'asc')
    .where('hide', 1)
    .where('from_date', '<=', currentDate)
    .andWhere('to_date', '>=', currentDate)
    .andWhere((builder) => {
      builder.whereRaw(`? = ANY(string_to_array(available_days, ','))`, [todayDay])
        .orWhereRaw(`'all' = ANY(string_to_array(available_days, ','))`);
    });

  if (results.length === 0) {
    return [];
  }
  // Prepare a list to return
  const customizedData = [];
  // Fetch all wishlist, cart, notify me, and subscription data for user in one go
  const [wishList, cartItems, notifyMeList, subscriptionProducts] = await Promise.all([
    knex('wishlist').where('user_id', user_id),
    knex('store_orders').where('store_approval', user_id).where('store_id', store_id).where('order_cart_id', 'incart').whereNull('subscription_flag'),
    knex('product_notify_me').where('user_id', user_id),
    knex('store_orders').select('varient_id').where('store_approval', user_id).where('subscription_flag', 1)
  ]);
  // Process each category
  for (const item of results) {
    //  const categoryProductIds = item.product_id.split(',');


    let categoryProductIds = [];

    if (item.product_id && item.subcat_id && item.cat_id) {
      productIds = item.product_id.split(',');
      categoryProductIds = productIds.map(Number);
    }

    if ((item.product_id == '' || item.product_id == null) && item.subcat_id && item.cat_id) {
      const subcategories = item.subcat_id.split(',');
      const productIds = await knex('product')
        .from('product')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
        .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
        .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
        .whereIn('product.cat_id', subcategories)
        .andWhere('product.hide', '=', 0)
        .andWhere('product.is_delete', '=', 0)
        .where('product.is_zap', true)
        .pluck('product.product_id');

      categoryProductIds = productIds;
    }

    if ((item.product_id == '' || item.product_id == null) && (item.subcat_id == '' || item.subcat_id == null) && item.cat_id) {
      const parent_cat_id = item.cat_id.split(',');
      const subcategories = await knex('categories')
        .whereIn('parent', parent_cat_id)
        .where('status', 1).pluck('cat_id');
      // console.log('subcategories',subcategories);

      const productIds = await knex('product')
        .from('product')
        .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
        .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
        .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
        .whereIn('product.cat_id', subcategories)
        .andWhere('product.hide', '=', 0)
        .andWhere('product.is_delete', '=', 0)
        .where('product.is_zap', true)
        .pluck('product.product_id');

      categoryProductIds = productIds;
    }

    const catid = item.cat_id;
    const subcatid = item.subcatid;
    // Fetch product details for current category
    const productDetail = await knex('product')
      .select(
        'store_products.store_id',
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
        'product.is_customized',
        knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper')
      )
      .from('product')
      .leftJoin('tbl_country', knex.raw('tbl_country.id::text = product.country_id'))
      .innerJoin('product_varient', 'product.product_id', 'product_varient.product_id')
      .innerJoin('store_products', 'product_varient.varient_id', 'store_products.varient_id')
      .leftJoin('add_occproduct_order', 'add_occproduct_order.product_id', '=', 'product.product_id')
      //   .where('store_products.stock','>',0)
      .whereIn('product.product_id', categoryProductIds)
      .andWhere('product.hide', '=', 0)
      .andWhere('product.is_delete', '=', 0)
      .where('product.is_zap', true)
      .where('store_products.stock', '>', 0)
      .where(builder => {
        builder
          .where('product.is_offer_product', 0)
          .whereNull('product.offer_date')
          .orWhereRaw('DATE(product.offer_date) != CURRENT_DATE')
      })
      //   .modify(function(queryBuilder) {
      //         if (subcatid) {
      //           queryBuilder.andWhere('product.cat_id', subcatid);
      //         }
      //       })
      .orderBy('add_occproduct_order.orders', 'asc');
    // Deduplicate products
    const productDetails = productDetail.filter((product, index, self) =>
      index === self.findIndex((p) => p.product_id === product.product_id)
    ).slice(0, 6);
    // Process product details and add additional info (wishlist, cart qty, subscription, etc.)
    //const customizedProductData = productDetails.map(product => {
    const customizedProductData = await Promise.all(productDetails.map(async product => {
      let featureTags = [];
      if (product.fcat_id) {
        const resultArray = product.fcat_id.split(',').map(Number);
        featureTags = await knex('feature_categories')
          .whereIn('id', resultArray)
          .where('status', 1)
          .where('is_deleted', 0)
          .select('id', knex.raw(`? || image as image`, [baseurl]));
      }
      const isFavourite = wishList.some(w => w.varient_id === product.varient_id) ? 'true' : 'false';
      const cartItem = cartItems.find(c => c.varient_id === product.varient_id);
      const cartQty = cartItem ? cartItem.qty : 0;
      const notifyMe = notifyMeList.some(n => n.varient_id === product.varient_id) ? 'true' : 'false';
      const isSubscription = subscriptionProducts.some(s => s.varient_id === product.varient_id) ? 'true' : 'false'; // Check if it's in subscription
      const sub_price = (product.mrp * product.percentage) / 100;
      const finalsubprice = product.mrp - sub_price;
      const subscription_price = parseFloat(finalsubprice.toFixed(2));

      let total_cart_qty = 0;
      let total_subcart_qty = 0;

      const features = await knex('product_features')
        .select('tbl_feature_value_master.id', 'tbl_feature_value_master.feature_value')
        .join('tbl_feature_value_master', 'tbl_feature_value_master.id', '=', 'product_features. feature_value_id')
        .where('product_id', product.product_id);

      let app = await knex('store_products')
        .join('product_varient', 'store_products.varient_id', '=', 'product_varient.varient_id')
        .select('store_products.store_id', 'store_products.stock', 'product_varient.varient_id', 'product_varient.description', 'store_products.price', 'store_products.mrp',
          'product_varient.varient_image', 'product_varient.unit', 'product_varient.quantity', knex.raw('100-((store_products.price*100)/store_products.mrp) as discountper'))
        .where('store_products.store_id', appDetatils.store_id)
        .where('product_varient.product_id', product.product_id)
        .whereNotNull('store_products.price')
        .where('product_varient.approved', 1)
        .where('product_varient.is_delete', 0);

      //prod.varient = app;
      const customizedVarientData = [];
      for (let i = 0; i < app.length; i++) {
        // prod.varient.dummy = 5678;
        const ProductList = app[i];
        const currentDate = new Date();
        const deal = await knex('deal_product')
          .where('varient_id', ProductList.varient_id)
          .where('store_id', appDetatils.store_id)
          .where('deal_product.valid_from', '<=', currentDate)
          .where('deal_product.valid_to', '>', currentDate)
          .first();


        // if (deal) {
        //   vprice = deal.deal_price;
        // } else {
        const sp = await knex('store_products')
          .where('varient_id', ProductList.varient_id)
          .where('store_id', appDetatils.store_id)
          .first();
        var vprice = sp.price;
        // }

        if (user_id) {
          // Wishlist check 
          var isFavourite1 = '';
          var notifyMe1 = '';
          var cartQty1 = 0;
          var subcartQty1 = 0;
          const wishList = await knex('wishlist')
            .select('*')
            .where('varient_id', ProductList.varient_id)
            .where('user_id', user_id);

          isFavourite1 = wishList.length > 0 ? 'true' : 'false';

          // cart qty check 
          const CartQtyList = await knex('store_orders')
            .where('varient_id', ProductList.varient_id)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .whereNull('subscription_flag')
            .where('store_id', appDetatils.store_id)
            .first();
          cartQty1 = CartQtyList ? CartQtyList.qty : 0;

          // Subscription cart qty
          const subCart = await knex('store_orders')
            .where('varient_id', ProductList.varient_id)
            .where('store_approval', user_id)
            .where('order_cart_id', 'incart')
            .where('subscription_flag', 1) // subscription order
            .where('store_id', appDetatils.store_id)
            .first();
          subcartQty1 = subCart ? subCart.qty : 0;

          const cnotify_me = await knex('product_notify_me')
            .where('varient_id', ProductList.varient_id)
            .where('user_id', user_id)
            .first();

          notifyMe1 = (cnotify_me) ? 'true' : 'false';

        } else {
          notifyMe1 = 'false';
          isFavourite1 = 'false';
          cartQty1 = 0;
          subcartQty1 = 0;
        }
        const baseurl = process.env.BUNNY_NET_IMAGE;

        const images = await knex('product_images')
          .select(knex.raw(`CONCAT('${baseurl}', image) as image`))
          .where('product_id', product.product_id)
          .orderBy('type', 'DESC');
        if (images.length < 0) {
          const images = await knex('product')
            .select(knex.raw(`CONCAT('${baseurl}', product_image) as image`))
            .where('product_id', product.product_id);
        }
        const CartQtyList = await knex('store_orders')
          .select('product_feature_id') // 👈 get the column you want
          .where('varient_id', ProductList.varient_id)
          .where('store_approval', user_id)
          .where('order_cart_id', 'incart')
          .where('store_id', appDetatils.store_id)
          .first();

        const productFeatureId = CartQtyList ? CartQtyList.product_feature_id : 0;

        total_cart_qty = total_cart_qty + cartQty1;
        total_subcart_qty = total_subcart_qty + subcartQty1;

        const firstImage = images.length > 0 ? images[0].image : '';

        const customizedVarient = {
          stock: ProductList.stock,
          varient_id: ProductList.varient_id,
          product_id: ProductList.product_id,
          product_name: ProductList.product_name,
          product_image: firstImage ? firstImage + "?width=200&height=200&quality=100" : '',
          thumbnail: firstImage,
          description: ProductList.description,
          price: vprice,
          mrp: ProductList.mrp,
          unit: ProductList.unit,
          quantity: ProductList.quantity,
          type: ProductList.type,
          discountper: ProductList.discountper,
          // discountper:0,
          notify_me: notifyMe1,
          isFavourite: isFavourite1,
          cart_qty: cartQty1,
          total_cart_qty: cartQty1,
          subcartQty: subcartQty1,
          total_subcart_qty: subcartQty1,
          product_feature_id: productFeatureId,
          country_icon: product.country_icon ? baseurl + product.country_icon : null,
        };

        customizedVarientData.push(customizedVarient);
      }
      varients = customizedVarientData;

      return {
        store_id: product.store_id,
        stock: product.stock,
        varient_id: product.varient_id,
        product_id: product.product_id,
        product_name: product.product_name,
        product_image: baseurl + product.product_image + "?width=200&height=200&quality=100",
        thumbnail: product.thumbnail,
        price: product.price,
        mrp: product.mrp,
        unit: product.unit,
        quantity: product.quantity,
        type: product.type,
        discountper: product.discountper || 0,
        country_icon: product.country_icon ? baseurl + product.country_icon : null,
        cart_qty: cartQty,
        total_cart_qty: total_cart_qty,
        avgrating: 0, // Placeholder for ratings
        notify_me: notifyMe,
        isFavourite: isFavourite,
        percentage: product.percentage,
        isSubscription: isSubscription,
        subscription_price: subscription_price,
        availability: product.availability,
        feature_tags: featureTags,
        is_customized: product.is_customized,
        features: features,
        varients: varients,
        total_subcart_qty: total_subcart_qty,
      };
    }));
    // Add category with its specific product details
    customizedData.push({
      id: item.id,
      title: item.title.toUpperCase(),
      sub_title: item.sub_title,
      color1: item.color1,
      color2: item.color2,
      from_date: item.from_date,
      to_date: item.to_date,
      available_days: item.available_days,
      product_details: customizedProductData
    });
  }
  return customizedData;
};

const getactivesubord = async (appDetails) => {

  const baseurl = process.env.BUNNY_NET_IMAGE;
  const { user_id, store_id } = appDetails;

  if (!user_id || user_id === "null") {
    return [];
  }
  const today = new Date();

  // Fetch latest pending subscription carts (max 5), one entry per cart_id.
  // A single subscription cart can have multiple pending deliveries (multiple rows in subscription_order),
  // but for oneapi we only want the latest pending entry per cart (product) to avoid duplicates.
  const latestPendingSubs = await knex('subscription_order as so')
    .join('orders as o', 'o.cart_id', '=', 'so.cart_id')
    .where('o.cart_id', '!=', 'incart')
    .where('o.user_id', user_id)
    .where('o.store_id', store_id)
    .where('o.is_subscription', 1)
    .whereNotNull('o.payment_method')
    .where('so.delivery_date', '>', today)
    // Only show pending subscription items (case-insensitive safety)
    .whereRaw('LOWER(so.order_status) = ?', ['pending'])
    // Safety: never show completed/cancelled orders in the "active" list
    .whereRaw('LOWER(COALESCE(o.order_status, \'\')) NOT IN (?, ?)', ['completed', 'cancelled'])
    .groupBy('so.cart_id')
    .select('so.cart_id', knex.raw('MAX(so.id) as max_so_id'))
    .orderByRaw('MAX(so.delivery_date) DESC')
    .limit(5);

  const latestSubRowIds = latestPendingSubs.map(r => r.max_so_id).filter(Boolean);
  if (latestSubRowIds.length === 0) {
    return [];
  }

  const ongoing = await knex('subscription_order as so')
    .join('orders as o', 'o.cart_id', '=', 'so.cart_id')
    .select(
      'o.group_id',
      'o.cart_id',
      'so.subscription_id',
      knex.raw("TO_CHAR(so.delivery_date, 'YYYY-MM-DD') as delivery_date"),
      knex.raw("TO_CHAR(o.order_date, 'YYYY-MM-DD') as order_date"),
      'o.total_delivery',
      'so.order_status',
      'o.si_order',
      'o.si_sub_ref_no',
      'o.pastorecentrder'
    )
    .whereIn('so.id', latestSubRowIds)
    .orderBy('so.id', 'DESC');

  // Extract cart_ids for batch processing
  const cartIds = ongoing.map(o => o.cart_id);

  // Fetch subscription orders, order details, and delivery ratings in parallel
  const [subscriptionOrders, orderDetails, deliveryRatings, storeOrders] = await Promise.all([
    knex('subscription_order')
      .whereIn('cart_id', cartIds)
      .whereRaw('LOWER(order_status) = ?', ['pending'])
      .orderBy('id', 'DESC')
      .select('cart_id', 'store_order_id', 'subscription_id', 'delivery_date'),
    knex('orders')
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'total_products_mrp', 'pastorecentrder'),
    knex('delivery_rating')
      .where('user_id', user_id)
      .whereIn('cart_id', cartIds)
      .select('cart_id', 'description', 'rating'),
    knex('store_orders')
      .whereIn('order_cart_id', cartIds)
      .select(
        'order_cart_id',
        'product_name',
        'qty',
        knex.raw(`? || varient_image || ? as varient_image`, [baseurl, '?width=100&height=100&quality=100'])
      )

  ]);

  // Create a map for delivery ratings
  const deliveryRatingsMap = Object.fromEntries(
    deliveryRatings.map(rating => [rating.cart_id, { rating: rating.rating, description: rating.description }])
  );

  // Create a map for store orders
  const storeOrderMap = storeOrders.reduce((acc, order) => {
    if (!acc[order.order_cart_id]) {
      acc[order.order_cart_id] = [];
    }
    // De-dupe identical product rows (can happen when subscription has multiple deliveries)
    const item = {
      order_cart_id: order.order_cart_id,
      product_name: `${order.product_name} X ${order.qty}`,
      varient_image: order.varient_image
    };
    const existing = acc[order.order_cart_id];
    const key = `${item.product_name}||${item.varient_image}`;
    if (!existing._dedupeKeys) existing._dedupeKeys = new Set();
    if (!existing._dedupeKeys.has(key)) {
      existing._dedupeKeys.add(key);
      existing.push(item);
    }
    return acc;
  }, {});

  // Fetch all group-level totals in one query
  const groupTotals = await knex('orders')
    .whereIn('group_id', ongoing.map(o => o.group_id))
    .groupBy('group_id')
    .select('group_id', knex.raw('SUM(total_products_mrp) as total_amount'));

  const groupTotalMap = {};
  groupTotals.forEach(gt => {
    groupTotalMap[gt.group_id] = gt.total_amount;
  });

  // Fetch all product details in one query
  const allProductDetails = await knex('store_orders')
    .whereIn('order_cart_id', cartIds)
    .groupBy('order_cart_id')
    .select(
      'order_cart_id',
      knex.raw("STRING_AGG(product_name || ' X ' || qty::text, ', ') as product_details")
    );

  const productDetailsMap = {};
  allProductDetails.forEach(pd => {
    productDetailsMap[pd.order_cart_id] = pd.product_details;
  });

  // Process ongoing subscriptions
  const customizedProductData = ongoing.map((product) => {
    const orderDetail = orderDetails.find(o => o.cart_id === product.cart_id) || {};
    const ratingData = deliveryRatingsMap[product.cart_id] || { rating: null, description: null };

    // Determine order status
    const deliveryDate = new Date(product.delivery_date);
    let orderStatus = deliveryDate > today ? 'Pending' : 'Completed';
    if (product.order_status === 'Cancelled') orderStatus = 'Cancelled';

    // Only one product should be returned for this subscription cart
    const prodListRaw = storeOrderMap[product.cart_id] || [];
    const prodList = prodListRaw.filter(x => x && typeof x === 'object' && !('_dedupeKeys' in x)).slice(0, 1);

    const cartAmount = groupTotalMap[product.group_id] || 0;

    return {
      cart_id: product.cart_id,
      group_id: product.group_id,
      order_date: product.order_date,
      total_mrp: parseFloat(cartAmount).toFixed(2),
      subscription_id: product.subscription_id || '',
      order_status: orderStatus,
      si_order: product.si_order,
      si_sub_ref_no: product.si_sub_ref_no,
      drating: ratingData.rating,
      dreview: ratingData.description,
      productname: productDetailsMap[product.cart_id] || '',
      prodList: prodList
    };
  });

  return customizedProductData;
};

const getOffer = async (appDetatils) => {
  const { store_id } = appDetatils;
  const currentDate = new Date().toISOString().split('T')[0];
  const baseurl = process.env.BUNNY_NET_IMAGE;
  const offer = await knex('popup_banner')
    .select('banner_id', 'banner_name', 'terms_and_conditions', knex.raw(`CONCAT('${baseurl}', banner_image) as banner_image`))
    .where('store_id', store_id)
    .where('type', 'offers')
    .whereRaw('DATE(start_date) <= ?', [currentDate])
    .whereRaw('DATE(end_date) >= ?', [currentDate])
    .first();

  return offer;
};



module.exports = {
  getBanner,
  getTopCat,
  getWhatsNew,
  getdealProduct,
  getsecondBanner,
  getrecentSelling,
  gettopSelling,
  getsubscriptonDetails,
  getadditionalCategory,
  getcategoryList,
  getsubcategoryList,
  prodDetails,
  similarProds,
  getcatProduct,
  getorderList,
  getBrand,
  getBrandlist,
  specialOfferBanner,
  sneakyOfferBanner,
  popupBanner,
  getaboutData,
  gettermsData,
  getoccasionalCategory,
  getfeaturecat,
  getfetcatProd,
  getactivesubord,
  trailpackimagedata,
  getfeaturecategory,
  getOffer,
  getfeaturecategoryDetail,
  getSubcategoryDetail,
  getOccasionalCategoryMeta,
  getAdditionalCategoryMeta
};
