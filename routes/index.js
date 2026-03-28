// routes/users.js
const express = require('express');
const router = express.Router();
const userController = require('../controllers/userController');
const categoryController = require('../controllers/categoryController');
const orderController = require('../controllers/orderController');
const productlistController = require('../controllers/productlistController');
const wishlistController = require('../controllers/wishlistController');
const cartController = require('../controllers/cartController');
const searchController = require('../controllers/searchController');
const walletController = require('../controllers/walletController');
const addressController = require('../controllers/addressController');
const timeslotController = require('../controllers/timeslotController');
const sneakyController = require('../controllers/sneakyController');
const checkoutController = require('../controllers/checkoutController');
const notificationController = require('../controllers/notificationController');
const couponController = require('../controllers/couponController');
const paymentController = require('../controllers/paymentController');
const guestController = require('../controllers/guestController');
const cardController = require('../controllers/cardController');
const notifymeController = require('../controllers/notifymeController');
const trailpackController = require('../controllers/trailpackController');
const ratingController = require('../controllers/ratingController');


router.post('/testnodejsapp/api/users/', userController.createUser);
router.get('/testnodejsapp/api/users/', userController.getUsers);
router.put('/testnodejsapp/api/users/:id', userController.updateUser);
router.delete('/testnodejsapp/api/users/:id', userController.deleteUser);
//router.get('/testnodejsapp/api/users/city', userController.getCity);

//Login API
router.post('/testnodejsapp/api/login/', userController.login);
router.post('/testnodejsapp/api/verify_otp/', userController.verify_otp);
router.post('/testnodejsapp/api/resend_otp/', userController.resend_otp);
router.post('/testnodejsapp/api/register_details/', userController.register_details);
router.post('/testnodejsapp/api/user_deactivate/', userController.user_deactivate);
router.post('/testnodejsapp/api/user_bank_details/', userController.user_bank_details);
router.post('/testnodejsapp/api/send_otp/', userController.send_otp);
router.post('/testnodejsapp/api/verify_otp_update/', userController.verify_otp_update);
router.post('/testnodejsapp/api/verify_details/', userController.verify_details);
router.post('/testnodejsapp/api/fetch_otp_testing/', userController.fetch_otp_testing);
router.post('/testnodejsapp/api/randomize_user_phone/', userController.randomize_user_phone);



//user profile

router.post('/testnodejsapp/api/myprofile/', userController.myprofile);
router.post('/testnodejsapp/api/profile_edit/', userController.profile_edit);
router.post('/testnodejsapp/api/faqslist/', userController.faqslist);
router.post('/testnodejsapp/api/show_profile', userController.showprofile);
router.post('/testnodejsapp/api/generate_invoice', userController.generate_invoice);

//App Home page API
router.post('/testnodejsapp/api/oneapi/', categoryController.oneapi);
router.post('/testnodejsapp/api/oneapi1/', categoryController.oneapi1);
router.post('/testnodejsapp/api/oneapi2/', categoryController.oneapi2);
router.post('/testnodejsapp/api/oneapi3/', categoryController.oneapi3);
router.post('/testnodejsapp/api/oneapiadditionalcategory/', categoryController.oneapiadditionalcategory);
router.post('/testnodejsapp/api/app_info/', categoryController.appinfo);
router.post('/testnodejsapp/api/updateproductdetails/', categoryController.updateproductdetails);
router.post('/testnodejsapp/api/offer/', categoryController.getoffer);
router.post('/testnodejsapp/api/additional_category_meta/', categoryController.getAdditionalCategoryMeta);
router.post('/testnodejsapp/api/occasional_category_meta/', categoryController.getOccasionalCategoryMeta);
router.post('/testnodejsapp/api/cron/refresh-oneapi-global', categoryController.refreshOneApiGlobal);
router.get('/testnodejsapp/api/clear_all_cache/', categoryController.clearAllCache);

//Catee API
router.post('/testnodejsapp/api/catee/', categoryController.catee);
router.post('/testnodejsapp/api/subcatee/', categoryController.subcatee);
router.post('/testnodejsapp/api/cat_product/', categoryController.cat_product);
router.get('/testnodejsapp/api/brand_list/', categoryController.brand_list);
router.get('/testnodejsapp/api/appaboutus/', categoryController.aboutus);
router.get('/testnodejsapp/api/appterms/', categoryController.terms);
router.post('/testnodejsapp/api/featurecat_prod/', categoryController.featurecat_prod);
router.get('/testnodejsapp/api/feature_category/', categoryController.featurecategory);
router.post('/testnodejsapp/api/subcategory/', categoryController.subcategoryDetail);
router.post('/testnodejsapp/api/feature_category_detail/', categoryController.featurecategoryDetail);


//My Orders API
router.post('/testnodejsapp/api/my_orders/', orderController.my_orders);
router.post('/testnodejsapp/api/my_dailyorders/', orderController.my_dailyorders);
router.post('/testnodejsapp/api/repeat_orders/', orderController.repeat_orders);
router.post('/testnodejsapp/api/my_orders_subscription/', orderController.ongoing_sub);
router.post('/testnodejsapp/api/orders_subscription_product/', orderController.product_ongoing_sub);
router.post('/testnodejsapp/api/my_orders_sub/', orderController.my_orders_sub)
router.get('/testnodejsapp/api/cancelorderreason/', orderController.cancelorderreason)
router.post('/testnodejsapp/api/my_subscription_pause_order/', orderController.my_subscription_pause_order)
router.post('/testnodejsapp/api/my_subscription_resume_order/', orderController.my_subscription_resume_order)
router.post('/testnodejsapp/api/place_repeated_order/', orderController.place_repeated_order)
router.post('/testnodejsapp/api/can_orders/', orderController.can_orders)
router.post('/testnodejsapp/api/cancelledproductorder/', orderController.cancelledproductorder)
router.post('/testnodejsapp/api/cancelledquickorder/', orderController.cancelledquickorder)
router.post('/testnodejsapp/api/cancelledquickorderprod/', orderController.cancelledquickorderprod)
router.post('/testnodejsapp/api/quick_place_repeated_order/', orderController.quick_place_repeated_order)
router.post('/testnodejsapp/api/my_orders_subscription_list/', orderController.ongoing_sub_list);
router.post('/testnodejsapp/api/orderwiselist/', orderController.orderwiselist);
router.post('/testnodejsapp/api/merge_orders/', orderController.merge_orders);
router.post('/testnodejsapp/api/can_autorenewal/', orderController.can_autorenewal);
router.post('/testnodejsapp/api/active_orders/', orderController.active_orders);

//Orders Details API
router.post('/testnodejsapp/api/orders_details/', orderController.orders_details);
router.get('/testnodejsapp/api/total_deliveries/', orderController.total_deliveries)
router.post('/testnodejsapp/api/groupwise_order/', orderController.groupwise_order);

//Product Listing API
router.post('/testnodejsapp/api/product_det/', categoryController.product_det);
router.post('/testnodejsapp/api/top_selling/', productlistController.top_selling);
router.post('/testnodejsapp/api/whatsnew/', productlistController.whatsnew);
router.post('/testnodejsapp/api/recentselling/', productlistController.recentselling);
router.post('/testnodejsapp/api/dealproduct/', productlistController.dealproduct);
router.post('/testnodejsapp/api/additionalcat_search/', productlistController.additionalcat_search);
router.post('/testnodejsapp/api/occasionalcat_search/', productlistController.occasionalcat_search);



//wishlist
router.post('/testnodejsapp/api/show_wishlist/', wishlistController.show_wishlist);
router.post('/testnodejsapp/api/add_rem_wishlist/', wishlistController.add_rem_wishlist);

//wallet
router.post('/testnodejsapp/api/wallet_recharge_history/', walletController.walletrechargehistory);
router.post('/testnodejsapp/api/wallet_recharge/', walletController.walletrecharge);
router.post('/testnodejsapp/api/order_card_changes/', walletController.order_card_changes);
// wallet analytics – G1
router.post('/testnodejsapp/api/spent_by_wallet/', walletController.spent_by_wallet);
router.get('/testnodejsapp/api/update_wallet_expiry/', walletController.update_wallet_expiry);

//cart
router.post('/testnodejsapp/api/add_to_cart/', cartController.add_to_cart);
router.post('/testnodejsapp/api/show_cart/', cartController.show_cart);
router.post('/testnodejsapp/api/update_cart/', cartController.update_cart);

//subcart
router.post('/testnodejsapp/api/add_to_subcart/', cartController.add_to_subcart);
router.post('/testnodejsapp/api/showsub_cart/', cartController.showsub_cart);
router.post('/testnodejsapp/api/show_spcatcart/', cartController.show_spcatcart);
router.post('/testnodejsapp/api/might_have_missed/', cartController.might_have_missed);
router.post('/testnodejsapp/api/update_subcart/', cartController.update_subcart);


//search 
router.post('/testnodejsapp/api/universal_search/', searchController.universal_search);
router.post('/testnodejsapp/api/searchbystore/', searchController.searchbystore);
router.post('/testnodejsapp/api/searchbystoreproduct/', searchController.searchbystore);
router.post('/testnodejsapp/api/similarproduct/', searchController.searchbyproduct);
router.post('/testnodejsapp/api/recent_search/', searchController.recentSearch);
router.post('/testnodejsapp/api/trensearchproducts/', searchController.trensearchproducts);
router.post('/testnodejsapp/api/trendingrecentsearch/', searchController.trendingrecentsearch);
router.post('/testnodejsapp/api/searchbybanner/', searchController.searchbybanner);
router.post('/testnodejsapp/api/searchbybrands/', searchController.searchbybrands);
router.post('/testnodejsapp/api/searchbypopupbanner/', searchController.searchbypopupbanner);
router.post('/testnodejsapp/api/getProducts/', searchController.getProducts);



//address
router.post('/testnodejsapp/api/add_address', addressController.upload.single('image'), addressController.uploadImage);
//router.post('/testnodejsapp/api/add_address/', addressController.add_address);
router.post('/testnodejsapp/api/show_address/', addressController.show_address);
router.post('/testnodejsapp/api/remove_address/', addressController.remove_address);
//router.post('/testnodejsapp/api/edit_address/', addressController.edit_add);
router.post('/testnodejsapp/api/edit_address', addressController.upload.single('image'), addressController.uploadedaddImage);
router.get('/testnodejsapp/api/city/', addressController.city);
router.post('/testnodejsapp/api/society/', addressController.society);
router.post('/testnodejsapp/api/upload_image', addressController.upload.single('image'), addressController.uploaddoorImage);

//timeslot
router.post('/testnodejsapp/api/timeslot/', timeslotController.timeslot);
router.post('/testnodejsapp/api/quickord_timeslot/', timeslotController.quickord_timeslot);
router.post('/testnodejsapp/api/resumeord_timeslot/', timeslotController.resumeord_timeslot);
router.post('/testnodejsapp/api/upquickorder_timeslot/', timeslotController.upquickorder_timeslot);


//sneakycontroller
router.post('/testnodejsapp/api/sneaky_productlist/', sneakyController.sneaky_productlist);

//notification
router.post('/testnodejsapp/api/notificationlist/', notificationController.notificationlist);
router.post('/testnodejsapp/api/paymentnotification/', notificationController.paymentnotification);
// router.post('/testnodejsapp/api/success/', notificationController.success);
router.get('/testnodejsapp/api/success/', notificationController.success);
router.post('/testnodejsapp/api/successfirst/', notificationController.successfirst);
router.get('/testnodejsapp/api/successfirst/', notificationController.successfirst);
router.post('/testnodejsapp/api/failure/', notificationController.failure);
router.post('/testnodejsapp/api/seo_source/', notificationController.seosource);


//coupons
router.post('/testnodejsapp/api/couponlist/', couponController.couponlist);
router.post('/testnodejsapp/api/apply_coupon/', couponController.apply_coupon);

//checkout
router.post('/testnodejsapp/api/checkout_subcribtionorder/', checkoutController.checkout_subcribtionorder)
router.post('/testnodejsapp/api/checkout_quickorder/', checkoutController.checkout_quickorder)
router.post('/testnodejsapp/api/checkout_quickorder_sdk/', checkoutController.checkout_quickordersdk)

//edit profile
router.post('/testnodejsapp/api/edit_profile', userController.upload.single('image'), userController.uploadImage);

//city Listing API
//router.get('/testnodejsapp/api/city/', userController.getCity);
//test email
router.get('/testnodejsapp/api/sendemail/', userController.sendemail);
router.post('/testnodejsapp/api/payment/', paymentController.payment);
router.post('/testnodejsapp/api/subpayment/', paymentController.subpayment);
router.post('/testnodejsapp/api/trailpayment/', paymentController.trailpayment);

//Guest Controller
router.post('/testnodejsapp/api/guestlogin/', guestController.guestlogin);

//Card Controller
router.post('/testnodejsapp/api/savecard/', cardController.savecard);
router.post('/testnodejsapp/api/savesuccess/', cardController.success);
router.get('/testnodejsapp/api/savesuccess/', cardController.success);
router.post('/testnodejsapp/api/savefailure/', cardController.failure);
router.post('/testnodejsapp/api/deletecard/', cardController.deletecard);
//router.get('/testnodejsapp/api/sendemail/', userController.sendemail);
router.get('/testnodejsapp/api/sendnotification/', userController.sendemail);

//Notify Me Controller
router.post('/testnodejsapp/api/shownotifyme/', notifymeController.shownotifyme);
router.post('/testnodejsapp/api/addnotifyme/', notifymeController.addnotifyme);
router.post('/testnodejsapp/api/deletenotifyme/', notifymeController.deletenotifyme);


//Trail Pack Controller
router.post('/testnodejsapp/api/trailpacklist/', trailpackController.trailpacklist);
router.post('/testnodejsapp/api/trailpackdetails/', trailpackController.trailpackdetails);
router.post('/testnodejsapp/api/add_trail_pack/', trailpackController.addtrail_pack);
router.post('/testnodejsapp/api/show_trailpack/', trailpackController.showtrail_pack);
router.post('/testnodejsapp/api/checkout_trailpack/', trailpackController.checkouttrail_pack);


//rating controller
router.post('/testnodejsapp/api/review_on_delivery/', ratingController.review_on_delivery);
router.post('/testnodejsapp/api/add_product_rating/', ratingController.add_product_rating);
router.post('/testnodejsapp/api/product_review_list/', ratingController.product_review_list);


module.exports = router;
