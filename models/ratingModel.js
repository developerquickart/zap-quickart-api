const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const axios = require('axios');


const reviewOndelivery = async (appDetatils) => {

    const { user_id, cart_id, rating, subscription_id} = appDetatils;

  description = appDetatils.description;
    if (description != null) {
        description = description;
    } else {
        description = "N/A";
    }
    
    //  const ordercheck = await knex('orders')
    // .where('cart_id', cart_id)
    // .select('is_subscription')
    // .first();

    // if(ordercheck.is_subscription == 1){
    //      check = await knex('subscription_order')
    //     .where('cart_id', cart_id)
    //     .where('subscription_id',subscription_id)
    //     .where('order_status', 'Completed')
    //     .select('dboy_id')
    //     .first();
    // }else{
          check = await knex('subscription_order')
        .where('cart_id', cart_id)
        .where('order_status', 'Completed')
        .select('dboy_id')
        .first();
   // }
    if (check) {
        // if(ordercheck.is_subscription == 1){
        //         const checkreview = await knex('delivery_rating')
        //         .where('cart_id', cart_id)
        //         .where('user_id', user_id)
        //         .where('subscription_id',subscription_id)
        //         .first();
        //         if(checkreview){
                   
        //             review = await knex('delivery_rating')
        //             .where('cart_id', cart_id)
        //             .where('user_id', user_id)
        //             .where('subscription_id',subscription_id)
        //             .update({'rating':rating,
        //             'description':description});
        
        //         }else{
                    
        //              review = await knex('delivery_rating').insert({
        //                 cart_id: cart_id,
        //                 user_id: user_id,
        //                 rating: rating,
        //                 dboy_id: check.dboy_id,
        //                 subscription_id:subscription_id,
        //                 description: description,
        //             });
        
        //         }
        //         return review

        // }else{

            
             const checkreview = await knex('delivery_rating')
                .where('cart_id', cart_id)
                .where('user_id', user_id)
                .first();
                if(checkreview){
                   
                    review = await knex('delivery_rating')
                    .where('cart_id', cart_id)
                    .where('user_id', user_id)
                    .update({'rating':rating,
                    'description':description});
        
                }else{
                    const maxRatingId = await knex('delivery_rating')
                        .max('rating_id as max_id')
                        .first();
                    const rating_id = (maxRatingId?.max_id ?? 0) + 1;

                    review = await knex('delivery_rating').insert({
                        rating_id,
                        cart_id: cart_id,
                        user_id: user_id,
                        rating: rating,
                        dboy_id: check.dboy_id,
                        description: description,
                    });
        
                }
                return review

        //}
            
        
        
    }else{
        throw new Error(`Please Wait for Order Completion.`);
    }

};


const productRating = async (appDetatils) => {

    const { user_id, store_id, varient_id, rating, description,cart_id, subscription_id} = appDetatils;
    
    const createdAt = new Date(); // Equivalent to Carbon::now()
    
    // const ordercheck = await knex('orders')
    // .where('cart_id', cart_id)
    // .select('is_subscription')
    // .first();

    // if(ordercheck.is_subscription == 1){
      
    //      check = await knex('subscription_order')
    //     .where('cart_id', cart_id)
    //     .where('subscription_id',subscription_id)
    //     .where('order_status', 'Completed')
    //     .select('dboy_id')
    //     .first();
   
    // }else{
      
          check = await knex('subscription_order')
        .where('cart_id', cart_id)
        .where('order_status', 'Completed')
        .select('dboy_id')
        .first();
        
    //}    
   
            if (check) {
        // Check if a review already exists
        
                // if(ordercheck.is_subscription == 1){
                // const checkReview = await knex('product_rating')
                //     .where('user_id', user_id)
                //     .where('varient_id', varient_id)
                //     .where('subscription_id',subscription_id)
                //     .where('store_id', store_id)
                //     .where('cart_id', cart_id)
                //     .first();
                //     if (checkReview) {
                     
                //         // Update the existing review
                //         const review = await knex('product_rating')
                //             .where('user_id', user_id)
                //             .where('varient_id', varient_id)
                //             .where('store_id', store_id)
                //             .where('cart_id', cart_id)
                //             .where('subscription_id',subscription_id)
                //             .update({
                //                 store_id: store_id,
                //                 rating: rating,
                //                 description: description,
                //                 updated_at: createdAt,
                //             });
    
                //             return review;
                //     } else {
                        
                //         // Insert a new review
                //         const review = await knex('product_rating').insert({
                //             user_id: user_id,
                //             varient_id: varient_id,
                //             store_id: store_id,
                //             rating: rating,
                //             description: description,
                //             cart_id:cart_id,
                //             subscription_id:subscription_id,
                //             created_at: createdAt,
                //             updated_at: createdAt,
                //         });
    
                //         return review;
                //     }
                    
                    
                // }else{
                    const checkReview = await knex('product_rating')
                    .where('user_id', user_id)
                    .where('varient_id', varient_id)
                    .where('store_id', store_id)
                    .where('cart_id', cart_id)
                    .first();
                    
                    if (checkReview) {
                     
                        // Update the existing review
                        const review = await knex('product_rating')
                            .where('user_id', user_id)
                            .where('varient_id', varient_id)
                            .where('store_id', store_id)
                            .where('cart_id', cart_id)
                            .update({
                                store_id: store_id,
                                rating: rating,
                                description: description,
                                updated_at: createdAt,
                            });
    
                            return review;
                    } else {
                        
                        // Insert a new review - get max+1 for rate_id (primary key)
                        const maxRateId = await knex('product_rating')
                            .max('rate_id as max_id')
                            .first();
                        const rate_id = (maxRateId?.max_id ?? 0) + 1;

                        const review = await knex('product_rating').insert({
                            rate_id,
                            user_id: user_id,
                            varient_id: varient_id,
                            store_id: store_id,
                            rating: rating,
                            description: description,
                            cart_id:cart_id,
                            created_at: createdAt,
                            updated_at: createdAt,
                        });
    
                        return review;
                    }
                    
                    
                //}
            
            }else{
                throw new Error(`Please Wait for Order Completion.`);
            }
    };
    
const prodReviewlist = async (appDetatils) => {
       varient_id  = appDetatils.varient_id;
       const baseurl =  process.env.BUNNY_NET_IMAGE;
         rating = await knex('product_rating')
                .join('users', 'product_rating.user_id', '=', 'users.id')
                .where('product_rating.varient_id',varient_id)
                .select('users.name','product_rating.cart_id','product_rating.rating','product_rating.description','product_rating.created_at',knex.raw(`CONCAT('${baseurl}', users.user_image) as user_image`));
         return rating
    
}

module.exports = {
reviewOndelivery,
productRating,
prodReviewlist
};
