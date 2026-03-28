const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library
const moment = require('moment');
require('moment-timezone');
const uaeTime = moment.tz(new Date(), "Asia/Dubai");

const resumetimeslot = async (appDetatils) => {
   const uaeTime = moment.tz(new Date(), "Asia/Dubai"); // Get the current time in Dubai timezone
   const dates = [];
   const currentDate = moment().tz('Asia/Dubai'); // Set current date to Dubai timezone
   // Generate the next 5 dates in Dubai timezone
   //for (let i = 2; i <= 5; i++) {
    for (let i = 2; i <= 31; i++) {
       dates.push(currentDate.clone().add(i, 'days').format('YYYY-MM-DD'));
   }

   const datelist = dates;
   const todayDate = currentDate.format('YYYY-MM-DD');
   const customizedProductData1 = [];

   for (let m = 0; m < datelist.length; m++) {
       const selected_date = datelist[m];
       let timeslots1 = {};

       if (todayDate === selected_date) {
           // Compare current Dubai time with 13:30 in Dubai timezone
           if (uaeTime.isBefore(moment.tz("12:00", "HH:mm", "Asia/Dubai"))) {
               const timeslots = await knex('tbl_time_slots')
                   .where('status', '=', 0)
                   .where('id', 5) // Adjust the ID condition based on your logic
                   .select('time_slots','discount','min_amount','max_amount')
                   .orderBy('seq', 'ASC');
                   
               if (timeslots.length > 0) {
                   timeslots1 = {
                       date: selected_date,
                       timeslots: timeslots
                   };
               }
           }
       } else {
           const timeslots = await knex('tbl_time_slots')
               .where('status', '=', 0)
               .whereIn('id', [1,4,5])  // Show only time slots 1 and 2
               .select('time_slots','discount','min_amount','max_amount')
               .orderBy('seq', 'ASC');
               
           if (timeslots.length > 0) {
               timeslots1 = {
                   date: selected_date,
                   timeslots: timeslots
               };
           }
       }

       // Only push non-empty timeslots objects
       if (Object.keys(timeslots1).length > 0) {
           customizedProductData1.push(timeslots1);
       }
   }

   // Limit the result to the first 4 elements
   //const timeslotarray = customizedProductData1.slice(0, 4);
   const timeslotarray = customizedProductData1.slice(0, 30);
   return timeslotarray;
}


const quicktimeslotlist = async (appDetatils) => {
        // Get the current date
        const currentDate = new Date();

        const currentTime = new Date();
        selected_date = appDetatils.selected_date

        if(selected_date == ''){
        throw new Error("Select date");
        }
        date = currentDate.toISOString().slice(0, 10);
        //       // Extract hours, minutes, and seconds from the current time
        const hours = currentTime.getHours();
        const minutes = currentTime.getMinutes();
        const current_time = `${hours}:${minutes}`;

        // Determine tomorrow's date
        const tomorrowDate = new Date(currentDate);
        tomorrowDate.setDate(tomorrowDate.getDate() + 1);
        const formattedTomorrowDate = tomorrowDate.toISOString().slice(0, 10);

        let timeslots;
        if(date == selected_date){



        if(current_time < "13:30")
        {

        timeslots = await knex('tbl_time_slots')
        .where('status','=',0)
        // .where('id','!=',1)
        .where('id',5)
        .orderBy('seq', 'ASC');

        }else{
        throw new Error("Oops No time slot present");
        }

        }

        // If the selected date is tomorrow and the current time is after 18:00
        else if (formattedTomorrowDate === selected_date && current_time >= "19:30") {
        // Fetch only time slots with ids 1 and 2
        timeslots = await knex('tbl_time_slots')
        .where('status', '=', 0)
        .whereIn('id', [1,4,5])  // Show only time slots 1 and 2
        .orderBy('seq', 'ASC');
        } 
        else
        {   
        timeslots = await knex('tbl_time_slots')
        .whereIn('id', [1,4,5])  // Show only time slots 1 and 2
        .where('status','=',0)
        .orderBy('seq', 'ASC')
        }   

      
        if(timeslots.length>0){
         const customizedProductData = [];
         for (let i = 0; i < timeslots.length; i++) {
            const ProductList = timeslots[i];
             // $data[]=array('timeslot'=>$timeslotss->time_slots,
                          //  'availibility'=>"available"); 
                            const customizedProduct = {
                              timeslot: ProductList.time_slots,        
                              availibility: "available"
                            
                              // Add or modify properties as needed
                              };
                          
                            customizedProductData.push(customizedProduct);  
         }
         return customizedProductData;
      }

}

const timeslotlist = async (appDetails) => {
   
    // Get the current date and time in Dubai time zone (UTC+4)
    const currentDateTime = moment().tz("Asia/Dubai");

    // Extract the current date and format it to YYYY-MM-DD
    const formattedDate = currentDateTime.format("YYYY-MM-DD");

    // Get selected_date from appDetails
    const selectedDate = appDetails.selected_date;

    // Extract hours and minutes for current time in Dubai
    const currentTime = currentDateTime.format("HH:mm");

    // Calculate tomorrow's date in Dubai timezone
    const tomorrowDate = currentDateTime.clone().add(1, 'days').format("YYYY-MM-DD");


    // Determine time slots based on the date and time
    let timeslots;

    
    if (formattedDate === selectedDate) {
        if (currentTime < "11:00") {
            // Fetch timeslots for the current date before 13:30
            timeslots = await knex('tbl_time_slots')
                .where('status', '=', 0)
                .where('id', 5)
                .select('time_slots','discount','min_amount','max_amount')
                .orderBy('seq', 'ASC');
        } else {
            // If the current time is past 13:30, throw an error
            throw new Error("Oops! No time slot present.");
        }
    } else if (formattedDate > selectedDate) {
        // If the selected date is in the past, throw an error
        throw new Error("Oops! No time slot present.");
    } else {
            if (tomorrowDate === selectedDate && currentTime >= "17:00") {
            // If the selected date is tomorrow and the current time is after 18:00
            timeslots = await knex('tbl_time_slots')
            .where('status', '=', 0)
            .whereIn('id', [4,5]) // Fetch both id 1 and id 2 time slots
            .select('time_slots','discount','min_amount','max_amount')
            .orderBy('seq', 'ASC');
            }else{
            // Fetch available timeslots for a future date
            timeslots = await knex('tbl_time_slots')
            .where('status', '=', 0)
            .whereIn('id', [1,4,5])  // Show only time slots 1 and 2
            .select('time_slots','discount','min_amount','max_amount')
            .orderBy('seq', 'ASC');
            }
    }

    // If there are timeslots, customize the output format
    if (timeslots.length > 0) {
        const customizedProductData = timeslots.map((slot) => ({
            timeslot: slot.time_slots,
            availibility: "available"
        }));
        return customizedProductData;
    }
};

const getDeliveryDate = async (appDetails) => {
    const repeatedDays = appDetails.repeated_days;
    let deliveryDate = "";

    if (repeatedDays) {
    const repeatedDaysArray = repeatedDays.split(",").map(day => day.trim().toLowerCase());
    const selectedDate = moment(appDetails.selected_date, "YYYY-MM-DD");

    for (let ikj = 0; ikj < 8; ikj++) {
    // Calculate the date by adding ikj days to the selected date
    const currentDate = moment(selectedDate).add(ikj, "days");
    const currentDay = currentDate.format("ddd").toLowerCase(); // Get day in three-letter format like 'Mon', 'Tue', etc.

    // Check if the current day matches any of the repeated days
    if (repeatedDaysArray.includes(currentDay)) {
    deliveryDate = currentDate.format("YYYY-MM-DD");
    break; // Exit the loop if the delivery date is found
    }
    }
    }
return deliveryDate;

};

const upquickordertimeslot = async (appDetatils) => {
    // Get the current date
    timearray = appDetatils.dataarray;
    userid = appDetatils.user_id;

    

        try {
                for (let i = 0; i < timearray.length; i++) {
                const ProductList = timearray[i];
            
                await knex('store_orders')  
                .where('store_approval',userid)
                .where('order_cart_id', 'incart')
                .whereNull('subscription_flag')
                .update({
                sub_delivery_date:ProductList.selected_date
                });

                if(ProductList.cat_id != 0 ){
                    
                    const catarray = await knex('categories') 
                    .where('parent', ProductList.cat_id)
                    .pluck('cat_id'); 
                
                    // Update the timeslots for all products with the specified cat_id
                    const vararray = await knex('product')
                                    .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
                                    .whereIn('product.cat_id', catarray)
                                    .pluck('product_varient.varient_id');
                   
                        await knex('store_orders')  
                        .whereIn('varient_id', vararray)
                        .where('store_approval',userid)
                        .where('order_cart_id', 'incart')
                        .whereNull('subscription_flag')
                        .update({
                            sub_time_slot: ProductList.timeslots,
                            sub_delivery_date:ProductList.selected_date
                        });
                        
                        
                       //for update special category start
                    const checktimeslots = await knex('tbl_time_slots')
                    .where('time_slots', 'like', `%${ProductList.timeslots}%`)
                    .select('id')
                    .first();
                    if(checktimeslots){
                            checktimeslotsids = checktimeslots.id;
                            const checkcategories  = await knex('categories')  
                            .where('cat_type', 'like', '%special%')
                            .where('cat_id',ProductList.cat_id)
                            .where('timeslots', 'like', `%${checktimeslotsids}%`)
                            .pluck('cat_id');

                            const checksubcategories  = await knex('categories')  
                            .whereIn('parent', checkcategories)
                            .pluck('cat_id');

                            const vararraylist = await knex('product')
                            .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
                            .whereIn('product.cat_id', checksubcategories)
                            .pluck('product_varient.varient_id');

                            await knex('store_orders') 
                            .where('store_approval',userid)
                            .whereIn('varient_id', vararraylist)
                            .where('order_cart_id', 'incart')
                            .whereNull('subscription_flag')
                            .update({
                                sub_time_slot: ProductList.timeslots,
                                sub_delivery_date:ProductList.selected_date
                            });
                        }
                    //for update special category end    
                
                }else{
                    
                    const categories = await knex('categories')
                    .where('cat_type',null)
                    .pluck('cat_id');
                    
                    const subcategories  = await knex('categories')  
                    .whereIn('parent', categories)
                    .pluck('cat_id');

                    const vararray = await knex('product')
                    .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
                    .whereIn('product.cat_id', subcategories)
                    .pluck('product_varient.varient_id');
                    

                   const checkprevdate =  await knex('store_orders') 
                                        .where('store_approval',userid)
                                        .whereIn('varient_id', vararray)
                                        .where('order_cart_id', 'incart')
                                        .whereNull('subscription_flag')
                                        .whereNotNull('sub_delivery_date')
                                        //.select('sub_delivery_date')
                                        .first();
                   
                    if(checkprevdate){
                        
                        if(checkprevdate.sub_delivery_date == ProductList.selected_date){

                            await knex('store_orders') 
                            .where('store_approval',userid)
                            .whereIn('varient_id', vararray)
                            .where('order_cart_id', 'incart')
                            .whereNull('subscription_flag')
                            .update({
                                //sub_time_slot: ProductList.timeslots,
                                sub_time_slot: null,
                                sub_delivery_date:ProductList.selected_date
                            });

                        }else{
                            //return 555
                            await knex('store_orders') 
                            .where('store_approval',userid)
                            .whereIn('varient_id', vararray)
                            .where('order_cart_id', 'incart')
                            .whereNull('subscription_flag')
                            .update({
                                sub_time_slot: null,
                                sub_delivery_date:null
                            });

                        }
                    }      
                    await knex('store_orders') 
                    .where('store_approval',userid)
                    .whereIn('varient_id', vararray)
                    .where('order_cart_id', 'incart')
                    .whereNull('subscription_flag')
                    .update({
                        sub_time_slot: ProductList.timeslots,
                        sub_delivery_date:ProductList.selected_date
                    });
                    
                    
                      //for update special category start
                    // const checktimeslots = await knex('tbl_time_slots')
                    // .where('time_slots', 'like', `%${ProductList.timeslots}%`)
                    // .select('id')
                    // .first();
                    // if(checktimeslots){
                    //         checktimeslotsids = checktimeslots.id;
                    //         const checkcategories  = await knex('categories')  
                    //         .where('cat_type',null))
                    //         .where('timeslots', 'like', `%${checktimeslotsids}%`)
                    //         .pluck('cat_id');

                    //         const checksubcategories  = await knex('categories')  
                    //         .whereIn('parent', checkcategories)
                    //         .pluck('cat_id');

                    //         const vararraylist = await knex('product')
                    //         .join('product_varient', 'product.product_id', '=', 'product_varient.product_id')
                    //         .whereIn('product.cat_id', checksubcategories)
                    //         .pluck('product_varient.varient_id');

                    //         await knex('store_orders') 
                    //         .where('store_approval',userid)
                    //         .whereIn('varient_id', vararraylist)
                    //         .where('order_cart_id', 'incart')
                    //         .whereNull('subscription_flag')
                    //         .update({
                    //             sub_time_slot: ProductList.timeslots,
                    //             sub_delivery_date:ProductList.selected_date
                    //         });
                    // }
                    
                    //for update special category end
                }
               
            }
           // return 1
        } catch (error) {
            console.error("Error updating timeslots:", error);
           // return 2
        }
   
};
   
module.exports = {
timeslotlist,
quicktimeslotlist,
resumetimeslot,
getDeliveryDate,
upquickordertimeslot
};
