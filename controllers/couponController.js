const couponModel = require('../models/couponModel');
const knex = require('../db'); // Import your Knex instance

const couponlist = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        
       const listcoupon = await couponModel.couponlists(appDetatils);
      

        // Check if listcoupon is empty
        if (!listcoupon || listcoupon.length === 0) {
        // If no coupons found, return empty array
        return res.status(200).json({
        status: "1",
        message: "Coupon List",
        data: []
        });
        }
       
        var data = {
        "status": "1",
        "message":"Coupon List",
        "data":listcoupon,
        };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        if (error.message === 'Coupon not Found') {
         // res.status(400).json({ message: 'Coupon not Found' });
          var data = {
            "message": "Coupon not Found",
            "data":[],
            };
            res.status(500).json(data);
         
        }else{
             var data = {
            "message": "Not Found",
            "data":[],
            };
             res.status(200).json({ status: 0, message: error.message  });
          //res.status(500).json({ status: 0, message: 'Not found' });
        }
  
      }

}

const apply_coupon = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        
       const coupon = await couponModel.applycoupon(appDetatils);
      
       

        var data = {
            "status": "1",
            "message":"Coupon Applied Successfully",
            "data":coupon,
            
            };
        res.status(200).json(data);
    }catch(error){
        

      
        console.error(error);
        if (error.message === 'Coupon code is not valid') {
          res.status(200).json({ status: 0,message: 'Coupon code is not valid' });
        }else if(error.message === 'Coupon is expired') {
            res.status(200).json({ status: 0,message: 'Coupon is expired' });
        }else if(error.message === 'Already used coupon code') {
            res.status(200).json({ status: 0,message: 'Already used coupon code' });
        }else if(error.message === 'Cart value is low.') {
            res.status(200).json({ status: 0,message: 'Cart value is low.' });
        }else if(error.message === 'Coupon cannot be applied on discounted products') {
            res.status(200).json({ status: 0,message: 'Coupon cannot be applied on discounted products' });
        }
        else{
            res.status(200).json({ status: 0, message: error.message  });
        }
  
      }
    
  

}

module.exports = {
    couponlist,
    apply_coupon
  };
