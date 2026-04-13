const checkoutModel = require('../models/checkoutModel');
const { generateCartHashKey, deleteCache } = require('../utils/redisClient');

const checkout_subcribtionorder = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getOrderlist = await checkoutModel.getSubordercheckout(appDetatils);

        var data = {
        "status":"1",
        "message":"Order Placed successfully",
        "data":getOrderlist,
        
        };
    res.status(200).json(data);
    } catch (error) {
      if (error.message === 'Select any Address') {
        res.status(400).json({ message: 'Select any Address' });
      } else if(error.message === 'cart is empty'){
        res.status(400).json({ message: 'Cart is empty' });
      }
      else {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message  });
      }
    }
  };
  
const checkout_quickorder = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getOrderlist = await checkoutModel.getQuickordercheckout(appDetatils);

      // On successful quick order, clear the corresponding cart hash in Redis
      try {
        const { user_id, store_id } = appDetatils || {};
        if (user_id && store_id) {
          const cartHashKey = generateCartHashKey(user_id, store_id);
          await deleteCache(cartHashKey);
        }
      } catch (redisError) {
        console.error('Error clearing cart hash after quickorder checkout:', redisError.message || redisError);
      }

        var data = {
        "status":"1",
        "message":"Order Placed successfully",
        "data":getOrderlist,
        
        };
    res.status(200).json(data);
    } 
    catch(error){
      if (error.message === 'Select any Address') {
        res.status(400).json({ message: 'Select any Address' });
      }else if (error.message === 'Select Payment Method') {
        res.status(400).json({ message: 'Select Payment Method' });
      }else if (error.message === 'exp_eta is required and must be an integer') {
        res.status(400).json({ message: 'exp_eta is required and must be an integer' });
      }else if(error.message === 'cart is empty'){
        res.status(400).json({ message: 'Cart is empty' });
      }
      else {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message  });
      }
    }
    
    
  };
  
const checkout_quickordersdk = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getOrderlist = await checkoutModel.getQuickordercheckout(appDetatils);

        var data = {
        "status":"1",
        "message":"Order Placed successfully",
        "data":getOrderlist,
        
        };
    res.status(200).json(data);
    } 
    catch(error){
      if (error.message === 'Select any Address') {
        res.status(400).json({ message: 'Select any Address' });
      }else if (error.message === 'Select Payment Method') {
        res.status(400).json({ message: 'Select Payment Method' });
      }else if (error.message === 'exp_eta is required and must be an integer') {
        res.status(400).json({ message: 'exp_eta is required and must be an integer' });
      }else if(error.message === 'cart is empty'){
        res.status(400).json({ message: 'Cart is empty' });
      }
      else {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message  });
      }
    }
    
    
  };  


  
module.exports = {
    checkout_subcribtionorder,
    checkout_quickorder,
    checkout_quickordersdk
}
