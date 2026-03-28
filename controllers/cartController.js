const cartModel = require('../models/cartModel');

const add_to_subcart = async (req, res) => {
  try{
    const appDetatils = req.body;   
    const product = await cartModel.addtosubCart(appDetatils);
    if(product == 2){
    var data = {
    "status": "0",
    "message": "No items in cart",
    };
    }else
    {
    var data = {
    "status": "1",
    "message": "Subscription Cart Updated",
    "data":product
    };
    }
    res.status(200).json(data);

  } catch(error){

      if (error.message === 'Invalid Quantity') {
          res.status(200).json({ status: 0, message: 'Invalid Quantity' });
        }else if(error.message === 'You have to order'){
          res.status(200).json({ status: 0, message: 'You have to order' });
        }else if(error.message === 'No more stock available'){
          res.status(200).json({ status: 0, message: 'No more stock available' });
        }else if(error.message === 'No more product available.'){
          res.status(200).json({ status: 0, message: 'No more product available.' });
        }else if(error.message === 'User ID is invalid.'){
          res.status(200).json({ status: 0, message: 'User ID is invalid.' });
        }else if(error.message === 'User not Found'){
          res.status(200).json({ status: 0, message: 'User not Found' });
        } else {
          console.error(error);
          res.status(500).json({ status: 0, message: error.message  });
        }

  }
}

const showsub_cart = async (req, res) => {
try{
  const appDetatils = req.body;   
  const product = await cartModel.showsubCart(appDetatils);
  if(product == 2){
  var data = {
    "status": "1",
    "message": "No items in cart.",
    };
  }else
  {
    var data = {
      "status": "1",
      "message": "Cart Items",
      "data":product
      };
  }
  res.status(200).json(data);

} catch(error){
  console.error(error);
  if (error.message === 'No Items in Cart') {
    res.status(400).json({ message: 'No Items in Cart' });
  }else{
    res.status(500).json({ status: 0, message: 'No Items in Cart' });
  }

}

}

const add_to_cart = async (req, res) => {
  try{
    const appDetatils = req.body;   
    const product = await cartModel.addtoCart(appDetatils);
    if(product == 2){
    var data = {
    "status": "1",
    "message": "No items in cart",
    };
    }else{
    var data = {
    "status": "1",
    "message": "Cart Updated",
    "data":product
    };
    }
    res.status(200).json(data);

  } catch(error){

      if (error.message === 'Invalid Quantity') {
          res.status(400).json({ message: 'Invalid Quantity' });
        }else if(error.message === 'You have to order'){
          res.status(400).json({ message: 'You have to order' });
        }else if(error.message === 'No more stock available'){
          res.status(200).json({ status: 0, message: 'No more stock available' });
        }
        else {
          console.error(error);
          res.status(500).json({ status: 0, message: error.message  });
        }

  }
}

const show_cart = async (req, res) => {
try{
  const appDetatils = req.body;   
  const product = await cartModel.showCart(appDetatils);
    if(product == 2){
    var data = {
    "status": "1",
    "message": "No items in cart.",
    };
    }else
    {
    var data = {
    "status": "1",
    "message": "Cart Items",
    "data":product
    };
    }
  res.status(200).json(data);

} catch(error){
  console.error(error);
  if (error.message === 'No Items in Cart') {
    res.status(400).json({ message: 'No Items in Cart' });
  }else{
    res.status(500).json({ status: 0, message: 'No Items in Cart' });
  }

}

}

const show_spcatcart = async (req, res) => {
  try{
    const appDetatils = req.body;
    const product = await cartModel.showspcatCart(appDetatils);
      if(product == 2){
      var data = {
      "status": "1",
      "message": "No items in cart.",
      };
      }else
      {
      var data = {
      "status": "1",
      "message": "Cart Items",
      "data":product
      };
      }
    res.status(200).json(data);
  } catch(error){
    console.error(error);
    if (error.message === 'No Items in Cart') {
      res.status(400).json({ message: 'No Items in Cart' });
    }else{
          res.status(500).json({ status: 0, message: error.message  });
    }
  }
}

const might_have_missed = async(req, res) =>{
  
      try{
          const appDetatils = req.body;   
          const prodList = await cartModel.might_have_missed(appDetatils);
          var data = {
              "status": "1",
              "message":"Product list",
              "data":prodList
              
              };
          res.status(200).json(data);
      }catch(error){
          console.error(error);
          res.status(500).json({ status: 0, message: 'Product not found' });
      }
  
      
  };
  
const update_cart = async (req, res) => {
try{
const appDetatils = req.body;   
const product = await cartModel.updateCart(appDetatils);
var data = {
"status": "1",
"message": "Feature added into cart successfully",
"data":product
};
res.status(200).json(data);
}catch(error){
console.error(error);
res.status(500).json({ status: 0, message: error.message  });
}
};



const update_subcart = async (req, res) => {
try{
const appDetatils = req.body;   
const product = await cartModel.updateSubCart(appDetatils);
var data = {
"status": "1",
"message": "Feature added into subcart successfully",
"data":product
};
res.status(200).json(data);
} catch(error){
console.error(error);
res.status(500).json({ status: 0, message: error.message  });
}
};  

module.exports = {
add_to_cart,
add_to_subcart,
show_cart,
showsub_cart,
show_spcatcart,
might_have_missed,
update_cart,
update_subcart
};
