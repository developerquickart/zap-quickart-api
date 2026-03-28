// controllers/orderController.js
const orderModel = require('../models/orderModel');

const groupwise_order = async (req , res) => {
    try {
        const appDetatils = req.body; 
        const grpordDetails = await orderModel.grpordDetails(appDetatils);
  
          var data = {
          "status":"1",
          "message":"Orders Data",
          "data":grpordDetails,
          };
      res.status(200).json(data);
      } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'Internal Server Error' });
      }

};

const can_autorenewal = async (req, res) => {

  try {
    const appDetatils = req.body; 
    const getCancelProd = await orderModel.canautorenewal(appDetatils);

      var data = {
      "status":"1",
      "message":"This product auto-renewal has been successfully removed",
      //"data":getCancelProd,
      
      };
  res.status(200).json(data);
  } catch (error) {
    console.error(error);
  //   res.status(500).json({ status: 0, message: 'Internal Server Error' });
  res.status(500).json({ status: 0, message: error.message  });
  }
};

const merge_orders = async (req , res) => {
  try {
      const appDetatils = req.body; 
      const ordersDetails = await orderModel.mergeOrders(appDetatils);

        var data = {
        "status":"1",
        "message":"My All orders",
        //"data":ordersDetails[0]
       "data":ordersDetails
       
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
      res.status(500).json({ status: 0, message: 'No Orders Yet' });
    }

};

const place_repeated_order = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getRepeatedorderplaced = await orderModel.getRepeatedplaceorder(appDetatils);

        var data = {
        "status":"1",
        "message":"Repeated order added sucessfully",
       "data":getRepeatedorderplaced,
        
        };
    res.status(200).json(data);
    } catch (error) {
      if (error.message === 'already added') {
        res.status(400).json({ message: 'Your order is already added in the cart' });
      } else if (error.message === 'OUT_OF_STOCK') {
        res.status(501).json({ status: 0, message: 'No more stock available' });
      } else {
        console.error(error);
        res.status(500).json({ message: 'Internal Server Error' });
      }
    }
};
  
const cancelledquickorderprod = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getCancelOrder = await orderModel.getCancelquickOrderProd(appDetatils);

        var data = {
        "status":"1",
        "message":"This product order is cancelled successfully",
        "data":getCancelOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
    //   res.status(500).json({ status: 0, message: 'Internal Server Error' });
    res.status(500).json({ status: 0, message: error.message  });
    }
};
  
const cancelledquickorder = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getCancelOrder = await orderModel.getCancelquickOrder(appDetatils);

        var data = {
        "status":"1",
        "message":"This product order is cancelled successfully",
        "data":getCancelOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
};

const my_dailyorders = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getOrder = await orderModel.getMydailyOrder(appDetatils);

        var data = {
        "status":"1",
        "message":"My All orders",
        "data":getOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
    //   res.status(500).json({ status: 0, message: 'Internal Server Error' });
    res.status(500).json({ status: 0, message: error.message  });
    }
};
  
const my_subscription_pause_order = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getPauseorder = await orderModel.getSubpauseorder(appDetatils);
        if(getPauseorder){
        var data = {
        "status":"1",
        "message":"Payment has already been made, This order should not be paused.",
        };
        }else{
        var data = {
        "status":"1",
        "message":"Order paused successfully",
        "data":getPauseorder,
        };
        }
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
};

const my_subscription_resume_order = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getResumeorder = await orderModel.getSubresumeorder(appDetatils);

        var data = {
        "status":"1",
        "message":"Order resumed successfully",
        "data":getResumeorder,
        
        };
    res.status(200).json(data);
    } 
    catch(error){
      console.error(error);
      if (error.message === 'No Orders Pause Yet') {
        res.status(400).json({ message: 'No Orders Pause Yet' });
      }else{
        res.status(500).json({ status: 0, message: 'Internal Server Error' });
      }

    }
};
    
const cancelledproductorder = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getCancelprdOrder = await orderModel.getCancelprdOrder(appDetatils);

        var data = {
        "status":"1",
        "message":"This product order is cancelled successfully",
        "data":getCancelprdOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
    //   res.status(500).json({ status: 0, message: 'Internal Server Error' });
    res.status(500).json({ status: 0, message: error.message  });
    }
};

const can_orders = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getCancelOrder = await orderModel.getCancelOrder(appDetatils);

        var data = {
        "status":"1",
        "message":"Cancelled orders",
        "data":getCancelOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
      res.status(500).json({ status: 0, message: 'No Orders Cancelled Yet' });
    }
};
    
const cancelorderreason = async (req, res) => {
        try {
          
          const cancelOrder = await orderModel.getcancelOrderres();
    
            var data = {
            "status":"1",
            "message":"Cancelled order reason",
            "data":cancelOrder,
            
            };
        res.status(200).json(data);
        } catch (error) {
          console.error(error);
          res.status(500).json({ status: 0, message: 'Internal Server Error' });
        }

};

const ongoing_sub = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const ongoingSub = await orderModel.getOngoingsub(appDetatils);

        var data = {
        "status":"1",
        "message":"My All Subscription Orders",
        "data":ongoingSub,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
    //   res.status(500).json({ status: 0, message: 'No Orders Yet' });
        res.status(500).json({ status: 0, message: error.message  });
    }
};

const total_deliveries = async (req , res) => {
    try {
      
        const deliveries = await orderModel.totaldeliveries();
  
          var data = {
          "status":"1",
          "message":"Total deliveries",
          "data":deliveries,
          };
      res.status(200).json(data);
      } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'No Yet' });
      }

};

const my_orders = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getMyOrder = await orderModel.getMyOrder(appDetatils);

        var data = {
        "status":"1",
        "message":"My All orders",
        "data":getMyOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
};
  
const my_orders_sub = async (req , res) => {
    try {
        const appDetatils = req.body; 
        const ordersDetails = await orderModel.ordsubDetails(appDetatils);
  
          var data = {
          "status":"1",
          "message":"My All orders",
          "data":ordersDetails[0],
         
          };
      res.status(200).json(data);
      } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'No Orders Yet' });
      }

};

const orders_details = async (req , res) => {
    try {
        const appDetatils = req.body; 
        const ordersDetails = await orderModel.ordersDetails(appDetatils);

        const data = {
          status: '1',
          message: 'Orders Data',
          data: ordersDetails[0] ?? null
        };
        res.status(200).json(data);
      } catch (error) {
        console.error(error);
      res.status(500).json({ status: 0, message: error.message  });
      }

};
  
const repeat_orders = async (req, res) => {

    try {
      const appDetatils = req.body; 
      const getreaptOrder = await orderModel.getrepeatOrder(appDetatils);

        var data = {
        "status":"1",
        "message":"My All orders",
        "data":getreaptOrder,
        
        };
    res.status(200).json(data);
    } catch (error) {
      console.error(error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
};

const quick_place_repeated_order = async (req, res) => {

  try {
    const appDetatils = req.body; 
    const getRepeatedorderplaced = await orderModel.getQuickRepeatedPlaceOrder(appDetatils);

      var data = {
      "status":"1",
      "message":"Repeated order added sucessfully",
     "data":getRepeatedorderplaced,
      
      };
  res.status(200).json(data);
  } catch (error) {
    if (error.message === 'already added') {
      res.status(400).json({ message: 'Your order is already added in the cart' });
    } else if (error.message === 'OUT_OF_STOCK') {
      res.status(501).json({ status: 0, message: 'No more stock available' });
    } else {
      console.error(error);
      res.status(500).json({ status: 0, message: error.message  });
    }
  }
};


const product_ongoing_sub = async (req, res) => {

  try {
    const appDetatils = req.body; 
    const ongoingSub = await orderModel.getProductOngoingsub(appDetatils);

      var data = {
      "status":"1",
      "message":"My All Subscription Orders",
      "data":ongoingSub,
      
      };
  res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'No Orders Yet' });
  }
};


const ongoing_sub_list = async (req, res) => {
  try {
    const appDetatils = req.body;
    const ongoingSub = await orderModel.getOngoingsublist(appDetatils);
      var data = {
      "status":"1",
      "message":"My All Subscription Orders",
      "data":ongoingSub,
      };
  res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'No Orders Yet' });
  }
};

const orderwiselist = async (req, res) => {
  try {
      
    const appDetatils = req.body;
    const ongoingSub = await orderModel.orderwiselist(appDetatils);
      var data = {
      "status":"1",
      "message":"My All Subscription Orders",
      "data":ongoingSub,
      };
  res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'No Orders Yet' });
  }
};

const active_orders = async(req, res) => {
  try {
    const appDetatils = req.body;   
    const orders = await orderModel.getActiveOrders(appDetatils);
      var data = {
      "status": "1",
      "message": "data found",
      "data":orders,
      };
      
    //   res.status(200).type('text/plain').send(orders);
      res.status(200).json(data);
  } catch (error) {
    console.error(error);
    // res.status(500).json({ status: 0, message: 'Internal Server Error' });
     res.status(500).json({ status: 0, message: error.message  });
  }
};


module.exports = {
my_orders,
orders_details,
repeat_orders,
ongoing_sub,
total_deliveries,
my_orders_sub,
cancelorderreason,
place_repeated_order,
can_orders,
cancelledproductorder,
my_subscription_pause_order,
my_dailyorders,
cancelledquickorder,
cancelledquickorderprod,
my_subscription_resume_order,
groupwise_order,
quick_place_repeated_order,
product_ongoing_sub,
ongoing_sub_list,
orderwiselist,
merge_orders,
can_autorenewal,
active_orders
};
