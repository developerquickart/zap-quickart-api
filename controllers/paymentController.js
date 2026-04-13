const paymentModel = require('../models/paymentModel');


const trailpayment = async (req, res) => {
  try{
    const appDetatils = req.body;   
    const product = await paymentModel.trailPaymentData(appDetatils);
    var data = {
      "status": "1",
      "message": "Success",
      "data":product
      };
    res.status(200).json(data);

  } catch(error){
          console.error(error);
          res.status(500).json({ status: 0, message: error.message  });
  }
}
const payment = async (req, res) => {
  try{
    const appDetatils = req.body;   
    const product = await paymentModel.preparePaymentData(appDetatils);
    var data = {
      "status": "1",
      "message": "Success",
      "data":product
      };
    res.status(200).json(data);

  } catch(error){
          if (error.message === 'exp_eta is required and must be an integer') {
            res.status(400).json({ message: 'exp_eta is required and must be an integer' });
          } else {
            console.error(error);
            res.status(500).json({ status: 0, message: error.message  });
          }
        

  }
}

const subpayment = async (req, res) => {
  try{
    const appDetatils = req.body;   
    const product = await paymentModel.subPaymentData(appDetatils);
   // const getOrderlist = await checkoutModel.getQuickordercheckout(product);
    var data = {
      "status": "1",
      "message": "Success",
      "data":product
      };
    res.status(200).json(data);

  } catch(error){

    
       //   console.error(error);
          res.status(500).json({ status: 0, message: error.message  });
    

  }
}

module.exports = {
    payment,
    subpayment,
    trailpayment
  };
