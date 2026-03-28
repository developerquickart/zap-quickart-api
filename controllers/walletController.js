const walletModel = require('../models/walletModel');



const walletrechargehistory = async(req, res) =>{

    try{
        const appDetatils = req.body;
           
        const walletDetails = await walletModel.wallethistory(appDetatils);
        var data = {
            "status": "1",
            "message":"data found",
            "data": walletDetails
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'something went wrong' });
    }

}

const walletrecharge = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        const walletDetails = await walletModel.walletrecharge(appDetatils);
        var data = {
            "status": "1",
            "message":"Wallet recharged successfully",
            "data": walletDetails
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'something went wrong' });
    }

}

const order_card_changes = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        const cardDetails = await walletModel.ordercardchanges(appDetatils);
        var data = {
            "status": "1",
            "message":"Your card has been successfully updated for this subscription. Your pending delivery amount will be deducted from updated card.",
            "data": cardDetails
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'Something went wrong' });
    }

}

// Spend by wallet – G1 (PostgreSQL compatible; model uses knex with pg)
const spent_by_wallet = async (req, res) => {
  try {
    const appDetatils = req.body;
    const walletDetails = await walletModel.spentbywallet(appDetatils);
    const data = {
      status: '1',
      message: 'data found',
      data: walletDetails
    };
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'something went wrong' });
  }
};

// Update wallet expiry – G1 (PostgreSQL compatible)
const update_wallet_expiry = async (req, res) => {
  try {
    const appDetatils = req.body || {};
    const walletDetails = await walletModel.updatewalletexpiry(appDetatils);
    const data = {
      status: '1',
      message: 'data found',
      data: walletDetails
    };
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'something went wrong' });
  }
};

module.exports = {
  walletrechargehistory,
  walletrecharge,
  order_card_changes,
  spent_by_wallet,
  update_wallet_expiry
};
