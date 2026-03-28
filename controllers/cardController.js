const cardModel = require('../models/cardModel');
const notificationModel = require('../models/notificationModel');
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));


const savecard = async (req, res) => {
  try {
    const appDetatils = req.body;
    const product = await cardModel.SaveCardDetails(appDetatils);
    var data = {
      "status": "1",
      "message": "Success",
      "data": product
    };
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: error.message });
  }
}

const success = async (req, res) => {
  const successLogId = `SUCCESS-${Date.now()}`;
  console.log(`[${successLogId}] ========== SAVESUCCESS REDIRECT RECEIVED ==========`);
  console.log(`[${successLogId}] Query params:`, JSON.stringify(req.query));
  console.log(`[${successLogId}] Waiting 2s for IPN to potentially arrive before InsertCard...`);
  await sleep(2000); // Brief wait for IPN to potentially arrive
  try {
    console.log(`[${successLogId}] Calling InsertCard now`);
    const SaveCard = await cardModel.InsertCard(req.query);
    var data = {
      "status": "1",
      "message": "Card saved successfully",
      "data": SaveCard,
    };
    console.log(`[${successLogId}] InsertCard completed successfully for order_id: ${req.query.order_id}`);
    res.status(200).json(data);
  } catch (error) {
    console.error(`[${successLogId}] SAVESUCCESS ERROR:`, error.message, error.stack);
    res.status(500).json({ status: 0, message: error.message });
  }
}

const failure = async (req, res) => {
  try {
    const appDetatils = req.body;
    const sneaky = await notificationModel.failureData(appDetatils);
    // var data = {
    // "status": "1",
    // "message":"Failure List",  
    // "data":appDetatils
    // };
    var data;
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Not found' });
  }
}

const recurringpayment = async (req, res) => {
  try {
    const appDetatils = req.body;
    const DeductionPayment = await cardModel.DeductionRecurringPayment(appDetatils);
    var data = {
      "status": "1",
      "message": "Success",
      "data": DeductionPayment
    };
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: error.message });
  }
}

const deletecard = async (req, res) => {
  try {
    const appDetatils = req.body;
    const CardDetails = await cardModel.DeleteCardDetails(appDetatils);
    var data = {
      "status": "1",
      "message": "Success",
    };
    res.status(200).json(data);
  } catch (error) {
    if (error.message === 'Card cannot be deleted as it is associated with pending orders') {
      //versionning changes
      res.status(200).json({ status: 0, message: 'Card cannot be deleted as it is associated with pending orders' });
    } else {

      res.status(500).json({ status: 0, message: error.message });
    }
  }
}

module.exports = {
  savecard,
  success,
  failure,
  recurringpayment,
  deletecard,
};
