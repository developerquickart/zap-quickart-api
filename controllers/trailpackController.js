const trailpackModel = require('../models/trailpackModel');

const checkouttrail_pack = async (req, res) => {

  try {
    const appDetatils = req.body;
    const getOrderlist = await trailpackModel.gettrailcheckout(appDetatils);

    var data = {
      "status": "1",
      "message": "Order Placed successfully",
      "data": getOrderlist,

    };
    res.status(200).json(data);
  }
  catch (error) {
    if (error.message === 'Select any Address') {
      res.status(400).json({ message: 'Select any Address' });
    } else if (error.message === 'Select Payment Method') {
      res.status(400).json({ message: 'Select Payment Method' });
    } else if (error.message === 'cart is empty') {
      res.status(400).json({ message: 'Cart is empty' });
    } else if (error.message.includes('Unable to place order for selected date time')) {
      res.status(400).json({ status: 0, message: error.message });
    }
    else {
      console.error(error);
      res.status(500).json({ status: 0, message: error.message });
    }
  }


};

const addtrail_pack = async (req, res) => {
  try {
    const appDetatils = req.body;
    const TrailPackDetails = await trailpackModel.addtrailpack(appDetatils);
    if (TrailPackDetails == 0) {
      var data = {
        "status": "0",
        "message": "Stock is not available for some products in this trial pack"
      };
    } else {

      var data = {
        "status": "1",
        "message": "Add Trail Pack",
        "data": TrailPackDetails
      };

    }
    res.status(200).json(data);
  } catch (error) {
    if (error.message === 'Trail pack not found') {
      res.status(404).json({ status: 0, message: 'Trail pack not found' });
    }
    else if (error.message === 'No more stock available') {
      res.status(400).json({ status: 0, message: 'No more stock available' });
    }
    else if (error.message === 'Stock is not available for some products in this trial pack') {
      res.status(400).json({ status: 0, message: 'Stock is not available for some products in this trial pack' });
    }
    else {
      console.error(error);
      res.status(500).json({ status: 0, message: error.message });
    }
  }
}

const showtrail_pack = async (req, res) => {
  try {
    const appDetatils = req.body;
    const product = await trailpackModel.showTrialpack(appDetatils);
    if (product == 2) {
      var data = {
        "status": "1",
        "message": "No items in cart.",
      };
    } else {
      var data = {
        "status": "1",
        "message": "Cart Items",
        "data": product
      };
    }
    res.status(200).json(data);

  } catch (error) {
    console.error(error);
    if (error.message === 'No Items in Cart') {
      res.status(400).json({ message: 'No Items in Cart' });
    } else {
      res.status(500).json({ status: 0, message: 'No Items in Cart' });
    }

  }

}

const trailpacklist = async (req, res) => {
  try {
    const appDetatils = req.body;
    const TrailPackDetails = await trailpackModel.trailPackList(appDetatils);
    var data = {
      "status": "1",
      "message": "Trail Pack List",
      "data": TrailPackDetails
    };
    res.status(200).json(data);
  } catch (error) {
    console.error('trailpacklist API error:', error);
    if (error.message === 'Trail Pack List Not found') {
      res.status(400).json({ message: 'Trail Pack List Not found' });
    } else {
      res.status(500).json({ status: 0, message: error.message || 'Trail Pack List Not found', data: [] });
    }
  }
}

const trailpackdetails = async (req, res) => {
  try {
    const appDetatils = req.body;
    const TrailPackDetails = await trailpackModel.trailPackDetails(appDetatils);
    var data = {
      "status": "1",
      "message": "Trail Pack Details",
      "data": TrailPackDetails
    };
    res.status(200).json(data);
  } catch (error) {
    console.error('trailpackdetails API error:', error);
    if (error.message === 'Trail Pack List Not found') {
      res.status(400).json({ message: 'Trail Pack List Not found' });
    } else {
      res.status(500).json({ status: 0, message: error.message || 'Trail Pack List Not found' });
    }
  }
}

module.exports = {
  trailpacklist,
  trailpackdetails,
  addtrail_pack,
  showtrail_pack,
  checkouttrail_pack
};
