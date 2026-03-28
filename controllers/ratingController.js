const ratingModel = require('../models/ratingModel');


const review_on_delivery = async(req, res) =>{
    try{
        const appDetatils = req.body;   
        const reviwDetails = await ratingModel.reviewOndelivery(appDetatils);
        var data = {
            "status": "1",
            "message":"Order review and rating have been successfully saved",
            //"data":reviwDetails
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(200).json({ status: 0, message: error.message  });
    }
}

const add_product_rating = async(req, res) =>{
    try{
        const appDetatils = req.body;   
        const reviwDetails = await ratingModel.productRating(appDetatils);
        var data = {
            "status": "1",
            "message":"Product review and rating have been successfully saved",
            //"data":reviwDetails
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(200).json({ status: 0, message: error.message  });
    }
}

const product_review_list = async(req, res) =>{
    try{
        const appDetatils = req.body;   
        const reviwDetails = await ratingModel.prodReviewlist(appDetatils);
        var data = {
            "status": "1",
            "message":"Product review & rating",
            "data":reviwDetails
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: error.message  });
    }
}

module.exports = {
review_on_delivery,
add_product_rating,
product_review_list
};
