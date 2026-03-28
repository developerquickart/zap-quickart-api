const notifymeModel = require('../models/notifymeModel');


const shownotifyme = async(req, res) =>{
    try{
        const appDetatils = req.body;   
        const NotifyDetails = await notifymeModel.showNotifyMe(appDetatils);
        var data = {
            "status": "1",
            "message":"Notification List",
            "data":NotifyDetails
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: error.message  });
    }
}

const addnotifyme = async(req, res) =>{
    try{
        const appDetatils = req.body;   
        const NotifyDetails = await notifymeModel.addNotifyMe(appDetatils);
        var data = {
            "status": "1",
            "message":"Notification saved successfully",
            };
        res.status(200).json(data);
    }catch(error){
          if (error.message === 'Notification already exists') {
          res.status(200).json({status:0, message: 'Notification already exists'});
        }
        else{
            res.status(500).json({ status: 0, message: error.message  });
        }
    }
}


const deletenotifyme = async(req, res) =>{
    try{
        const appDetatils = req.body;   
        const NotifyDetails = await notifymeModel.deleteNotifyMe(appDetatils);
        var data = {
            "status": "1",
            "message":"Product removed from notify me list",
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: error.message  });
    }
}

module.exports = {
shownotifyme,
addnotifyme,
deletenotifyme
};
