const guestModel = require('../models/guestModel');

const guestlogin = async(req, res) =>{
    try{
        const appDetails = req.body;   
        const guestDetails = await guestModel.addGuestDetails(appDetails);
        
        const data = {
            "status": "1",
            "message": "Saved Details", 
            "data": guestDetails           
        };
        
        res.status(200).json(data);
    }catch(error){
        console.error('Guest login error:', error);
        res.status(500).json({ 
            status: 0, 
            message: 'Something went wrong',
            error: process.env.NODE_ENV === 'development' ? error.message : undefined
        });
    }
}

module.exports = {
    guestlogin,
};
