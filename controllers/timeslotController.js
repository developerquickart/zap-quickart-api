const timeslotModel = require('../models/timeslotModel');
const knex = require('../db'); // Import your Knex instance

const timeslot = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        const timeSlot = await timeslotModel.timeslotlist(appDetatils);
        const deliveryDate = await timeslotModel.getDeliveryDate(appDetatils);

        var data = {
            "status": "1",
            "message":"Present time Slot",
            "deliveryDate":deliveryDate,
            "data":timeSlot,
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'Oops No time slot present' });
    }

}
const resumeord_timeslot = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        const timeSlot = await timeslotModel.resumetimeslot(appDetatils);
      


        var data = {
            "status": "1",
            "message":"Present time Slot",
            "data":timeSlot,
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'Oops No time slot present' });
    }

}
const quickord_timeslot = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        const timeSlot = await timeslotModel.quicktimeslotlist(appDetatils);
      


        var data = {
            "status": "1",
            "message":"Present time Slot",
            "data":timeSlot,
            
            };
        res.status(200).json(data);
    }
    catch(error){
        console.error(error);
        if (error.message === 'Select date') {
          res.status(400).json({ message: 'Select date' });
        }else{
          res.status(500).json({ status: 0, message: 'Oops No time slot present' });
        }
  
      }

}

const upquickorder_timeslot = async(req, res) =>{
    try{
        const appDetatils = req.body;
        const timeSlot = await timeslotModel.upquickordertimeslot(appDetatils);
        var data = {
            "status": "1",
            "message":"Timeslots updated successfully",
            "data":timeSlot,
            };
        res.status(200).json(data);
    }
    catch(error){
        console.error(error);
          res.status(500).json({ status: 0, message: 'Someting went wrong' });
      }
}

module.exports = {
timeslot,
quickord_timeslot,
resumeord_timeslot,
upquickorder_timeslot
};
