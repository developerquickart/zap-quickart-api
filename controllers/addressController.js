const addressModel = require('../models/addressModel');
const bcryptjs = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const axios = require('axios');
const fs = require('fs');

const city = async(req, res) =>{

    try{
        const cityDetails = await addressModel.city();
        var data = {
            "status": "1",
            "message":"city list",
            "data":cityDetails
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'city not found' });
    }

}

const society = async(req, res) =>{

    try{
        const appDetatils = req.body;   
        const societyDetails = await addressModel.society(appDetatils);
        var data = {
            "status": "1",
            "message":"Society list",
            "data":societyDetails
            
            };
        res.status(200).json(data);
    }catch(error){
        console.error(error);
        res.status(500).json({ status: 0, message: 'Society not found' });
    }

    
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'doorimage/');
    //cb(null, 'https://quickart.ae/adminDev/images/profile_image/');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});

const upload = multer({ dest: 'doorimage/' }); // Temporary storage for uploaded files

const uploadImage = async (req, res) => {
    let filePath = null;

    try {
        const file = req.file;
        const body = req.body;

        if (file) {
            const date = new Date().toISOString().slice(0, 10);
            const time = Date.now();
            const fileName = file.originalname;

            filePath = `images/doorimage/${date}/${time}_${fileName}`;
            const fileFullPath = `doorimage/${file.filename}`;

            const url = `https://sg.storage.bunnycdn.com/quickart/${filePath}`;
            await axios.put(url, fs.createReadStream(fileFullPath), {
                headers: {
                    AccessKey: "d6822d0a-db42-4b2b-b926261b56b0-e522-4650",
                    'Content-Type': 'application/octet-stream',
                },
            });

            fs.unlink(fileFullPath, (err) => {
                if (err) console.error('Error deleting temp file:', err);
            });
        }

        const imageData = {
            user_id: body.user_id,
            type: body.type,
            receiver_name: body.receiver_name,
            receiver_phone: body.receiver_phone,
            receiver_phone_code: body.receiver_phone_code,
            receiver_email: body.receiver_email,
            society_name: body.society_name,
            house_no: body.house_no,
            landmark: body.landmark,
            lat: body.lat,
            lng: body.lng,
            dial_code: body.dial_code,
            path: filePath
        };

        const result = await addressModel.addAddress(imageData);

        res.status(201).json({
            status: '1',
            message: 'Data saved successfully',
            data: { ...imageData, address_id: result.address_id }
        });

    } catch (error) {
        console.error('Error uploading file:', error);
        const message = error.code === '23505' ? 'Duplicate address' : (error.message || 'An error occurred while uploading the file.');
        res.status(500).json({ status: 0, message });
    }
};

const uploadedaddImage = async (req, res) => {
    let filePath = null;

    try {
        const file = req.file;
        const body = req.body;

        if (file) {
            const date = new Date().toISOString().slice(0, 10);
            const time = Date.now();
            const fileName = file.originalname;

            filePath = `images/doorimage/${date}/${time}_${fileName}`;
            const fileFullPath = `doorimage/${file.filename}`;

            const url = `https://sg.storage.bunnycdn.com/quickart/${filePath}`;
            await axios.put(url, fs.createReadStream(fileFullPath), {
                headers: {
                    AccessKey: "d6822d0a-db42-4b2b-b926261b56b0-e522-4650",
                    'Content-Type': 'application/octet-stream',
                },
            });

            fs.unlink(fileFullPath, (err) => {
                if (err) console.error('Error deleting temp file:', err);
            });
        }

        const imageData = {
            user_id: body.user_id,
            type: body.type,
            receiver_name: body.receiver_name,
            receiver_phone: body.receiver_phone,
            receiver_phone_code: body.receiver_phone_code,
            receiver_email: body.receiver_email,
            society_name: body.society_name,
            house_no: body.house_no,
            landmark: body.landmark,
            lat: body.lat,
            lng: body.lng,
            dial_code: body.dial_code,
            address_id:body.address_id,
            path: filePath, // The full URL to the uploaded file
        };

        // Save the data to the database or any further operations
        if (imageData.type === 'Home') {
        const checkaddresshome = await addressModel.checkaddresshome(imageData);
        if (checkaddresshome.length > 0) {
        return res.status(201).json({
        message: "You've already added a Home address. Please choose another address type.",
        });   
        }
        }

        
        
        await addressModel.editAddress(imageData);
        res.status(201).json({
            status: '1',
            message: 'Data saved successfully',
            data: { ...imageData, address_id: imageData.address_id },
        });
    } catch (error) {
        console.error('Error uploading file:', error);
        const code = error.code;
        const message =
            code === '23505' ? 'Duplicate address.'
                : code === '23503' ? 'Invalid user or address.'
                    : code === '22P02' ? 'Invalid address id.'
                        : (error.message || 'An error occurred while updating the address.');
        res.status(500).json({ status: 0, message });
    }
};

const uploaddoorImage = async (req, res) => {
    let filePath = null;

    try {
        const file = req.file;
        const body = req.body;

        if (file) {
            const date = new Date().toISOString().slice(0, 10);
            const time = Date.now();
            const fileName = file.originalname;

            filePath = `images/doorimage/${date}/${time}_${fileName}`;
            const fileFullPath = `doorimage/${file.filename}`;

            const url = `https://sg.storage.bunnycdn.com/quickart/${filePath}`;
            await axios.put(url, fs.createReadStream(fileFullPath), {
                headers: {
                    AccessKey: "d6822d0a-db42-4b2b-b926261b56b0-e522-4650",
                    'Content-Type': 'application/octet-stream',
                },
            });

            fs.unlink(fileFullPath, (err) => {
                if (err) console.error('Error deleting temp file:', err);
            });
        }

        const imageData = {
            user_id: body.user_id,
            address_id:body.address_id,
            path: filePath, // The full URL to the uploaded file
        };

        // Save the data to the database or any further operations
        const newImage = await addressModel.doorimage(imageData);

        // Send a response back to the client
        res.status(201).json({
            message: 'Data saved successfully',
            data: imageData,
        });

    } catch (error) {
        console.error('Error uploading file:', error);
        res.status(500).json({ message: 'An error occurred while uploading the file.' });
    }
};

const add_address = async (req, res) => {
    try {
        const appDetatils = req.body;
        const result = await addressModel.addAddress(appDetatils);
        res.status(200).json({
            status: '1',
            message: 'Address Saved',
            data: { address_id: result.address_id }
        });
    } catch (error) {
        console.error(error);
        const message = error.code === '23505' ? 'Duplicate address' : error.message;
        res.status(500).json({ status: 0, message });
    }
};

const remove_address = async (req, res) => {
    try {
        const appDetatils = req.body;
        await addressModel.removeAddress(appDetatils);
        res.status(200).json({ status: '1', message: 'Address Removed' });
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message || 'Try Again Later' });
    }
};

const show_address = async (req, res) => {
    try {
        const appDetatils = req.body;
        const addressDetails = await addressModel.showAddress(appDetatils);
        res.status(200).json({
            status: '1',
            message: 'Address list',
            data: addressDetails
        });
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message || 'something went wrong' });
    }
};

const edit_add = async (req, res) => {
    try {
        const appDetatils = req.body;
        await addressModel.editAddress(appDetatils);
        res.status(200).json({ status: '1', message: 'Address Updated' });
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: error.message || 'something went wrong' });
    }
};

module.exports = {
    add_address,
    show_address,
    remove_address,
    edit_add,
    city,
    society,
    storage,
    upload,
    uploadImage,
    uploadedaddImage,
    uploaddoorImage
  };
