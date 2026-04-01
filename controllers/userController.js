// controllers/userController.js
const userModel = require('../models/userModel');
const cityModel = require('../models/cityModel');
const sendsmsModel = require('../models/sendsmsModel');
const bcryptjs = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const axios = require('axios');
const fs = require('fs');
const logToFile = require("../utils/logger");

const generate_invoice = async (req, res) => {

  try {
    const appDetatils = req.body;
    const userdetails = await userModel.generateInvoice(appDetatils);
    var userdata = {
      "status": 1,
      "message": "Invoice Created",
      "data": userdetails
    };
    res.status(200).json(userdata);
  } catch (error) {
    if (error.message === 'Email address already registerd') {
      res.status(400).json({ message: 'Email address already registerd' });
    } else if (error.message === 'Mobile No already registerd') {
      res.status(400).json({ message: 'Mobile No already registerd' });
    }
    else {
      console.error(error);
      res.status(500).json({ message: 'User not found' });
    }
  }


};

const send_otp = async (req, res) => {
  try {
    const appDetatils = req.body;
    const userdetails = await userModel.sendOtp(appDetatils);

    if (userdetails && userdetails.otp_value != null && userdetails.country_code != null && userdetails.user_phone != null) {
      otpval = userdetails.otp_value;
      phoneNumber = userdetails.country_code + userdetails.user_phone;
      message = "Your ultimate shopping experience begins in a few seconds! Your OTP to access your profile is " + otpval;
      response = await sendsmsModel.sendSMS(phoneNumber, message);
      logToFile("send otp sms response " + JSON.stringify(response));
    }

    var userdata = {
      "status": 1,
      "message": "OTP sent successfully",
      "data": userdetails
    };
    res.status(200).json(userdata);
  } catch (error) {
    if (error.message === 'Email address already registerd') {
      res.status(400).json({ message: 'Email address already registerd' });
    } else if (error.message === 'Mobile No already registerd') {
      res.status(400).json({ message: 'Mobile No already registerd' });
    } else if (error.message === 'Missing required parameters') {
      res.status(400).json({ message: 'Missing required parameters' });
    } else if (error.message === 'User not found') {
      res.status(404).json({ message: 'User not found' });
    } else if (error.message.startsWith('Required field is missing')) {
      res.status(400).json({ message: error.message });
    } else {
      console.error(error);
      res.status(500).json({ message: 'User not found' });
    }
  }


};

const sendemail = async (req, res) => {

  try {
    const city = await userModel.getSendemail();
    res.status(200).json(city);
  } catch (error) {
    res.status(500).json({ message: 'Internal Server Error' });
  }
};

const verify_otp_update = async (req, res) => {
  try {
    const appDetatils = req.body;
    const userdetails = await userModel.verifyOtpupdate(appDetatils);

    // Determine success message based on return value
    const msg = userdetails === 1
      ? "Email updated successfully"
      : "Mobile no updated successfully";

    const userdata = {
      "status": 1,
      "message": msg
    };

    res.status(200).json(userdata);
  } catch (error) {
    // Handle specific error cases with appropriate HTTP status codes
    const errorMessage = error.message || 'An error occurred';

    if (errorMessage === 'OTP is invalid') {
      res.status(400).json({
        status: 0,
        message: 'OTP is invalid'
      });
    }
    else if (errorMessage === 'OTP is expired' || errorMessage.includes('expired')) {
      res.status(400).json({
        status: 0,
        message: 'OTP has been expired. Please click resend to get new OTP'
      });
    }
    else if (errorMessage === 'Record not found' || errorMessage === 'Invalid request parameters') {
      res.status(400).json({
        status: 0,
        message: errorMessage
      });
    }
    else if (errorMessage.includes('Failed to update') || errorMessage.includes('Invalid mobile number format')) {
      res.status(500).json({
        status: 0,
        message: errorMessage
      });
    }
    else {
      // Log unexpected errors for debugging
      console.error('verify_otp_update error:', error);
      res.status(500).json({
        status: 0,
        message: 'An unexpected error occurred. Please try again.'
      });
    }
  }
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, 'profile_image/');
    //cb(null, 'https://quickart.ae/adminDev/images/profile_image/');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});

const upload = multer({ dest: 'profileimage/' }); // Temporary storage for uploaded files

const uploadImage = async (req, res) => {
  try {
    const file = req.file;
    const body = req.body;

    // Validate required fields
    if (!body.user_id) {
      return res.status(400).json({
        status: 0,
        message: 'User ID is required'
      });
    }

    let filePath = null;

    // Handle file upload if provided
    if (file) {
      try {
        const date = new Date().toISOString().slice(0, 10); // Format as YYYY-MM-DD
        const time = Date.now(); // Unique timestamp for filename
        const fileName = file.originalname; // Original file name

        // Construct file path
        filePath = `images/profileimage/${date}/${time}_${fileName}`;
        const fileFullPath = `profileimage/${file.filename}`; // Path to the temporarily stored file

        // Upload the file to Bunny.net
        const url = `https://sg.storage.bunnycdn.com/quickart/${filePath}`;
        await axios.put(url, fs.createReadStream(fileFullPath), {
          headers: {
            AccessKey: "d6822d0a-db42-4b2b-b926261b56b0-e522-4650",
            'Content-Type': 'application/octet-stream',
          },
        });

        // Remove file from temp storage (async, don't block)
        fs.unlink(fileFullPath, (err) => {
          if (err) {
            console.error('Error deleting temp file:', err);
          } else {
            console.log('Temp file deleted:', fileFullPath);
          }
        });
      } catch (uploadError) {
        console.error('Error uploading file to CDN:', uploadError);
        // Continue with profile update even if file upload fails
        // The existing image will be preserved
      }
    }

    // Initialize imageData with common fields
    const imageData = {
      user_id: body.user_id,
      user_name: body.user_name,
      country_code: body.country_code,
      user_phone: body.user_phone,
      device_id: body.device_id,
      path: filePath  // Use null if no file uploaded (PostgreSQL optimized)
    };

    // Update user profile (optimized PostgreSQL query)
    const updatedUser = await userModel.createImage(imageData);

    res.status(200).json({
      status: 1,
      message: 'Profile updated successfully',
      data: updatedUser
    });

  } catch (error) {
    console.error('Error in uploadImage:', error);

    // Handle specific error messages
    if (error.message === 'User not found') {
      return res.status(404).json({
        status: 0,
        message: 'User not found'
      });
    }

    if (error.message === 'Invalid user ID') {
      return res.status(400).json({
        status: 0,
        message: 'Invalid user ID'
      });
    }

    if (error.message === 'You can not change the details for demo account') {
      return res.status(403).json({
        status: 0,
        message: 'You can not change the details for demo account'
      });
    }

    // Generic error response
    res.status(500).json({
      status: 0,
      message: 'An error occurred while updating the profile',
      error: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
};

const getCity = async (req, res) => {

  try {
    const city = await cityModel.getCity();
    res.status(200).json(city);
  } catch (error) {
    res.status(500).json({ message: 'Internal Server Error' });
  }
};

const user_bank_details = async (req, res) => {

  try {
    const appDetatils = req.body;
    const bankDetails = await userModel.bankDetails(appDetatils);
    var data = {
      "status": "1",
      "message": "Account details fatch successfully",
      "data": bankDetails

    };
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: 0, message: 'Please add your card detail' });
  }

};

const login = async (req, res) => {
  try {
    const userdata = await req.body;
    // findUser now returns complete user data (optimized - combines findUser + getUsersforLogin)
    const getuser = await userModel.findUser(userdata);

    // Allow login only for Zap users
    if (!getuser || !getuser.is_zap_user) {
      return res.status(200).json({
        status: 0,
        message: 'Sorry you are not a zap customer'
      });
    }

    // Prepare response immediately
    let userlist;
    if (getuser.is_verified == 0) {
      userlist = {
        "message": "User not verified",
        "code": getuser.otp_value,
        "data": getuser,
      };
      // Send response immediately (don't wait for SMS)
      res.status(200).json({ status: 0, message: 'go to register details page' });
    } else {
      userlist = {
        "status": 1,
        "message": "Verify OTP for Login",
        "code": getuser.otp_value,
        "data": getuser,
      };
      // Send response immediately (don't wait for SMS)
      res.status(200).json(userlist);
    }

    // Send SMS asynchronously after response (fire and forget - don't block)
    if (getuser.user_phone) {
      const otpval = getuser.otp_value;
      const phoneNumber = getuser.country_code + getuser.user_phone;
      const message =
        `Your ultimate shopping experience begins in a few seconds! ` +
        `Your OTP to access your profile is ${otpval}`;

      // Send SMS asynchronously - don't await, don't block response
      sendsmsModel.sendSMS(phoneNumber, message)
        .catch(smsError => {
          logToFile("SMS sending failed: " + JSON.stringify(smsError.message));
        });
    }
  } catch (error) {
    if (error.message === 'deactivate') {
      res.status(400).json({ message: 'Your account has been deleted with this phone number. Please enter new contact number' });
    } else if (error.message === 'Register') {
      res.status(200).json({ status: 0, message: 'go to register details page' });
    }
    else {
      console.error(error);
      //   res.status(500).json({ message: 'Internal Server Error' });
      res.status(500).json({ status: 0, message: error.message });
    }
  }
};

const resend_otp = async (req, res) => {
  try {
    const userdata = await req.body;
    await userModel.resendOtp(userdata);
    const getuser = await userModel.getUsersforLogin(userdata);
    // PostgreSQL users.otp_value is TEXT – normalize for consistent API response
    const userlist = {
      message: 'OTP sent on register mobile number',
      code: getuser && getuser.otp_value != null ? String(getuser.otp_value) : null
    };
    res.status(200).json(userlist);
  } catch (error) {
    if (error.message === 'Otp Off') {
      res.status(400).json({ message: 'Otp Off' });
    } else if (error.message === 'User not found') {
      res.status(400).json({ message: 'User not found' });
    } else {
      res.status(500).json({ status: 0, message: error.message });
    }
  }
};

const verify_otp = async (req, res) => {
  try {
    const userdata = await req.body;
    const user = await userModel.verifyOtp(userdata);
    const getuser = await userModel.getUsersforLogin(userdata);
    const token = jwt.sign({
      email: getuser.email,
      userId: getuser.id

    }, 'secret', function (err, token) {

      if (err) {
        return res.status(500).json({ status: 0, message: 'Internal Server Error' });
      }
      res.status(201).json({
        message: "Phone Verified! login successfully",
        data: getuser,
        otherdata: user,
        token: token
      });

    });

  } catch (error) {
    if (error.message === 'User not registered') {
      res.status(400).json({ message: 'User not registered' });
    } else if (error.message === 'Wrong OTP') {
      res.status(400).json({ message: 'Wrong OTP' });
    } else if (error.message === 'OTP is expired') {
      res.status(400).json({ message: 'OTP has been expired. Please click resend to get new OTP' });
    }
    else {
      res.status(500).json({ message: 'Internal Server Error' });
    }
  }
};

const user_deactivate = async (req, res) => {
  try {
    const userdata = await req.body;
    const user = await userModel.userDeactivate(userdata);
    var userlist = {
      "message": "Account deleted successfully",
    };
    res.status(200).json(userlist);

  } catch (error) {
    if (error.message === 'User not Found') {
      res.status(400).json({ message: 'User not Found' });
    } else if (error.message === 'User is deactivated') {
      res.status(400).json({ message: 'User is already deactivated' });
    } else {
      console.error(error);
      res.status(500).json({ message: 'Internal Server Error' });
    }
  }
};

const register_details = async (req, res) => {
  try {
    const userdata = await req.body;
    const user = await userModel.register(userdata);
    const getuser = await userModel.getUsersforLogin(userdata);

    if (getuser.user_phone) {
      const otpval = getuser.otp_value;
      const phoneNumber = getuser.country_code + getuser.user_phone;
      const message = 'Your ultimate shopping experience begins in a few seconds! Your OTP to access your profile is ' + otpval;
      sendsmsModel.sendSMS(phoneNumber, message).catch(smsError =>
        logToFile('signup SMS sending failed: ' + JSON.stringify(smsError && smsError.message))
      );
    }

    const userlist = {
      "message": "Verify OTP",
      "data": getuser,
      // "data1":user
    };
    res.status(200).json(userlist);
  } catch (error) {
    if (error.message === 'deactivate') {
      res.status(400).json({ message: 'Your account has been deleted with this phone number.Please enter new contact number' });
    } else if (error.message === 'Registered') {
      //res.status(400).json({ message: 'User Already Registered with this phone number' });
      res.status(500).json({ status: 0, message: error.message });
    }
    else {
      //console.error(error);
      //res.status(500).json({ message: 'User Already Registered with this phone number' });
      res.status(500).json({ status: 0, message: error.message });
    }
  }
};

const createUser = async (req, res) => {
  try {
    const user = req.body;
    await userModel.createUser(user);
    res.status(201).json({ message: 'User created successfully' });
  } catch (error) {
    if (error.message === 'Email already exists') {
      res.status(400).json({ message: 'Email already exists' });
    } else {
      console.error(error);
      res.status(500).json({ message: 'Internal Server Error' });
    }
  }
};

const getUsers = async (req, res) => {
  try {
    const users = await userModel.getUsers();
    res.status(200).json(users);
  } catch (error) {

    res.status(500).json({ message: 'Internal Server Error' });
  }
};

const updateUser = async (req, res) => {
  try {
    const { id } = req.params;
    const updatedUser = req.body;
    await userModel.updateUser(id, updatedUser);
    res.status(200).json({ message: 'User updated successfully' });
  } catch (error) {

    res.status(500).json({ message: 'Internal Server Error' });
  }
};

const deleteUser = async (req, res) => {
  try {
    const { id } = req.params;
    await userModel.deleteUser(id);
    res.status(200).json({ message: 'User deleted successfully' });
  } catch (error) {

    res.status(500).json({ message: 'Internal Server Error' });
  }
};

const myprofile = async (req, res) => {
  try {
    const appDetatils = req.body;
    const userdetails = await userModel.myprofile(appDetatils);
    var userdata = {
      "status": 1,
      "message": "User Details",
      "data": userdetails
    };
    res.status(200).json(userdata);
  } catch {

    res.status(500).json({ message: 'User not found' });
  }
};

const profile_edit = async (req, res) => {
  try {
    const appDetatils = req.body;
    const userdetails = await userModel.editprofile(appDetatils);
    var userdata = {
      "status": 1,
      "message": "Profile Updated",
      "data": userdetails
    };
    res.status(200).json(userdata);
  } catch {

    res.status(500).json({ message: 'User not found' });
  }
};

const faqslist = async (req, res) => {
  try {
    const appDetatils = req.body;
    const faqslist = await userModel.getFaqsList(appDetatils);
    var userdata = {
      "status": 1,
      "message": "FAQS List",
      "data": faqslist
    };
    res.status(200).json(userdata);
  } catch {

    res.status(500).json({ message: 'User not found' });
  }
};

const showprofile = async (req, res) => {
  try {
    const appDetatils = req.body;
    const userlist = await userModel.getShowProfile(appDetatils);
    var userdata = {
      "status": 1,
      "message": "User Details",
      "data": userlist
    };
    res.status(200).json(userdata);
  } catch (error) {
    // Optimized error handling - check error type early for faster response
    if (error.message === 'User not found' || error.message === 'Invalid user_id') {
      res.status(404).json({ status: 0, message: error.message });
    } else {
      console.error('Error in showprofile:', error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
  }
};

const verify_details = async (req, res) => {
  try {
    const userdata = await req.body;
    const userDetails = await userModel.verifyDetails(userdata);
    var userlist = {
      "message": "Transfer successfully",
      "data": userDetails
    };
    res.status(200).json(userlist);

  } catch (error) {

    console.error(error);
    res.status(500).json({ message: 'Internal Server Error' });

  }
};

const fetch_otp_testing = async (req, res) => {
  try {
    const userdata = req.body;
    const userOtp = await userModel.fetchOtpTesting(userdata);
    const data = {
      status: 1,
      message: "OTP fetched successfully",
      otp: userOtp.otp_value
    };
    res.status(200).json(data);
  } catch (error) {
    if (error.message === 'User not found') {
      res.status(404).json({ status: 0, message: 'User not found' });
    } else if (error.message === 'User phone is required') {
      res.status(400).json({ status: 0, message: 'User phone is required' });
    } else {
      console.error(error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
  }
};

const randomize_user_phone = async (req, res) => {
  try {
    const userdata = req.body;
    const result = await userModel.randomizeUserPhone(userdata);
    const data = {
      status: 1,
      message: "User phone numbers randomized successfully",
      data: result
    };
    res.status(200).json(data);
  } catch (error) {
    if (error.message === 'User not found') {
      res.status(404).json({ status: 0, message: 'User not found' });
    } else if (error.message === 'User phone is required') {
      res.status(400).json({ status: 0, message: 'User phone is required' });
    } else {
      console.error(error);
      res.status(500).json({ status: 0, message: 'Internal Server Error' });
    }
  }
};

module.exports = {
  createUser,
  getUsers,
  updateUser,
  deleteUser,
  getCity,
  login,
  verify_otp,
  register_details,
  myprofile,
  resend_otp,
  profile_edit,
  user_deactivate,
  user_bank_details,
  upload,
  uploadImage,
  send_otp,
  sendemail,
  verify_otp_update,
  faqslist,
  showprofile,
  verify_details,
  generate_invoice,
  fetch_otp_testing,
  randomize_user_phone
};
