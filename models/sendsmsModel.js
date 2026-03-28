const axios = require('axios');
require('dotenv').config();
const logToFile = require("../utils/logger");
const https = require('https'); 

const sendSMS = async (phoneNumber, message) => {

    const user_name = process.env.SMARTVISION_LOGIN;
    const password = "WelQ@26."; // "Quick@78";


    const user_phone = phoneNumber; // Replace with the actual phone number
   // const Contacts = "971" + user_phone;
    const SenderId = "Quickart";
    
    const url = `https://rslr.connectbind.com:8443/bulksms/bulksms?username=${user_name}&password=${password}&type=0&dlr=1&destination=${user_phone}&source=${SenderId}&message=${encodeURIComponent(message)}`;
    
    const httpsAgent = new https.Agent({
     rejectUnauthorized: false, // bypass certificate validation
    });
    
      try {
        const response = await axios.get(url, {
          timeout: 15000,
          httpsAgent,
        });
    
        logToFile("sendSMS Response " + JSON.stringify(response.data));
        return response.data;
    
      } catch (error) {
        logToFile("sendSMS Error " + error.message);
        throw error; // handled by caller
      }
    // const response = await axios.get(url, {
    //   timeout: 15000,
    // });

    // logToFile("sendSMS Response " + JSON.stringify(response.data));

    // return response.data;   // ✅ RETURN RESPONSE
    // axios.get(url)
    // .then(response => {
    //      logToFile("sendSMS Response " + JSON.stringify(response));
    
    // })
    // .catch(error => {
    //     console.error('Error:', error);
    // });
};

module.exports = {
    sendSMS,
};
