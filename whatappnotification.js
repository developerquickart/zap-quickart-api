const { application } = require('express');
const knex = require('./db'); // Import your Knex instance
const axios = require('axios');





const welcomeMessage = async (mobileno) => {
  try {
    const phone_with_country_code = `+${mobileno}`;
    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';

    const payload = {
      apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      campaignName: 'welcome_message',
      destination: phone_with_country_code,
      userName: 'Quickart General Trading Co LLC',
      templateParams: [],
      source: 'new-landing-page form',
      media: {},
      buttons: [
      {
      type: 'button',
      sub_type: 'url',
      index: 0,
      parameters: [
        {
          type: 'text'
        }
      ]
      }
      ],
      carouselCards: [],
      location: {},
      attributes: {},
      paramsFallbackValue: {}
    };

    const response = await axios.post(apiUrl, payload, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    return response.data;
  } catch (error) {
    console.error('Error sending welcome message:', error);
    throw error;
  }
};
// Function to log messages
const welcomeMessageold = async (mobileno) => {
    const url = 'https://Apisocial.telebu.com/whatsapp-api/v1.0/customer/96345/bot/6748075e65d94186/template';
    const authHeader = 'Basic 1072eb29-af12-4a65-ab5b-a5a8bfb9ff09-HwOnAFh';
    const receiverPhoneNumber = mobileno; // Replace with actual phone number
    
    const data = {
      payload: {
        name: 'welcomemessage',
        components: [],
        language: {
          code: 'en_US',
          policy: 'deterministic'
        },
        namespace: 'a95f9847_fb73_48ca_bed5_b5c5b3fbc1bc'
      },
      phoneNumber: receiverPhoneNumber
    };
  
    try {
      const response = await axios.post(url, data, {
        headers: {
          'Authorization': authHeader,
          'Content-Type': 'application/json'
        }
      });
    //   console.log('Response:', response.data);
    return "Success";
    } catch (error) {
    //   console.error('Error:', error.response ? error.response.data : error.message);
    }
};



// Function to log messages
const sendWhatsAppTemplate = async (phone_with_country_code,otp) => {
  const url = 'https://apisocial.telebu.com/whatsapp-api/v1.0/customer/96345/bot/6748075e65d94186/template';
  const headers = {
    'Authorization': 'Basic 1072eb29-af12-4a65-ab5b-a5a8bfb9ff09-HwOnAFh',
    'Content-Type': 'application/json'
  };
   const authHeader = 'Basic 1072eb29-af12-4a65-ab5b-a5a8bfb9ff09-HwOnAFh';

  const data = {
    payload: {
      name: "otp13aug",
      language: {
        code: "en"
      },
      components: [
        {
          type: "body",
          parameters: [
            {
              type: "text",
              text:otp
            }
          ]
        },
        {
          type: "button",
          sub_type: "url",
          index: "0",
          parameters: [
            {
              type: "text",
              text: "CopyOTP"
            }
          ]
        }
      ]
    },
    phoneNumber:phone_with_country_code
  };

//   try {
//     const response = await fetch(url, {
//       method: 'POST',
//       headers: headers,
//       body: JSON.stringify(data)
//     });

//     const responseData = await response.json();
//     // console.log('Response:', responseData);
//   } 

try {
      const response = await axios.post(url, data, {
        headers: {
          'Authorization': authHeader,
          'Content-Type': 'application/json'
        }
      });
    //   console.log('Response:', response.data);
    return "Success";
    }  
  catch (error) {
      return  error.message
    // console.error('Error:', error.message);
  }
};

module.exports = {
welcomeMessage,
welcomeMessageold,
sendWhatsAppTemplate,
};
