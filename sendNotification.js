const { GoogleAuth } = require('google-auth-library');
const { application } = require('express');

const axios = require('axios');
const knex = require('./db'); // Import your Knex instance


const moment = require('moment');

async function sendRejectNotification(cause, user, cart_id, user_id) {
    try {
        const notification_title = "Sorry! We are cancelling your order";
        const notification_text = `Hello ${user.name}, We are cancelling your order (${cart_id}) due to the following reason: ${cause}`;
        const date = moment().format('DD-MM-YYYY');

        // Fetch user device ID
        const getDevice = await knex('users')
            .where('id', user_id)
            .select('device_id')
            .first();

        if (!getDevice) {
            console.log("User device not found");
            return 10; // Return code for debugging
        }

        // Fetch FCM Server Key
        const getFcm = await knex('fcm')
            .where('id', 1)
            .select('server_key')
            .first();

        if (!getFcm) {
            console.log("FCM Key not found");
            return 11; // Return code for debugging
        }

        const getFcmKey = getFcm.server_key;
        const fcmUrl = 'https://fcm.googleapis.com/fcm/send';
        const token = getDevice.device_id;

        // FCM Notification Payload
        const fcmNotification = {
            to: token,
            notification: {
                title: notification_title,
                body: notification_text,
                sound: true
            },
            data: {
                message: {
                    title: notification_title,
                    body: notification_text
                }
            }
        };

        // FCM Headers
        const headers = {
            'Authorization': `key=${getFcmKey}`,
            'Content-Type': 'application/json'
        };

        console.log("FCM URL:", fcmUrl);
        console.log("FCM Token:", token);
        console.log("FCM Server Key:", getFcmKey);
        console.log("Notification Payload:", JSON.stringify(fcmNotification, null, 2));

        // Send Notification using Axios
        const response = await axios.post(fcmUrl, fcmNotification, { headers });
        console.log('FCM Response:', response.data); // log before returning

        // Store Notification in Database
        await knex('user_notification').insert({
            user_id: user_id,
            noti_title: notification_title,
            noti_message: notification_text,
            created_at: moment().format('YYYY-MM-DD HH:mm:ss')
        });

        return response.data; // return after storing in DB

    } catch (error) {
        console.error('Error sending notification:', error.response ? error.response.data : error.message);
        return error.response ? error.response.data : error.message; // return after logging
    }
}


// FCM URL
const fcmUrl = 'https://fcm.googleapis.com/fcm/send';

// Function to send a notification
const sendNotification = async (group_id,prod_name,user_name,user_id,device_id) => {
    //const { title, body, deviceToken } = req.body;



   

    const user = await knex('users')
            .where('id', user_id)
            .select('device_id')
            .first(); 

    if (user) {
      const deviceId = user.device_id;
    
       
      getFcm = await knex('fcm')
      .first();
      
      serverKey = getFcm.server_key;
      deviceToken = device_id;

      const title = `Hey ${user_name}, Your Order is Placed`;
      const body = `Order Successfully Placed: Your order id #${group_id} contains ${prod_name} is placed successfully. All the subscriptions will be delivered to you as per your chosen schedule.`;

      
        // Notification payload
        const notificationPayload = {
          notification: {
              title,
              body,
          },
          to: deviceToken,  // Replace with the target device token or "/topics/your-topic-name" for topics
      };

      // Headers
      const headers = {
          'Authorization': `key=${serverKey}`,
          'Content-Type': 'application/json',
      };

      try {
          // Send notification
          const response = await axios.post(fcmUrl, notificationPayload, { headers });
          const result = await knex('user_notification').insert({
                user_id: user_id,
                noti_title: title,
                noti_message: body
            });
          console.log('Notification sent successfully:', response.data);
          return 1
         // res.status(200).json({ message: 'Notification sent successfully', data: response.data });
      } catch (error) {
          console.error('Error sending notification:', error.response ? error.response.data : error.message);
          return 0
          //res.status(500).json({ error: 'Failed to send notification', details: error.response ? error.response.data : error.message });
      }

    }

};





const sendNotification1 = async (user_id) => {
  try {
      // Fetch the user's device token from the database
      const user = await knex('users')
          .where('id', user_id)
          .select('device_id')
          .first();

      if (!user) {
          console.error('User not found');
          return 0;
      }
    
      const deviceId = user.device_id;
     // return deviceId

      // Load the service account key file
      const serviceAccount = require('./service-account-file.json');

      // Initialize GoogleAuth instance
      const auth = new GoogleAuth({
          credentials: {
              client_email: serviceAccount.client_email,
              private_key: serviceAccount.private_key,
          },
          scopes: ['https://www.googleapis.com/auth/firebase.messaging'],
      });

      // Get the client and access token
      const client = await auth.getClient();
      const accessToken = await client.getAccessToken();

      // FCM v1 API URL
      const fcmUrl = `https://fcm.googleapis.com/v1/projects/quickart-customer/messages:send`;

      // Notification payload
      const notificationPayload = {
          message: {
              token: deviceId,
              notification: {
                  title: 'Hey, Your Order is Placed',
                  body: 'Order Successfully Placed: All the subscriptions will be delivered to you as per your chosen schedule.',
              },
              data: {
                screen: "QuickartsplashScreen",
                key2: "value2"
              }
          },
      };

      // Headers for the FCM request
      const headers = {
          'Authorization': `Bearer ${accessToken.token}`,
          'Content-Type': 'application/json',
      };

      try {
          // Send the notification
          const response = await axios.post(fcmUrl, notificationPayload, { headers });

          // Save the notification details in the database
          await knex('user_notification').insert({
              user_id: user_id,
              noti_title: notificationPayload.message.notification.title,
              noti_message: notificationPayload.message.notification.body,
          });

          console.log('Notification sent successfully:', response.data);
          return accessToken;
          return 1; // Return success
      } catch (error) {
          const errorResponse = error.response ? error.response.data : error.message;

          // Check if the error is related to an unregistered device token
          if (errorResponse && errorResponse.error && errorResponse.error.details) {
              const fcmError = errorResponse.error.details.find(
                  (detail) => detail['@type'] === 'type.googleapis.com/google.firebase.fcm.v1.FcmError'
              );

              if (fcmError && fcmError.errorCode === 'UNREGISTERED') {
                  return 'Device token is unregistered. Removing token from database.';
                 // console.error('Device token is unregistered. Removing token from database.');
                //   await knex('users')
                //       .where('id', user_id)
                //       .update({ device_id: null });
              }
          }
          return error.response.data

          console.error('Error sending notification:', errorResponse);
          return 0; // Return failure
      }
  } catch (error) {
      console.error('Error during notification process:', error.message);
      return 0;
  }
};

module.exports = {
  sendNotification,
  sendNotification1,
  sendRejectNotification
};
