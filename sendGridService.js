// services/sendGridService.js
const sgMail = require('@sendgrid/mail');
const ejs = require('ejs');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

sgMail.setApiKey(process.env.SENDGRID_API_KEY);

const getSendemailer = async (templateData) => {
sgMail.setApiKey(process.env.SENDGRID_API_KEY);
// Read and render the EJS template
const templatePath = path.join(__dirname, 'views/storecreation.ejs');
const htmlContent = await ejs.renderFile(templatePath, templateData);
// Path to the attachment file
const attachmentPath = path.join(__dirname, 'attachments/sample.pdf');
// Read the file as a base64 string
const attachment = fs.readFileSync(attachmentPath).toString('base64');
const msg = {
to: 'deeksha.gupta@evonix.co',
from: 'info@quickart.ae',
subject: 'Welcome mail',
html: htmlContent,
};
await sgMail.send(msg);   
};

const otpMail = async (email, templateData,subject) => {

    sgMail.setApiKey(process.env.SENDGRID_API_KEY);
    // Read and render the EJS template
    const templatePath = path.join(__dirname, 'views/sendotp.ejs');
    const htmlContent = await ejs.renderFile(templatePath, templateData);
    const msg = {
    to: email,
    from: 'info@quickart.ae',
    subject: subject,
    html: htmlContent
    };
    await sgMail.send(msg);
    
};

const welcomeMail = async (email, templateData,subject) => {

sgMail.setApiKey(process.env.SENDGRID_API_KEY);
// Read and render the EJS template
const templatePath = path.join(__dirname, 'views/welcome.ejs');
const htmlContent = await ejs.renderFile(templatePath, templateData);
const msg = {
to: email,
from: 'info@quickart.ae',
subject: subject,
html: htmlContent
};
await sgMail.send(msg);
};

const codorderplacedMail = async (email,templateData,subject,group_id) => {
sgMail.setApiKey(process.env.SENDGRID_API_KEY);
// Read and render the EJS template
const templatePath = path.join(__dirname, 'views/codorderplaced.ejs');
const htmlContent = await ejs.renderFile(templatePath, templateData);
const msg = {
to: email,
from: 'info@quickart.ae',
subject: subject,
html: htmlContent,
cc: ['store1@quickart.ae','priyanka.surti@evonix.co'],
};
await sgMail.send(msg);  
};

const cancelorderMail = async (email,templateData,subject) => { 
sgMail.setApiKey(process.env.SENDGRID_API_KEY);
// Read and render the EJS template
const templatePath = path.join(__dirname, 'views/cancelorder.ejs');
const htmlContent = await ejs.renderFile(templatePath, templateData);
const msg = {
to:'store1@quickart.ae',
from: 'info@quickart.ae',
subject: subject,
html: htmlContent,
// cc:'store1@quickart.ae',
};
//return msg
await sgMail.send(msg);
};

const sendOrderPlacedEmail = async (to, templateData) => {
try {
// Render the EJS template
const templatePath = path.join(__dirname, '/views/orderPlacedTemplate.ejs');
const htmlContent = await ejs.renderFile(templatePath, templateData);
const msg = {
to: data.user_email,
from: 'info@quickart.ae',
subject: 'Welcome to Your New Store',
html: htmlContent,
};
await sgMail.send(msg);
console.log('Order placed email sent successfully.');
} catch (error) {
console.error('Error sending email:', error);
if (error.response) {
console.error(error.response.body);
}
}
};

const pauseorderMail = async (email,templateData,subject) => {
sgMail.setApiKey(process.env.SENDGRID_API_KEY);
// Read and render the EJS template
const templatePath = path.join(__dirname, 'views/pauseorder.ejs');   
const htmlContent = await ejs.renderFile(templatePath, templateData);
const msg = {
to: email,
from: 'info@quickart.ae',
subject: subject,
html: htmlContent,
};
//return msg
await sgMail.send(msg);
};

const resumeorderMail = async (email,templateData,subject) => {
sgMail.setApiKey(process.env.SENDGRID_API_KEY);
// Read and render the EJS template
const templatePath = path.join(__dirname, 'views/resumeorder.ejs');
const htmlContent = await ejs.renderFile(templatePath, templateData);
const msg = {
to: email,
from: 'info@quickart.ae',
subject: subject,
html: htmlContent,

};
//return msg
await sgMail.send(msg);
};

module.exports = {
sendOrderPlacedEmail,
getSendemailer,
codorderplacedMail,
cancelorderMail,
welcomeMail,
pauseorderMail,
resumeorderMail,
otpMail
};
