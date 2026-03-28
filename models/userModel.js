// models/userModel.js
const knex = require('../db'); // Import your Knex instance
const speakeasy = require('speakeasy');
const bcrypt = require('bcryptjs');
const sgMail = require('@sendgrid/mail');
const ejs = require('ejs');
const fs = require('fs');
const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
const { welcomeMessage } = require('../whatappnotification');
const { welcomeMail } = require('../sendGridService');
const { otpMail } = require('../sendGridService');
const { sendWhatsAppTemplate } = require('../whatappnotification');
const PDFDocument = require('pdfkit'); // For PDF generation
const crypto = require('crypto');
const path = require('path');
const logToFile = require("../utils/logger");

const generateInvoice = async (user) => {

  const { user_id, cart_id } = user;
  user_details = await knex('users')
    .select('name', 'user_phone')
    .where('id', user_id)
    .first();
  const pdfurl = process.env.INVOICE_PDF_URL;
  ongoing_sub = await knex('subscription_order')
    .leftJoin('orders', 'subscription_order.cart_id', '=', 'orders.cart_id')
    .leftJoin('store_orders', 'subscription_order.cart_id ', '=', 'store_orders.order_cart_id')
    .where('subscription_order.cart_id', cart_id)
    .select('orders.payment_method', 'store_orders.product_name', 'store_orders.unit', 'store_orders.qty', 'store_orders.quantity', 'orders.address_id', knex.raw("TO_CHAR(subscription_order.delivery_date, 'YYYY-MM-DD') as delivery_date"))
    .first()
  const addressVal = await knex('address')
    .where('address_id', ongoing_sub.address_id)
    .select('type', 'house_no', 'landmark', 'society', 'city', 'state', 'pincode')
    .first();

  const address = [addressVal.house_no, addressVal.landmark, addressVal.state, addressVal.pincode]
    .filter(Boolean)  // Remove null/empty values
    .join(', ');

  const productname = `${ongoing_sub.product_name} ${ongoing_sub.quantity} ${ongoing_sub.unit} (${ongoing_sub.qty})`;
  const varData = await knex('subscription_order')
    .where('cart_id', cart_id)
    .select('order_status', 'delivery_date', 'cart_id');
  // return varData;
  const data = {
    orderId: cart_id,
    customerName: user_details.name,
    contact: user_details.user_phone,
    deliveryDate: ongoing_sub.delivery_date,
    address: address,
    address_type: addressVal.type,
    productname: productname,
    items: varData,
    deliveryCharge: "+0.00",
    paymentMethod: ongoing_sub.payment_method
  };



  const doc = new PDFDocument({ margin: 30 });


  const now = new Date();
  // Define the file path
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const timeOnly = now.toTimeString().split(' ')[0].replace(/:/g, '-'); // HH-MM-SS
  const randomFourDigit = Math.floor(1000 + Math.random() * 9000);
  //  const invoiceDir = path.join(__dirname, '..', 'invoice');
  const invoiceDir = path.join(__dirname, '..', '..', 'public_html', 'invoice');
  // return invoiceDir;
  if (!fs.existsSync(invoiceDir)) {
    fs.mkdirSync(invoiceDir, { recursive: true });
  }

  const invoicePath = path.join(invoiceDir, `${data.orderId}_${timeOnly}_${randomFourDigit}.pdf`);

  const finalpathval = `${data.orderId}_${timeOnly}_${randomFourDigit}.pdf`;
  const stream = fs.createWriteStream(invoicePath);
  doc.pipe(stream);


  // Add Logo
  doc.image('logo.png', 220, 30, { width: 150 });


  // Add Customer Info
  doc.fontSize(12).moveDown(3)
    .text(`Customer name: ${data.customerName}`)
    .moveDown()
    .text(`Contact: ${data.contact}`)
    .moveDown()
    .text(`Delivery Address: ${data.address_type} : ${data.address}`, { width: 450 })
    .moveDown()
    .text(`Product Name: ${data.productname}`, { width: 450 });

  doc.moveDown(2);

  //nikitas code start
  // Draw Table Header
  // Define consistent X and column widths
  const startX = 50;
  const colWidths = {
    orderId: 1,
    deliveryDate: 150,
    orderStatus: 230
  };
  // Set font and draw table headers
  doc.fontSize(12).font('Helvetica-Bold');
  let currentY = doc.y;
  //doc.text("Order Id", startX, currentY, { width: colWidths.orderId, align: 'left' });
  // doc.text("Order Status", startX + colWidths.orderId, currentY, { width: colWidths.orderStatus, align: 'left' });
  // doc.text("Delivery Date", startX + colWidths.orderId + colWidths.orderStatus, currentY, { width: colWidths.deliveryDate, align: 'left' });
  doc.text("Delivery Date", startX + colWidths.orderId, currentY, { width: colWidths.deliveryDate, align: 'left' });
  doc.text("Order Status", startX + colWidths.orderId + colWidths.deliveryDate, currentY, { width: colWidths.orderStatus, align: 'left' });


  doc.moveDown(0.3);
  // Header bottom line
  doc.moveTo(startX, doc.y).lineTo(startX + colWidths.orderId + colWidths.orderStatus + colWidths.deliveryDate, doc.y)
    .lineWidth(1)
    .strokeColor('#000000')
    .stroke();
  doc.moveDown(0.5);
  // Reset to normal font for body
  doc.font('Helvetica').fontSize(12);
  // Add order rows
  data.items.forEach((item, index) => {
    if (index > 0) {
      // Line between rows
      doc.moveTo(startX, doc.y).lineTo(startX + colWidths.orderId + colWidths.orderStatus + colWidths.deliveryDate, doc.y)
        .lineWidth(0.5)
        .strokeColor('#CCCCCC')
        .stroke();
      doc.moveDown(0.3);
    }
    const currentY = doc.y;
    // Format delivery date
    const dateObj = new Date(item.delivery_date);
    const formattedDate = dateObj.toLocaleDateString('en-US', {
      weekday: 'short', year: 'numeric', month: 'short', day: 'numeric'
    });
    // Add row data (perfectly aligned with header)
    //  doc.text(item.cart_id, startX, currentY, { width: colWidths.orderId, align: 'left' });
    //doc.text(item.order_status, startX + colWidths.orderId, currentY, { width: colWidths.orderStatus, align: 'left' });
    //doc.text(formattedDate, startX + colWidths.orderId + colWidths.orderStatus, currentY, { width: colWidths.deliveryDate, align: 'left' });
    doc.text(formattedDate, startX + colWidths.orderId, currentY, { width: colWidths.deliveryDate, align: 'left' });

    if (item.order_status == 'Payment_failed') {
      var ordstatus = 'Payment Failed';
    } else {
      var ordstatus = item.order_status;
    }

    doc.text(ordstatus, startX + colWidths.orderId + colWidths.deliveryDate, currentY, { width: colWidths.orderStatus, align: 'left' });

    doc.moveDown(1);
  });
  //nikitas code end

  doc.moveDown();

  // Delivery Charge
  // Delivery Charge
  // doc.fontSize(12).text("Delivery Charge: ", 50, doc.y)
  //    .text(`+${data.deliveryCharge}`, 450, doc.y);
  doc.moveDown(2);
  // Payment Info (centered)
  doc.fontSize(14).font('Helvetica-Bold')
    .text("Paid By " + data.paymentMethod, { align: 'center' });
  doc.moveDown();
  doc.font('Helvetica')
    .fontSize(12)
    .text("Thanks for your purchase!", { align: 'center' });

  doc.end();
  const relativePath = path.relative(process.cwd(), invoicePath);

  console.log(`Invoice saved: ${invoicePath}`);

  const finalpath = pdfurl + finalpathval;

  getinvoice = await knex('product_wise_invoice')
    .where('user_id', user_id)
    .where('cart_id', cart_id)
    .select('invoice_path')
    .first();

  if (getinvoice) {

    const fileName = getinvoice.invoice_path;
    // const filePath = path.join(__dirname, 'nodejsapp', 'invoice', fileName);
    const filePath = path.join(__dirname, '..', '..', 'public_html', 'invoice', fileName);

    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log(`${fileName} has been deleted.`);
    }

    insertinvoice = await knex('product_wise_invoice')
      .where('user_id', user_id)
      .where('cart_id', cart_id)
      .update({
        'invoice_path': finalpathval,
      });

    // Also update subscription_order table for consistency with orderModel retrieval
    await knex('subscription_order')
      .where('cart_id', cart_id)
      .update({
        'invoice_path': finalpathval
      });

  } else {

    const maxIdRow = await knex('product_wise_invoice').max('id as max_id').first();
    const nextId = (maxIdRow?.max_id || 0) + 1;
    insertinvoice = await knex('product_wise_invoice')
      .insert({
        id: nextId,
        user_id: user_id,
        cart_id: cart_id,
        invoice_path: finalpathval,
        status: 1
      });

    // Also update subscription_order table for consistency with orderModel retrieval
    await knex('subscription_order')
      .where('cart_id', cart_id)
      .update({
        'invoice_path': finalpathval
      });


  }

  return finalpath;

}

const generateInvoiceold = async (user) => {

  const { user_id, cart_id } = user;
  user_details = await knex('users')
    .select('name', 'user_phone')
    .where('id', user_id)
    .first();
  const imageurl = process.env.IMAGE_URL;
  ongoing_sub = await knex('subscription_order')
    .leftJoin('orders', 'subscription_order.cart_id', '=', 'orders.cart_id')
    .where('subscription_order.cart_id', cart_id)
    .select('orders.address_id', knex.raw("TO_CHAR(subscription_order.delivery_date, 'YYYY-MM-DD') as delivery_date"))
    .first()
  const addressVal = await knex('address')
    .where('address_id', ongoing_sub.address_id)
    .select('type', 'house_no', 'landmark', 'society', 'city', 'state', 'pincode')
    .first();

  const address = [addressVal.house_no, addressVal.society, addressVal.landmark, addressVal.state, addressVal.pincode]
    .filter(Boolean)  // Remove null/empty values
    .join(', ');

  const varData = await knex('subscription_order')
    .where('cart_id', cart_id)
    .select('order_status', 'delivery_date', 'cart_id');
  // return varData;
  const data = {
    orderId: cart_id,
    customerName: user_details.name,
    contact: user_details.user_phone,
    deliveryDate: ongoing_sub.delivery_date,
    address: address,
    items: varData,
    deliveryCharge: "+0.00",
    paymentMethod: "applepay"
  };



  const doc = new PDFDocument({ margin: 30 });



  // Define the file path
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const invoiceDir = path.join(__dirname, '..', 'invoice');
  if (!fs.existsSync(invoiceDir)) {
    fs.mkdirSync(invoiceDir, { recursive: true });
  }

  const invoicePath = path.join(invoiceDir, `i_${data.orderId}_${timestamp}.pdf`);


  const stream = fs.createWriteStream(invoicePath);
  doc.pipe(stream);


  // Add Logo
  doc.image('logo.png', 220, 30, { width: 150 });


  // Add Customer Info
  doc.fontSize(12).moveDown(3)
    .text(`Customer name: ${data.customerName}`)
    .moveDown()
    .text(`Contact: ${data.contact}`)
    .moveDown()
    .text(`Delivery Date: ${data.deliveryDate}`)
    .moveDown()
    .text(`Delivery Address: Home: ${data.address}`, { width: 450 });

  doc.moveDown(2);

  // Draw Table Header
  doc.fontSize(12).text("Order Id", 50, doc.y, { bold: true })
    .text("Order Status", 150, doc.y, { bold: true })
    .text("Delivey Date", 450, doc.y, { bold: true });

  doc.moveDown(1);

  // Draw Line
  // doc.moveTo(50, doc.y).lineTo(550, doc.y).stroke();
  doc.moveTo(50, doc.y).lineTo(550, doc.y)
    .lineWidth(1)
    .strokeColor('#CCCCCC')
    .stroke();

  // Add Order Items
  data.items.forEach((item) => {
    doc.text(item.cart_id, 50, doc.y, { continued: true })
      .text(item.order_status, 150, doc.y, { continued: true, width: 250 })
      .text(item.delivery_date.toString(), 450, doc.y);
    doc.moveDown(1);
  });


  doc.moveDown();

  // Delivery Charge
  doc.fontSize(12).text("Delivery Charge: ", 50, doc.y, { bold: true })
    .text(`+${data.deliveryCharge}`, 450, doc.y);

  doc.moveDown(2);

  // Payment Info
  doc.fontSize(14).text("Paid By " + data.paymentMethod, { align: 'center', bold: true });
  doc.moveDown();
  doc.text("Thanks for your purchase!", { align: 'center' });

  doc.end();
  const relativePath = path.relative(process.cwd(), invoicePath);

  console.log(`Invoice saved: ${invoicePath}`);
  return imageurl + relativePath;
}

const createUser = async (user) => {
  const { name, email } = user;

  // Check if the email already exists
  const existingUser = await knex('users').where({ email }).first();

  if (existingUser) {
    throw new Error('Email already exists');
  }

  // If not, insert the new user
  return await knex('users').insert({ name, email });
};

const getSendemail = async () => {
  //const baseurl =  process.env.BUNNY_NET_IMAGE;
  sgMail.setApiKey(process.env.SENDGRID_API_KEY);


  const data = {
    storelink: 'https://snehal.com/store',
    store_name: 'Example Store',
    password: 'temporary-password',
    user_email: 'snehal.more@evonix.co'
  };

  // Read and render the EJS template


  // Read and render the EJS template
  fs.readFile('views/storecreation.ejs', 'utf8', (err, template) => {
    if (err) {
      console.error('Error reading template file:', err);
      return;
    }

    const message = ejs.render(template, data);

    // Define the email options
    const msg = {
      to: data.user_email,
      from: 'info@quickart.ae',
      subject: 'Welcome to Your New Store',
      html: message,
    };

    // Send the email
    sgMail
      .send(msg)

  });


};

const getUsers = async () => {
  return await knex('users').select('*');
};

const sendOtp = async (userdata) => {
  try {
    // Input validation and extraction (country_code/dial_code only required for mobile)
    const user_id = userdata.user_id;
    const change_type = userdata.change_type;
    const new_info = userdata.new_info;
    const country_code = userdata.country_code || '';
    const dial_code = userdata.dial_code || '';

    // Validate required fields
    if (!user_id || !change_type || !new_info) {
      throw new Error('Missing required parameters');
    }
    if (change_type !== 'email' && (!userdata.country_code || !userdata.dial_code)) {
      throw new Error('Missing required parameters');
    }

    // Generate 4-digit OTP code
    const code = Math.floor(1000 + Math.random() * 9000);
    const expiresAt = new Date(Date.now() + 60 * 1000); // 60 seconds expiration

    // OPTIMIZATION: Parallel execution of independent operations
    // 1. Delete old pending OTPs
    // 2. Fetch user details
    // Both can run in parallel for better latency
    const [deleteResult, user_details] = await Promise.all([
      knex('tbl_mobile_email_update_info')
        .where({
          'user_id': user_id,
          'change_type': change_type,
          'status': 'pending'
        })
        .delete(),
      knex('users')
        .select('*')
        .where('id', user_id)
        .first()
    ]);

    // Validate user exists
    if (!user_details) {
      throw new Error('User not found');
    }

    if (change_type === 'email') {
      // Email update flow
      // Check if email already exists
      const existingEmailUser = await knex('users')
        .select('id')
        .where('email', new_info)
        .first();

      if (existingEmailUser) {
        throw new Error('Email address already registerd');
      }

      // Get next id (max + 1) for tables without sequence/serial
      const maxIdRow = await knex('tbl_mobile_email_update_info')
        .max('id as max_id')
        .first();
      const nextId = (maxIdRow?.max_id != null ? Number(maxIdRow.max_id) : 0) + 1;

      // Insert OTP record with PostgreSQL-compatible return handling
      const insertResult = await knex('tbl_mobile_email_update_info')
        .insert({
          id: nextId,
          user_id: user_id,
          change_type: change_type,
          old_info: user_details.email || '',
          new_info: new_info,
          otp: code,
          status: 'pending',
          remark: userdata.remark ?? '',
          created_date: knex.fn.now(),
          expire_at: expiresAt,
        })
        .returning('id'); // PostgreSQL returns array of objects

      // Handle PostgreSQL return format: [{id: 123}] vs MySQL: [123]
      const lastid = Array.isArray(insertResult) && insertResult.length > 0
        ? (insertResult[0].id || insertResult[0])
        : insertResult[0];

      if (!lastid) {
        throw new Error('Failed to create OTP record');
      }

      const data = { 'lastid': lastid, 'otp': code };

      // OPTIMIZATION: Parallel fetch of logo and currency for email template
      // These are independent queries and can run simultaneously
      const [logo, currency] = await Promise.all([
        knex('tbl_web_setting').select('name').first(),
        knex('currency').select('currency_sign').first()
      ]);

      const app_name = logo?.name || null;
      const currency_sign = currency?.currency_sign || null;

      const templateData = {
        baseurl: process.env.BASE_URL,
        user_name: user_details.name,
        user_email: new_info,
        otp: code,
        app_name: app_name,
        currency_sign: currency_sign,
      };

      const subject = 'Verification code';
      // Send email (non-blocking for better UX, but await for error handling)
      await otpMail(new_info, templateData, subject);

      return data;

    } else {
      // Mobile number update flow
      // Check if mobile number already exists
      const existingMobileUser = await knex('users')
        .select('id')
        .where({
          'user_phone': new_info,
          'country_code': country_code
        })
        .first();

      if (existingMobileUser) {
        throw new Error('Mobile No already registerd');
      }
      // Ensure all parts are strings for concatenation
      const formattedNewInfo = `${String(dial_code)}-${String(country_code)}-${String(new_info)}`;

      // Get next id (max + 1) for tables without sequence/serial
      const maxIdRow = await knex('tbl_mobile_email_update_info')
        .max('id as max_id')
        .first();
      const nextId = (maxIdRow?.max_id != null ? Number(maxIdRow.max_id) : 0) + 1;

      // Insert OTP record with PostgreSQL-compatible return handling
      const insertResult = await knex('tbl_mobile_email_update_info')
        .insert({
          id: nextId,
          user_id: user_id,
          change_type: change_type,
          old_info: user_details.user_phone || '',
          new_info: formattedNewInfo,
          otp: code,
          status: 'pending',
          remark: userdata.remark ?? '',
          created_date: knex.fn.now(),
          expire_at: expiresAt,
        })
        .returning('id'); // PostgreSQL returns array of objects

      // Handle PostgreSQL return format: [{id: 123}] vs MySQL: [123]
      const lastid = Array.isArray(insertResult) && insertResult.length > 0
        ? (insertResult[0].id || insertResult[0])
        : insertResult[0];

      if (!lastid) {
        throw new Error('Failed to create OTP record');
      }

      // CRITICAL FIX: Convert OTP code to string for PostgreSQL text column
      // PostgreSQL users.otp_value is TEXT type, not INTEGER
      const phone_with_country_code = `${country_code}${new_info}`;

      // Update user OTP value (non-blocking, can run in parallel with WhatsApp send)
      const updatePromise = knex('users')
        .where({
          'user_phone': user_details.user_phone,
          'country_code': user_details.country_code
        })
        .update({
          'otp_value': String(code), // Convert to string for TEXT column
          'expire_at': expiresAt
        });

      // Prepare WhatsApp payload (non-blocking operation)
      const otpString = String(code);
      const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
      const payload = {
        apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
        campaignName: 'Otp_verification_code',
        destination: "+" + phone_with_country_code,
        userName: 'Quickart General Trading Co LLC',
        templateParams: [otpString],
        source: 'new-landing-page form',
        media: {},
        buttons: [
          {
            type: 'button',
            sub_type: 'url',
            index: 0,
            parameters: [
              {
                type: 'text',
                text: otpString
              }
            ]
          }
        ],
        carouselCards: [],
        location: {},
        attributes: {},
        paramsFallbackValue: {
          FirstName: 'user'
        }
      };

      // OPTIMIZATION: Execute database update and WhatsApp send in parallel
      // Both are independent operations and don't need to wait for each other
      const [updateResult, whatsappResponse] = await Promise.all([
        updatePromise,
        fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(payload)
        }).then(res => res.json()).catch(err => {
          // Log error but don't fail the request
          logToFile("send otp whatsapp error " + JSON.stringify(err));
          return null;
        })
      ]);

      // Log WhatsApp response (non-blocking)
      if (whatsappResponse) {
        logToFile("send otp whatsapp response " + JSON.stringify(whatsappResponse));
      }

      const data = { 'lastid': lastid, 'otp': code };
      return data;
    }

  } catch (error) {
    // PostgreSQL-specific error handling
    if (error.code === '23505') { // Unique violation
      if (error.message && error.message.includes('email')) {
        throw new Error('Email address already registerd');
      } else {
        throw new Error('Mobile No already registerd');
      }
    } else if (error.code === '23503') { // Foreign key violation
      throw new Error('Referenced record not found');
    } else if (error.code === '23502') { // Not null violation
      // Include column name from PostgreSQL error if available
      const column = error.column ? ` (column: ${error.column})` : '';
      throw new Error('Required field is missing' + column);
    }
    // Re-throw custom errors as-is
    throw error;
  }
}

const verifyOtpupdate = async (userdata) => {
  // Input validation and type conversion for PostgreSQL
  const lastid = parseInt(userdata.lastid);
  const otp = parseInt(userdata.otp);

  if (!lastid || isNaN(lastid) || !otp || isNaN(otp)) {
    throw new Error('Invalid request parameters');
  }

  // Use transaction for data consistency and atomicity
  return await knex.transaction(async (trx) => {
    // Optimized: Single query combining OTP validation, expiration check, and data retrieval
    // This reduces 3 database round trips to 1, significantly improving latency
    const user_details = await trx('tbl_mobile_email_update_info')
      .select('*')
      .where('id', lastid)
      .where('otp', otp)
      .where('expire_at', '>', trx.fn.now()) // PostgreSQL-compatible date comparison
      .first();

    // If query returns no result, determine the specific failure reason
    if (!user_details) {
      // Single additional query to check which validation failed (only if needed)
      const record = await trx('tbl_mobile_email_update_info')
        .select('otp', 'expire_at')
        .where('id', lastid)
        .first();

      if (!record) {
        throw new Error('Record not found');
      }

      // Check OTP mismatch first (more common failure)
      if (record.otp !== otp) {
        throw new Error('OTP is invalid');
      }

      // If OTP matches but query failed, it's expired
      throw new Error('OTP is expired');
    }

    // Update based on change type with proper error handling
    if (user_details.change_type?.toLowerCase() === 'email') {
      // Update user email and verify success
      const updatedUser = await trx('users')
        .where('id', user_details.user_id)
        .update({ 'email': user_details.new_info })
        .returning('id'); // PostgreSQL returns updated rows

      if (!updatedUser || updatedUser.length === 0) {
        throw new Error('Failed to update user email');
      }

      // Update status atomically in same transaction
      await trx('tbl_mobile_email_update_info')
        .where('id', lastid)
        .update({ 'status': 'completed' });

      return 1; // Email updated successfully
    } else {
      // Mobile number update - validate format before processing
      const mobile_code = user_details.new_info.split('-');

      if (mobile_code.length !== 3) {
        throw new Error('Invalid mobile number format');
      }

      // Update user mobile fields and verify success
      const updatedUser = await trx('users')
        .where('id', user_details.user_id)
        .update({
          'dial_code': mobile_code[0],
          'country_code': mobile_code[1],
          'user_phone': mobile_code[2]
        })
        .returning('id'); // PostgreSQL returns updated rows

      if (!updatedUser || updatedUser.length === 0) {
        throw new Error('Failed to update mobile number');
      }

      // Update status atomically in same transaction
      await trx('tbl_mobile_email_update_info')
        .where('id', lastid)
        .update({ 'status': 'completed' });

      return 2; // Mobile number updated successfully
    }
  });
}

const userDeactivate = async (userdata) => {
  const activate_deactivate_status = userdata.activate_deactivate_status;
  const deactivate_by = userdata.deactivate_by;
  const user_id = userdata.user_id;
  const country_code = userdata.country_code;
  const user_phone = userdata.user_phone;
  const platform = userdata.platform;
  const deactivate_datetime = new Date();

  if (platform === 'web') {
    const user_detail = await knex('users')
      .where('country_code', country_code)
      .where('user_phone', user_phone)
      .select('id', 'activate_deactivate_status', 'deactivate_datetime')
      .first();

    if (!user_detail) {
      throw new Error('User not Found');
    }
    if (user_detail.deactivate_datetime && user_detail.activate_deactivate_status === 'deactivate') {
      throw new Error('User is deactivated');
    }

    const updated = await knex('users')
      .where('user_phone', user_phone)
      .where('country_code', country_code)
      .update({
        activate_deactivate_status,
        deactivate_datetime,
        deactivate_by
      })
      .returning('id');

    if (!updated || updated.length === 0) {
      throw new Error('User not Found');
    }
    return updated;
  }

  // App: validate user exists and not already deactivated (single minimal SELECT)
  const user_detail = await knex('users')
    .where('id', user_id)
    .select('id', 'activate_deactivate_status', 'deactivate_datetime')
    .first();

  if (!user_detail) {
    throw new Error('User not Found');
  }
  if (user_detail.deactivate_datetime && user_detail.activate_deactivate_status === 'deactivate') {
    throw new Error('User is deactivated');
  }

  const updated = await knex('users')
    .where('id', user_id)
    .update({
      activate_deactivate_status,
      deactivate_datetime,
      deactivate_by
    })
    .returning('id');

  if (!updated || updated.length === 0) {
    throw new Error('User not Found');
  }
  return updated;
};


const getUsersforLogin = async (userdata) => {
  const imageurl = process.env.IMAGE_URL;
  // Optimized: PostgreSQL uses || for string concatenation (already correct)
  // Using COALESCE to handle NULL values properly
  let user = await knex('users')
    .where('user_phone', userdata.user_phone)
    .where('country_code', userdata.country_code)
    .select(
      'id',
      'name',
      'email',
      'user_phone',
      'dial_code',
      'otp_value',
      'status',
      'wallet',
      'rewards',
      'is_verified',
      'app_update',
      'referral_code',
      'noti_popup',
      'country_code',
      'activate_deactivate_status',
      'referral_amount',
      'referral_return_amount',
      knex.raw('COALESCE(?, \'\') || COALESCE(user_image, \'\') as user_image', [imageurl])
    )
    .first();

  return user;
};

const updateUser = async (id, updatedUser) => {
  return await knex('users').where({ id }).update(updatedUser);
};

const deleteUser = async (id) => {
  return await knex('users').where({ id }).del();
};

const bankDetails = async (appDetatils) => {
  user_id = appDetatils.user_id;
  users_acc_details = await knex('tbl_user_bank_details')
    .select('*')
    .where('user_id', user_id)
    .where('is_delete', '!=', '1')
    .where('bank_type', 'totalpay')
  // return users_acc_details
  const usersdetails = [];
  for (let j = 0; j < users_acc_details.length; j++) {
    const ProductList = users_acc_details[j];
    const first = ProductList.card_no.substr(0, 2)
    const end = ProductList.card_no.substr(-4)
    const customizedProduct = {
      id: ProductList.id,
      user_id: ProductList.user_id,
      holder_name: ProductList.holder_name,
      account_no: ProductList.card_no,
      first: ProductList.card_no.substr(0, 2),
      end: ProductList.card_no.substr(-4),
      card_no: first + "**********" + end,
      si_sub_ref_no: ProductList.si_sub_ref_no,
    };

    usersdetails.push(customizedProduct);

  }


  return usersdetails;


}

const register = async (userdata) => {
  const uuid = userdata.uuid;
  // Parallel: existing user, existing device, referrer (if code), clear unverified phone (1 round-trip)
  const [existingUser, existingUserDevice, referrerUser] = await Promise.all([
    knex('users')
      .where('user_phone', userdata.user_phone)
      .where('country_code', userdata.country_code)
      .where('is_verified', 1)
      .first(),
    knex('users')
      .where('uuid', uuid)
      .where('is_verified', 0)
      .first(),
    userdata.referral_code
      ? knex('users')
        .where('referral_code', userdata.referral_code)
        .where('is_verified', 1)
        .first()
      : Promise.resolve(null),
    knex('users')
      .where('user_phone', userdata.user_phone)
      .where('is_verified', 0)
      .update({ user_phone: '' })
  ]);


  const hash = crypto.createHash('md5').update(String(Date.now())).digest('hex');
  const hashSubstring = hash.substring(0, 2); // Adjust substring length as needed
  //   const referralCode = generateRandomLetters(6) + generateRandomDigits(2) + hashSubstring;
  const referralCode = userdata.name.substring(0, 4).toUpperCase() + generateRandomDigits(4);
  let userId;
  var code = Math.floor(1000 + Math.random() * 9000);
  const expiresAt = new Date(Date.now() + 60 * 1000); // 60 seconds

  if (existingUser) {
    //return existingUser.is_verified
    if (existingUser.is_verified == 1) {
      throw new Error('Registered');
    }
    else if (existingUser.activate_deactivate_status === 'deactivate') {
      throw new Error('deactivate');
    } else {
      throw new Error('Already exist');
    }
  } else {

    // const hash = bcrypt.hashSync(userdata.password, 10);
    let date_ob = new Date();
    const secret = speakeasy.generateSecret({ length: 20 });

    // Generate a TOTP code using the secret key 
    // const code = speakeasy.totp({ 
    //     secret: secret.base32, 
    //     encoding: 'base32'
    // });
    // Ensure the code is 4 digits
    //code = code.slice(0, 4);

    if (existingUserDevice && uuid) {

      let date_ob = new Date();
      const secret = speakeasy.generateSecret({ length: 20 });

      // Generate a TOTP code using the secret key 
      // const code = speakeasy.totp({ 
      //     secret: secret.base32, 
      //     encoding: 'base32'
      // });
      // Ensure the code is 4 digits
      //  const code = 1234
      update_app_open = await knex('users')
        .where('id', existingUserDevice.id)
        .update({
          "email": userdata.user_email,
          "user_phone": userdata.user_phone,
          "country_code": userdata.country_code,
          "name": userdata.name,
          "referral_code": referralCode,
          "is_terms_cond_unable": userdata.is_terms_cond_unable,
          "is_whatapp_msg_unable": userdata.is_whatapp_msg_unable,
          "reg_date": date_ob,
          "dial_code": userdata.dial_code,
          "otp_value": code,
          "expire_at": expiresAt,
          "actual_device_id": userdata.actual_device_id,
          "device_id": userdata.device_id
        });

      userId = existingUserDevice.id;

    } else {
      const vreferral_amount = (referrerUser && Number(referrerUser.referral_amount) > 0)
        ? referrerUser.referral_amount
        : 0;

      const [maxIdRow, maxNotiIdRow] = await Promise.all([
        knex('users').max('id as max_id').first(),
        knex('notificationby').max('noti_id as max_id').first()
      ]);
      const nextId = (maxIdRow && maxIdRow.max_id != null) ? Number(maxIdRow.max_id) + 1 : 1;
      const nextNotiId = (maxNotiIdRow && maxNotiIdRow.max_id != null) ? Number(maxNotiIdRow.max_id) + 1 : 1;

      const user = await knex('users')
        .insert({
          id: nextId,
          user_phone: userdata.user_phone,
          email: userdata.user_email,
          country_code: userdata.country_code,
          name: userdata.name,
          referral_code: referralCode,
          is_terms_cond_unable: userdata.is_terms_cond_unable,
          is_whatapp_msg_unable: userdata.is_whatapp_msg_unable,
          reg_date: date_ob,
          dial_code: userdata.dial_code,
          otp_value: code,
          expire_at: expiresAt,
          actual_device_id: userdata.actual_device_id,
          device_id: userdata.device_id,
          uuid: knex.raw('gen_random_uuid()'),
          referral_amount: vreferral_amount,
          coupon_referral_amount: 10,
          referral_return_amount: 5,
          order_referral_return_amount: 5
        })
        .returning('id');

      userId = user[0].id;
      await knex('notificationby').insert({
        noti_id: nextNotiId,
        user_id: userId,
        sms: 1,
        app: 1,
        email: 1
      });
    }

    // Reuse referrer fetched in parallel at top (one less round-trip)
    if (referrerUser) {
      const UserDetails = referrerUser;
      if (Number(UserDetails.referral_amount) > 0) {
        const [isrefferalExist, maxRefTempIdRow] = await Promise.all([
          knex('tbl_referral_temp').where('user_id', userId).first(),
          knex('tbl_referral_temp').max('id as max_id').first()
        ]);
        const nextRefTempId = (maxRefTempIdRow && maxRefTempIdRow.max_id != null) ? Number(maxRefTempIdRow.max_id) + 1 : 1;
        if (!isrefferalExist) {
          await knex('tbl_referral_temp')
            .insert({
              id: nextRefTempId,
              user_id: userId,
              referral_by: UserDetails.id,
              referral_code: userdata.referral_code,
              referral_amount: UserDetails.referral_amount,
              coupon_referral_amount: UserDetails.coupon_referral_amount,
              referral_return_amount: UserDetails.referral_return_amount,
              order_referral_return_amount: UserDetails.order_referral_return_amount,
              created_at: new Date()
            });
        } else {
          await knex('tbl_referral_temp')
            .where('user_id', userId)
            .update({
              user_id: userId,
              referral_by: UserDetails.id,
              referral_code: userdata.referral_code,
              referral_amount: UserDetails.referral_amount,
              coupon_referral_amount: UserDetails.coupon_referral_amount,
              referral_return_amount: UserDetails.referral_return_amount,
              order_referral_return_amount: UserDetails.order_referral_return_amount,
              created_at: new Date()
            });
        }
        // const UpdateUser = await knex('users')
        // .where('id', userId)
        // .update({'wallet':UserDetails.referral_amount});

        // await knex('wallet_history').insert({
        // user_id: userId,
        // amount: UserDetails.referral_amount,
        // resource: 'referral',
        // type:'Add',
        // group_id:"",
        // cart_id:""
        // });
      }

    }

    if (referrerUser && Number(referrerUser.referral_return_amount) > 0 && Number(referrerUser.coupon_referral_amount) > 0) {
      const UserDetails = referrerUser;
      const [isrefferalExist, maxRefTempIdRow2, maxCouponIdRow] = await Promise.all([
        knex('tbl_referral_temp').where('user_id', userId).first(),
        knex('tbl_referral_temp').max('id as max_id').first(),
        knex('coupon').max('coupon_id as max_id').first()
      ]);
      const nextRefTempId2 = (maxRefTempIdRow2 && maxRefTempIdRow2.max_id != null) ? Number(maxRefTempIdRow2.max_id) + 1 : 1;
      const nextCouponId = (maxCouponIdRow && maxCouponIdRow.max_id != null) ? Number(maxCouponIdRow.max_id) + 1 : 1;
      if (!isrefferalExist) {
        await knex('tbl_referral_temp')
          .insert({
            id: nextRefTempId2,
            user_id: userId,
            referral_by: UserDetails.id,
            referral_code: userdata.referral_code,
            referral_amount: UserDetails.referral_amount,
            coupon_referral_amount: UserDetails.coupon_referral_amount,
            referral_return_amount: UserDetails.referral_return_amount,
            order_referral_return_amount: UserDetails.order_referral_return_amount,
            created_at: new Date()
          });
      } else {
        await knex('tbl_referral_temp')
          .where('user_id', userId)
          .update({
            user_id: userId,
            referral_by: UserDetails.id,
            referral_code: userdata.referral_code,
            referral_amount: UserDetails.referral_amount,
            coupon_referral_amount: UserDetails.coupon_referral_amount,
            referral_return_amount: UserDetails.referral_return_amount,
            order_referral_return_amount: UserDetails.order_referral_return_amount,
            created_at: new Date()
          });
      }

      // const UpdateUser = await knex('users')
      // .where('id', UserDetails.id)
      // .increment({'wallet': UserDetails.referral_return_amount});

      // await knex('wallet_history').insert({
      // user_id: UserDetails.id,
      // amount: UserDetails.referral_return_amount,
      // resource: 'referral_return amount_after_registration',
      // type:'Add',
      // group_id:"",
      // cart_id:""
      // });

      const coupon_code = Math.random().toString(36).substring(2, 8).toUpperCase() + UserDetails.coupon_referral_amount;
      const valid_from = new Date();
      const valid_to = new Date();
      valid_to.setDate(valid_from.getDate() + 180);
      await knex('coupon').insert({
        coupon_id: nextCouponId,
        user_id: userId,
        coupon_name: 'User Coupon',
        coupon_image: 'N/A',
        coupon_description: (UserDetails.coupon_referral_amount) ? 'Get ' + UserDetails.coupon_referral_amount + '% off' : 'Get 10% off',
        coupon_code: coupon_code,
        start_date: valid_from,
        end_date: valid_to,
        type: 'percentage',
        uses_restriction: 1,
        amount: (UserDetails.coupon_referral_amount) ? UserDetails.coupon_referral_amount : 10,
        cart_value: 0,
        coupon_visibility: 'Show To Customer',
        store_id: 7,
        referrer: UserDetails.id,
      });
    }
  }

  const phone_with_country_code = `${userdata.country_code}${userdata.user_phone}`;
  const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
  const otpString = '' + code + '';

  const payload = {
    apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
    campaignName: 'Otp_verification_code',
    destination: "+" + phone_with_country_code,
    userName: 'Quickart General Trading Co LLC',
    templateParams: [otpString],
    source: 'new-landing-page form',
    media: {},
    buttons: [
      {
        type: 'button',
        sub_type: 'url',
        index: 0,
        parameters: [
          {
            type: 'text',
            text: otpString
          }
        ]
      }
    ],
    carouselCards: [],
    location: {},
    attributes: {},
    paramsFallbackValue: {
      FirstName: 'user'
    }
  };

  await knex('users')
    .where('user_phone', userdata.user_phone)
    .where('country_code', userdata.country_code)
    .update({ otp_value: code, expire_at: expiresAt });

  fetch(apiUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  }).catch(err => logToFile('register aisensy fetch failed: ' + (err && err.message)));
};

const findUser = async (userdata) => {
  const imageurl = process.env.IMAGE_URL;
  const existingUser = await knex('users').where('user_phone', userdata.user_phone).where('country_code', userdata.country_code).first();
  //const existingUser = await knex('users').where('user_phone', userdata.user_phone).first();
  const expiresAt = new Date(Date.now() + 60 * 1000); // 60 seconds
  if (existingUser) {

    if (userdata.user_phone == '541234567') {
      var code = 1234;
    } else {
      var code = Math.floor(1000 + Math.random() * 9000);
    }

    if (existingUser.is_verified == 1) {
      if (existingUser.activate_deactivate_status === 'deactivate') {
        throw new Error('deactivate');
      } else {
        const secret = speakeasy.generateSecret({ length: 20 });

        // Generate a TOTP code using the secret key 
        // const code = speakeasy.totp({ 
        //     secret: secret.base32, 
        //     encoding: 'base32'
        // });
        // Ensure the code is 4 digits

        // const code = 1234
        //await knex('users').where('user_phone', userdata.user_phone).update({device_id: userdata.device_id});
        await knex('users').where('user_phone', userdata.user_phone)
          .where('country_code', userdata.country_code)
          .update({ otp_value: code, expire_at: expiresAt });
      }
    } else {
      if (existingUser.is_verified == 0) {
        const secret = speakeasy.generateSecret({ length: 20 });

        // Generate a TOTP code using the secret key 
        // const code = speakeasy.totp({ 
        //     secret: secret.base32, 
        //     encoding: 'base32'
        // });
        // Ensure the code is 4 digits

        // const code = 1234
        const updatevalue = await knex('users').where('user_phone', userdata.user_phone).where('country_code', userdata.country_code)
          .update({ otp_value: code, expire_at: expiresAt });
      }
    }

    // Get updated user with all required fields (combining getUsersforLogin functionality)
    const updatedUser = await knex('users').where('user_phone', userdata.user_phone)
      .where('country_code', userdata.country_code)
      .select('id', 'name', 'email', 'user_phone', 'dial_code', 'otp_value', 'status', 'wallet', 'rewards', 'is_verified', 'app_update', 'referral_code', 'noti_popup'
        , 'country_code', 'activate_deactivate_status', 'referral_amount', 'referral_return_amount', knex.raw('COALESCE(?, \'\') || COALESCE(user_image, \'\') as user_image', [imageurl]))
      .first();

    // Send WhatsApp OTP asynchronously (fire and forget) - don't block response
    const phone_with_country_code = `${userdata.country_code}${userdata.user_phone}`;
    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
    const otpString = "" + code + "";

    const payload = {
      apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      campaignName: 'Otp_verification_code',
      destination: "+" + phone_with_country_code,
      userName: 'Quickart General Trading Co LLC',
      templateParams: [otpString],
      source: 'new-landing-page form',
      media: {},
      buttons: [
        {
          type: 'button',
          sub_type: 'url',
          index: 0,
          parameters: [
            {
              type: 'text',
              text: otpString
            }
          ]
        }
      ],
      carouselCards: [],
      location: {},
      attributes: {},
      paramsFallbackValue: {
        FirstName: 'user'
      }
    };

    // Send WhatsApp asynchronously - don't await, don't block response
    fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    })
      .then(response => response.json())
      .then(result => {
        logToFile("send otp whatsapp response " + JSON.stringify(result));
      })
      .catch(error => {
        logToFile("send otp whatsapp error " + JSON.stringify(error.message));
      });

    return updatedUser;

  }
  else {
    await knex('users').where('user_phone', userdata.user_phone).where('country_code', userdata.country_code).where('is_verified', 0).del();
    let date_ob = new Date();


    //      const [userId] =   await knex('users').insert({
    //       user_phone: userdata.user_phone,
    //       country_code:userdata.country_code,
    //       name: "User",
    //       is_verified: 0,
    //       reg_date: date_ob,
    //       dial_code:userdata.dial_code,
    //       user_type: "guest"
    //      })
    //      .returning('id');

    //   await knex('notificationby').insert({
    //           user_id : userId,
    //           sms : 1,
    //           app : 1,
    //           email : 1
    //       });
    throw new Error('Register');


  }

}

const verifyOtp = async (userdata) => {
  const uuid = userdata.uuid;
  // Optimized: Single query to get user with all needed fields
  const existingUser = await knex('users')
    .where('user_phone', userdata.user_phone)
    .where('country_code', userdata.country_code)
    .first();

  if (!existingUser) {
    throw new Error('User not registered');
  }

  // Validate OTP
  if (existingUser.otp_value != userdata.otp) {
    // Check expiration before throwing wrong OTP error
    if (existingUser.expire_at) {
      const is_expired = await knex('users')
        .where('user_phone', userdata.user_phone)
        .where('expire_at', '>', knex.fn.now())
        .first();

      if (!is_expired) {
        throw new Error('OTP is expired');
      }
    }
    throw new Error('Wrong OTP');
  }

  // Check OTP expiration - optimized query
  if (existingUser.expire_at) {
    const is_expired = await knex('users')
      .where('user_phone', userdata.user_phone)
      .where('expire_at', '>', knex.fn.now())
      .first();

    if (!is_expired) {
      throw new Error('OTP is expired');
    }
  }

  // Handle first-time verification (welcome messages, emails)
  const isFirstVerification = existingUser.is_verified != 1;

  if (isFirstVerification) {
    const phone_with_country_code = `${existingUser.country_code}${userdata.user_phone}`;

    // Parallel execution of independent operations for better latency
    const [logo, currency, referralCodeDetails] = await Promise.all([
      knex('tbl_web_setting').first(),
      knex('currency').first(),
      knex('tbl_referral').where('user_id', existingUser.id).first()
    ]);

    // Fire and forget for non-critical operations (don't block response)
    welcomeMessage(phone_with_country_code).catch(err => logToFile("WhatsApp welcome message failed: " + err.message));

    const templateData = {
      baseurl: process.env.BASE_URL,
      user_name: existingUser.name,
      user_email: existingUser.email,
      app_name: logo ? logo.name : null,
      currency_sign: currency ? currency.currency_sign : null,
    };
    const subject = 'Welcome to QuicKart';

    // Fire and forget email (don't block response)
    welcomeMail(existingUser.email, templateData, subject).catch(err => logToFile("Welcome email failed: " + err.message));

    // Handle QKFIVE referral code WhatsApp message
    if (referralCodeDetails && referralCodeDetails.referral_code == 'QKFIVE') {
      const url = 'https://apisocial.telebu.com/whatsapp-api/v1.0/customer/96345/bot/6748075e65d94186/template';
      const headers = {
        'Authorization': 'Basic 1072eb29-af12-4a65-ab5b-a5a8bfb9ff09-HwOnAFh',
        'Content-Type': 'application/json'
      };
      const data = {
        payload: {
          name: 'referral',
          components: [],
          language: {
            code: 'en_US',
            policy: 'deterministic'
          },
          namespace: 'a95f9847_fb73_48ca_bed5_b5c5b3fbc1bc'
        },
        phoneNumber: phone_with_country_code
      };

      // Fire and forget (don't block response)
      fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify(data)
      }).then(response => response.json())
        .catch(error => logToFile("Referral WhatsApp message failed: " + error.message));
    }
  }

  // Use transaction for related database operations to ensure data consistency
  await knex.transaction(async (trx) => {
    // Update user verification status
    await trx('users')
      .where('user_phone', userdata.user_phone)
      .update({ is_verified: 1 });

    // Handle referral logic with proper type conversions
    const referralAmount = parseFloat(existingUser.referral_amount) || 0;
    if (referralAmount > 0) {
      // Parallel fetch of referral data
      const [isrefferalTempExist, isrefferalExist] = await Promise.all([
        trx('tbl_referral_temp').where('user_id', existingUser.id).first(),
        trx('tbl_referral').where('user_id', existingUser.id).first()
      ]);

      if (isrefferalTempExist) {
        const referralAmountValue = parseFloat(isrefferalTempExist.referral_amount) || 0;

        if (!isrefferalExist) {
          const maxRefIdRow = await trx('tbl_referral').max('id as max_id').first();
          const nextRefId = (maxRefIdRow?.max_id != null ? Number(maxRefIdRow.max_id) + 1 : 1);
          // Insert into tbl_referral (not tbl_referral_temp) - FIXED BUG
          await trx('tbl_referral').insert({
            id: nextRefId,
            user_id: isrefferalTempExist.user_id,
            referral_by: isrefferalTempExist.referral_by,
            referral_code: isrefferalTempExist.referral_code,
            referral_amount: isrefferalTempExist.referral_amount,
            coupon_referral_amount: isrefferalTempExist.coupon_referral_amount,
            referral_return_amount: isrefferalTempExist.referral_return_amount,
            order_referral_return_amount: isrefferalTempExist.order_referral_return_amount,
            created_at: trx.fn.now() // PostgreSQL compatible - use trx in transaction context
          });
        } else {
          // Update existing referral record - FIXED: use existingUser.id instead of undefined userId
          await trx('tbl_referral')
            .where('user_id', existingUser.id)
            .update({
              referral_by: isrefferalTempExist.referral_by,
              referral_code: isrefferalTempExist.referral_code,
              referral_amount: isrefferalTempExist.referral_amount,
              coupon_referral_amount: isrefferalTempExist.coupon_referral_amount,
              referral_return_amount: isrefferalTempExist.referral_return_amount,
              order_referral_return_amount: isrefferalTempExist.order_referral_return_amount
            });
        }

        // Next w_id for wallet_history (PG: w_id is PK, no default)
        const maxWIdRef = await trx('wallet_history').max('w_id as maxWId').first();
        const nextWIdRef = (maxWIdRef?.maxWId != null ? parseInt(maxWIdRef.maxWId, 10) : 0) + 1;
        // Update referral_balance: read current (may be null), add amount, set explicitly so float column is never left null
        const userRow = await trx('users').where('id', existingUser.id).select('referral_balance').first();
        const currentRefBalance = (userRow && userRow.referral_balance != null) ? parseFloat(userRow.referral_balance) : 0;
        const newRefBalance = currentRefBalance + parseFloat(referralAmountValue);
        await Promise.all([
          trx('users')
            .where('id', existingUser.id)
            .update({ referral_balance: newRefBalance }),
          trx('wallet_history').insert({
            w_id: nextWIdRef,
            user_id: existingUser.id,
            amount: String(referralAmountValue), // Convert to string as per schema
            resource: 'referral',
            type: 'Add',
            group_id: "",
            cart_id: "",
            expiry_date: knex.raw("NOW() + INTERVAL '3 months'"),
            created_at: trx.fn.now() // Use trx in transaction context
          })
        ]);

        // Clean up temp referral if conditions met
        const returnAmount = parseFloat(isrefferalTempExist.referral_return_amount) || 0;
        const couponAmount = parseFloat(isrefferalTempExist.coupon_referral_amount) || 0;

        if (returnAmount == 0 && couponAmount == 0) {
          await trx('tbl_referral_temp')
            .where('user_id', isrefferalTempExist.user_id)
            .delete();
        }
      }
    }

    // Handle referral return amount logic
    const referralReturnAmount = parseFloat(existingUser.referral_return_amount) || 0;
    const couponReferralAmount = parseFloat(existingUser.coupon_referral_amount) || 0;

    if (referralReturnAmount > 0 && couponReferralAmount > 0) {
      // Parallel fetch
      const [isrefferalExist, isrefferalTempExist] = await Promise.all([
        trx('tbl_referral').where('user_id', existingUser.id).first(),
        trx('tbl_referral_temp').where('user_id', existingUser.id).first()
      ]);

      if (isrefferalTempExist) {
        const returnAmountValue = parseFloat(isrefferalTempExist.referral_return_amount) || 0;

        if (!isrefferalExist) {
          const maxRefIdRow2 = await trx('tbl_referral').max('id as max_id').first();
          const nextRefId2 = (maxRefIdRow2?.max_id != null ? Number(maxRefIdRow2.max_id) + 1 : 1);
          await trx('tbl_referral').insert({
            id: nextRefId2,
            user_id: isrefferalTempExist.user_id,
            referral_by: isrefferalTempExist.referral_by,
            referral_code: isrefferalTempExist.referral_code,
            referral_amount: isrefferalTempExist.referral_amount,
            coupon_referral_amount: isrefferalTempExist.coupon_referral_amount,
            referral_return_amount: isrefferalTempExist.referral_return_amount,
            order_referral_return_amount: isrefferalTempExist.order_referral_return_amount,
            created_at: trx.fn.now() // Use trx in transaction context
          });
        } else {
          // FIXED: use existingUser.id instead of undefined userId
          await trx('tbl_referral')
            .where('user_id', existingUser.id)
            .update({
              referral_by: isrefferalTempExist.referral_by,
              referral_code: isrefferalTempExist.referral_code,
              referral_amount: isrefferalTempExist.referral_amount,
              coupon_referral_amount: isrefferalTempExist.coupon_referral_amount,
              referral_return_amount: isrefferalTempExist.referral_return_amount,
              order_referral_return_amount: isrefferalTempExist.order_referral_return_amount
            });
        }

        // Next w_id for wallet_history (PG: w_id is PK, no default)
        const maxWIdReturn = await trx('wallet_history').max('w_id as maxWId').first();
        const nextWIdReturn = (maxWIdReturn?.maxWId != null ? parseInt(maxWIdReturn.maxWId, 10) : 0) + 1;
        // Update referrer referral_balance: read current (may be null), add amount, set explicitly
        const referrerRow = await trx('users').where('id', isrefferalTempExist.referral_by).select('referral_balance').first();
        const referrerCurrentRefBalance = (referrerRow && referrerRow.referral_balance != null) ? parseFloat(referrerRow.referral_balance) : 0;
        const referrerNewRefBalance = referrerCurrentRefBalance + parseFloat(returnAmountValue);
        await Promise.all([
          trx('tbl_referral_temp')
            .where('user_id', isrefferalTempExist.user_id)
            .delete(),
          trx('users')
            .where('id', isrefferalTempExist.referral_by)
            .update({ referral_balance: referrerNewRefBalance }),
          trx('wallet_history').insert({
            w_id: nextWIdReturn,
            user_id: isrefferalTempExist.referral_by,
            amount: String(returnAmountValue),
            resource: 'referral_return_amount_after_registration',
            type: 'Add',
            group_id: "",
            cart_id: "",
            expiry_date: knex.raw("NOW() + INTERVAL '3 months'"),
            created_at: trx.fn.now() // Use trx in transaction context
          })
        ]);
      }
    }
  });

  // Optimized: Fetch guest user data in parallel for transfer detection
  const userDetails = await knex('users').where('uuid', uuid).first();

  let addressDetail = 'false';
  let userBankDetail = 'false';
  let ordersDetail = 'false';
  let subscriptionOrderDetail = 'false';

  if (userDetails) {
    // Parallel fetch of all related data for better latency
    const [addrDetails, bankDetails, ordDetails, subOrderDetails] = await Promise.all([
      knex('address').where('user_id', String(userDetails.id)).first(),
      knex('tbl_user_bank_details').where('user_id', userDetails.id).first(),
      knex('orders').where('user_id', String(userDetails.id)).first(),
      knex('subscription_order')
        .where('order_status', 'Pending')
        .where('user_id', userDetails.id)
        .first()
    ]);

    addressDetail = addrDetails ? 'true' : 'false';
    userBankDetail = bankDetails ? 'true' : 'false';
    ordersDetail = ordDetails ? 'true' : 'false';
    subscriptionOrderDetail = subOrderDetails ? 'true' : 'false';
  }

  const ErrorMessage = (subscriptionOrderDetail == 'true')
    ? "Click Yes to transfer the guest data to your account"
    : "Would you like to transfer the guest data to your account? Click Yes to transfer, or No to decline. Guest data will not be accessible to you once you click No";

  const customizedProduct = {
    appuserid: userDetails ? userDetails.id : "",
    appuuid: userdata.uuid,
    serveruserid: existingUser.id,
    serveruuid: existingUser.uuid,
    addressdetail: addressDetail,
    userbankdetail: userBankDetail,
    ordersdetail: ordersDetail,
    subscriptionOrderDetail: subscriptionOrderDetail,
    message: ErrorMessage
  };

  return customizedProduct;
};

const resendOtp = async (userdata) => {
  const user_phone = userdata.user_phone;
  const country_code = userdata.country_code;

  // Latency: single round-trip – fetch firebase, smsby, and user in parallel
  const [firebase, smsby, existingUser] = await Promise.all([
    knex('firebase').first(),
    knex('smsby').first(),
    knex('users')
      .where('user_phone', user_phone)
      .where('country_code', country_code)
      .first()
  ]);

  if (!existingUser) {
    throw new Error('User not found');
  }

  const otp = Math.floor(1000 + Math.random() * 9000);
  const otpString = String(otp); // PostgreSQL users.otp_value is TEXT

  try {
    // Single update with PostgreSQL .returning() – verify one row updated
    const updated = await knex('users')
      .where('user_phone', user_phone)
      .where('country_code', country_code)
      .update({ otp_value: otpString, expire_at: null })
      .returning('id');

    if (!updated || updated.length === 0) {
      throw new Error('Failed to update OTP');
    }

    const phone_with_country_code = `${country_code}${user_phone}`;
    const apiUrl = 'https://backend.aisensy.com/campaign/t1/api/v2';
    const payload = {
      apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyYiIsIm5hbWUiOiJRdWlja2FydCBHZW5lcmFsIFRyYWRpbmcgQ28gTExDIiwiYXBwTmFtZSI6IkFpU2Vuc3kiLCJjbGllbnRJZCI6IjY3NjE1ZGJmODRjN2RiMjVlMzg0NGMyNSIsImFjdGl2ZVBsYW4iOiJGUkVFX0ZPUkVWRVIiLCJpYXQiOjE3MzQ0MzQyMzl9.FXBdWtjPyBXl0AONmLnOZa6zuInsaQaa8MtWvOAyZCs",
      campaignName: 'Otp_verification_code',
      destination: "+" + phone_with_country_code,
      userName: 'Quickart General Trading Co LLC',
      templateParams: [otpString],
      source: 'new-landing-page form',
      media: {},
      buttons: [{
        type: 'button',
        sub_type: 'url',
        index: 0,
        parameters: [{ type: 'text', text: otpString }]
      }],
      carouselCards: [],
      location: {},
      attributes: {},
      paramsFallbackValue: { FirstName: 'user' }
    };

    // Latency: run WhatsApp send and conditional DB update in parallel (no wait on external API before update)
    const conditionalUpdate = (async () => {
      if (firebase && firebase.status === 1) {
        const cleared = await knex('users')
          .where('user_phone', user_phone)
          .where('country_code', country_code)
          .update({ otp_value: null })
          .returning('id');
        return cleared && cleared.length > 0 ? cleared[0] : null;
      }
      if (smsby && smsby.status === 1) {
        const set = await knex('users')
          .where('user_phone', user_phone)
          .where('country_code', country_code)
          .update({ otp_value: otpString })
          .returning('id');
        return set && set.length > 0 ? set[0] : null;
      }
      return updated[0];
    })();

    const whatsappSend = fetch(apiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    }).then(async (response) => {
      const result = await response.json();
      logToFile("send otp whatsapp response " + JSON.stringify(result));
      return result;
    }).catch((err) => {
      logToFile("send otp whatsapp error " + JSON.stringify(err));
      return null;
    });

    await Promise.all([conditionalUpdate, whatsappSend]);

    return updated[0];
  } catch (error) {
    if (error.code === '23505') {
      throw new Error('Mobile No already registerd');
    }
    if (error.code === '23503') {
      throw new Error('Referenced record not found');
    }
    if (error.code === '23502') {
      const column = error.column ? ` (column: ${error.column})` : '';
      throw new Error('Required field is missing' + column);
    }
    throw error;
  }
};

const editprofile = async (appDetatils) => {
  const user_id = appDetatils.user_id
  const user_name = appDetatils.user_name
  const user_city = appDetatils.user_city
  const user_area = appDetatils.user_area
  const user_email = appDetatils.user_email
  const user_phone = appDetatils.user_phone
  const user_image = appDetatils.user_image
  const device_id = appDetatils.device_id
  const uu = await knex('users')
    .where('id', user_id)
    .first();
  if ($uu.user_phone == "9999999999") {
    throw new Error('You can not change the details for demo account');
  }

}

const myprofile = async (appDetatils) => {
  const user_id = appDetatils.user_id
  const user = await knex('users')
    .join('city', 'users.user_city', '=', 'city.city_id')
    .join('society', 'users.user_area', '=', 'society.society_id')
    .select('users.*', 'city.city_name', 'society.society_name')
    .where('users.id', user_id)
    .first();
  const order = await knex('orders')
    .join('store', 'orders.store_id', '=', 'store.id')
    .join('users', 'orders.user_id', '=', 'users.id')
    .join('address', 'orders.address_id', '=', 'address.address_id')
    .where('orders.user_id', user_id)
    .whereNotNull('orders.order_status')
    .whereNotNull('orders.payment_method')
    .count();
  const orderspent = await knex('orders')
    .where('order_status', '!=', 'Cancelled')
    .whereNotNull('orders.order_status')
    .whereNotNull('orders.payment_method')
    .where('payment_method', '!=', 'COD')
    .where('payment_method', '!=', 'cod')
    .where('user_id', $user_id)
    .sum('total_price');
  const ordersaved = await knex('orders')
    .select(knex.raw('SUM(total_products_mrp)- SUM(price_without_delivery)+SUM(coupon_discount) as overalldiscount'))
    .where('order_status', '!=', 'Cancelled')
    .where('order_status', '!=', NULL)
    .where('payment_method', '!=', NULL)
    .where('payment_method', '!=', 'COD')
    .where('payment_method', '!=', 'cod')
    .where('user_id', user_id)
    .first();
  return 123;
  if (user) {
    return ordersaved;
  }
}

const createImage = async (imageData) => {
  // Extract and validate input data
  const user_id = imageData.user_id;
  const user_phone = imageData.user_phone;
  const country_code = imageData.country_code;
  const device_id = imageData.device_id;
  const user_name = imageData.user_name;
  const image = imageData.path;

  // Ensure user_id is integer for PostgreSQL (optimized type conversion)
  const userIdInt = typeof user_id === 'string' ? parseInt(user_id, 10) : user_id;

  if (!userIdInt || isNaN(userIdInt)) {
    throw new Error('Invalid user ID');
  }

  // Get user by ID (primary key lookup - fastest)
  const uu = await knex('users')
    .where('id', userIdInt)
    .first();

  if (!uu) {
    throw new Error('User not found');
  }

  // Check for demo account protection
  if (uu.user_phone === "9999999999") {
    throw new Error('You can not change the details for demo account');
  }

  // Determine file path (use new image if provided, otherwise keep existing)
  const filePath = image || uu.user_image || null;

  // Check if phone number already exists for another user (only if phone is being updated)
  // Performance: Only check if phone/country_code are different from current
  if (user_phone && country_code && (user_phone !== uu.user_phone || country_code !== uu.country_code)) {
    const checkUser = await knex('users')
      .where('user_phone', user_phone)
      .where('country_code', country_code)
      .where('id', '!=', userIdInt)
      .first();

    // Optional: Uncomment if you want to prevent phone number reuse
    // if (checkUser && checkUser.is_verified === 1) {
    //   throw new Error('Phone number already registered');
    // }
  }

  // Prepare update data (only include fields that are provided)
  const updateData = {};
  if (filePath !== null) {
    updateData.user_image = filePath;
  }
  if (user_name) {
    updateData.name = user_name;
  }
  if (user_phone) {
    updateData.user_phone = user_phone;
  }
  if (country_code) {
    updateData.country_code = country_code;
  }
  if (device_id) {
    updateData.device_id = device_id;
  }

  // Performance optimization: Only update if there's data to update
  if (Object.keys(updateData).length > 0) {
    // Optimized: Update user data (single indexed query by primary key)
    await knex('users')
      .where('id', userIdInt)
      .update(updateData);
  }

  // Optimized: Single SELECT with computed image URL (indexed lookup by primary key)
  // Uses primary key index - very fast, minimal latency
  const imgurl = process.env.IMAGE_URL || '';
  const user = await knex('users')
    .where('id', userIdInt)
    .select(
      '*',
      knex.raw(`? || COALESCE(user_image, '') as user_image`, [imgurl])
    )
    .first();

  return user;
};


const getFaqsList = async (userdata) => {
  const imageurl = process.env.IMAGE_URL;
  let faqslist = await knex('tbl_faqs')
    .select('id', 'question', 'answer');
  return faqslist;
};

// Generate random letters, digits, and hash substring
const generateRandomLetters = (length) => {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
};

const generateRandomDigits = (length) => {
  const chars = "0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
};

const getShowProfile = async (userdata) => {
  // Validate and parse user_id early (fail fast for better performance)
  const user_id = parseInt(userdata.user_id, 10);
  if (isNaN(user_id) || user_id <= 0) {
    throw new Error('Invalid user_id');
  }

  // Optimized PostgreSQL query - using || operator (fastest string concatenation)
  // NULLIF handles empty strings, COALESCE ensures safe concatenation
  // This approach is more performant than CASE WHEN while handling all edge cases
  let userlist = await knex('users')
    .select('*')
    .select(knex.raw(`COALESCE('https://quickart.b-cdn.net/' || NULLIF(user_image, ''), NULL) AS user_image`))
    .where('id', user_id)
    .first();

  // Handle user not found (early return for performance)
  if (!userlist) {
    throw new Error('User not found');
  }

  // Build referral message only if referral_code exists (avoid unnecessary string operations)
  userlist.referral_message = '';
  if (userlist.referral_code) {
    userlist.referral_message = 'Free delivery, no minimum spend, and super fresh products - all on the QuicKart app! \n Use my referral code ' + userlist.referral_code + ' to get an exclusive 10% off your order. \n ✨ Sign up now and shop the freshness!';
  }

  return userlist;
};

const verifyDetails = async (appDetails) => {
  let { transfertype, appuuid, serveruuid, appuserid, serveruserid } = appDetails;

  // Normalize user IDs to integers for users table operations (PostgreSQL optimization)
  appuserid = typeof appuserid === 'string' ? parseInt(appuserid, 10) : appuserid;
  serveruserid = typeof serveruserid === 'string' ? parseInt(serveruserid, 10) : serveruserid;

  // Helper function for conditional updates with proper type conversions and transaction for performance
  const updateUserReferences = async (oldUserId, newUserId) => {
    // Convert to appropriate types based on PostgreSQL schema
    const oldUserIdInt = typeof oldUserId === 'string' ? parseInt(oldUserId, 10) : oldUserId;
    const newUserIdInt = typeof newUserId === 'string' ? parseInt(newUserId, 10) : newUserId;
    const oldUserIdText = String(oldUserId);
    const newUserIdText = String(newUserId);

    // Use transaction for atomicity and better performance (single round trip)
    return knex.transaction(async (trx) => {
      await Promise.all([
        // Integer fields - update in parallel within transaction
        trx('tbl_user_bank_details')
          .where('user_id', oldUserIdInt)
          .update({ "user_id": newUserIdInt }),
        trx('subscription_order')
          .where('user_id', oldUserIdInt)
          .update({ "user_id": newUserIdInt }),
        // Text fields - update in parallel within transaction
        trx('orders')
          .where('user_id', oldUserIdText)
          .update({ "user_id": newUserIdText }),
        trx('store_orders')
          .where('store_approval', oldUserIdText)
          .update({ "store_approval": newUserIdText }),
        trx('address')
          .where('user_id', oldUserIdText)
          .update({ "user_id": newUserIdText, "type": "Others" })
      ]);
    });
  };

  // Verify UUIDs and transfer type
  if (appuuid === serveruuid) {
    if (transfertype === 'no') {
      // Use transaction for atomicity - create new user and update old user in one transaction
      return knex.transaction(async (trx) => {
        // Fetch user details once
        const userDetails = await trx('users')
          .where('uuid', appuuid)
          .first();

        if (!userDetails) {
          throw new Error('User not found with provided UUID');
        }

        // Insert new user with PostgreSQL UUID generation
        const result = await trx('users')
          .insert({
            uuid: trx.raw('gen_random_uuid()'), // PostgreSQL UUID generation
            user_phone: userDetails.user_phone,
            email: userDetails.email,
            country_code: userDetails.country_code,
            name: userDetails.name,
            referral_code: userDetails.referral_code,
            is_terms_cond_unable: userDetails.is_terms_cond_unable,
            is_whatapp_msg_unable: userDetails.is_whatapp_msg_unable,
            reg_date: userDetails.reg_date,
            dial_code: userDetails.dial_code,
            otp_value: userDetails.otp_value,
            actual_device_id: userDetails.actual_device_id,
            device_id: userDetails.device_id
          })
          .returning(['id', 'uuid']); // Return both id and uuid in one query

        const userId = result[0]?.id || result[0];
        const newUuid = result[0]?.uuid;

        // Update the existing user's status in same transaction
        await trx('users')
          .where('id', appuserid)
          .update({
            "name": null,
            "is_verified": 0,
            "user_phone": null,
            "email": null,
            "referral_code": null
          });

        // Return result without additional query (already have uuid from insert)
        return { user_id: userId, uuid: newUuid };
      });
    } else {
      return { user_id: serveruserid, uuid: serveruuid };
    }
  } else {
    if (transfertype === 'yes') {
      // Update references if transfertype is 'yes' - uses transaction for atomicity
      await updateUserReferences(appuserid, serveruserid);
    }
    return { user_id: serveruserid, uuid: serveruuid };
  }
};


const fetchOtpTesting = async (userdata) => {
  try {
    const { user_phone } = userdata;
    if (!user_phone) {
      throw new Error('User phone is required');
    }

    const user = await knex('users')
      .select('otp_value')
      .where('user_phone', user_phone)
      .first();

    if (!user) {
      throw new Error('User not found');
    }

    return user;
  } catch (error) {
    throw error;
  }
};

const randomizeUserPhone = async (userdata) => {
  const { user_phone } = userdata;
  if (!user_phone) {
    throw new Error('User phone is required');
  }

  return await knex.transaction(async (trx) => {
    // 1. Find all users with this phone number
    const usersToUpdate = await trx('users')
      .select('id')
      .where('user_phone', user_phone);

    if (usersToUpdate.length === 0) {
      throw new Error('User not found');
    }

    const updatedUsers = [];

    // 2. Loop through each user and assign a unique random phone number
    for (const user of usersToUpdate) {
      let uniquePhoneFound = false;
      let randomPhone;

      while (!uniquePhoneFound) {
        // Generate a random 9-digit number as a string
        randomPhone = Math.floor(100000000 + Math.random() * 900000000).toString();

        // Check if this random number already exists in the table
        const exists = await trx('users')
          .select('id')
          .where('user_phone', randomPhone)
          .first();

        if (!exists) {
          uniquePhoneFound = true;
        }
      }

      // 3. Update the user with the new unique random phone
      await trx('users')
        .where('id', user.id)
        .update({
          user_phone: randomPhone,
          updated_at: trx.fn.now()
        });

      updatedUsers.push({ id: user.id, old_phone: user_phone, new_phone: randomPhone });
    }

    return updatedUsers;
  });
};


module.exports = {
  fetchOtpTesting,
  randomizeUserPhone,
  createUser,
  getUsers,
  updateUser,
  deleteUser,
  findUser,
  getUsersforLogin,
  verifyOtp,
  register,
  myprofile,
  resendOtp,
  editprofile,
  userDeactivate,
  bankDetails,
  createImage,
  sendOtp,
  verifyOtpupdate,
  getSendemail,
  getFaqsList,
  getShowProfile,
  verifyDetails,
  generateInvoice
};
