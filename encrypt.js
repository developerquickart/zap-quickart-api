// const crypto = require('crypto');

// // 🔐 Hardcoded base64 values
// const SECRET_KEY_BASE64 = 'IG72/fBkmDnZNw0Vj6PNMhOzk1yp7ovIQPsM6G3Clys=';
// const IV_BASE64 = 'w6wFNFMRqgMzWK+BaUvg5A==';

// // 🔓 Convert base64 to Buffer
// const SECRET_KEY = Buffer.from(SECRET_KEY_BASE64, 'base64');
// const IV = Buffer.from(IV_BASE64, 'base64');

// // 🔐 Encrypt function
// function encryptJson(jsonData) {
//   const cipher = crypto.createCipheriv('aes-256-cbc', SECRET_KEY, IV);
//   const jsonString = JSON.stringify(jsonData);
//   let encrypted = cipher.update(jsonString, 'utf8', 'base64');
//   encrypted += cipher.final('base64');
//   return encrypted;
// }

// // 🔓 Decrypt function
// function decryptJson(encryptedData) {
//   const decipher = crypto.createDecipheriv('aes-256-cbc', SECRET_KEY, IV);
//   let decrypted = decipher.update(encryptedData, 'base64', 'utf8');
//   decrypted += decipher.final('utf8');
//   return JSON.parse(decrypted);
// }

// module.exports = {
//   encryptJson,
//   decryptJson,
// };



const crypto = require('crypto');

const algorithm = 'aes-256-cbc';

// Your base64-encoded 32-byte key
const secretKeyBase64 = 'IG72/fBkmDnZNw0Vj6PNMhOzk1yp7ovIQPsM6G3Clys=';
const secretKey = Buffer.from(secretKeyBase64, 'base64');

// 16-byte IV (base64)
const IV_BASE64 = 'w6wFNFMRqgMzWK+BaUvg5A==';
const staticIV = Buffer.from(IV_BASE64, 'base64'); 

function encrypt(text) {
  const cipher = crypto.createCipheriv(algorithm, secretKey, staticIV);
  let encrypted = cipher.update(text, 'utf8', 'base64');
  encrypted += cipher.final('base64');
  return encrypted; // We already know the IV
}

function decrypt(encryptedText) {
  try {
    const encrypted = Buffer.from(encryptedText, 'base64'); // Ensure it's base64
    const decipher = crypto.createDecipheriv(algorithm, secretKey, staticIV);
    let decrypted = decipher.update(encrypted, 'base64', 'utf8');
    decrypted += decipher.final('utf8');
    return decrypted;
  } catch (e) {
    console.error('Decryption error:', e.message);
    throw new Error('Failed to decrypt payload. Please check encryption format or key.');
  }
}

module.exports = { encrypt, decrypt };
