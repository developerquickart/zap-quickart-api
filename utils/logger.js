const fs = require('fs');
const path = require('path');

function logToFile(message) {
  const timestamp = new Date().toISOString();
  const finalLog = `[${timestamp}] ${message}\n`;

  // Mirror to console so Railway logs catch it
  console.log(`[LOG] ${message}`);

  const logDir = path.join(__dirname, '../logs');
  if (!fs.existsSync(logDir)) {
    try {
      fs.mkdirSync(logDir, { recursive: true });
    } catch (err) {
      // Don't crash if filesystem is read-only or full
      return;
    }
  }
  const logPath = path.join(logDir, 'api.log');

  fs.appendFile(logPath, finalLog, (err) => {
    if (err) {
      // Silent fail for filesystem errors in container
    }
  });
}


// Add methods to support logToFile.error() and logToFile.info() pattern used in parts of the app
logToFile.error = (message, error) => {
  const errorMessage = error ? `${message} - ${error.message || error}` : message;
  console.error(`[ERROR] ${errorMessage}`);
  logToFile(`ERROR: ${errorMessage}`);
};

logToFile.info = (message) => {
  console.log(`[INFO] ${message}`);
  logToFile(`INFO: ${message}`);
};


module.exports = logToFile;
