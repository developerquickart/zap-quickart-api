require('dotenv').config();

console.log('--- Environment Variable Check ---');
const invoicePdfUrl = process.env.BUNNY_NET_INVOICE_PDF;
console.log('BUNNY_NET_INVOICE_PDF:', invoicePdfUrl);

if (!invoicePdfUrl) {
    console.error('ERROR: BUNNY_NET_INVOICE_PDF is not defined in .env');
    process.exit(1);
}

if (!invoicePdfUrl.endsWith('/')) {
    console.error('WARNING: BUNNY_NET_INVOICE_PDF should probably end with a slash (/)');
}

console.log('\n--- URL Construction Logic Check ---');
const mockFilename = '12345_10-00-00_9999.pdf';
const fullUrl = invoicePdfUrl + mockFilename;
console.log('Mock Filename:', mockFilename);
console.log('Constructed Full URL:', fullUrl);

const expectedStart = 'https://quickart.b-cdn.net/invoice/';
if (fullUrl.startsWith(expectedStart)) {
    console.log('SUCCESS: URL starts with the expected production domain.');
} else {
    console.error('ERROR: URL does not start with the expected production domain.');
    console.log('Expected start:', expectedStart);
}

console.log('\n--- Database Field Check ---');
console.log('Verify that both userModel.js and orderModel.js use this logic.');
console.log('userModel.js (generateInvoice): return process.env.BUNNY_NET_INVOICE_PDF + filename');
console.log('orderModel.js (ordersDetails): prodwiseinvoice = process.env.BUNNY_NET_INVOICE_PDF + invoice_path');
