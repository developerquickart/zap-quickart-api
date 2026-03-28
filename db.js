// db.js
const knex = require('knex');
const config = require('./knexfile'); // Assuming your knexfile is in the root directory

const db = knex(config);

module.exports = db;
