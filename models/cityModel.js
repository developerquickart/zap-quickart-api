const knex = require('../db'); // Import your Knex instance

const getCity = async () => {
    return await knex('city').select('*');
  };

  module.exports = {
    getCity
  };
