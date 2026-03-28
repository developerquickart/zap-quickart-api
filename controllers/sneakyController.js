const sneakyModel = require('../models/sneakyModel');
const knex = require('../db'); // Import your Knex instance

const sneaky_productlist = async (req, res) => {

    try {
        const appDetatils = req.body;
        const sneaky = await sneakyModel.sneakyprodlist(appDetatils);



        var data = {
            "status": "1",
            "message": "Nearby users list",
            "data": sneaky,

        };
        res.status(200).json(data);
    } catch (error) {
        console.error(error);
        res.status(500).json({ status: 0, message: 'Error finding nearby users' });
    }

}

module.exports = {
    sneaky_productlist
};
