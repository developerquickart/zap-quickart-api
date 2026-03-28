const knex = require('./db');
async function run() {
    const result = await knex.raw(`
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'orders' 
    AND column_name IN ('user_id', 'group_id', 'payment_method');
  `);
    console.table(result.rows);
    await knex.destroy();
}
run();
