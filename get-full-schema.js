const knex = require('./db');
async function run() {
    const table = process.argv[2] || 'orders';
    const result = await knex.raw(`
    SELECT column_name, data_type, column_default, is_nullable
    FROM information_schema.columns 
    WHERE table_name = ?
    ORDER BY ordinal_position;
  `, [table]);
    console.table(result.rows);
    await knex.destroy();
}
run();
