const { application } = require('express');
const knex = require('../db'); // Import your Knex instance
const math = require('mathjs');  // If using a third-party math library

const wallethistory = async (appDetatils) => {

    user_id = appDetatils.user_id
   
         return wallet = await knex('wallet_recharge_history')
              .join('users', 'wallet_recharge_history.user_id','=','users.id')
              .where('users.id',user_id)
              .select('wallet_recharge_history.*')
              .orderBy('wallet_recharge_history.wallet_recharge_history', 'DESC' )
 
   };
   
   const walletrecharge = async (appDetatils) => {

    user_id = appDetatils.user_id
    amount = appDetatils.amount
   
 

      const maxWIdResult = await knex('wallet_history').max('w_id as maxWId').first();
      const nextWId = (maxWIdResult?.maxWId != null ? parseInt(maxWIdResult.maxWId, 10) : 0) + 1;
      const addwallet = await knex('wallet_history').insert({
            w_id: nextWId,
            user_id: user_id,
            amount: amount,
            resource: 'card_recharge',
            type:'wallet'
      })
    
      if(addwallet){
        walletlist = await knex('users')
        .select('wallet')
        .where('id', user_id)
        .first();
    
        walletamount = walletlist.wallet + appDetatils.amount
    
        walupdate = await knex('users')
        .where('id',user_id)
        .update({wallet:walletamount});
        
         return walletlist.wallet
      }



       
   };
   
const ordercardchanges = async(appDetatils) => {

  const si_sub_ref_no = appDetatils.si_sub_ref_no
  const cart_id = appDetatils.cart_id
    if(appDetatils.user_id != "null" ){
        user_id = appDetatils.user_id
    }else{
        user_id = appDetatils.device_id
    }

    Orderlist = await knex('orders')
    .select('group_id')
    .where('cart_id', cart_id)
    .first();

    si_check = await knex('tbl_user_bank_details')
    .where('si_sub_ref_no', si_sub_ref_no)
    .where('user_id', user_id)
    if(si_check.length == 0 ){
      throw new Error('You are wrong card details send');
         
    }
    else
    {
    group_id=Orderlist.group_id; 
    userupdate2 = await knex('orders')
    .where('group_id', group_id)  
    .update({si_sub_ref_no: si_sub_ref_no}); 
    return 1
    }


}

// Added by G1 – PostgreSQL compatible
const spentbywallet = async (appDetatils) => {
  try {
    const { user_id, start_date, end_date, type } = appDetatils;

    if (!user_id) {
      throw new Error('user_id is required');
    }

    let query = knex('wallet_history')
      .join('users', 'wallet_history.user_id', '=', 'users.id')
      .where('users.id', user_id)
      .select('wallet_history.*');

    if (start_date && end_date) {
      query = query.whereBetween('wallet_history.created_at', [
        `${start_date} 00:00:00`,
        `${end_date} 23:59:59`
      ]);
    }

    const normalizedType = type ? type.toLowerCase() : null;
    if (normalizedType && normalizedType !== 'all') {
      query = query.whereRaw('LOWER(wallet_history.type) = ?', [normalizedType]);
    }

    const rows = await query.orderBy('wallet_history.w_id', 'DESC');

    const grouped = {};
    rows.forEach(item => {
      if (!item.created_at) return;
      const year = new Date(item.created_at).getFullYear();
      if (!grouped[year]) grouped[year] = [];
      grouped[year].push(item);
    });

    return Object.keys(grouped)
      .sort((a, b) => b - a)
      .map(year => ({ year: Number(year), items: grouped[year] }));
  } catch (error) {
    console.error('Model Error:', error.message);
    throw error;
  }
};

// Update wallet history – G1, PostgreSQL compatible
const updatewalletexpiry = async (appDetatils) => {
  try {
    const result = await knex('wallet_history')
      .where('type', 'Add')
      .whereNotNull('expiry_date')
      .whereRaw('expiry_date < NOW()')
      .where('created_at', '>=', '2026-01-01')
      .update({
        type: 'wallet_expired',
        updated_at: knex.raw('NOW()')
      });
    return result;
  } catch (error) {
    console.error('Model Error:', error.message);
    throw error;
  }
};

module.exports = {
  wallethistory,
  walletrecharge,
  ordercardchanges,
  spentbywallet,
  updatewalletexpiry
};
