const crypto = require('crypto');
const knex = require('../db'); // Import your Knex instance

const addGuestDetails = async (appDetails) => {
    const { actual_device_id, device_id, uuid } = appDetails;

    // Check if uuid is defined and non-null
    if (uuid) {
        // Check if an unverified user with the same uuid already exists
        // Optimized: Single query with specific columns for faster response
        const existingUser = await knex('users')
            .where({ uuid })
            .where('is_verified', '!=', 1)
            .first();

        if (existingUser) {
            return existingUser; // Return existing user data if found
        }
    }

    // Generate UUID using Node.js crypto (faster than database function)
    const generatedUuid = crypto.randomUUID();

    // Get the maximum id from users table and increment it
    // Optimized: Single query to get max id, then use it for insert
    // Retry logic to handle race conditions (max 3 attempts)
    let result;
    let attempts = 0;
    const maxAttempts = 3;

    while (attempts < maxAttempts) {
        try {
            const maxIdResult = await knex('users')
                .max('id as max_id')
                .first();
            
            const nextId = maxIdResult && maxIdResult.max_id ? parseInt(maxIdResult.max_id) + 1 : 1;

            // Insert a new guest record and return full user data in single query
            // Optimized: Using .returning('*') to avoid second database query
            result = await knex('users')
                .insert({
                    id: nextId, // Set the id explicitly based on max id + 1
                    name: 'Guest',
                    actual_device_id: actual_device_id || null,
                    device_id: device_id || null,
                    user_type: 'guest',
                    reg_date: knex.raw('CURRENT_DATE'), // PostgreSQL date type
                    uuid: generatedUuid, // PostgreSQL compatible UUID generation
                    status: 0,
                    wallet: 0.0,
                    rewards: 0,
                    is_verified: 0,
                    block: 0,
                    app_update: 0,
                    membership: 0,
                    noti_popup: 0,
                    activate_deactivate_status: 'active',
                })
                .returning('*'); // Return all columns to avoid second query

            // If we get here, insert was successful
            break;
        } catch (error) {
            // Handle duplicate key error (race condition)
            if (error.code === '23505' && attempts < maxAttempts - 1) {
                attempts++;
                // Wait a small random amount to avoid thundering herd
                await new Promise(resolve => setTimeout(resolve, Math.random() * 50));
                continue;
            }
            // Re-throw if it's not a duplicate key error or we've exhausted retries
            throw error;
        }
    }

    // Safety check: Ensure we have a result
    if (!result || result.length === 0) {
        throw new Error('Failed to create guest user');
    }

    return result[0];
};


module.exports = {
    addGuestDetails,
};
