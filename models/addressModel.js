const knex = require('../db');
const axios = require('axios');
const apiKey = 'AIzaSyADPEHze6hgRTG83JXfEJ6owhtNTmJJWwg'; // Replace with your Geolocation API key

const city = async () => {
    return await knex('city')
         .join('society','city.city_id','=','society.city_id')
         .where('city.status',1)
         .select('city.city_id','city.city_name')
         .groupBy('city.city_id','city.city_name')
 
};

const society = async (appDetatils) => {
    const city_id = appDetatils.city_id;
    return await knex('society')
    .join('city', 'society.city_id','=','city.city_id')
    .join('store_society','store_society.society_id','=','society.society_id')
    .where('city.city_id',city_id)
 
};

const showAddress = async (appDetatils) => {
    const user_id = appDetatils.user_id;
    const baseurl = process.env.BUNNY_NET_IMAGE || '';

    // OPTIMIZATION: Fetch city lists and addresses in parallel (2 round trips → 1)
    const [cityNamesResult, cityNamesAResult, addresses] = await Promise.all([
        knex('city').where('status', 1).select(knex.raw("string_agg(city_name::text, ',') as \"cityName\"")),
        knex('city').where('status', 1).select(knex.raw("string_agg(arabic_name::text, ',') as \"cityName\"")),
        knex('address')
            .select(
                'address.address_id',
                'address.type',
                'address.user_id',
                'address.city',
                'address.receiver_name',
                'address.receiver_phone',
                'address.receiver_email',
                'address.house_no',
                'address.landmark',
                'address.lat',
                'address.lng',
                'address.country_code',
                'address.society as society_name',
                knex.raw('(COALESCE(?::text, \'\') || COALESCE(address.doorimage, \'\')) as doorimage', [baseurl]),
                knex.raw("(COALESCE(address.lat::text, '') || ',' || COALESCE(address.lng::text, '')) as latlng"),
                'address.dial_code'
            )
            .where('user_id', user_id)
            .where('select_status', '!=', 2)
            .where('is_zap_address', true)
    ]);

    const cityNameList = (cityNamesResult[0]?.cityName || '').split(',').filter(Boolean);
    const cityNameListA = (cityNamesAResult[0]?.cityName || '').split(',').filter(Boolean);

    // Function to fetch address details using Google Maps API
    const getFormattedAddress = async (lat, lng) => {
        const url = `https://maps.googleapis.com/maps/api/geocode/json?latlng=${lat},${lng}&key=${apiKey}`;
        try {
            const response = await axios.get(url);
            const data = response.data;
            if (data.results.length > 0) {
                return data.results[0].formatted_address;
            }
            return null;
        } catch (error) {
            console.error('Error fetching the geolocation:', error);
            return null;
        }
    };

    // Step 3: Check if the address's city name exists in the city lists and validate using Geocoding API
    const updatedAddresses = await Promise.all(
        addresses.map(async (address) => {
            let cityExists =
                cityNameList.includes(address.city) || cityNameListA.includes(address.city);

            if (!cityExists && address.lat && address.lng) {
                const formattedAddress = await getFormattedAddress(address.lat, address.lng);
                if (formattedAddress) {
                    // Dynamically check if the formatted address contains any city from the dynamic city lists
                    cityExists = cityNameList.concat(cityNameListA).some(city =>
                        formattedAddress.includes(city)
                    );
                }
            }

            return {
                ...address,
                cityExists // true or false based on both checks
            };
        })
    );

    return updatedAddresses;
};

const removeAddress = async (appDetatils) => {
    const address_id = appDetatils.address_id;
    const checkcart = await knex('orders').where('address_id', address_id);

    if (checkcart.length === 0) {
        await knex('address').where('address_id', address_id).delete();
    } else {
        await knex('address').where('address_id', address_id).update({ select_status: 2 });
    }
    return { success: true };
};

const doorimage = async (imageData) => {
    const address_id = imageData.address_id;
    const filename = imageData.path;

    if (filename) {
        await knex('address')
            .where('address_id', address_id)
            .update({ doorimage: filename });
    }
    return { success: true };
};

const editAddress = async (imageData) => {
    const address_id = Number(imageData.address_id);
    const receiver_phone_code = imageData.receiver_phone_code;
    const society_name = imageData.society_name || '';
    const house_no = imageData.house_no;
    const landmark = imageData.landmark;
    const lat = imageData.lat;
    const lng = imageData.lng;
    const latlng = [lat, lng].filter(Boolean).join(',');
    const filename = imageData.path;
    const now = new Date();

    const updateData = {
        receiver_name: imageData.receiver_name,
        receiver_phone: imageData.receiver_phone,
        receiver_email: imageData.receiver_email,
        society: society_name,
        society_id: society_name
            ? knex.raw("COALESCE((SELECT society_id::text FROM society WHERE society_name = ? LIMIT 1), '')", [society_name])
            : '',
        house_no,
        landmark,
        select_status: 1,
        lat,
        lng,
        type: imageData.type,
        added_at: now,
        updated_at: now,
        building_villa: '',
        street: '',
        country_code: receiver_phone_code,
        latlng,
        dial_code: imageData.dial_code,
        is_zap_address: true
    };
    if (filename) {
        updateData.doorimage = filename;
    }

    await knex('address').where('address_id', address_id).update(updateData);
    return { success: true };
};

const addAddress = async (imageData) => {
    const user_id = String(imageData.user_id);
    const type = imageData.type || 'Others'; // Default to Others to avoid undefined binding in WHERE
    const receiver_name = imageData.receiver_name;
    const receiver_phone = imageData.receiver_phone;
    const receiver_phone_code = imageData.receiver_phone_code;
    const receiver_email = imageData.receiver_email;
    const society_name = imageData.society_name || '';
    const house_no = imageData.house_no;
    const landmark = imageData.landmark;
    const lat = imageData.lat;
    const lng = imageData.lng;
    const latlng = [lat, lng].filter(Boolean).join(',');
    const dial_code = imageData.dial_code;
    const filename = imageData.path;
    const added_at = new Date();
    const now = new Date();

    // OPTIMIZATION: Fetch society, max address_id, and existing address (for Home/Office) in parallel
    const isInsertOnly = type === 'Others' || type === 'Work' || type === 'Home';
    const promises = [
        society_name ? knex('society').where('society_name', society_name).first() : Promise.resolve(null),
        knex('address').max('address_id as max_id').first(),
        isInsertOnly ? Promise.resolve(null) : knex('address').where('user_id', user_id).where('type', type).first()
    ];
    const [socitydet, maxRow, getaddress] = await Promise.all(promises);

    const society_id = socitydet ? String(socitydet.society_id) : '';
    const nextAddressId = (maxRow?.max_id != null) ? Number(maxRow.max_id) + 1 : 1;

    const baseData = {
        receiver_name,
        receiver_phone,
        receiver_email,
        society: society_name,
        society_id,
        house_no,
        landmark,
        select_status: 1,
        lat,
        lng,
        type,
        country_code: receiver_phone_code,
        latlng,
        dial_code
    };

    if (isInsertOnly) {
        const result = await knex('address')
            .insert({
                address_id: nextAddressId,
                user_id,
                ...baseData,
                added_at,
                doorimage: filename,
                is_zap_address: true
            })
            .returning('address_id');
        return { address_id: result[0]?.address_id ?? nextAddressId, inserted: true };
    }

    if (getaddress) {
        const updateData = {
            ...baseData,
            added_at,
            updated_at: now,
            doorimage: filename
        };
        await knex('address')
            .where('user_id', user_id)
            .where('type', type)
            .update(updateData);
        return { address_id: getaddress.address_id, inserted: false };
    }

    const result = await knex('address')
        .insert({
            address_id: nextAddressId,
            user_id,
            ...baseData,
            added_at,
            doorimage: filename,
            is_zap_address: true
        })
        .returning('address_id');
    return { address_id: result[0]?.address_id ?? nextAddressId, inserted: true };
};


const checkaddresshome = async (imageData) => {
    const address_id = Number(imageData.address_id);
    const user_id = String(imageData.user_id);

    return await knex('address')
        .where({ user_id, type: 'Home' })
        .where('address_id', '!=', address_id);
};

module.exports = {
addAddress,
editAddress,
showAddress,
removeAddress,
city,
society,
doorimage,
checkaddresshome
};
