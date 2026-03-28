const data = [
    {
        "id": 9,
        "title": "TEST OCCASIONAL SUPA",
        "sub_title": "HG",
        "color1": null,
        "color2": null,
        "product_details": [
            {
                "product_id": 2921,
                "product_name": "Coconut water (Glass bottle)"
            }
        ]
    },
    {
        "id": 10,
        "title": "SECOND CAT",
        "sub_title": "SC",
        "product_details": [
            {
                "product_id": 2922,
                "product_name": "Test Product 2"
            }
        ]
    }
];

const flattenedProducts = data.flatMap(cat => cat.product_details || []);

console.log(JSON.stringify(flattenedProducts, null, 2));

if (flattenedProducts.length === 2 && flattenedProducts[0].product_id === 2921 && flattenedProducts[1].product_id === 2922) {
    console.log("Verification Successful: Products flattened correctly.");
} else {
    console.error("Verification Failed: Products not flattened correctly.");
    process.exit(1);
}
