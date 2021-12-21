const csv = require('csv-parser');
const { hashSync } = require('bcryptjs');
const path = require('path');
const fs = require('fs')
const file = 'materials.csv';
const results = [];
const { names, password } = require('./const.js')

//=================DB Connection==========================
const mysql = require('mysql');
const { exit } = require('process');
const connection = mysql.createConnection({
    host: 'mysqlserverdb-qfitestvm2.mysql.database.azure.com',
    port: '3306',
    user: 'mysqldbadmin@mysqlserverdb-qfitestvm2',
    password: 'Mysql@dmin**362',
    database: 'ordersmart',
    ssl: true
});
connection.connect((err) => {
    if (err) throw err;
    console.log('Connected to DB!');
    init();
});

/**
 * main function to start after db connection
 */
function init() {
    let result = []
    for (var i = 0; i < names.length; i++) {
        let key = names[i]
        let user = generateUserObj(key)
        console.log('Key=', key, 'count=', i, ' Object=', user)
        result.push([
            user['name'],
            user['email'],
            user['password'],
            user['role'],
            user['application_access'],
            user['created_at']
        ])
    }
    insertRows(result);
}


//============================================================

/* switch (path.extname(file)) {
    case '.csv':
        //===============Inserting CSV values to DB===================
        fs.createReadStream(file)
            .pipe(csv())
            .on('data', (data) => {
                if (Object.keys(data).length != 6) { //stop parsing if there are any missing values in csv
                    stopParsing();
                }
                else {
                    if (!Number(data.mapping_material_id)) { //stop parsing if mapping material id is not an Integer
                        stopParsing();
                    }
                    else {//push the line items in an array format
                        results.push([data.material_description, data.brand, data.category, data.pack_size, data.pack_type, parseInt(data.mapping_material_id)])
                    }
                }
            })
            .on('end', () => {
                    insertRows(results);
            })
        // ============================================================
        break;

    case '.json':
        //====================JSON TO DB ==========================

        fs.readFile(file, function (err, data) {
            // Check for errors
            if (err) throw err;
            // Converting to JSON
            const materials = JSON.parse(data);
            for (let i in materials) {
                // console.log(i)
                if (!materials[i].mapping_material_id) {//check if any mandatory fields are missing
                    stopParsing();
                }
                else {
                    if (!Number(data.mapping_material_id)) {// check if mapping material id is Integer
                        stopParsing();
                    }
                    else {//push the json object properties in an array format
                        results.push([materials[i].material_description, materials[i].brand, materials[i].category, materials[i].pack_size, materials[i].pack_type, parseInt(materials[i].mapping_material_id)])
                    }
                }
            }
            insertRows(results);
            
        })
        //=====================================
        break;

    default:
        console.log('File extension not supported! Please provide data in .csv or .json format');
        break;
} */


// =========================================


/**
 * function to generate an object for a user
 * @param {*} name // name of the user
 * @returns result // an object with all other details of the user
 */
function generateUserObj(name) {
    let sa = 'Shivam Kumar'
    let result = { name: name }
    result['email'] = name.split(" ")[0].toLowerCase();
    result['email'] += '@mail.com'
    result['password'] = hashSync(password, 10);
    result['created_at'] = new Date()
    result['role'] = 1
    result['application_access'] = 3
    return result
}

/**
 * function to add rows in the table
 * @param {*} arr / /array of users to add
 */
function insertRows(arr) {
    // let sql = "INSERT INTO materials ( material_description,brand,category,pack_size,pack_type,mapping_material_id) VALUES ?";
    let sql = "INSERT INTO users ( user_name,email,hash,role,application_access,created_at) VALUES ?";
    connection.query(sql, [arr]);
    console.log('Inserted rows')
}

/**
 * function to stop parsing and exit the process
 */
function stopParsing() {
    console.log(" Program terminated due to error while parsing");
    exit();
}
