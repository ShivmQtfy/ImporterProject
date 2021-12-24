const csv = require('csv-parser')
const path = require('path');
const fs = require('fs')
const file = 'materials.csv';
var results = [];
var usersMeta = {}
var customersMeta = {}
var visitsMeta = {}

//=================DB Connection==========================
const mysql = require('mysql');
const { exit } = require('process');
const { resolve } = require('path');
const { rejects } = require('assert');
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
    // fetchData();
    // init()
});
//============================================================

// ==========================Promises to get data from users,customers and visits==========

userPromise = () => new Promise((resolve) => {
    let userQuery = 'SELECT * FROM users';
    connection.query(userQuery, (err, rows) => {
        if (!err)
            return resolve(rows)
    })
})

customerPromise = () => new Promise((resolve) => {
    let customerQuery = 'SELECT * FROM customer';
    connection.query(customerQuery, (err, rows) => {
        if (!err)
            return resolve(rows)
    })
})

visitPromise = () => new Promise((resolve) => {
    let visitQuery = 'SELECT * FROM visit_list v WHERE v.visit_status = 0';
    connection.query(visitQuery, (err, rows) => {
        if (!err)
            return resolve(rows)
    })
})

// ==================================================

init()

async function init() {
    const p = await Promise.all([userPromise(), customerPromise(), visitPromise()])

    //=============================Logic for initial insert======================

    /* let customer = p[1][Math.floor(Math.random() * p[1].length)]
    console.log('Random cust=',customer) */
    // loop through users
   /*  for (var i in p[0]) {
        let user = p[0][i]
        results = []
        duplicateCheck = []
        // randomize 5 customers
        for(var j=0; j<5; j++){
            let customer = p[1][Math.floor(Math.random() * p[1].length)]
            if(duplicateCheck.includes(customer['customer_id']))
                customer = p[1][Math.floor(Math.random() * p[1].length)]
            duplicateCheck.push(customer['customer_id'])
            // console.log('Random cust=',customer)
            // increase dates by 30 dates
            for(var k=0; k<30; k++){
                let date = new Date().setDate(new Date().getDate() + k)
                // console.log('USER=',user,'Cust=',customer,'date=',new Date(date))
                results.push([user['user_id'],customer['customer_id'],new Date(date)])
            }
        }
        console.log('result length=>>>>>>>>>>>>>',results.length,'for user=',i)
        // insert result
        insertRows(results)
    } */

    //=============================Logic for initial insert ends=================

    usersMeta = createMeta(p[0], 'email')
    customersMeta = createMeta(p[1], 'customer_email')
    visitsMeta = createMeta(p[2], 'visits')

    switch (path.extname(file)) {
        case '.csv':
            //===============Inserting CSV values to DB===================
            fs.createReadStream(file)
                .pipe(csv())
                .on('data', (data) => {
                    if (Object.keys(data).length != 3) { 
                        //stop parsing if there are any missing values in csv
                        stopParsing();
                    }
                    else {
                        let foo = ''
                        try {
                            foo = `${usersMeta[data['user_id']]['user_id']}_${customersMeta[data['customer_id']]['customer_id']}_${data['order_date']}`
                        } catch (error) { }
                        console.log('VISIT KEY=',foo,Object.keys(data).length)
                        if (!usersMeta[data['user_id']] || !customersMeta[data['customer_id']] || !data['order_date']) {
                            //stop parsing if some misisng user/customer 
                            stopParsing();
                        }
                        else if (foo && visitsMeta[foo]) {
                            //stop parsing if duplicate visit
                            stopParsing();
                        }
                        else {
                            //push the line items in an array format
                            let userId = usersMeta[data['user_id']]['user_id']
                            let customerId = customersMeta[data['customer_id']]['customer_id']
                            results.push([userId, customerId, data['order_date']])
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
                    let data = materials[i];

                    if (Object.keys(data).length != 3) { 
                        //stop parsing if there are any missing values in csv
                        stopParsing();
                    }
                    else {
                        let foo = ''
                        try {
                            foo = `${usersMeta[data['user_id']]['user_id']}_${customersMeta[data['customer_id']]['customer_id']}_${data['order_date']}`
                        } catch (error) { }
                        console.log('VISIT KEY=',foo,Object.keys(data).length)
                        if (!usersMeta[data['user_id']] || !customersMeta[data['customer_id']] || !data['order_date']) {
                            //stop parsing if some misisng user/customer 
                            stopParsing();
                        }
                        else if (foo && visitsMeta[foo]) {
                            //stop parsing if duplicate visit
                            stopParsing();
                        }
                        else {
                            //push the line items in an array format
                            let userId = usersMeta[data['user_id']]['user_id']
                            let customerId = customersMeta[data['customer_id']]['customer_id']
                            results.push([userId, customerId, data['order_date']])
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
    }
}

/**
 * function to create a meta object with a specified key
 * @param {*} list array for which meta object has to be created
 * @param {*} key key which should be used as a differentiator for the metaObj
 */
function createMeta(list, key) {
    let result = {}
    if (!list.length)
        return result
    try {
        for (var i in list) {
            let obj = list[i]
            if (key == 'visits') {
                /* 
                 *make a custom key incase of visits
                 *it is a combination of 3 columns joined by '_'
                 */
                let foo = `${obj['user_id']}_${obj['customer_id']}_${obj['order_date']}`
                result[foo] = obj
            }
            else
                result[obj[key]] = obj
        }
    } catch (error) {
        console.log(err)
        exit()
    }
    // console.log(result)
    return result
}

function insertRows(arr) {
    let sql = "INSERT INTO visit_list ( user_id,customer_id,order_date) VALUES ?";
    connection.query(sql, [arr]);
    console.log('Inserted rows')
    // exit()
}

function stopParsing() {
    console.log(" Program terminated due to error while parsing");
    exit();
}
