const csv = require('csv-parser')
const path = require('path');
const fs = require('fs')
const file = 'materials.json';
var results = [];

init();

function init() {
    switch (path.extname(file)) {
        case '.csv':
            //===============Checking CSV Data===================
            let rowCount = 2;
            let missingValue = false;
            fs.createReadStream(file)
                .pipe(csv())
                .on('data', (data) => {
                    if (Object.keys(data).length != 3) { //check if there are any missing values in csv
                        console.log('Missing important values at line ', rowCount, ' of CSV')
                        rowCount++;
                        console.log(data)
                        missingValue = true;
                    }
                    if (checkDuplicate(data)) { //check duplicate entry
                        console.log('Duplicate entry on row ', rowCount, " of CSV with value=", data)
                        rowCount++;
                        missingValue = true;
                    }
                    results.push(data)

                })
                .on('end', () => {
                    // console.log(results)
                    if (!missingValue) {
                        console.log("File is ready to be inserted to DB")
                    }
                    else {
                        console.log("File is corrupt")
                    }
                })
            // ============================================================
            break;

        case '.json':
            //====================Checking JSON Data==========================
            console.log('hello json')
            fs.readFile(file, function (err, data) {
                // Check for errors
                if (err) throw err;

                // Converting to JSON
                const materials = JSON.parse(data);
                let missingValue = false;
                for (let i in materials) {
                    if (!materials[i].user_id || !materials[i].customer_id || !materials[i].order_date) {//check if any mandatory fields are missing
                        console.log('Missing important key values at',i)
                        missingValue = true;
                        break;
                    }
                    if (checkDuplicate(materials[i])) {// check if duplicate visit present
                        console.log("Duplicate visit at=",i)
                        missingValue = true;
                        break;
                    }
                    results.push(materials[i])
                }
                if (!missingValue) {
                    console.log("File is ready to be inserted to DB")

                }
                else {
                    console.log("File is corrupt")
                }
            })
            //=====================================
            break;

        default:
            console.log('File extension not supported! Please provide data in .csv or .json format');
            break;
    }
}

function checkDuplicate(obj) {
    for(var i in results){
        let key = results[i]
        if (obj['user_id'] == key['user_id'] &&
            obj['customer_id'] == key['customer_id'] &&
            new Date(obj['order_date']).getTime() == new Date(key['order_date']).getTime())
            return true
    }
    return false
}

