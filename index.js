const extract = require('extract-zip');
const path = require('path');
const fs = require('fs');
const glob = require('glob');
const moment = require('moment');
const csvStream = require('csv-stream-to-json');

const PATH_TO_ARCHIVE = process.argv[2] || path.resolve('Carrier_Integration_-_Data.zip');
const PATH_TO_RESULT_JSON = path.join(process.argv[3] || path.dirname(PATH_TO_ARCHIVE), 'result.json');
let firstElement = true;

async function main() {
    //create folder for csv's
    const pathToResultCSV = path.join(path.dirname(PATH_TO_ARCHIVE), 'result','');
    if (!fs.existsSync(pathToResultCSV)) {
        fs.mkdirSync(pathToResultCSV)
    }

    //extract csv from zip to folder
    await extractCSV(PATH_TO_ARCHIVE, pathToResultCSV);

    //list of scv's files
    const listCSV = glob.sync(path.join(pathToResultCSV, '**/*.csv'));
    console.log(`List of CSV files: ${JSON.stringify(listCSV, null, 4)}`);

    //create JSON file
    fs.writeFileSync(PATH_TO_RESULT_JSON, '[');

    //write data from SCV to JSON
    for (const pathToCSV of listCSV) {
        await new Promise((res, rej) => {
            const readStream = fs.createReadStream(pathToCSV);
            csvStream.parse(readStream, '||', false, json => {
                json = JSON.parse(JSON.stringify(json).replace(/\\"/g, ''))
                const resultObj = {
                    name: json['last_name'] + ' ' + json['first_name'],
                    phone: json['phone'].replace(/\D/g, ''),
                    person: {
                        firstName: json['first_name'],
                        lastName: json['last_name']
                    },
                    amount: +json['amount'],
                    date: moment(json['date'], 'D/M/YYYY'),
                    costCenterNum: json['cc'].replace(/\D/g, '')
                };
                fs.appendFileSync(PATH_TO_RESULT_JSON, 
                    ((firstElement ? '\n' : ',\n') + 
                    JSON.stringify(resultObj, null, 4)).split('\n').map(line => line === ',' || !line ? line : '    ' + line).join('\n'));
                firstElement = false;
            }, res);
        });
    };
    fs.appendFileSync(PATH_TO_RESULT_JSON, '\n]');
    //remove folder with csv files
    fs.rmdirSync(pathToResultCSV, { recursive: true });

}

async function extractCSV (pathToArhive, pathToResult) {
    try {
        await extract(pathToArhive, { dir: pathToResult })
        console.log('Extraction complete')
    } catch (err) {
        console.log(err);
    }
}

main();
