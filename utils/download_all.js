const child_process = require('child_process');
const async = require('async');
const fs = require('fs-extra');
const tmp = require('tmp');
const logger = require('pelias-logger').get('openaddresses-download');
const request = require("request");

function downloadAll(config, callback) {
    logger.info('Attempting to download all data');

    const targetDir = config.imports['openaddresses-venues'].datapath;

    fs.ensureDir(targetDir, (err) => {
        if (err) {
            logger.error(`error making directory ${targetDir}`, err);
            return callback(err);
        }

        let dataHost = config.get('imports.openaddresses-venues.dataHost') || 'https://data.openaddresses.io';

        request(dataHost, function (error, response, body) {
            if (error) {
                return console.error('Failed, error: ' + error);
            }
            if (response) console.log('Got response from ' + dataHost + ', response: ' + response.statusCode);
            if (body) {
                let item = JSON.parse(body).filter(function(i){return i.vvnr === 18 && !('kov' in i);});
                dataHost = dataHost + item[0].fail;
                console.log(dataHost);
                async.each(
                    [
                        dataHost
                    ],
                    downloadBundle.bind(null, targetDir),
                    callback);
            }
        });


    });
}

function downloadBundle(targetDir, sourceUrl, callback) {

    const tmpZipFile = tmp.tmpNameSync({postfix: '.zip'});

    async.series(
        [
            // download the zip file into the temp directory
            (callback) => {
                logger.debug(`downloading ${sourceUrl}`);
                child_process.exec(`curl -s -L -X GET -o ${tmpZipFile} ${sourceUrl}`, callback);
            },
            // unzip file into target directory
            (callback) => {
                logger.debug(`unzipping ${tmpZipFile} to ${targetDir}`);
                child_process.exec(`unzip -o -qq -d ${targetDir} ${tmpZipFile}`, callback);
            },
            // delete the temp downloaded zip file
            fs.remove.bind(null, tmpZipFile)
        ],
        callback);
}

module.exports = downloadAll;
