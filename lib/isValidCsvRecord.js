/*
 * For venues
 * Return true if a record has all of LON, LAT and NAME defined
 */
function isValidCsvRecord(record) {
    console.log(record.AVALIK_NIMI);
    return onlyHasTheseProperties(record) && notTransportItem(record) && notClub(record);
}

const KOORD = ['AVALIK_X', 'AVALIK_Y'];
const KOORD_NIMI = KOORD.concat(['AVALIK_NIMI']);
const properties = [
    KOORD,
    KOORD_NIMI];

function onlyHasTheseProperties(record) {
    return !!properties.find(propArray => {
        return propArray.every(function (prop) {
            return record[prop] && record[prop].length > 0;
        });
    });
}

function notTransportItem(record) {
    return record.TYYP_YLEMGRUPP !== 15;
}

function notClub(record) {
    return !(record.TYYP_YLEMGRUPP === 14 && record.TYYP_ALAMGRUPP === 'klubi');
}

module.exports = isValidCsvRecord;
