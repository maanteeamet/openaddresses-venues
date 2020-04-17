var _ = require('lodash');

/*
 * Return true if a record has all of LON, LAT, NUMBER and STREET defined
 */
function isValidCsvRecord(record) {
    console.log(hasAllProperties(record));
    console.log(!houseNumberIsExclusionaryWord(record));
    console.log(!streetContainsExclusionaryWord(record));
    console.log(!latLonAreOnNullIsland(record));
    return hasAllProperties(record) &&
        !houseNumberIsExclusionaryWord(record) &&
        !streetContainsExclusionaryWord(record) &&
        !latLonAreOnNullIsland(record);
}

/*
 * Return false if record.NUMBER is literal word 'NULL', 'UNDEFINED',
 * or 'UNAVAILABLE' (case-insensitive)
*/
function houseNumberIsExclusionaryWord(record) {
    return ['NULL', 'UNDEFINED', 'UNAVAILABLE'].indexOf(_.toUpper(record.TASE6_NIMETUS_LIIGIGA)) !== -1 ||
        ['NULL', 'UNDEFINED', 'UNAVAILABLE'].indexOf(_.toUpper(record.TASE7_NIMETUS_LIIGIGA)) !== -1;
}

/*
 * Return false if record.STREET contains literal word 'NULL', 'UNDEFINED',
 * or 'UNAVAILABLE' (case-insensitive)
*/
function streetContainsExclusionaryWord(record) {
    return /\b(NULL|UNDEFINED|UNAVAILABLE)\b/i.test(record.TASE5_NIMETUS_LIIGIGA);
}

function hasAllProperties(record) {
    return (
        ['VIITEPUNKT_X',
            'VIITEPUNKT_Y',
            'TASE6_NIMETUS_LIIGIGA',
            'TASE7_NIMETUS_LIIGIGA'].every(function (prop) {
            return record[prop] && record[prop].length > 0;
        }) ||
        ['VIITEPUNKT_X',
            'VIITEPUNKT_Y',
            'TASE2_NIMETUS_LIIGIGA',
            'TASE3_NIMETUS_LIIGIGA',
            'TASE4_NIMETUS_LIIGIGA'].every(function (prop) {
            return record[prop] && record[prop].length > 0;
        })
    );
}

// returns true when LON and LAT are both parseable as 0
// > parseFloat('0');
// 0
// > parseFloat('0.000000');
// 0
// > parseFloat('0.000001');
// 0.000001
function latLonAreOnNullIsland(record) {
    return ['VIITEPUNKT_X', 'VIITEPUNKT_Y'].every(prop => parseFloat(record[prop]) === 0);
}

module.exports = isValidCsvRecord;
