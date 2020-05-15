
/*
 * Return true if a record has all of LON, LAT, NUMBER and STREET defined
 */
function isValidCsvRecord(record) {
    return hasAllProperties(record) &&
        !hasApartmentNumber(record);
}

/*
 * Return false if record.STREET contains literal word 'NULL', 'UNDEFINED',
 * or 'UNAVAILABLE' (case-insensitive)
*/
// function streetContainsExclusionaryWord(record) {
//     return /\b(NULL|UNDEFINED|UNAVAILABLE)\b/i.test(record.TASE5_NIMETUS_LIIGIGA);
// }

function hasAllProperties(record) {
    return ['VIITEPUNKT_X', 'VIITEPUNKT_Y', 'TASE1_NIMETUS_LIIGIGA']
            .every(function (prop) {
                return record[prop] && record[prop].length > 0;
            }) &&
        (
            ( record.TASE2_NIMETUS_LIIGIGA && record.TASE2_NIMETUS_LIIGIGA.trim().length > 0 ) ||
            ['TASE2_NIMETUS_LIIGIGA', 'TASE3_NIMETUS_LIIGIGA']
                .every(function (prop) {
                    return record[prop] && record[prop].length > 0;
                }) ||
            ['TASE2_NIMETUS_LIIGIGA', 'TASE3_NIMETUS_LIIGIGA', 'TASE5_NIMETUS_LIIGIGA']
                .every(function (prop) {
                    return record[prop] && record[prop].length > 0;
                }) ||
            ['TASE2_NIMETUS_LIIGIGA', 'TASE3_NIMETUS_LIIGIGA', 'TASE6_NIMETUS_LIIGIGA']
                .every(function (prop) {
                    return record[prop] && record[prop].length > 0;
                }) ||
            ['TASE2_NIMETUS_LIIGIGA', 'TASE3_NIMETUS_LIIGIGA', 'TASE5_NIMETUS_LIIGIGA', 'TASE7_NIMETUS_LIIGIGA']
                .every(function (prop) {
                    return record[prop] && record[prop].length > 0;
                })
        );
}

/*
    We dont need records with apartment numbers
 */
function hasApartmentNumber(record) {
    return ['VIITEPUNKT_X', 'VIITEPUNKT_Y'].every(function (prop) {
        return record[prop] && record[prop].length > 0;
    }) && record.TASE8_NIMETUS_LIIGIGA && record.TASE8_NIMETUS_LIIGIGA.trim().length > 0;
}

module.exports = isValidCsvRecord;
