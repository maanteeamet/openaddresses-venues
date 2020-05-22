const through = require('through2');

const peliasModel = require('pelias-model');
const transformation = require('transform-coordinates');


function isBlank(str) {
    return str === '';
}

/*
 * Create a stream of Documents from valid, cleaned CSV records
 */
function createDocumentStream(id_prefix, stats) {

    return through.obj(
        function write(record, enc, next) {
            const transform = transformation('EPSG:3301', 'EPSG:4326');

            const maakond = record.TASE1_NIMETUS_LIIGIGA.trim();
            /*
                omavalitsus (vald või omavalitsuslik linn).
             */
            const omavalitsus = record.TASE2_NIMETUS_LIIGIGA.trim();
            /*
                asula (küla, alevik, alev, omavalitsuse sisene linn) ja linnaosa.
             */
            const asula = record.TASE3_NIMETUS_LIIGIGA.trim();
            /*
                liikluspind (nt tänav, puiestee, tee).
             */
            const street = record.TASE5_NIMETUS_LIIGIGA.trim();
            /*
                maaüksuse nimi (võib olla kohanimi).
             */
            const place = record.TASE6_NIMETUS_LIIGIGA.trim();
            /*
                aadressinumber
                (maaüksuse või hoone number vajaduse korral koos eristava numbri või tähega e tähtlisandiga).
             */
            const number = record.TASE7_NIMETUS_LIIGIGA.trim();

            let name;
            let venue = false;
            let locality = false;
            const getStreetWithOrWithoutNumber = (str, num) => num ? str + ' ' + num : str;

            name = place || getStreetWithOrWithoutNumber(street, number) || asula || omavalitsus || maakond;
            if (isBlank(street) && isBlank(place) && isBlank(number)) {
                venue = true;
            }

            const addrId = record.ADR_ID.trim();
            //addr fields 'name', 'number', 'unit', 'street', 'cross_street', 'zip'
            try {
                // console.log('before try - record: 1-' + record.TASE1_NIMETUS_LIIGIGA,
                //     ' 2-', record.TASE2_NIMETUS_LIIGIGA, ' 5-', record.TASE5_NIMETUS_LIIGIGA,
                //     ' 6-', record.TASE6_NIMETUS_LIIGIGA,' 7-', record.TASE7_NIMETUS_LIIGIGA,
                //     ' x-', record.VIITEPUNKT_X,' y-', record.VIITEPUNKT_Y);
                const coordinates = transform.forward(
                    {x: parseFloat(record.VIITEPUNKT_X), y: parseFloat(record.VIITEPUNKT_Y)}
                );
                const addrDoc = new peliasModel.Document('openaddresses',
                    (venue ? 'venue' : locality ? 'locality' : 'address'), addrId)
                    .setName('default', name)
                    .setCentroid({lon: coordinates.x, lat: coordinates.y});

                if (number && number.length > 0) {
                    addrDoc.setAddress('number', number);
                }
                if (street && street.length > 0) {
                    addrDoc.setAddress('street', street);
                }

                //add region, county, localadmin, locality
                if (maakond && maakond.length > 0) {
                    addrDoc.addParent('region', maakond, 'r:' + addrId);
                }
                if (omavalitsus && omavalitsus.length > 0) {
                    addrDoc.addParent('county', omavalitsus, 'c:' + addrId);
                }
                if (place && place.length > 0) {
                    addrDoc.addParent('localadmin', place, 'la:' + addrId);
                }
                if (asula && asula.length > 0) {
                    addrDoc.addParent('locality', asula, 'l:' + addrId);
                }

                this.push(addrDoc);
            } catch (ex) {
                console.log(ex.message);
                console.log('record: 1-' + record.TASE1_NIMETUS_LIIGIGA,
                    ' 2-' + record.TASE2_NIMETUS_LIIGIGA, ' 3-' + record.TASE3_NIMETUS_LIIGIGA,
                    ' 5-' + record.TASE5_NIMETUS_LIIGIGA, ' 6-' + record.TASE6_NIMETUS_LIIGIGA,
                    ' 7-' + record.TASE7_NIMETUS_LIIGIGA,
                    ' x-' + record.VIITEPUNKT_X, ' y-' + record.VIITEPUNKT_Y);
                stats.badRecordCount++;
            }

            next();
        }
    );
}

module.exports = {
    create: createDocumentStream
};
