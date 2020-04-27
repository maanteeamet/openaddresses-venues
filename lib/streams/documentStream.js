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
    /**
     * Used to track the UID of individual records passing through the stream if
     * there is no HASH that can be used as a more unique identifier.  See
     * `peliasModel.Document.setId()` for information about UIDs.
     */
    let uid = 0;

    return through.obj(
        function write(record, enc, next) {
            const id_number = record.HASH || uid;
            const model_id = `${id_prefix}:${id_number}`;
            uid++;
            const transform = transformation('EPSG:3301', 'EPSG:4326');
            const number = (record.TASE6_NIMETUS_LIIGIGA.trim() + ' ' + record.TASE7_NIMETUS_LIIGIGA.trim()).trim();
            const street = record.TASE5_NIMETUS_LIIGIGA.trim();
            const maakond = record.TASE1_NIMETUS_LIIGIGA.trim();
            const vald = record.TASE2_NIMETUS_LIIGIGA.trim();
            const omavalitsus = record.TASE3_NIMETUS_LIIGIGA.trim();
            let name = (street + ' ' + number).trim();
            if (isBlank(name)) {
                if (isBlank(vald)) {
                    name = omavalitsus;
                } else { name = vald; }
            }
            const addrId = record.ADOB_ID.trim();

            //addr fields 'name', 'number', 'unit', 'street', 'cross_street', 'zip'
            try {
                // console.log('before try - record: 1-' + record.TASE1_NIMETUS_LIIGIGA,
                //     ' 2-', record.TASE2_NIMETUS_LIIGIGA, ' 5-', record.TASE5_NIMETUS_LIIGIGA,
                //     ' 6-', record.TASE6_NIMETUS_LIIGIGA,' 7-', record.TASE7_NIMETUS_LIIGIGA,
                //     ' x-', record.VIITEPUNKT_X,' y-', record.VIITEPUNKT_Y);
                const coordinates = transform.forward(
                    {x: parseFloat(record.VIITEPUNKT_X), y: parseFloat(record.VIITEPUNKT_Y)}
                );
                const addrDoc = new peliasModel.Document('openaddresses', 'address', model_id)
                    .setName('default', name)
                    .setCentroid({lon: coordinates.x, lat: coordinates.y});

                if (number && number.length > 0) {
                    addrDoc.setAddress('number', number);
                }
                if (street && street.length > 0) {
                    addrDoc.setAddress('street', street);
                }

                //add region, localadmin
                if (maakond && maakond.length > 0) {
                    addrDoc.addParent('county', maakond, addrId);
                }
                if (omavalitsus && omavalitsus.length > 0) {
                    addrDoc.addParent('localadmin', omavalitsus, addrId);
                }

                // console.log('beforePush - ', addrDoc);
                this.push(addrDoc);
            } catch (ex) {
                console.log(ex.message);
                stats.badRecordCount++;
            }

            next();
        }
    );
}

module.exports = {
    create: createDocumentStream
};
