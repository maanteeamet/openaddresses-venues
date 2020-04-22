const through = require('through2');

const peliasModel = require('pelias-model');
const transformation = require('transform-coordinates');

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
            const number = (record.TASE6_NIMETUS_LIIGIGA + ' ' + record.TASE7_NIMETUS_LIIGIGA).trim();
            const street = record.TASE5_NIMETUS_LIIGIGA.trim();
            // const maakond = record.TASE1_NIMETUS_LIIGIGA;
            // const omavalitsus = record.TASE2_NIMETUS_LIIGIGA;

            //addr fields 'name', 'number', 'unit', 'street', 'cross_street', 'zip'
            try {
                console.log('before try - record: 1-' + record.TASE1_NIMETUS_LIIGIGA,
                    ' 2-', record.TASE2_NIMETUS_LIIGIGA, ' 5-', record.TASE5_NIMETUS_LIIGIGA,
                    ' 6-', record.TASE6_NIMETUS_LIIGIGA,' 7-', record.TASE7_NIMETUS_LIIGIGA,
                    ' x-', record.VIITEPUNKT_X,' y-', record.VIITEPUNKT_Y);
                const coordinates = transform.forward(
                    {x: parseFloat(record.VIITEPUNKT_X), y: parseFloat(record.VIITEPUNKT_Y)}
                );
                console.log('1');
                const addrDoc = new peliasModel.Document('openaddresses', 'address', model_id)
                    .setName('default', (number + ' ' + street))
                    .setCentroid({lon: coordinates.x, lat: coordinates.y});

                console.log('2');
                if (number && number.length > 0) {
                    addrDoc.setAddress('number', number);
                }

                console.log('3');
                if (street && street.length > 0) {
                    addrDoc.setAddress('street', street);
                }

                // console.log('4');
                // //add region, localadmin
                // if (maakond && maakond.length > 0) {
                //     addrDoc.addParent('region', maakond);
                // }
                // console.log('5');
                // if (omavalitsus && omavalitsus.length > 0) {
                //     addrDoc.addParent('localadmin', omavalitsus);
                // }

                console.log('beforePush - ', addrDoc);
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
