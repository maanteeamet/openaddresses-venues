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
            let lon, lat;
            const number = (record.TASE6_NIMETUS_LIIGIGA + ' ' + record.TASE7_NIMETUS_LIIGIGA).trim();
            const street = ('' + record.TASE5_NIMETUS_LIIGIGA).trim();
            const maakond = record.TASE1_NIMETUS_LIIGIGA;
            const omavalitsus = record.TASE2_NIMETUS_LIIGIGA;

            //addr fields 'name', 'number', 'unit', 'street', 'cross_street', 'zip'
            try {
                console.log('x isFinite: ', Number.isFinite(record.VIITEPUNKT_X));
                console.log('y isFinite: ', Number.isFinite(record.VIITEPUNKT_Y));
                const coordinates = transform.forward({x: record.VIITEPUNKT_X, y: record.VIITEPUNKT_Y});
                lon = coordinates.x;
                lat = coordinates.y;
                console.log('1');
                const addrDoc = new peliasModel.Document('openaddresses', 'address', model_id)
                    .setName('default', (number + ' ' + street))
                    .setCentroid({lon, lat});

                if (number && number.length > 0) {
                    addrDoc.setAddress('number', number);
                }

                if (street && street.length > 0) {
                    addrDoc.setAddress('street', street);
                }

                console.log('2');
                //add region, localadmin
                addrDoc.addParent('region', maakond);
                console.log('3');
                addrDoc.addParent('localadmin', omavalitsus);

                console.log('beforePush - ', addrDoc);
                this.push(addrDoc);
            } catch (ex) {
                console.log(ex.message, '\n##error - number:', number, ' street:', street, ' lon:', lon, ' lat:', lat);
                stats.badRecordCount++;
            }

            next();
        }
    );
}

module.exports = {
    create: createDocumentStream
};
