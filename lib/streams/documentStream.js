const through = require('through2');

const peliasModel = require('pelias-model');
const transformation = require('transform-coordinates');
const peliasLogger = require( 'pelias-logger' ).get( 'openaddresses' );

/*
 * Create a stream of Documents from valid, cleaned CSV records
 */
function createDocumentStream(id_prefix, stats) {

    return through.obj(
        function write(record, enc, next) {
            const transform = transformation('EPSG:3301', 'EPSG:4326');

            const oid = record.ADS_OID.trim();
            const name = record.AVALIK_NIMI.trim();
            const nameAlias = record.AVALIK_ALIAS.trim();
            const grupp = record.TYYP_GRUPP.trim();
            const alamgrupp = record.TYYP_ALAMGRUPP.trim();
            const tyyp = record.TYYPNIMI.trim();

            peliasLogger.info('Name: ' + name + ', alias: ' + nameAlias + ', grupp: ' +
            grupp + ', alamgrupp: ' + alamgrupp + ', tüüp: ' + tyyp);

            //venue fields 'name', 'nameAlias', 'category'
            try {
                const coordinates = transform.forward(
                    {x: parseFloat(record.AVALIK_X), y: parseFloat(record.AVALIK_Y)}
                );
                const addrDoc = new peliasModel.Document('openaddresses', 'venue', oid)
                    .setName('default', name)
                    .setNameAlias('default', nameAlias)
                    .addCategory(grupp)
                    .addCategory(alamgrupp)
                    .addCategory(tyyp)
                    .setCentroid({lon: coordinates.x, lat: coordinates.y});

                this.push(addrDoc);
            } catch (ex) {
                peliasLogger.error(ex.message);
                stats.badRecordCount++;
            }

            next();
        }
    );
}

module.exports = {
    create: createDocumentStream
};
