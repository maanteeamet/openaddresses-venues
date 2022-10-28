const through = require('through2');

const peliasModel = require('pelias-model');
const transformation = require('transform-coordinates');
const peliasLogger = require('pelias-logger').get('openaddresses');

/*
 * Create a stream of Documents from valid, cleaned CSV records
 */
function createDocumentStream(id_prefix, stats) {

    return through.obj(
        function write(record, enc, next) {
            const transform = transformation('EPSG:3301', 'EPSG:4326');

            const oid = record.ADS_OID;
            const name = record.AVALIK_NIMI;
            const nameAlias = record.AVALIK_ALIAS;
            const grupp = record.TYYP_GRUPP;
            const alamgrupp = record.TYYP_ALAMGRUPP;
            const tyyp = record.TYYPNIMI;
            const aadress = record.TAISAADRESS;
            //console.log('Name: ' + name + ', alias: ' + nameAlias + ', grupp: ' +
            //    grupp + ', alamgrupp: ' + alamgrupp + ', tüüp: ' + tyyp + ', aadress: ' + aadress);

            const admin_parts = aadress.split(',');//0-county, 1-localadmin, 2-locality
            const admin_parts_length = admin_parts.length - 1;
            let county, localadmin, locality = '';

            if (admin_parts_length >= 3) {
                county = admin_parts[0];
                localadmin = admin_parts[1];
                locality = admin_parts[2];
            } else {
                county = admin_parts[0];
                locality = admin_parts[1];
            }


            //venue fields 'name', 'nameAlias', 'category'
            try {
                const coordinates = transform.forward(
                    {x: parseFloat(record.AVALIK_X), y: parseFloat(record.AVALIK_Y)}
                );
                const addrDoc = new peliasModel.Document('openaddresses', 'venue', `${id_prefix}:${oid}`);
                addrDoc.setName('default', name);
                if (nameAlias && nameAlias.length > 0) {
                    addrDoc.setNameAlias('default', nameAlias);
                }
                addrDoc.addCategory(grupp);
                addrDoc.addCategory(alamgrupp);
                addrDoc.addCategory(tyyp);
                addrDoc.setCentroid({lon: coordinates.x, lat: coordinates.y});

                if (county && county.length > 0) {
                    addrDoc.addParent('county', county, 'c:' + oid);
                }
                if (localadmin && localadmin.length > 0) {
                    addrDoc.addParent('localadmin', localadmin, 'la:' + oid);
                }
                if (locality && locality.length > 0) {
                    addrDoc.addParent('locality', locality, 'l:' + oid);
                }

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
