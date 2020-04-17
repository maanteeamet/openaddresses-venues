const through = require( 'through2' );

const peliasModel = require( 'pelias-model' );

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
    function write( record, enc, next ){
      const id_number = record.HASH || uid;
      const model_id = `${id_prefix}:${id_number}`;
      uid++;
      const number = record.TASE6_NIMETUS_LIIGIGA + ',' + record.TASE7_NIMETUS_LIIGIGA;
      const street = record.TASE5_NIMETUS_LIIGIGA;
      const lon = record.VIITEPUNKT_X;
      const lat = record.VIITEPUNKT_Y;

      try {
        const addrDoc = new peliasModel.Document( 'openaddresses', 'address', model_id )
        .setName( 'default', (number + ' ' + street) )
        .setCentroid( { lon, lat } );

        addrDoc.setAddress( 'number', number );

        addrDoc.setAddress( 'street', street );

        this.push( addrDoc );
      }
      catch ( ex ){
        stats.badRecordCount++;
      }

      next();
    }
  );
}

module.exports = {
  create: createDocumentStream
};
