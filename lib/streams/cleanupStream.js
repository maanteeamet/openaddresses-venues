const through2 = require( 'through2' );

const cleanup = require( '../cleanup' );

/*
 * create a stream that performs any needed cleanup on a record
 */

function createCleanupStream() {
  return through2.obj(function( record, enc, next ) {
    record.TASE5_NIMETUS_LIIGIGA = cleanup.streetName( record.TASE5_NIMETUS_LIIGIGA );

    // csvParse will only trim unquoted fields
    // so we have to do it ourselves to handle all whitespace
    Object.keys(record).forEach(function(key) {
      if (typeof record[key].trim === 'function') {
        record[key] = record[key].trim();
      }
    });

    next(null, record);
  });
}

module.exports = {
  create: createCleanupStream
};
