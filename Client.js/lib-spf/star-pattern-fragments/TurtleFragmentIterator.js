/*! @license MIT ©2013-2016 Ruben Verborgh, Ghent University - imec */
/* A TurtleFragmentIterator reads data and metadata from Linked Data Fragments in Turtle. */

var TransformIterator = require('../asynciterator/asynciterator.js').TransformIterator,
  BufferedIterator = require('../asynciterator/asynciterator.js').BufferedIterator,
  rdf = require('../util/RdfUtil'),
  N3 = require('../n3');

// Creates a new TurtleFragmentIterator
function TurtleFragmentIterator(source, fragmentUrl) {
  if (!(this instanceof TurtleFragmentIterator)) {
    return new TurtleFragmentIterator(source, fragmentUrl);
  }
  TransformIterator.call(this, source);
  this._fragmentUrl = fragmentUrl;
  var single = !fragmentUrl.includes('triple');

  // Expose an additional metadata stream
  this.metadataStream = new BufferedIterator();
  if (source && source.ended) return this.metadataStream._push(null);
  // When a metadata listener is added, drain the source to read metadata
  var self = this;
  this.metadataStream.on('newListener', function metadataListenerAdded(event) {
    if (event === 'data' || event === 'end') {
      this.removeListener('newListener', metadataListenerAdded);
      self.maxBufferSize = Infinity;
    }
  });

  // Convert Turtle into triples using the N3 parser
  this._parser = new N3.Parser({ documentURI: fragmentUrl, single : single });
  this._parser.parse({
      // Use dummy stream to capture `data` and `end` callbacks
      on: function (event, callback) {
        if (event === 'data') {
          self._parseData = callback;
        }
        else if (event === 'end') self._parseEnd = callback;
      },
    },
    // Process each triple and emit possible errors
    function (error, star) {
      if (error) {
        self.emit('error', error);
      }
      else if (star) self._processStar(star);
    });
}

TransformIterator.subclass(TurtleFragmentIterator);

// Sends a chunk of Turtle to the N3 parser to convert it to triples
TurtleFragmentIterator.prototype._transform = function (chunk, done) {
  this._parseData(chunk), done();
};

// Sends the given parsed triple to the data or metadata stream
TurtleFragmentIterator.prototype._processTriple = function (triple) {
  // This separation between data and metadata/controls is an approximation;
  // for a proper separation, use an RDF format with graph support (see TrigFragmentParser).
  if (triple.subject !== this._fragmentUrl && triple.predicate.indexOf(rdf.HYDRA) !== 0) {
    this._push(triple);
  }
  else {
    this.metadataStream._push(triple);
  }
};

// Sends the given parsed star to the data or metadata stream
TurtleFragmentIterator.prototype._processStar = function (star) {
  // This separation between data and metadata/controls is an approximation;
  // for a proper separation, use an RDF format with graph support (see TrigFragmentParser).

  if (star.star === undefined) {
    this._processTriple(star);
    return;
  }

  if (JSON.stringify(star.star).indexOf(rdf.HYDRA) === -1)
    this._push(star);
  else {
    var metadataStream = this.metadataStream;
    star.star.forEach(function (item) {
      metadataStream._push({subject : star.subject, predicate : item.predicate, object : item.object});
    });
    this.metadataStream = metadataStream;
  }
};

// Closes the streams after the source has ended
TurtleFragmentIterator.prototype._flush = function (done) {
  // Ensure the parser processes possible pending triples
  this._parseEnd && this._parseEnd();
  // Once all triples have been processed, close both streams
  this.metadataStream.close();
  done();
};

// Indicates whether the class supports the content type
TurtleFragmentIterator.supportsContentType = function (contentType) {
  return (/^(?:text\/turtle|text\/n3|application\/n-triples)$/).test(contentType);
};

module.exports = TurtleFragmentIterator;
