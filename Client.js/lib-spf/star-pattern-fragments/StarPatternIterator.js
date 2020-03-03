/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A StarPatternIterator builds bindings by reading matches for a triple pattern. */

var AsyncIterator = require('../asynciterator/asynciterator.js'),
    MultiTransformIterator = AsyncIterator.MultiTransformIterator,
    rdf = require('../util/RdfUtil'),
    Logger = require('../util/ExecutionLogger')('StarPatternIterator');

// Creates a new StarPatternIterator
function StarPatternIterator(parent, pattern, options) {
  if (!(this instanceof StarPatternIterator))
    return new StarPatternIterator(parent, pattern, options);
  MultiTransformIterator.call(this, parent, options);

  this._pattern = pattern;
  this._client = options && options.fragmentsClient;
}
MultiTransformIterator.subclass(StarPatternIterator);

// Creates a transformer that extends upstream bindings with matches for the triple pattern.
// For example, if the iterator's triple pattern is '?s rdf:type ?o',
// and the upstream sends a binding { ?o: dbpedia-owl:City' },
// then we return an iterator for [{ ?o: dbpedia-owl:City', ?s: dbpedia:Ghent' }, …].
StarPatternIterator.prototype._createTransformer = function (bindings, options) {
  // Apply the upstream bindings to the iterator's triple pattern.
  // example: apply { ?o: dbpedia-owl:City } to '?s rdf:type ?o'
  var boundPattern = this._pattern,
      pattern = this._pattern;
  if (!Array.isArray(bindings))
    boundPattern = rdf.applyBindings(bindings, this._pattern);

  // Retrieve the fragment that corresponds to the bound pattern.
  // example: retrieve the fragment for '?s rdf:type dbpedia-owl:City'

  var fragment;
  if (Array.isArray(boundPattern)) {
    if (!Array.isArray(bindings))
      fragment = this._client.getFragmentByStarPattern(boundPattern);
    else
      fragment = this._client.getFragmentByStarPatternWithBindings(boundPattern, bindings);
  }
  else {
    if (!Array.isArray(bindings))
      fragment = this._client.getFragmentByTriplePattern(boundPattern);
    else
      fragment = this._client.getFragmentByTriplePatternWithBindings(boundPattern, bindings);
  }
  Logger.logFragment(this, fragment, bindings);
  fragment.on('error', function (error) { Logger.warning(error.message); });

  // Transform the fragment's triples into bindings for the triple pattern.
  // example: [{ ?o: dbpedia-owl:City', ?s: dbpedia:Ghent' }, …]
  return fragment.map(function (triple) {
    // Extend the bindings such that they bind the iterator's pattern to the triple.
    try {
      if (!Array.isArray(bindings))
        return rdf.extendBindingsStar(bindings, pattern, triple);
      else
        return rdf.extendBindingsStarBindings(bindings, pattern, triple);
    }
    // If the triple conflicted with the bindings (e.g., non-data triple), skip it.
    catch (error) { return null; }
  });
};

// Generates a textual representation of the iterator
StarPatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + rdf.toQuickString(this._pattern) + ')}' +
         '\n  <= ' + this.getSourceString();
};

module.exports = StarPatternIterator;
