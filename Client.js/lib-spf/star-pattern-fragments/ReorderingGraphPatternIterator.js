/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* A ReorderingGraphPatternIterator builds bindings by reading matches for a basic graph pattern. */

var AsyncIterator = require('../asynciterator/asynciterator.js'),
    TransformIterator = AsyncIterator.TransformIterator,
    MultiTransformIterator = AsyncIterator.MultiTransformIterator,
    rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    Logger = require('../util/ExecutionLogger')('ReorderingGraphPatternIterator');

var TriplePatternIterator = require('./StarPatternIterator');

// Creates a new ReorderingGraphPatternIterator
function ReorderingGraphPatternIterator(parent, pattern, options) {
  // Empty patterns have no effect
  if (!pattern || !pattern.length)
    return new TransformIterator(parent, options);
  // A one-element pattern can be solved by a triple pattern iterator
  if (pattern.length === 1)
    return new TriplePatternIterator(parent, pattern[0], options);
  // For length two or more, construct a ReorderingGraphPatternIterator
  if (!(this instanceof ReorderingGraphPatternIterator))
    return new ReorderingGraphPatternIterator(parent, pattern, options);
  MultiTransformIterator.call(this, parent, options);

  this._pattern = pattern;
  this._options = options || (options = {});
  this._client = options.fragmentsClient;
}
MultiTransformIterator.subclass(ReorderingGraphPatternIterator);

// Creates a pipeline with triples matching the binding of the iterator's graph pattern
ReorderingGraphPatternIterator.prototype._createTransformer = function (bindings) {
  // Apply the context bindings to the iterator's graph pattern
  var boundPattern = this._pattern, options = this._options;
  if (!Array.isArray(bindings))
    boundPattern = rdf.applyBindings(bindings, this._pattern);
  // Make star patterns

  var temp = {};
  boundPattern.forEach(function (triplePattern) {
    if (triplePattern.subject in temp)
      temp[triplePattern.subject].push(triplePattern);
    else
      temp[triplePattern.subject] = [triplePattern];
  });

  var subPatterns = [];
  for (var k in temp)
    subPatterns.push(temp[k]);
  var remainingPatterns = subPatterns.length, pipeline;

  // If this subpattern has only one triple pattern, use it to create the pipeline
  if (remainingPatterns === 1)
    return createPipeline(subPatterns.pop());

  // Otherwise, we must first find the best triple pattern to start the pipeline
  pipeline = new TransformIterator();
  // Retrieve and inspect the triple patterns' metadata to decide which has least matches
  var bestIndex = 0, minMatches = Infinity;
  subPatterns.forEach(function (starPattern, index) {
    var fragment;
    if (!Array.isArray(bindings))
      fragment = this._client.getFragmentByStarPattern(starPattern);
    else
      fragment = this._client.getFragmentByStarPatternWithBindings(starPattern, bindings);
    fragment.getProperty('metadata', function (metadata) {
      // We don't need more data from the fragment
      fragment.close();
      // If this triple pattern has no matches, the entire graph pattern has no matches
      // totalTriples can either be 0 (no matches) or undefined (no count in fragment)
      if (!metadata.totalTriples)
        return pipeline.close();
      // This triple pattern is the best if it has the lowest number of matches
      if (metadata.totalTriples < minMatches)
        bestIndex = index, minMatches = metadata.totalTriples;
      // After all patterns were checked, create the pipeline from the best pattern
      if (--remainingPatterns === 0)
        pipeline.source = createPipeline(subPatterns.splice(bestIndex, 1)[0]);
    });
    // If the fragment errors, pretend it was empty
    fragment.on('error', function (error) {
      Logger.warning(error.message);
      if (!fragment.getProperty('metadata'))
        fragment.setProperty('metadata', { totalTriples: 0 });
    });
  }, this);
  return pipeline;

  // Creates the pipeline of iterators for the bound graph pattern,
  // starting with a TriplePatternIterator for the triple pattern,
  // then a ReorderingGraphPatternIterator for the remainder of the subpattern,
  // and finally, ReorderingGraphPatternIterators for the remaining subpatterns.
  function createPipeline(subPattern) {
    // Create the iterator for the triple pattern
    var startIterator = AsyncIterator.single(bindings),
        pipeline = new TriplePatternIterator(startIterator, subPattern, options),
        boundVars = getBoundVars([], subPattern);
    // If the chosen subpattern has more triples, create a ReorderingGraphPatternIterator for it
    // if (subPattern && subPattern.length !== 0)
    //  pipeline = new ReorderingGraphPatternIterator(pipeline, subPattern, options);
    // Create ReorderingGraphPatternIterators for all interconnected subpatterns
    while ((subPattern = getLargestBound(boundVars)) !== undefined) {
      pipeline = new ReorderingGraphPatternIterator(pipeline, subPattern, options);
      boundVars = getBoundVars(boundVars, subPattern);
    }
    return pipeline;
  }

  function getLargestBound(boundVars) {
    var maxBound = 0,
        index = 0;

    for (var i = 0; i < subPatterns.length; i++) {
      var pattern = subPatterns[i],
          num = getNumBound(boundVars, pattern);

      if (num > maxBound) {
        maxBound = num;
        index = i;
      }
    }
    var ptn = subPatterns[index];
    subPatterns.splice(index, 1);
    return ptn;
  }

  function getNumBound(boundVars, pattern) {
    var num = 0;
    for (var i = 0; i < pattern.length; i++) {
      var star = pattern[i];
      if (rdf.isVariable(star.subject) && boundVars.includes(star.subject)) num++;
      if (rdf.isVariable(star.predicate) && boundVars.includes(star.predicate)) num++;
      if (rdf.isVariable(star.object) && boundVars.includes(star.object)) num++;
    }
    return num;
  }

  function getBoundVars(boundVars, pattern) {
    for (var i = 0; i < pattern.length; i++) {
      var star = pattern[i];
      if (rdf.isVariable(star.subject) && !boundVars.includes(star.subject)) boundVars.push(star.subject);
      if (rdf.isVariable(star.predicate) && !boundVars.includes(star.predicate)) boundVars.push(star.predicate);
      if (rdf.isVariable(star.object) && !boundVars.includes(star.object)) boundVars.push(star.object);
    }
    return boundVars;
  }
};

// Generates a textual representation of the iterator
ReorderingGraphPatternIterator.prototype.toString = function () {
  return '[' + this.constructor.name +
         ' {' + this._pattern.map(rdf.toQuickString).join(' ') + '}]' +
         '\n  <= ' + this.getSourceString();
};

module.exports = ReorderingGraphPatternIterator;
