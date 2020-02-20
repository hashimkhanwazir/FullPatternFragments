package org.linkeddatafragments.datasource.hdt;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.shared.InvalidPropertyURIException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForBindingsRestrictedTriplePatterns;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForStarPatterns;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.linkeddatafragments.fragments.spf.IStarPatternFragmentRequest;
import org.linkeddatafragments.fragments.spf.StarPatternFragmentImpl;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.fragments.tpf.TriplePatternFragmentImpl;
import org.linkeddatafragments.util.StarString;
import org.linkeddatafragments.util.TripleElement;
import org.linkeddatafragments.util.Tuple;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionaryUtil;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;
import org.rdfhdt.hdtjena.NodeDictionary;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

import static org.linkeddatafragments.util.CommonResources.INVALID_URI;
import static org.linkeddatafragments.util.RDFTermParser.STRINGPATTERN;

public class HdtBasedRequestProcessorForSPFs
        extends AbstractRequestProcessorForStarPatterns<RDFNode,String,String> {
    private Map<String, Long> sizeCache = new ConcurrentHashMap<>();
    private final String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    protected final Model model;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    /**
     * Creates the request processor.
     *
     * @throws IOException if the file cannot be loaded
     */
    public HdtBasedRequestProcessorForSPFs( HDT hdt, NodeDictionary dict )
    {
        datasource = hdt;
        dictionary = dict;
        model = ModelFactory.createModelForGraph(new HDTGraph(datasource));
    }

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getSPFSpecificWorker(
            final IStarPatternFragmentRequest<RDFNode,String,String> request )
            throws IllegalArgumentException
    {
        return new Worker( request );
    }

    /**
     * Worker class for HDT
     */
    protected class Worker
            extends AbstractRequestProcessorForStarPatterns.Worker<RDFNode,String,String>
    {


        /**
         * Create HDT Worker
         *
         * @param req
         */
        public Worker(
                final IStarPatternFragmentRequest<RDFNode,String,String> req )
        {
            super( req );
        }

        /**
         * Creates an {@link ILinkedDataFragment} from the HDT
         *
         * @param subject
         * @param bindings
         * @param offset
         * @param limit
         * @return
         */
        @Override
        protected ILinkedDataFragment createFragment(
                final IStarPatternElement<RDFNode,String,String> subject,
                final List<Tuple<IStarPatternElement<RDFNode,String,String>,
                        IStarPatternElement<RDFNode,String,String>>> stars,
                final List<Binding> bindings,
                final long offset,
                final long limit )
        {
            List<Tuple<CharSequence, CharSequence>> s = new ArrayList<>();
            int i = 1;
            for(Tuple<IStarPatternElement<RDFNode,String,String>,
                    IStarPatternElement<RDFNode,String,String>> tpl : stars) {
                IStarPatternElement<RDFNode,String,String> pe = tpl.x;
                IStarPatternElement<RDFNode,String,String> oe = tpl.y;
                String pred = pe.isVariable() ? "" : pe.asConstantTerm().toString();
                String obj = oe.isVariable() ? "" : oe.asConstantTerm().toString();

                s.add(new Tuple<>(pred, obj));
                i++;
            }

            String subj = subject.isVariable() ? "" : subject.asConstantTerm().toString();
            StarString star = new StarString(subj, s);

            int size = star.size();

            if(size == 1)
                return createFragmentByTriplePatternSubstitutionSingle(star, bindings, offset, limit);
            return createFragmentByTriplePatternSubstitution(star, bindings, offset, limit);
        }

        private ILinkedDataFragment createFragmentByTriplePatternSubstitutionSingle(
                final StarString star,
                final List<Binding> bindings,
                final long offset,
                final long limit ) {
            final StarStringIterator it = new StarStringIterator(bindings, star);

            final List<Model> stars = new ArrayList<>();
            int triplesCheckedSoFar = 0;
            int triplesAddedInCurrentPage = 0;
            boolean atOffset;
            int countBindingsSoFar = 0;
            int found = 0;
            int skipped = 0;
            Dictionary dict = datasource.getDictionary();
            while (it.hasNext()) {
                StarString st = it.next();
                final TripleString ts = st.getTriple(0);
                final TripleID t = DictionaryUtil.tripleStringtoTripleID(dict, ts);
                final IteratorTripleID matches = datasource.getTriples().search(t);

                final boolean hasMatches = matches.hasNext();
                if (hasMatches) {
                    matches.goToStart();
                    while (!(atOffset = (triplesCheckedSoFar == offset))
                            && matches.hasNext()) {
                        matches.next();
                        triplesCheckedSoFar++;
                    }

                    // try to add `limit` triples to the result model
                    if (atOffset) {
                        for (int i = found; i < limit && matches.hasNext(); i++) {
                            TripleID tid = matches.next();

                            Model triples = ModelFactory.createDefaultModel();
                            triples.add(triples.asStatement(toTriple(tid)));
                            stars.add(triples);
                        }
                    }
                }
                countBindingsSoFar++;
            }

            final int bindingsSize = bindings.size();
            final long minimumTotal = offset + triplesAddedInCurrentPage + 1;
            final long estimatedTotal;
            if (triplesAddedInCurrentPage < limit) {
                estimatedTotal = offset + triplesAddedInCurrentPage;
            }
//         else // This else block is for testing purposes only. The next else block is the correct one.
//         {
//             estimatedTotal = minimumTotal;
//         }
            else {
                final int THRESHOLD = 10;
                final int maxBindingsToUseInEstimation;
                if (bindingsSize <= THRESHOLD) {
                    maxBindingsToUseInEstimation = bindingsSize;
                } else {
                    maxBindingsToUseInEstimation = THRESHOLD;
                }

                long estimationSum = 0L;
                it.reset();
                int i = 0;
                while (it.hasNext() && i < maxBindingsToUseInEstimation) {
                    i++;
                    StarString st = it.next();
                    final TripleString ts = st.getTriple(0);
                    final TripleID t = DictionaryUtil.tripleStringtoTripleID(dict, ts);
                    estimationSum += estimateResultSetSize(t);
                }

                if (bindingsSize <= THRESHOLD) {
                    if (estimationSum <= minimumTotal)
                        estimatedTotal = minimumTotal;
                    else
                        estimatedTotal = estimationSum;
                } else // bindingsSize > THRESHOLD
                {
                    final double fraction = bindingsSize / maxBindingsToUseInEstimation;
                    final double estimationAsDouble = fraction * estimationSum;
                    final long estimation = Math.round(estimationAsDouble);
                    if (estimation <= minimumTotal)
                        estimatedTotal = minimumTotal;
                    else
                        estimatedTotal = estimation;
                }
            }

            final long estimatedValid = estimatedTotal;

            boolean isLastPage = found < limit;
            return new StarPatternFragmentImpl(stars, estimatedValid, request.getFragmentURL(), request.getDatasetURL(), request.getPageNumber(), isLastPage);
        }

        private ILinkedDataFragment createFragmentByTriplePatternSubstitution(
                final StarString star,
                final List<Binding> bindings,
                final long offset,
                final long limit ) {
            final StarStringIterator it = new StarStringIterator(bindings, star);

            final List<Model> stars = new ArrayList<>();
            Set<String> processed = new HashSet<>();
            int found = 0;
            long estSize = 0;
            int skipped = 0;
            while (it.hasNext()) {
                StarString st = it.next();
                String queryString = "select * where { ";
                String subj = "<" + st.getSubject().toString() + ">";
                if (subj.equals("<>"))
                    subj = "?s";

                List<Tuple<CharSequence, CharSequence>> tpls = st.getTriples();
                int s = tpls.size();
                for (int i = 0; i < s; i++) {
                    Tuple<CharSequence, CharSequence> tpl = tpls.get(i);
                    String pred = tpl.x.toString();
                    String obj = tpl.y.toString();

                    pred = pred.equals("") ? "?p" + (i + 1) : "<" + pred + ">";
                    obj = obj.equals("") ? "?o" + (i + 1) : "<" + obj + ">";

                    queryString += subj + " " + pred + " " + obj + " . ";
                }

                int indx = queryString.lastIndexOf(".");
                queryString = queryString.substring(0, indx) + " }";

                if (processed.contains(queryString)) continue;
                processed.add(queryString);

                Query query = QueryFactory.create(queryString);
                QueryExecution qe = QueryExecutionFactory.create(query, model);
                ResultSet results = qe.execSelect();

                final boolean hasMatches = results.hasNext();

                boolean cacheContains = sizeCache.containsKey(queryString);
                long size = offset;

                if (hasMatches) {
                    boolean atOffset;

                    for (int i = skipped; !(atOffset = i == offset)
                            && results.hasNext(); i++) {
                        results.next();
                        skipped++;
                    }

                    // try to add `limit` triples to the result model
                    if (atOffset) {
                        for (int i = found; i < limit && results.hasNext(); i++) {
                            List<Triple> ts = toTriples(results.next(), st);

                            Model triples = ModelFactory.createDefaultModel();
                            int sz = ts.size();
                            for (int j = 0; j < sz; j++) {
                                triples.add(triples.asStatement(ts.get(j)));
                            }
                            size++;
                            found++;

                            stars.add(triples);
                        }

                        if (!cacheContains) {
                            while (results.hasNext()) {
                                size++;
                                results.next();
                            }

                            sizeCache.put(queryString, size);
                        }
                    }
                } else if (!cacheContains) {
                    sizeCache.put(queryString, 0L);
                }

                size = sizeCache.get(queryString);

                estSize += size;
            }

            final long estimatedValid = estSize;

            boolean isLastPage = found < limit;
            return new StarPatternFragmentImpl(stars, estimatedValid, request.getFragmentURL(), request.getDatasetURL(), request.getPageNumber(), isLastPage);
        }

        private List<Triple> toTriples(QuerySolution sol, StarString star) {
            List<Triple> ret = new ArrayList<>();

            String subj = star.getSubject().toString();
            subj = subj.equals("") ? sol.get("s").toString() : subj;

            Node subjNode = NodeFactory.createURI(subj);

            List<Tuple<CharSequence, CharSequence>> tpls = star.getTriples();
            int s = tpls.size();
            for (int i = 0; i < s; i++) {
                Tuple<CharSequence, CharSequence> tpl = tpls.get(i);
                if (tpl.x == null || tpl.y == null)continue;
                String pred = tpl.x.toString();
                String obj = tpl.y.toString();

                pred = pred.equals("") ? sol.get("p" + (i + 1)).toString() : pred;
                obj = obj.equals("") ? sol.get("o" + (i + 1)).toString() : obj;

                ret.add(new Triple(subjNode, NodeFactory.createURI(pred),
                        obj.matches(regex) ? NodeFactory.createURI(obj) : NodeFactory.createLiteral(obj)));
            }

            return ret;
        }

        private long estimateResultSetSize(final TripleID t) {
            final IteratorTripleID matches = datasource.getTriples().search(t);

            if (matches.hasNext())
                return Math.max(matches.estimatedNumResults(), 1L);
            else
                return 0L;
        }

        /**
         * Parses the given value as an RDF resource.
         *
         * @param value the value
         * @return the parsed value, or null if unspecified
         */
        private TripleElement parseAsResource(String value) {
            final TripleElement subject = parseAsNode(value);
            if (subject.object == null) {
                return new TripleElement("null", null);
            }
            if (subject.name.equals("Var")) {
                return subject;
            }
            return subject.object == null || subject.object instanceof Resource ? new TripleElement(
                    "RDFNode", (Resource) subject.object) : new TripleElement(
                    "Property", INVALID_URI);
        }

        /**
         * Parses the given value as an RDF property.
         *
         * @param value the value
         * @return the parsed value, or null if unspecified
         */
        private TripleElement parseAsProperty(String value) {
            // final RDFNode predicateNode = parseAsNode(value);
            final TripleElement predicateNode = parseAsNode(value);
            if (predicateNode.object == null) {
                return new TripleElement("null", null);
            }
            if (predicateNode.name.equals("Var")) {
                return predicateNode;
            }
            if (predicateNode.object instanceof Resource) {
                try {
                    return new TripleElement(
                            "Property",
                            ResourceFactory
                                    .createProperty(((Resource) predicateNode.object)
                                            .getURI()));
                } catch (InvalidPropertyURIException ex) {
                    return new TripleElement("Property", INVALID_URI);
                }
            }
            return predicateNode.object == null ? null : new TripleElement(
                    "Property", INVALID_URI);
        }

        /**
         * Parses the given value as an RDF node.
         *
         * @param value the value
         * @return the parsed value, or null if unspecified
         */
        private TripleElement parseAsNode(String value) {
            // nothing or empty indicates an unknown
            if (value == null || value.length() == 0) {
                return new TripleElement("null", null);
            }
            // find the kind of entity based on the first character
            final char firstChar = value.charAt(0);
            switch (firstChar) {
                // variable or blank node indicates an unknown
                case '?':
                    return new TripleElement(Var.alloc(value.replaceAll("\\?", "")));
                case '_':
                    return null;
                // angular brackets indicate a URI
                case '<':
                    return new TripleElement(
                            ResourceFactory.createResource(value.substring(1,
                                    value.length() - 1)));
                // quotes indicate a string
                case '"':
                    final Matcher matcher = STRINGPATTERN.matcher(value);
                    if (matcher.matches()) {
                        final String body = matcher.group(1);
                        final String lang = matcher.group(2);
                        final String type = matcher.group(3);
                        if (lang != null) {
                            return new TripleElement(
                                    ResourceFactory.createLangLiteral(body, lang));
                        }
                        if (type != null) {
                            return new TripleElement(
                                    ResourceFactory.createTypedLiteral(body,
                                            TypeMapper.getInstance().getSafeTypeByName(type)));
                        }
                        return new TripleElement(
                                ResourceFactory.createPlainLiteral(body));
                    }
                    return new TripleElement("Property", INVALID_URI);
                // assume it's a URI without angular brackets
                default:
                    return new TripleElement(
                            ResourceFactory.createResource(value));
            }
        }

        private class StarStringIterator implements Iterator<StarString> {
            private List<Binding> bindings;
            private StarString star;
            private int current = 0;
            private StarString next = null;

            StarStringIterator(List<Binding> bindings, StarString star) {
                this.bindings = bindings;
                this.star = star;
            }

            private void bufferNext() {
                if(bindings == null && current == 0) {
                    next = star;
                    current++;
                    return;
                }
                if(bindings == null) {
                    next = null;
                    return;
                }
                if (current >= bindings.size()) {
                    next = null;
                    return;
                }
                Binding binding = bindings.get(current);
                current++;
                StarString s = new StarString(star.getSubject(), star.getTriples());

                Iterator<Var> vars = binding.vars();
                while (vars.hasNext()) {
                    Var var = vars.next();
                    Node node = binding.get(var);

                    String val = "";
                    if (node.isLiteral())
                        val = node.getLiteral().toString();
                    else if (node.isURI())
                        val = node.getURI();

                    s.updateField(var.getVarName(), val);
                }

                next = s;
            }

            public void reset() {
                current = 0;
            }

            @Override
            public boolean hasNext() {
                if (next == null)
                    bufferNext();
                return next != null;
            }

            @Override
            public StarString next() {
                StarString n = next;
                next = null;
                return n;
            }
        }
    } // end of Worker

    /**
     * Converts the HDT triple to a Jena Triple.
     *
     * @param tripleId the HDT triple
     * @return the Jena triple
     */
    private Triple toTriple(TripleID tripleId) {
        return new Triple(
                dictionary.getNode(tripleId.getSubject(), TripleComponentRole.SUBJECT),
                dictionary.getNode(tripleId.getPredicate(), TripleComponentRole.PREDICATE),
                dictionary.getNode(tripleId.getObject(), TripleComponentRole.OBJECT)
        );
    }
}
