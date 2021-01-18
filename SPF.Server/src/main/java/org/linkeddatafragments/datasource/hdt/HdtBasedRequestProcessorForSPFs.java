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
import org.linkeddatafragments.characteristicset.CharacteristicSetImpl;
import org.linkeddatafragments.characteristicset.ICharacteristicSet;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForStarPatterns;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.spf.IStarPatternElement;
import org.linkeddatafragments.fragments.spf.IStarPatternFragmentRequest;
import org.linkeddatafragments.fragments.spf.StarPatternFragmentImpl;
import org.linkeddatafragments.util.*;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.iterator.DictionaryTranslateIteratorStar;
import org.rdfhdt.hdt.triples.*;
import org.rdfhdt.hdt.triples.impl.CompoundIteratorStarID;
import org.rdfhdt.hdtjena.HDTGraph;
import org.rdfhdt.hdtjena.NodeDictionary;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;

import static org.linkeddatafragments.util.CommonResources.INVALID_URI;
import static org.linkeddatafragments.util.RDFTermParser.STRINGPATTERN;

public class HdtBasedRequestProcessorForSPFs
        extends AbstractRequestProcessorForStarPatterns<RDFNode, String, String> {
    private final String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    private final LRUCache<Tuple<Long, Long>, IteratorStarString> pageCache;

    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    protected final Model model;

    /**
     * The Characteristic Sets
     */
    protected final List<ICharacteristicSet> characteristicSets;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    protected static Map<StarString, Double> elemSizeCache = new HashMap<>();

    /**
     * Creates the request processor.
     *
     * @throws IOException if the file cannot be loaded
     */
    public HdtBasedRequestProcessorForSPFs(HDT hdt, NodeDictionary dict, List<ICharacteristicSet> css, LRUCache<Tuple<Long, Long>, IteratorStarString> cache) {
        datasource = hdt;
        dictionary = dict;
        characteristicSets = css;
        pageCache = cache;
        model = ModelFactory.createModelForGraph(new HDTGraph(datasource));
    }

    /**
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getSPFSpecificWorker(
            final IStarPatternFragmentRequest<RDFNode, String, String> request)
            throws IllegalArgumentException {
        return new Worker(request);
    }

    /**
     * Worker class for HDT
     */
    protected class Worker
            extends AbstractRequestProcessorForStarPatterns.Worker<RDFNode, String, String> {


        /**
         * Create HDT Worker
         *
         * @param req
         */
        public Worker(
                final IStarPatternFragmentRequest<RDFNode, String, String> req) {
            super(req);
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
                final IStarPatternElement<RDFNode, String, String> subject,
                final List<Tuple<IStarPatternElement<RDFNode, String, String>,
                        IStarPatternElement<RDFNode, String, String>>> stars,
                final List<Binding> bindings,
                final long offset,
                final long limit,
                final long requestHash) {
            List<Tuple<CharSequence, CharSequence>> s = new ArrayList<>();
            for (Tuple<IStarPatternElement<RDFNode, String, String>,
                    IStarPatternElement<RDFNode, String, String>> tpl : stars) {
                IStarPatternElement<RDFNode, String, String> pe = tpl.x;
                IStarPatternElement<RDFNode, String, String> oe = tpl.y;
                String pred = pe.isVariable() ? "?" + pe.asNamedVariable() : pe.asConstantTerm().toString();
                String obj = oe.isVariable() ? "?" + oe.asNamedVariable() : oe.asConstantTerm().toString();

                s.add(new Tuple<>(pred, obj));
            }

            String subj = subject.isVariable() ? "?" + subject.asNamedVariable() : subject.asConstantTerm().toString();
            StarString star = new StarString(subj, s);

            return createFragmentByTriplePatternSubstitution(star, bindings, offset, limit, requestHash);
        }

        private ILinkedDataFragment createFragmentByTriplePatternSubstitution(
                final StarString star,
                final List<Binding> bindings,
                final long offset,
                final long limit,
                final long requestHash) {
            final List<Model> stars = new ArrayList<>();
            int found = 0;
            int skipped = 0, count = 0;
            double size = 0;

            boolean isNew = true;
            Tuple<Long, Long> initialKey = new Tuple<>(requestHash, offset);
            IteratorStarString results;

            if (pageCache.containsKey(initialKey)) {
                results = pageCache.get(initialKey);
                isNew = false;
            } else {
                results = datasource.searchStarBindings(star, bindings, characteristicSets);
            }

            final boolean hasMatches = results.hasNext();

            if (hasMatches) {
                boolean atOffset;

                if (isNew) {
                    for (int i = skipped; !(atOffset = i == offset)
                            && results.hasNext(); i++) {
                        results.next();
                        skipped++;
                    }
                } else {
                    atOffset = true;
                    skipped = (int) offset;
                }
                count = skipped;

                if (atOffset) {
                    for (int i = found; i < limit && results.hasNext(); i++) {
                        List<Triple> tpl = toTriples(results.next());
                        Model triples = ModelFactory.createDefaultModel();

                        int sz = tpl.size();
                        for (int j = 0; j < sz; j++) {
                            triples.add(triples.asStatement(tpl.get(j)));
                        }
                        found++;

                        stars.add(triples);
                    }
                }

                count += found;
            }


            if (count >= (limit + offset)) {
                size = results.estimatedNumResults();
                //size = DictionaryTranslateIteratorStar.estimateCardinality(star, bindings, ConfigReader.getInstance().getCharacteristicSets());
            } else {
                size = count;
            }

            //if(size == 0 && count > 0)
            //    size = DictionaryTranslateIteratorStar.estimateCardinality(star, bindings, ConfigReader.getInstance().getCharacteristicSets());

            if(size == 0 && count > 0)
                size = count;

            final long estimatedValid = (long) size;

            boolean isLastPage = found < limit;

            pageCache.remove(initialKey);
            if (results.hasNext()) {
                Tuple<Long, Long> key = new Tuple<>(requestHash, (long) count);
                pageCache.put(key, results);
            }

            return new StarPatternFragmentImpl(stars, estimatedValid, request.getFragmentURL(), request.getDatasetURL(), request.getPageNumber(), isLastPage, found);
        }

        private List<Triple> toTriples(StarString star) {
            List<Triple> ret = new ArrayList<>();

            int s = star.size();
            for (int i = 0; i < s; i++) {
                TripleString tpl = star.getTriple(i);
                String obj = tpl.getObject().toString();
                ret.add(new Triple(NodeFactory.createURI(tpl.getSubject().toString()),
                        NodeFactory.createURI(tpl.getPredicate().toString()),
                        obj.matches(regex) ? NodeFactory.createURI(obj) : NodeFactory.createLiteral(obj.replace("\"", ""))));
            }

            return ret;
        }
    } // end of Worker
}
