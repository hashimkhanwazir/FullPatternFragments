package org.linkeddatafragments.datasource.hdt;

import java.io.IOException;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;

import org.linkeddatafragments.datasource.AbstractRequestProcessorForTriplePatterns;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;



 
public class HdtBasedRequestProcessorForTPFs
    extends AbstractRequestProcessorForTriplePatterns<RDFNode,String,String>
{

    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    /**
     * Creates the request processor.
     *
     * @throws IOException if the file cannot be loaded
     */
    public HdtBasedRequestProcessorForTPFs( HDT hdt, NodeDictionary dict )
    {
        datasource = hdt;
        dictionary = dict;
    }

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getTPFSpecificWorker(
            final ITriplePatternFragmentRequest<RDFNode,String,String> request )
                                                throws IllegalArgumentException
    {   
        System.out.println("\nClass HdtBasedRequestProcessorForTPFs.java - Method getTPFSpecificWorker(request)"); 
        System.out.println(" ---Here is the request: " + request);
        return new Worker( request );
    }

    /**
     * Worker class for HDT
     */
    protected class Worker
       extends AbstractRequestProcessorForTriplePatterns.Worker<RDFNode,String,String>
    {

        /**
         * Create HDT Worker
         * 
         * @param req
         */
        public Worker(
                final ITriplePatternFragmentRequest<RDFNode,String,String> req )
        {   
            super( req );
            System.out.println("\n ---Worker instance created !!!");
        }

        /**
         * Creates an {@link ILinkedDataFragment} from the HDT
         * 
         * @param subject
         * @param predicate
         * @param object
         * @param offset
         * @param limit
         * @return
         */
        @Override
        protected ILinkedDataFragment createFragment(
                   final ITriplePatternElement<RDFNode,String,String> subject,
                   final ITriplePatternElement<RDFNode,String,String> predicate,
                   final ITriplePatternElement<RDFNode,String,String> object,
                   final long offset,
                   final long limit )
        {
            // FIXME: The following algorithm is incorrect for cases in which
            //        the requested triple pattern contains a specific variable
            //        multiple times;
            //        e.g., (?x foaf:knows ?x ) or (_:bn foaf:knows _:bn)
            // see https://github.com/LinkedDataFragments/Server.Java/issues/23

            // look up the result from the HDT datasource)
            
            System.out.println("\n ---Look up the result from the HDT datasource");
            int subjectId = subject.isVariable() ? 0 : dictionary.getIntID(subject.asConstantTerm().asNode(), TripleComponentRole.SUBJECT);
            int predicateId = predicate.isVariable() ? 0 : dictionary.getIntID(predicate.asConstantTerm().asNode(), TripleComponentRole.PREDICATE);
            int objectId = object.isVariable() ? 0 : dictionary.getIntID(object.asConstantTerm().asNode(), TripleComponentRole.OBJECT);
            System.out.println("\n ---Triple details:" + " subjectId = "+subjectId+" predicateId = "+predicateId+" objectId = "+objectId );
        
            if (subjectId < 0 || predicateId < 0 || objectId < 0) {
                return createEmptyTriplePatternFragment();
            }
        
            final Model triples = ModelFactory.createDefaultModel();
            IteratorTripleID matches = datasource.getTriples().search(new TripleID(subjectId, predicateId, objectId));
            boolean hasMatches = matches.hasNext();
            System.out.println("\n--- Searching for triple matches.... Found: (True/False) " + hasMatches);
		
            if (hasMatches) {
                // try to jump directly to the offset
                boolean atOffset;
                if (matches.canGoTo()) {
                    try {
                        System.out.println("\n--- Attempting to jump to offset: " + offset);
                        matches.goTo(offset);
                        atOffset = true;
                        //System.out.println(" ------ Step 2a: Successfully jumped to offset.");
                    } // if the offset is outside the bounds, this page has no matches
                    catch (IndexOutOfBoundsException exception) {
                        atOffset = false;
                        //System.out.println(" ----- Step 2b: Offset out of bounds. Setting atOffset = false.");
                    }
                } // if not possible, advance to the offset iteratively
                else {
                      //System.out.println(" ----- Step 3: Cannot jump directly, advancing iteratively.");
                      matches.goToStart();
                    for (int i = 0; !(atOffset = i == offset) && matches.hasNext(); i++) {
                        matches.next();
                        //if (i % 10 == 0) { // Print every 10 iterations to avoid too much output
                         //   System.out.println(" ------ Step 3a: Moved to match " + i);
                       // }
                    }
                   // System.out.println(" ------ Step 3b: Reached offset: " + offset + " -> atOffset = " + atOffset);  
                }
                // try to add `limit` triples to the result model
                if (atOffset) {
                    System.out.println("---Adding triples to the result model, limit = " + limit);
                    // toTriple method convert the triple to Jena triple //
                    for (int i = 0; i < limit && matches.hasNext(); i++) {
                        triples.add(triples.asStatement(toTriple(matches.next())));

                       // if (i % 10 == 0 || i == limit - 1) { // Print every 10th triple and last one
                       //     System.out.println(" ------ Step 4a: Added " + (i + 1) + " triples so far.");
                       // }
                    }
                    System.out.println("\n---Finished selecting triples.");
                }
            }
            else {
                System.out.println("\n---No matches found.");
            }

            // estimates can be wrong; ensure 0 is returned if there are no results, 
            // and always more than actual results
            final long estimatedTotal = triples.size() > 0 ?
                    Math.max(offset + triples.size() + 1, matches.estimatedNumResults())
                    : hasMatches ?
                            Math.max(matches.estimatedNumResults(), 1)
                            : 0;

            // create the fragment
            final boolean isLastPage = ( estimatedTotal < offset + limit );
            System.out.println("\n(Estimated total triples) estimatedTotal: " + estimatedTotal + " matches.estimatedNumResults(): " +matches.estimatedNumResults()+ 
                               " (No. of triples in this page) triples.size(): " +triples.size());
            return createTriplePatternFragment( triples, estimatedTotal, isLastPage );
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