package org.linkeddatafragments.fragments.spf;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.iterator.NiceIterator;
import org.linkeddatafragments.fragments.LinkedDataFragmentBase;
import org.linkeddatafragments.util.CommonResources;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.linkeddatafragments.util.CommonResources.*;
import static org.linkeddatafragments.util.CommonResources.HYDRA_TEMPLATE;
import static org.linkeddatafragments.util.CommonResources.RDF_OBJECT;

public abstract class StarPatternFragmentBase extends LinkedDataFragmentBase
        implements IStarPatternFragment {
    private final long totalSize;

    /**
     * Creates an empty Triple Pattern Fragment.
     * @param fragmentURL
     * @param datasetURL
     */
    public StarPatternFragmentBase( final String fragmentURL,
                                      final String datasetURL ) {
        this( 0L, fragmentURL, datasetURL, 1, true );
    }

    /**
     * Creates an empty Triple Pattern Fragment page.
     * @param fragmentURL
     * @param isLastPage
     * @param datasetURL
     * @param pageNumber
     */
    public StarPatternFragmentBase( final String fragmentURL,
                                      final String datasetURL,
                                      final long pageNumber,
                                      final boolean isLastPage ) {
        this( 0L, fragmentURL, datasetURL, pageNumber, isLastPage );
    }

    /**
     * Creates a new Triple Pattern Fragment.
     * @param totalSize the total size
     * @param fragmentURL
     * @param datasetURL
     * @param pageNumber
     * @param isLastPage
     */
    public StarPatternFragmentBase( long totalSize,
                                      final String fragmentURL,
                                      final String datasetURL,
                                      final long pageNumber,
                                      final boolean isLastPage ) {
        super( fragmentURL, datasetURL, pageNumber, isLastPage );
        this.totalSize = totalSize < 0L ? 0L : totalSize;
    }

    @Override
    public StmtIterator getTriples() {
        return emptyStmtIterator;
    }

    public List<StmtIterator> getTriplesStar() {
        if ( totalSize == 0L )
            return new ArrayList<>();
        else
            return getNonEmptyStmtIterators();
    }

    /**
     *
     * @return
     */
    abstract protected List<StmtIterator> getNonEmptyStmtIterators();

    @Override
    public long getTotalSize() {
        return totalSize;
    }

    @Override
    public void addMetadata( final Model model )
    {
        super.addMetadata( model );
        final Resource fragmentId = model.createResource( fragmentURL );

        final Literal totalTyped = model.createTypedLiteral( totalSize,
                XSDDatatype.XSDinteger );
        final Literal limitTyped = model.createTypedLiteral( getMaxPageSize(),
                XSDDatatype.XSDinteger );

        fragmentId.addLiteral( CommonResources.VOID_TRIPLES, totalTyped );
        fragmentId.addLiteral( CommonResources.HYDRA_TOTALITEMS, totalTyped );
        fragmentId.addLiteral( CommonResources.HYDRA_ITEMSPERPAGE, limitTyped );
    }

    @Override
    public void addControls( final Model model )
    {
        super.addControls( model );

        final Resource datasetId = model.createResource( getDatasetURI() );

        final Resource triplePattern = model.createResource();

        datasetId.addProperty( CommonResources.HYDRA_SEARCH, triplePattern );

        String template = "{?s,triples";
        for(int i = 0; i < CommonResources.MAX_TPR; i++) {
            int ii = i+1;
            template += ",p"+ii+",o"+ii;

            final Resource predMapping = model.createResource();
            triplePattern.addProperty(HYDRA_MAPPING, predMapping);
            predMapping.addProperty(HYDRA_VARIABLE, model.createLiteral("p"+ii));
            predMapping.addProperty(HYDRA_PROPERTY, RDF_PREDICATE);

            final Resource objMapping = model.createResource();
            triplePattern.addProperty(HYDRA_MAPPING, objMapping);
            objMapping.addProperty(HYDRA_VARIABLE, model.createLiteral("o"+ii));
            objMapping.addProperty(HYDRA_PROPERTY, RDF_OBJECT);
        }
        template += "}";

        triplePattern.addProperty(HYDRA_TEMPLATE, model.createLiteral(datasetURL + template));

        final Resource subjectMapping = model.createResource();
        triplePattern.addProperty(HYDRA_MAPPING, subjectMapping);
        subjectMapping.addProperty(HYDRA_VARIABLE, model.createLiteral("s"));
        subjectMapping.addProperty(HYDRA_PROPERTY, RDF_SUBJECT);

        final Resource triplesMapping = model.createResource();
        triplePattern.addProperty(HYDRA_MAPPING, triplesMapping);
        triplesMapping.addProperty(HYDRA_VARIABLE, model.createLiteral("triples"));
        triplesMapping.addProperty(HYDRA_PROPERTY, XSD_INTEGER);
    }

    /**
     *
     * @return
     */
    public String getTemplate() {
        String template = "{?s,triples";
        for(int i = 0; i < CommonResources.MAX_TPR; i++) {
            int ii = i+1;
            template += ",p"+ii+",o"+ii;
        }
        template += "}";
        return template;
    }

    /**
     *
     */
    public static final StmtIterator emptyStmtIterator = new EmptyStmtIterator();

    /**
     *
     */
    public static class EmptyStmtIterator
            extends NiceIterator<Statement>
            implements StmtIterator
    {

        /**
         *
         * @return
         */
        public Statement nextStatement() { throw new NoSuchElementException(); }
    }
}
