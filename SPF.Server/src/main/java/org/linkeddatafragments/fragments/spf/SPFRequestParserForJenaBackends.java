package org.linkeddatafragments.fragments.spf;

import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.fragments.tpf.TPFRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.linkeddatafragments.util.StarPatternElementParserForJena;
import org.linkeddatafragments.util.TriplePatternElementParserForJena;

public class SPFRequestParserForJenaBackends extends SPFRequestParser<RDFNode,String,String> {
    private static SPFRequestParserForJenaBackends instance = null;

    /**
     *
     * @return
     */
    public static SPFRequestParserForJenaBackends getInstance()
    {
        if ( instance == null ) {
            instance = new SPFRequestParserForJenaBackends();
        }
        return instance;
    }

    /**
     *
     */
    protected SPFRequestParserForJenaBackends()
    {
        super( StarPatternElementParserForJena.getInstance() );
    }
}
