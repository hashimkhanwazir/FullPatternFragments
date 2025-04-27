package org.linkeddatafragments.views;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.linkeddatafragments.datasource.IDataSource;

 
public class LinkedDataFragmentWriterFactory {
    
    private final static String HTML = "text/html";
    
    /**
     * Creates {@link ILinkedDataFragmentWriter} for a given mimeType
     * 
     * @param prefixes Configured prefixes to be used in serialization
     * @param datasources Configured datasources
     * @param mimeType mimeType to create writer for
     * @return created writer
     * @throws IOException 
     */
    public static ILinkedDataFragmentWriter create(Map <String, String> prefixes, HashMap<String, IDataSource> datasources, String mimeType) throws IOException {
        switch (mimeType) {
            case HTML:
                //System.out.println("\nThe mimeType is " + mimeType + " so the case HTML is called\n\n ");
                return new HtmlTriplePatternFragmentWriterImpl(prefixes, datasources);
            default:
                //System.out.println("\nThe mimeType is " + mimeType + " so the case DEFAULT is called\n\n ");
                return new RdfWriterImpl(prefixes, datasources, mimeType);
        }
    }
}
