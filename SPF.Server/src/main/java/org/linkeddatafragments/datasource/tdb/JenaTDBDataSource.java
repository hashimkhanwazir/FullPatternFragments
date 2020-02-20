package org.linkeddatafragments.datasource.tdb;

import java.io.File;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;


public class JenaTDBDataSource extends DataSourceBase {

    /**
     * The request processor
     * 
     */
    protected final JenaTDBBasedRequestProcessorForTPFs requestProcessor;

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor)
    {
        return TPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor)
    {
        return requestProcessor;
    }

    /**
     * Constructor
     *
     * @param title
     * @param description
     * @param tdbdir directory used for TDB backing
     */
    public JenaTDBDataSource(String title, String description, File tdbdir) {
        super(title, description);
        requestProcessor = new JenaTDBBasedRequestProcessorForTPFs( tdbdir );
    }
}
