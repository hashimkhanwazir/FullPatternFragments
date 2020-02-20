package org.linkeddatafragments.datasource.hdt;

import java.io.IOException;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.spf.SPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.BRTPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.NodeDictionary;


public class HdtDataSource extends DataSourceBase {

    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    /**
     * Creates a new HdtDataSource.
     *
     * @param title title of the datasource
     * @param description datasource description
     * @param hdtFile the HDT datafile
     * @throws IOException if the file cannot be loaded
     */
    public HdtDataSource(String title, String description, String hdtFile) throws IOException {
        super(title, description);

        datasource = HDTManager.mapIndexedHDT( hdtFile, null );
        dictionary = new NodeDictionary( datasource.getDictionary() );
    }

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor)
    {
        if(processor == ProcessorType.TPF)
            return TPFRequestParserForJenaBackends.getInstance();
        else if(processor == ProcessorType.BRTPF)
            return BRTPFRequestParserForJenaBackends.getInstance();
        return SPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor)
    {
        if(processor == ProcessorType.TPF)
            return new HdtBasedRequestProcessorForTPFs(datasource, dictionary);
        if(processor == ProcessorType.BRTPF)
            return new HdtBasedRequestProcessorForBRTPFs(datasource, dictionary);
        return new HdtBasedRequestProcessorForSPFs(datasource, dictionary);
    }

}
