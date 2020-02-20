package org.linkeddatafragments.datasource;

import java.io.Closeable;

import org.linkeddatafragments.fragments.IFragmentRequestParser;

public interface IDataSource extends Closeable {

    /**
     *
     * @return
     */
    public String getTitle();
        
    /**
     *
     * @return
     */
    public String getDescription();

    /**
     * Returns a data source specific {@link IFragmentRequestParser}.
     * @return 
     */
    IFragmentRequestParser getRequestParser(ProcessorType processor);

    /**
     * Returns a data source specific {@link IFragmentRequestProcessor}.
     * @return 
     */
    IFragmentRequestProcessor getRequestProcessor(ProcessorType processor);

    public enum ProcessorType {
        TPF, BRTPF, SPF
    }
}
