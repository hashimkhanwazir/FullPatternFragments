package org.linkeddatafragments.views;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;

import org.apache.jena.query.ARQ;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.spf.IStarPatternFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragment;


class RdfWriterImpl extends LinkedDataFragmentWriterBase implements ILinkedDataFragmentWriter {

    private final Lang contentType;

    public RdfWriterImpl(Map<String, String> prefixes, HashMap<String, IDataSource> datasources, String mimeType) {
        super(prefixes, datasources);
        this.contentType = RDFLanguages.contentTypeToLang(mimeType);
        System.out.println("\nRdfWriterImpl class constructor is called so ARQ.init() started");
        ARQ.init();
    }

    @Override
    public void writeNotFound(ServletOutputStream outputStream, HttpServletRequest request) throws IOException {
        outputStream.println(request.getRequestURL().toString() + " not found!");
        outputStream.close();
    }

    @Override
    public void writeError(ServletOutputStream outputStream, Exception ex) throws IOException {
        outputStream.println(ex.getMessage());
        outputStream.close();
    }

    @Override
    public void writeFragment(ServletOutputStream outputStream, IDataSource datasource, ILinkedDataFragment fragment, ILinkedDataFragmentRequest ldfRequest) throws Exception {
        if(fragment instanceof ITriplePatternFragment) { 
            writeTriplePatternFragment(outputStream, datasource, (ITriplePatternFragment) fragment, ldfRequest);
        } else if(fragment instanceof IStarPatternFragment){
            writeStarPatternFragment(outputStream, datasource, (IStarPatternFragment) fragment, ldfRequest);
        }
    }

    private void writeTriplePatternFragment(ServletOutputStream outputStream, IDataSource datasource, ITriplePatternFragment fragment, ILinkedDataFragmentRequest ldfRequest) {
        final Model output = ModelFactory.createDefaultModel();
        output.setNsPrefixes(getPrefixes());
        output.add(fragment.getMetadata());
        output.add(fragment.getTriples());
        output.add(fragment.getControls());

//##############################################################################################
//########### Step 1 and 2 are for displaying the triples that have been selected after the query executed ############
//    Step 1: Capture the content into a string before writing
//    ByteArrayOutputStream tempOutputStream = new ByteArrayOutputStream();
//    RDFDataMgr.write(tempOutputStream, output, RDFFormat.TURTLE_PRETTY);

    //Step 2: Convert to String and Print it
//    String rdfContent = tempOutputStream.toString();
//    System.out.println("=== Triple Pattern Fragment ===");
//    System.out.println(rdfContent);
//    System.out.println("==============================");

//########### Same is the case for the controls, we can display them here ##################################
// Step 1: Extract only the controls
//Model controlsModel = ModelFactory.createDefaultModel();
//controlsModel.add(fragment.getControls());

// Step 2: Convert controls to a string for printing
//ByteArrayOutputStream controlsStream = new ByteArrayOutputStream();
//RDFDataMgr.write(controlsStream, controlsModel, RDFFormat.TURTLE_PRETTY);

// Step 3: Print the controls separately
//String controlsContent = controlsStream.toString();
//System.out.println("=== Controls ===");
//System.out.println(controlsContent);
//System.out.println("================");
//##################################################################################################

        RDFDataMgr.write(outputStream, output, contentType);
    System.out.println("\nwriteTriplePatternFragment() - The selected triples have been written \n ");
    System.out.println("*********************************************************************************");
    }

    private void writeStarPatternFragment(ServletOutputStream outputStream, IDataSource datasource, IStarPatternFragment fragment, ILinkedDataFragmentRequest ldfRequest) {
        final Model output = ModelFactory.createDefaultModel();
        output.setNsPrefixes(getPrefixes());
        output.add(fragment.getMetadata());
        output.add(fragment.getControls());
        RDFDataMgr.write(outputStream, output, contentType);

        for(Model out : fragment.getModels()) {
            RDFDataMgr.write(outputStream, out, contentType);
        }
    }
}
