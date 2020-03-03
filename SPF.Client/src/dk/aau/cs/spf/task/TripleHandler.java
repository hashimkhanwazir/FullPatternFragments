package dk.aau.cs.spf.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import dk.aau.cs.spf.callable.BrtpfHttpRequestThread;
import dk.aau.cs.spf.main.SparqlQueryProcessor;
import dk.aau.cs.spf.model.BindingHashMap;
import dk.aau.cs.spf.model.HttpRequestConfig;
import dk.aau.cs.spf.model.TriplePattern;
import dk.aau.cs.spf.util.QueryProcessingUtils;

public class TripleHandler extends AbstractRDFHandler {
    private ArrayList<Statement> triples;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private BrtpfParseResponseTask parseResponseTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private AtomicInteger numberOfTasks;
    private HashSet<Statement> processedTriples;
    private static final int HYDRA_NEXTPAGE_HASH =
            new String("http://www.w3.org/ns/hydra/core#next").hashCode();
    private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
    private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();

    /**
     *
     */
    public TripleHandler(ExecutorCompletionService<Boolean> executorCompletionService,
                         BrtpfParseResponseTask parseResponseTask,
                         ConcurrentHashMap<String, Content> httpResponseCache, AtomicInteger numberOfTasks) {
        this.triples = new ArrayList<Statement>();
        this.executorCompletionService = executorCompletionService;
        this.parseResponseTask = parseResponseTask;
        this.httpResponseCache = httpResponseCache;
        this.numberOfTasks = numberOfTasks;
        this.processedTriples = new HashSet<Statement>();
    }

    private boolean isTripleValid(Statement st) {
        if (st.getSubject().stringValue()
                .equals(parseResponseTask.getHttpRequestTask().getFragmentURL())) {
            if (st.getPredicate().stringValue().hashCode() == HYDRA_NEXTPAGE_HASH) {
                String fragmentURL = st.getObject().stringValue();
                BrtpfHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
                ArrayList<TriplePattern> tpOrder = currHttpRequestTask.getTpOrder();
                BrtpfHttpRequestTask httpRequestTask = new BrtpfHttpRequestTask(tpOrder,
                        currHttpRequestTask.getBindings(), currHttpRequestTask.getTpIdx(), fragmentURL,
                        currHttpRequestTask.getOutputBindings());
                httpRequestTask.setStartingFragment(currHttpRequestTask.getStartingFragment());
                numberOfTasks.incrementAndGet();
                BrtpfHttpRequestThread hrt = new BrtpfHttpRequestThread(httpRequestTask, httpResponseCache,
                        executorCompletionService, numberOfTasks);
                executorCompletionService.submit(hrt);
            }
            return false;
        } else if (st.getPredicate().stringValue().contains("hydra/")
                || st.getObject().stringValue().contains("hydra/")
                || st.getObject().stringValue().hashCode() == DATASET_HASH
                || st.getPredicate().stringValue().hashCode() == SUBSET_HASH) {
            return false;
        } else {
            return true;
        }

    }

    @Override
    public void endRDF() throws RDFHandlerException {
        if (triples.size() != 0) {
            SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(triples.size());
            sendRequestWithExtendedBindings();
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see org.openrdf.rio.helpers.AbstractRDFHandler#handleStatement(org.openrdf.model.Statement)
     */
    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        if (processedTriples.contains(st)) {
            return;
        } else {
            processedTriples.add(st);
        }
        if (isTripleValid(st)) {
            triples.add(st);
            if (triples.size() == HttpRequestConfig.MAX_NUMBER_OF_BINDINGS) {
                SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(triples.size());
                sendRequestWithExtendedBindings();
                triples.clear();
            }
        }
    }

    private void sendRequestWithExtendedBindings() {
        BrtpfHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
        ArrayList<BindingHashMap> extendedBindings = QueryProcessingUtils.extendBindings(
                currHttpRequestTask.getBindings(), currHttpRequestTask.getTriplePattern(), triples);
        ConcurrentLinkedQueue<BindingHashMap> outputBindings = currHttpRequestTask.getOutputBindings();
        ArrayList<TriplePattern> tpOrder = currHttpRequestTask.getTpOrder();
        int noOfTPs = tpOrder.size();
        int tpIdx = currHttpRequestTask.getTpIdx();
        if (tpIdx == noOfTPs - 1) {
            if(!SparqlQueryProcessor.RECEIVED_RESULT) {
                SparqlQueryProcessor.RECEIVED_RESULT = true;
                SparqlQueryProcessor.RESPONSE_TIME = System.currentTimeMillis() - SparqlQueryProcessor.START_TIME;
            }

            outputBindings.addAll(extendedBindings);
        } else {
            int size = extendedBindings.size();
            for(int i = 0; i < size; i += HttpRequestConfig.MAX_NUMBER_OF_BINDINGS) {
                int endIndex = i + HttpRequestConfig.MAX_NUMBER_OF_BINDINGS;
                if (endIndex > size) endIndex = size;
                ArrayList<BindingHashMap> sublist = new ArrayList<>(extendedBindings.subList(i, endIndex));

                BrtpfHttpRequestTask httpRequestTask = new BrtpfHttpRequestTask(tpOrder,
                        currHttpRequestTask.getStartingFragment(), sublist, tpIdx + 1, outputBindings);
                numberOfTasks.incrementAndGet();
                BrtpfHttpRequestThread hrt = new BrtpfHttpRequestThread(httpRequestTask, httpResponseCache,
                        executorCompletionService, numberOfTasks);
                executorCompletionService.submit(hrt);
            }
        }
    }
}
