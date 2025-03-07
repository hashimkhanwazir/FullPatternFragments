package dk.aau.cs.spf.task;

import dk.aau.cs.spf.callable.BrtpfHttpRequestThread;
import dk.aau.cs.spf.callable.SpfHttpRequestThread;
import dk.aau.cs.spf.main.SparqlQueryProcessor;
import dk.aau.cs.spf.model.BindingHashMap;
import dk.aau.cs.spf.model.HttpRequestConfig;
import dk.aau.cs.spf.model.StarPattern;
import dk.aau.cs.spf.model.TriplePattern;
import dk.aau.cs.spf.util.QueryProcessingUtils;
import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

public class StarHandlerSingle extends AbstractRDFHandler {
    private List<List<Statement>> stars;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private SpfParseResponseTask parseResponseTask;
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
    public StarHandlerSingle(ExecutorCompletionService<Boolean> executorCompletionService,
                       SpfParseResponseTask parseResponseTask,
                       ConcurrentHashMap<String, Content> httpResponseCache, AtomicInteger numberOfTasks) {
        this.stars = new ArrayList<>();
        this.executorCompletionService = executorCompletionService;
        this.parseResponseTask = parseResponseTask;
        this.httpResponseCache = httpResponseCache;
        this.numberOfTasks = numberOfTasks;
        this.processedTriples = new HashSet<>();
    }

    private boolean isTripleValid(Statement st) {
        if (st.getSubject().stringValue()
                .equals(parseResponseTask.getHttpRequestTask().getFragmentURL())) {
            if (st.getPredicate().stringValue().hashCode() == HYDRA_NEXTPAGE_HASH) {
                String fragmentURL = st.getObject().stringValue();
                SpfHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
                ArrayList<StarPattern> tpOrder = currHttpRequestTask.getSpOrder();
                SpfHttpRequestTask httpRequestTask = new SpfHttpRequestTask(tpOrder,
                        currHttpRequestTask.getBindings(), currHttpRequestTask.getTpIdx(), fragmentURL,
                        currHttpRequestTask.getOutputBindings());
                httpRequestTask.setStartingFragment(currHttpRequestTask.getStartingFragment());
                numberOfTasks.incrementAndGet();
                SpfHttpRequestThread hrt = new SpfHttpRequestThread(httpRequestTask, httpResponseCache,
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
        if (stars.size() != 0) {
            SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(stars.size());
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
            List<Statement> star = new ArrayList<>();
            star.add(st);
            stars.add(star);
            if (stars.size() == HttpRequestConfig.MAX_NUMBER_OF_BINDINGS) {
                SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED.addAndGet(stars.size());
                sendRequestWithExtendedBindings();
                stars.clear();
            }
        }
    }

    private void sendRequestWithExtendedBindings() {
        SpfHttpRequestTask currHttpRequestTask = parseResponseTask.getHttpRequestTask();
        ArrayList<BindingHashMap> extendedBindings = QueryProcessingUtils.extendBindings(
                currHttpRequestTask.getBindings(), currHttpRequestTask.getStarPattern(), stars);
        ConcurrentLinkedQueue<BindingHashMap> outputBindings = currHttpRequestTask.getOutputBindings();
        ArrayList<StarPattern> tpOrder = currHttpRequestTask.getSpOrder();
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
                if(endIndex > size) endIndex = size;
                ArrayList<BindingHashMap> sublist = new ArrayList<>(extendedBindings.subList(i, endIndex));

                SpfHttpRequestTask httpRequestTask = new SpfHttpRequestTask(tpOrder,
                        currHttpRequestTask.getStartingFragment(), sublist, tpIdx + 1, outputBindings);
                numberOfTasks.incrementAndGet();
                SpfHttpRequestThread hrt = new SpfHttpRequestThread(httpRequestTask, httpResponseCache,
                        executorCompletionService, numberOfTasks);
                executorCompletionService.submit(hrt);
            }
        }
    }
}
