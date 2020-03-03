/**
 *
 */
package dk.aau.cs.spf.main;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import dk.aau.cs.spf.callable.SpfHttpRequestThread;
import dk.aau.cs.spf.model.StarPattern;
import dk.aau.cs.spf.model.VarBinding;
import dk.aau.cs.spf.task.SpfHttpRequestTask;
import org.apache.http.client.fluent.Content;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.RDFNode;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import dk.aau.cs.spf.callable.BrtpfHttpRequestThread;
import dk.aau.cs.spf.callable.InitialHttpRequestThread;
import dk.aau.cs.spf.callable.TpfHttpRequestThread;
import dk.aau.cs.spf.main.QueryInput.QueryProcessingMethod;
import dk.aau.cs.spf.model.BindingHashMap;
import dk.aau.cs.spf.model.TriplePattern;
import dk.aau.cs.spf.task.BrtpfHttpRequestTask;
import dk.aau.cs.spf.task.InitialHttpRequestTask;
import dk.aau.cs.spf.task.TpfHttpRequestTask;
import dk.aau.cs.spf.util.QueryProcessingUtils;

/**
 * @author Ilkcan Keles
 */
public class SparqlQueryProcessor {
    private ConcurrentHashMap<String, Content> httpResponseCache;
    public static Queue<String> cacheQueue;
    private ArrayList<TriplePattern> unorderedTriplePatterns;
    private ArrayList<TriplePattern> triplePatterns;

    private ArrayList<StarPattern> unorderedStarPatterns;
    private ArrayList<StarPattern> starPatterns;

    private String queryString;
    private String endpoint;

    private List<ProjectionElem> projectionElemList;
    private final int nThreads;
    private ExecutorService executorService;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private ArrayList<TriplePattern> triplePatternOrder = new ArrayList<TriplePattern>();
    private ArrayList<StarPattern> starPatternOrder = new ArrayList<StarPattern>();
    public static QueryProcessingMethod method;
    private boolean starPattern = false;

    private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
    private AtomicInteger numberOfTasks;
    private QueryInput input;
    private long queryProcessingTime;
    public static AtomicInteger SERVER_REQUESTS = new AtomicInteger(0);
    public static AtomicLong TRANSFERRED_BYTES = new AtomicLong(0);
    public static boolean RECEIVED_RESULT = false;
    public static long RESPONSE_TIME = 0;
    public static long START_TIME = 0;
    public static final int MAX_CACHE_ENTRIES = 1000;

    public boolean printOutput;
    public static AtomicInteger NUMBER_OF_HTTP_REQUESTS = new AtomicInteger(0);
    public static AtomicInteger NUMBER_OF_BINDINGS_SENT = new AtomicInteger(0);
    public static AtomicInteger NUMBER_OF_BINDINGS_RECEIVED = new AtomicInteger(0);
    public static int NUMBER_OF_OUTPUT_BINDINGS = 0;
    public static List<Integer> FRAGMENT_SIZES = new ArrayList<>();

    /**
     *
     */
    public SparqlQueryProcessor(ArrayList<TriplePattern> triplePatterns,
                                List<ProjectionElem> projectionElemList, QueryInput input, QueryProcessingMethod method,
                                boolean isMultiThreaded, boolean printOutput) {
        this.triplePatterns = triplePatterns;
        this.unorderedTriplePatterns = new ArrayList<TriplePattern>();
        for (TriplePattern triplePattern : triplePatterns) {
            unorderedTriplePatterns.add(triplePattern);
        }
        this.projectionElemList = projectionElemList;
        this.input = input;
        triplePatternOrder = new ArrayList<TriplePattern>();
        httpResponseCache = new ConcurrentHashMap<String, Content>();
        cacheQueue = new LinkedList<>();
        outputBindings = new ConcurrentLinkedQueue<BindingHashMap>();
        this.method = method;
        if (isMultiThreaded) {
            nThreads = Runtime.getRuntime().availableProcessors();
        } else {
            nThreads = 1;
        }
        this.printOutput = printOutput;
        this.queryString = "";
        this.endpoint = "";
    }

    public SparqlQueryProcessor(ArrayList<StarPattern> starPatterns,
                                List<ProjectionElem> projectionElemList, QueryInput input, QueryProcessingMethod method,
                                boolean isMultiThreaded, boolean printOutput, boolean starPattern) {
        this.starPatterns = starPatterns;
        this.unorderedStarPatterns = new ArrayList<StarPattern>();
        for (StarPattern triplePattern : starPatterns) {
            unorderedStarPatterns.add(triplePattern);
        }
        this.projectionElemList = projectionElemList;
        this.input = input;
        starPatternOrder = new ArrayList<StarPattern>();
        httpResponseCache = new ConcurrentHashMap<String, Content>();
        cacheQueue = new LinkedList<>();
        outputBindings = new ConcurrentLinkedQueue<BindingHashMap>();
        this.method = method;
        if (isMultiThreaded) {
            nThreads = Runtime.getRuntime().availableProcessors();
        } else {
            nThreads = 1;
        }
        this.printOutput = printOutput;
        this.starPattern = starPattern;
        this.queryString = "";
        this.endpoint = "";
    }

    public SparqlQueryProcessor(String queryString,
                                List<ProjectionElem> projectionElemList, QueryInput input, QueryProcessingMethod method,
                                boolean isMultiThreaded, boolean printOutput, String endpoint) {
        this.projectionElemList = projectionElemList;
        this.input = input;
        httpResponseCache = new ConcurrentHashMap<String, Content>();
        cacheQueue = new LinkedList<>();
        outputBindings = new ConcurrentLinkedQueue<BindingHashMap>();
        this.method = method;
        if (isMultiThreaded) {
            nThreads = Runtime.getRuntime().availableProcessors();
        } else {
            nThreads = 1;
        }
        this.printOutput = printOutput;
        this.queryString = queryString;
        this.endpoint = endpoint;
    }


    /**
     * @throws ExecutionException
     * @throws InterruptedException
     *
     */
    private void initializeOrderOfTriplePatterns() throws InterruptedException, ExecutionException {
        int minTriplesCount = Integer.MAX_VALUE;
        TriplePattern firstTriplePattern = null;
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        for (TriplePattern triplePattern : unorderedTriplePatterns) {
            InitialHttpRequestTask httpRequestTask = new InitialHttpRequestTask(input.getStartFragment(), triplePattern);
            int triplesCount = executorService.submit(new InitialHttpRequestThread(httpRequestTask, httpResponseCache)).get();
            FRAGMENT_SIZES.add(triplesCount);
            if (triplesCount < minTriplesCount) {
                minTriplesCount = triplesCount;
                firstTriplePattern = triplePattern;
            }
        }

        executorService.shutdown();
        if (firstTriplePattern != null) {
            triplePatternOrder.add(firstTriplePattern);
            unorderedTriplePatterns.remove(firstTriplePattern);
            orderRemainingTPs();
        }
    }

    private void initializeOrderOfStarPatterns() throws InterruptedException, ExecutionException {
        int minTriplesCount = Integer.MAX_VALUE;
        StarPattern firstStarPattern = null;
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        for (StarPattern starPattern : unorderedStarPatterns) {
            InitialHttpRequestTask httpRequestTask = new InitialHttpRequestTask(input.getStartFragment(), starPattern);
            Future<Integer> f = executorService.submit(new InitialHttpRequestThread(httpRequestTask, httpResponseCache));
            int triplesCount = f.get();
            if (triplesCount < minTriplesCount) {
                minTriplesCount = triplesCount;
                firstStarPattern = starPattern;
            }
        }

        executorService.shutdown();
        if (firstStarPattern != null) {
            starPatternOrder.add(firstStarPattern);
            unorderedStarPatterns.remove(firstStarPattern);
            orderRemainingSPs();
        }
    }

    private void orderRemainingTPs() {
        while (!unorderedTriplePatterns.isEmpty()) {
            ArrayList<String> boundVariables = QueryProcessingUtils.getBoundVariables(triplePatternOrder);
            TriplePattern nextTP = QueryProcessingUtils
                    .findAndRemoveNextWithMaxNumberOfBV(unorderedTriplePatterns, boundVariables);
            triplePatternOrder.add(nextTP);
        }
    }

    private void orderRemainingSPs() {
        while (!unorderedStarPatterns.isEmpty()) {
            ArrayList<String> boundVariables = QueryProcessingUtils.getBoundVariablesSP(starPatternOrder);
            StarPattern nextSP = QueryProcessingUtils
                    .findAndRemoveNextWithMaxNumberOfBVSP(unorderedStarPatterns, boundVariables);
            starPatternOrder.add(nextSP);
        }
    }

    private void initializeProcessingQuery() {
        if (method == QueryProcessingMethod.TPF) {
            executorService = Executors.newFixedThreadPool(nThreads);
            executorCompletionService = new ExecutorCompletionService<Boolean>(executorService);
            TpfHttpRequestTask httpRequestTask = new TpfHttpRequestTask(triplePatternOrder,
                    input.getStartFragment(), null, 0, outputBindings);
            numberOfTasks = new AtomicInteger(1);
            TpfHttpRequestThread hrt = new TpfHttpRequestThread(httpRequestTask, httpResponseCache,
                    executorCompletionService, numberOfTasks);
            executorCompletionService.submit(hrt);
        } else if(method == QueryProcessingMethod.BRTPF) {
            executorService = Executors.newFixedThreadPool(nThreads);
            executorCompletionService = new ExecutorCompletionService<Boolean>(executorService);
            BrtpfHttpRequestTask httpRequestTask = new BrtpfHttpRequestTask(triplePatternOrder,
                    input.getStartFragment(), new ArrayList<BindingHashMap>(), 0, outputBindings);
            numberOfTasks = new AtomicInteger(1);
            BrtpfHttpRequestThread hrt = new BrtpfHttpRequestThread(httpRequestTask, httpResponseCache,
                    executorCompletionService, numberOfTasks);
            executorCompletionService.submit(hrt);
        } else {
            executorService = Executors.newFixedThreadPool(nThreads);
            executorCompletionService = new ExecutorCompletionService<Boolean>(executorService);
            SpfHttpRequestTask httpRequestTask = new SpfHttpRequestTask(starPatternOrder,
                    input.getStartFragment(), new ArrayList<BindingHashMap>(), 0, outputBindings);
            numberOfTasks = new AtomicInteger(1);
            SpfHttpRequestThread hrt = new SpfHttpRequestThread(httpRequestTask, httpResponseCache,
                    executorCompletionService, numberOfTasks);
            executorCompletionService.submit(hrt);
        }
    }

    public void processQuery() throws InterruptedException, ExecutionException {
        SERVER_REQUESTS = new AtomicInteger(0);
        TRANSFERRED_BYTES = new AtomicLong(0);
        RECEIVED_RESULT = false;
        START_TIME = System.currentTimeMillis();
        RESPONSE_TIME = 0;
        NUMBER_OF_OUTPUT_BINDINGS = 0;
        NUMBER_OF_BINDINGS_RECEIVED.set(0);
        FRAGMENT_SIZES.clear();
        FRAGMENT_SIZES = new ArrayList<>();

        if(method == QueryProcessingMethod.ENDPOINT) {
            processQueryEndpoint();
            return;
        }

        long start = System.currentTimeMillis();
        if(!starPattern)
            initializeOrderOfTriplePatterns();
        else
            initializeOrderOfStarPatterns();

        //todo

        initializeProcessingQuery();
        while (numberOfTasks.get() != 0) {
            executorCompletionService.take();
            numberOfTasks.decrementAndGet();
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        if (printOutput) {
            System.out.println(outputBindings.size());
        }
        NUMBER_OF_OUTPUT_BINDINGS = outputBindings.size();

        long end = System.currentTimeMillis();
        queryProcessingTime = end - start;
    }

    public void processQueryEndpoint() {
        Query query = QueryFactory.create(queryString);
        long start = System.currentTimeMillis();

        START_TIME = start;
        SERVER_REQUESTS.incrementAndGet();
        NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
        QueryExecution qExec = QueryExecutionFactory.sparqlService(endpoint, query, "http://spf.cs.aau.dk/data");

        ResultSet results;
        try {
            results = qExec.execSelect();
        } catch(Exception e) {
            e.printStackTrace();
            return;
        }
        while(results.hasNext()) {
            QuerySolution soln = results.next();
            TRANSFERRED_BYTES.addAndGet(soln.toString().getBytes().length);
            NUMBER_OF_BINDINGS_RECEIVED.incrementAndGet();
            BindingHashMap binding = new BindingHashMap();
            Iterator<String> varNames = soln.varNames();

            while(varNames.hasNext()) {
                String vn = varNames.next();
                RDFNode node = soln.get(vn);

                if(node.isLiteral()) {
                    binding.put(vn, new VarBinding(node.toString(), VarBinding.VarBindingType.LITERAL));
                } else {
                    binding.put(vn, new VarBinding(node.toString(), VarBinding.VarBindingType.IRI));
                }
            }
            outputBindings.add(binding);
            NUMBER_OF_OUTPUT_BINDINGS++;

            if(!RECEIVED_RESULT) {
                RECEIVED_RESULT = true;
                long resp = System.currentTimeMillis();

                RESPONSE_TIME = resp - start;
            }
        }

        qExec.close();

        long end = System.currentTimeMillis();
        queryProcessingTime = end-start;
    }

    public void printBindings() {
        Iterator<BindingHashMap> outputBindingsIterator = outputBindings.iterator();
        while (outputBindingsIterator.hasNext()) {
            BindingHashMap currentBinding = outputBindingsIterator.next();
            System.out.println(currentBinding.toString());
        }
        System.err.println("Number of output bindings: " + outputBindings.size());
        System.err.println("Runtime: " + Duration.ofMillis(queryProcessingTime));
    }

    public void terminate() {
        if(executorService != null) executorService.shutdownNow();
    }

    public void close() {
        httpResponseCache.clear();
        outputBindings.clear();
        cacheQueue.clear();
    }

    public long getQueryProcessingTime() {
        return queryProcessingTime;
    }

    public ConcurrentLinkedQueue<BindingHashMap> getOutputBindings() {
        return outputBindings;
    }
}
