package dk.aau.cs.spf.callable;

import dk.aau.cs.spf.main.SparqlQueryProcessor;
import dk.aau.cs.spf.task.BrtpfHttpRequestTask;
import dk.aau.cs.spf.task.BrtpfParseResponseTask;
import dk.aau.cs.spf.task.SpfHttpRequestTask;
import dk.aau.cs.spf.task.SpfParseResponseTask;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;


public class SpfHttpRequestThread implements Callable<Boolean> {
    private SpfHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private AtomicInteger numberOfTasks;

    
    public SpfHttpRequestThread(SpfHttpRequestTask httpRequestTask,
                                  ConcurrentHashMap<String, Content> httpResponseCache,
                                  ExecutorCompletionService<Boolean> executorCompletionService, 
                                  AtomicInteger numberOfTasks) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
        this.executorCompletionService = executorCompletionService;
        this.numberOfTasks = numberOfTasks;
        System.out.println("************* I am in the spfHttpRequestThread() constructor ************");
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    
    @Override
    public Boolean call() throws ClientProtocolException, IOException {
        String httpUrl = httpRequestTask.getFragmentURL();
        Content content = null;
        if (httpResponseCache.containsKey(httpUrl)) {
            content = httpResponseCache.get(httpUrl);
        } else {
            SparqlQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();
            SparqlQueryProcessor.NUMBER_OF_BINDINGS_SENT.addAndGet(httpRequestTask.getBindings().size());

            SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
            content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
            SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);

            httpResponseCache.put(httpUrl, content);
        }
        if (content != null) {
            SpfParseResponseTask prTask = new SpfParseResponseTask(httpRequestTask, content.asStream());
            numberOfTasks.incrementAndGet();
            SpfResponseParserThread rpThread = new SpfResponseParserThread(prTask, executorCompletionService,
                    httpResponseCache, numberOfTasks);
            executorCompletionService.submit(rpThread);

        }
        return true;
    }
}
