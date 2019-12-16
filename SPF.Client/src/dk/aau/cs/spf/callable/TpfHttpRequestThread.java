package dk.aau.cs.spf.callable;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import dk.aau.cs.spf.main.SparqlQueryProcessor;
import dk.aau.cs.spf.task.TpfHttpRequestTask;
import dk.aau.cs.spf.task.TpfParseResponseTask;

public class TpfHttpRequestThread implements Callable<Boolean> {
    private TpfHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;
    private ExecutorCompletionService<Boolean> executorCompletionService;
    private AtomicInteger numberOfTasks;

    public TpfHttpRequestThread(TpfHttpRequestTask httpRequestTask,
                                ConcurrentHashMap<String, Content> httpResponseCache,
                                ExecutorCompletionService<Boolean> executorCompletionService, AtomicInteger numberOfTasks) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
        this.executorCompletionService = executorCompletionService;
        this.numberOfTasks = numberOfTasks;
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
            SparqlQueryProcessor.NUMBER_OF_BINDINGS_SENT.addAndGet(1);

            SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
            content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();
            SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);
            if(httpResponseCache.size() == SparqlQueryProcessor.MAX_CACHE_ENTRIES) {
                httpResponseCache.remove(SparqlQueryProcessor.cacheQueue.poll());
            }
            httpResponseCache.put(httpUrl, content);
            SparqlQueryProcessor.cacheQueue.add(httpUrl);
        }
        if (content != null) {
            TpfParseResponseTask prTask = new TpfParseResponseTask(httpRequestTask, content.asStream());
            numberOfTasks.incrementAndGet();
            TpfResponseParserThread rpThread = new TpfResponseParserThread(prTask,
                    executorCompletionService, httpResponseCache, numberOfTasks);
            executorCompletionService.submit(rpThread);
        }
        return true;
    }
}
