package dk.aau.cs.spf.callable;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import dk.aau.cs.spf.main.SparqlQueryProcessor;
import dk.aau.cs.spf.task.CountMetadataHandler;
import dk.aau.cs.spf.task.InitialHttpRequestTask;

public class InitialHttpRequestThread implements Callable<Integer> {
    private InitialHttpRequestTask httpRequestTask;
    private ConcurrentHashMap<String, Content> httpResponseCache;

    public InitialHttpRequestThread(InitialHttpRequestTask httpRequestTask, ConcurrentHashMap<String, Content> httpResponseCache) {
        this.httpRequestTask = httpRequestTask;
        this.httpResponseCache = httpResponseCache;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Integer call() throws Exception {
        int triplesCount = 0;
        try {
            String httpUrl = httpRequestTask.getFragmentURL();
            System.out.println("\n** InitialHttpRequestThread() call The httpUrl is = " + httpUrl);
            Content content = null;
            boolean cacheContains = false;
            if (httpResponseCache.containsKey(httpUrl)) {
                cacheContains = true;
                content = httpResponseCache.get(httpUrl);
            } else {
                SparqlQueryProcessor.NUMBER_OF_HTTP_REQUESTS.incrementAndGet();

                SparqlQueryProcessor.SERVER_REQUESTS.incrementAndGet();
                // The request goes to the server for getting cardinality estimation for a star pattern
                content = Request.Get(httpUrl).addHeader("accept", "text/turtle").execute().returnContent();

            // Print raw HTTP response
                System.out.println("\n**Initial HTTP Response Content:\n" + content.asString() + "\n ------------------------------");
                SparqlQueryProcessor.TRANSFERRED_BYTES.addAndGet(content.asBytes().length + httpUrl.getBytes().length);
            
                //   System.out.println("\n ** The content returned as Bytes:\n" + content.asBytes().length + httpUrl.getBytes().length);
            //    System.out.println("\n ** The content returned:\n" + content.toString());
            // Stop the program after the first request-response cycle
            //System.exit(0);

            }
            InputStream stream = content.asStream();
            CountMetadataHandler countMetadataHandler = new CountMetadataHandler(stream);

            TurtleParser turtleParser = new TurtleParser();
            turtleParser.set(BasicParserSettings.VERIFY_URI_SYNTAX, false);
            turtleParser.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
            turtleParser.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);

            turtleParser.setRDFHandler(countMetadataHandler);
            turtleParser.parse(stream, "");
            if (!cacheContains) {
                if(httpResponseCache.size() == SparqlQueryProcessor.MAX_CACHE_ENTRIES) {
                    httpResponseCache.remove(SparqlQueryProcessor.cacheQueue.poll());
                }
                httpResponseCache.put(httpUrl, content);
                SparqlQueryProcessor.cacheQueue.add(httpUrl);
            }
            triplesCount = countMetadataHandler.getTriplesCount();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            return Integer.MAX_VALUE;
        }
        return triplesCount;
    }
}
