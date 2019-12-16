package dk.aau.cs.spf.task;

import java.io.InputStream;

public class SpfParseResponseTask {
    private SpfHttpRequestTask httpRequestTask;
    private InputStream responseStream;

    public SpfParseResponseTask(SpfHttpRequestTask httpRequestTask, InputStream responseStream) {
        this.httpRequestTask = httpRequestTask;
        this.responseStream = responseStream;
    }


    public SpfHttpRequestTask getHttpRequestTask() {
        return httpRequestTask;
    }


    public InputStream getResponseStream() {
        return responseStream;
    }
}
