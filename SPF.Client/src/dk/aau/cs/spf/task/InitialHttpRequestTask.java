package dk.aau.cs.spf.task;

import dk.aau.cs.spf.model.StarPattern;
import org.apache.commons.codec.EncoderException;
import dk.aau.cs.spf.model.TriplePattern;
import dk.aau.cs.spf.util.QueryProcessingUtils;

public class InitialHttpRequestTask {
    private String startingFragment;
    private TriplePattern triplePattern;
    private StarPattern starPattern;
    private String fragmentURL;


    public InitialHttpRequestTask(String startingFragment, TriplePattern triplePattern) {
        this.startingFragment = startingFragment;
        this.triplePattern = triplePattern;
        try {
            this.fragmentURL = QueryProcessingUtils.constructFragmentURL(startingFragment, triplePattern);
        } catch (EncoderException e) {
            e.printStackTrace();
        }
    }

    public InitialHttpRequestTask(String startingFragment, StarPattern starPattern) {
        this.startingFragment = startingFragment;
        this.starPattern = starPattern;
        try {
            System.out.println("***** I am in the initialHttpRequestTask()  ******** ");
            this.fragmentURL = QueryProcessingUtils.constructFragmentURL(startingFragment, starPattern);
            System.out.println("***** InitialHttpRequestTask() Constructed Fragment URL: " + this.fragmentURL);
        } catch (EncoderException e) {
            e.printStackTrace();
        }
    }

    public String getStartingFragment() {
        return startingFragment;
    }

    public TriplePattern getTriplePattern() {
        return triplePattern;
    }

    public String getFragmentURL() {
        return fragmentURL;
    }
}
