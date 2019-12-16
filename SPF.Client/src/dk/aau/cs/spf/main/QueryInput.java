package dk.aau.cs.spf.main;

public class QueryInput {
    private String startFragment;
    private String queryFile;

    public enum QueryProcessingMethod {
        TPF, BRTPF, SPF, ENDPOINT
    }


    public QueryInput() {

    }

    public String getStartFragment() {
        return startFragment;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public void setStartFragment(String startFragment) {
        this.startFragment = startFragment;
    }

    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }
}
