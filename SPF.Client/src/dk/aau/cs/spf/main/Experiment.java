package dk.aau.cs.spf.main;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import dk.aau.cs.spf.callable.InitialHttpRequestThread;
import dk.aau.cs.spf.model.StarPattern;
import dk.aau.cs.spf.task.InitialHttpRequestTask;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.fluent.Content;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;
import dk.aau.cs.spf.main.QueryInput.QueryProcessingMethod;
import dk.aau.cs.spf.model.TriplePattern;

public class Experiment {

    private static List<ProjectionElem> projectionElemList;
    private static ArrayList<TriplePattern> triplePatterns;
    private static ArrayList<StarPattern> starPatterns;
    private static String queryStr;

    public static void main(String[] args)
            throws IllegalArgumentException, IOException, InterruptedException, ExecutionException {
        if (!(args.length == 7)) {
            System.out.println("Usage: java -jar [filename].jar [starting fragment] [query directory] [method] [output dir] [number of clients] [client no.] [load]");
            return;
        }

        String currSF = args[0];
        String queryDir = args[1];
        QueryProcessingMethod method = QueryProcessingMethod.valueOf(args[2]);
        String outDir = args[3];
        int num_clients = Integer.parseInt(args[4]);
        int client_num = Integer.parseInt(args[5]);
        String load = args[6];
        int executing = 0;

        if(load.equals("all")) {
            File[] dirs = new File(queryDir).listFiles();

            for (File f : dirs) {
                String dir = outDir + "/" + num_clients + "_clients/" + method.toString() + "/client" + client_num + "/" + load;
                new File(dir).mkdirs();

                String filename = dir + "/" + f.getName() + ".csv";

                FileWriter writer = new FileWriter(filename);

                File[] queries = f.listFiles();

                for (File qf : queries) {
                    System.out.println(qf.getName());

                    initializeQueryAndConfig(qf.getPath());
                    QueryInput input = new QueryInput();
                    input.setStartFragment(currSF);

                    SparqlQueryProcessor sqp;
                    if (method == QueryProcessingMethod.SPF) {
                        sqp = new SparqlQueryProcessor(starPatterns, projectionElemList,
                                input, QueryProcessingMethod.SPF, false, false, true);
                    } else if(method == QueryProcessingMethod.ENDPOINT) {
                        sqp = new SparqlQueryProcessor(queryStr, projectionElemList,
                                input, QueryProcessingMethod.ENDPOINT, false, false, "http://130.226.98.177:8890/sparql");
                    } else {
                        sqp = new SparqlQueryProcessor(triplePatterns, projectionElemList,
                                input, method, false, false);
                    }

                    /*sqp.processQuery();

                    String sizeStr = "" + SparqlQueryProcessor.FRAGMENT_SIZES.get(0);
                    for(int i = 1; i < SparqlQueryProcessor.FRAGMENT_SIZES.size(); i++) {
                        sizeStr += "," + SparqlQueryProcessor.FRAGMENT_SIZES.get(i);
                    }

                    String tpsStr = "" + starPatterns.get(0).getNumberOfTriplePatterns();
                    for(int i = 1; i < starPatterns.size(); i++) {
                        tpsStr += "," + starPatterns.get(i).getNumberOfTriplePatterns();
                    }

                    String str = qf.getName() + ";" + sizeStr + ";" + tpsStr + ";" + SparqlQueryProcessor.NUMBER_OF_BINDINGS_RECEIVED;
                    System.out.println(str);
                    writer.write(str + "\n");*/

                    final Duration timeout = Duration.ofMinutes(5);
                    ExecutorService executor = Executors.newSingleThreadExecutor();

                    final Future handler = executor.submit(new Callable() {
                        @Override
                        public String call() throws Exception {
                            sqp.processQuery();
                            return "";
                        }
                    });

                    try {
                        executing++;
                        handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                        String str = qf.getName() + ";" + SparqlQueryProcessor.SERVER_REQUESTS.get() + ";" + sqp.getQueryProcessingTime() + ";" + SparqlQueryProcessor.TRANSFERRED_BYTES.get() + ";" + SparqlQueryProcessor.RESPONSE_TIME + ";" + SparqlQueryProcessor.NUMBER_OF_OUTPUT_BINDINGS;
                        System.out.println(str);
                        writer.write(str + "\n");
                    } catch (TimeoutException | InterruptedException e) {
                        handler.cancel(true);
                        sqp.terminate();

                        String str = qf.getName() + ";-1;-1;-1;-1;-1";
                        System.out.println(str);
                        writer.write(str + "\n");
                    } finally {
                        executing--;
                        executor.shutdownNow();
                    }

                    sqp.close();
                }

                writer.close();
            }
        } else {
            File f = new File(queryDir + "/" + load);
            String dir = outDir + "/" + num_clients + "_clients/" + method.toString() + "/client" + client_num + "/" + load;
            new File(dir).mkdirs();

            String filename = dir + "/" + f.getName() + ".csv";

            FileWriter writer = new FileWriter(filename);

            File[] queries = f.listFiles();

            for (File qf : queries) {
                System.out.println(qf.getName());

                initializeQueryAndConfig(qf.getPath());
                QueryInput input = new QueryInput();
                input.setStartFragment(currSF);

                SparqlQueryProcessor sqp;
                if (method == QueryProcessingMethod.SPF) {
                    sqp = new SparqlQueryProcessor(starPatterns, projectionElemList,
                            input, QueryProcessingMethod.SPF, false, false, true);
                } else if(method == QueryProcessingMethod.ENDPOINT) {
                    sqp = new SparqlQueryProcessor(queryStr, projectionElemList,
                            input, QueryProcessingMethod.ENDPOINT, false, false, "http://130.226.98.177:8890/sparql");
                } else {
                    sqp = new SparqlQueryProcessor(triplePatterns, projectionElemList,
                            input, method, false, false);
                }


                final Duration timeout = Duration.ofMinutes(5);
                ExecutorService executor = Executors.newSingleThreadExecutor();

                final Future handler = executor.submit(new Callable() {
                    @Override
                    public String call() throws Exception {
                        sqp.processQuery();
                        return "";
                    }
                });

                try {
                    executing++;
                    handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                    String str = qf.getName() + ";" + SparqlQueryProcessor.SERVER_REQUESTS.get() + ";" + sqp.getQueryProcessingTime() + ";" + SparqlQueryProcessor.TRANSFERRED_BYTES.get() + ";" + SparqlQueryProcessor.RESPONSE_TIME + ";" + SparqlQueryProcessor.NUMBER_OF_OUTPUT_BINDINGS;
                    System.out.println(str);
                    writer.write(str + "\n");
                } catch (TimeoutException | InterruptedException e) {
                    handler.cancel(true);
                    sqp.terminate();

                    String str = qf.getName() + ";-1;-1;-1;-1;-1";
                    System.out.println(str);
                    writer.write(str + "\n");
                } finally {
                    executing--;
                    executor.shutdownNow();
                }

                sqp.close();
            }

            writer.close();
        }

        if(executing == 0) System.exit(0);
/*
        String currSF = startingFragment + datasource;
        QueryInput input = new QueryInput();
        input.setStartFragment(currSF);
        SparqlQueryProcessor sqp = new SparqlQueryProcessor(triplePatterns, projectionElemList,
                input, QueryProcessingMethod.TPF, true, false);
        sqp.processQuery();
        sqp = new SparqlQueryProcessor(triplePatterns, projectionElemList,
                input, QueryProcessingMethod.TPF, true, false);
        sqp.processQuery();
        int numberOfOutputBindings = sqp.getOutputBindings().size();
        long tpfClientQPT = sqp.getQueryProcessingTime();
        SparqlQueryProcessor sqp2 = new SparqlQueryProcessor(triplePatterns, projectionElemList,
                input, QueryProcessingMethod.BRTPF, true, false);
        sqp2.processQuery();
        sqp2 = new SparqlQueryProcessor(triplePatterns, projectionElemList,
                input, QueryProcessingMethod.BRTPF, true, false);
        sqp2.processQuery();
        int numberOfOutputBindings2 = sqp2.getOutputBindings().size();
        long brtpfClientQPT = sqp2.getQueryProcessingTime();

        SparqlQueryProcessor sqp3 = new SparqlQueryProcessor(starPatterns, projectionElemList,
                input, QueryProcessingMethod.SPF, true, false, true);
        sqp3.processQuery();
        sqp3 = new SparqlQueryProcessor(starPatterns, projectionElemList,
                input, QueryProcessingMethod.SPF, true, false, true);
        sqp3.processQuery();
        int numberOfOutputBindings3 = sqp3.getOutputBindings().size();
        long spfClientQPT = sqp3.getQueryProcessingTime();

        System.out.println(numberOfOutputBindings + " " + numberOfOutputBindings2 + " " + numberOfOutputBindings3);
        System.out.println(Duration.ofMillis(tpfClientQPT) + "," + Duration.ofMillis(brtpfClientQPT) + "," + Duration.ofMillis(spfClientQPT));*/
    }

    private static void initializeQueryAndConfig(String queryFile)
            throws IOException, IllegalArgumentException {
        triplePatterns = new ArrayList<TriplePattern>();
        String queryString = FileUtils.readFileToString(new File(queryFile), StandardCharsets.UTF_8);
        queryStr = queryString;
        SPARQLParserFactory factory = new SPARQLParserFactory();
        QueryParser parser = factory.getParser();
        ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
        TupleExpr query = parsedQuery.getTupleExpr();
        if (query instanceof Projection) {
            Projection proj = (Projection) query;
            projectionElemList = proj.getProjectionElemList().getElements();
        } else {
            throw new IllegalArgumentException("The given query should be a select query.");
        }

        List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);
        Map<String, List<StatementPattern>> patterns = new HashMap<>();
        for (StatementPattern statementPattern : statementPatterns) {
            TriplePattern tp = new TriplePattern(statementPattern);
            triplePatterns.add(tp);
            String subj = tp.getSubjectVarName();

            if (patterns.containsKey(subj)) {
                patterns.get(subj).add(statementPattern);
            } else {
                List<StatementPattern> lst = new ArrayList<>();
                lst.add(statementPattern);
                patterns.put(subj, lst);
            }
        }

        starPatterns = new ArrayList<>();
        Collection<List<StatementPattern>> lst = patterns.values();
        for (List<StatementPattern> stps : lst) {
            starPatterns.add(new StarPattern(stps));
        }
    }
}
