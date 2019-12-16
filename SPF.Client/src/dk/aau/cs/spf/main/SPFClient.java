package dk.aau.cs.spf.main;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;

import dk.aau.cs.spf.model.StarPattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
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

/**
 * @author Ilkcan Keles
 */
public class SPFClient {
    private static ArrayList<TriplePattern> triplePatterns = new ArrayList<TriplePattern>();
    private static List<ProjectionElem> projectionElemList;
    private static QueryInput input;
    private static QueryProcessingMethod qpMethod = QueryProcessingMethod.SPF;
    private static ArrayList<StarPattern> starPatterns;
    private static String queryStr = "";
    private static boolean tests = false;

    public static void main(String[] args) {
        try {
            initializeInput(args);
            if(tests) {
                Experiment.main(Arrays.copyOfRange(args, 2, args.length-1));
                return;
            }

            initializeQueryAndConfig();
            SparqlQueryProcessor sqp =
                    new SparqlQueryProcessor(starPatterns, projectionElemList, input, qpMethod, true, true, true);
            sqp.processQuery();
            sqp.printBindings();

            System.out.println(SparqlQueryProcessor.SERVER_REQUESTS.get() + " " + SparqlQueryProcessor.TRANSFERRED_BYTES.get() + " " + SparqlQueryProcessor.RESPONSE_TIME);
        } catch (ParseException e) {
            System.err.println("usage: java skytpf-client.jar -f startFragment -q query.sparql");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void initializeQueryAndConfig() throws IOException, IllegalArgumentException {
        String queryString =
                FileUtils.readFileToString(new File(input.getQueryFile()), StandardCharsets.UTF_8);
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

            if(patterns.containsKey(subj)) {
                patterns.get(subj).add(statementPattern);
            } else {
                List<StatementPattern> lst = new ArrayList<>();
                lst.add(statementPattern);
                patterns.put(subj, lst);
            }
        }

        starPatterns = new ArrayList<>();
        Collection<List<StatementPattern>> lst = patterns.values();
        for(List<StatementPattern> stps : lst) {
            starPatterns.add(new StarPattern(stps));
        }
    }

    private static void initializeInput(String[] args)
            throws ParseException, IllegalArgumentException {
        Option optionF =
                Option.builder("f").required(false).desc("Start fragment").longOpt("startFr").build();
        optionF.setArgs(1);
        Option optionQ =
                Option.builder("q").required(false).desc("SPARQL query file").longOpt("query").build();
        optionQ.setArgs(1);
        Option optionT =
                Option.builder("t").required(true).desc("Run tests").longOpt("tests").build();
        optionT.setArgs(1);
        Options options = new Options();
        options.addOption(optionF);
        options.addOption(optionQ);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        String s = commandLine.getOptionValue("t");
        if(s.equals("true")) {
            tests = true;
            return;
        }

        input = new QueryInput();
        input.setStartFragment(commandLine.getOptionValue("f"));
        input.setQueryFile(commandLine.getOptionValue("q"));
    }
}

