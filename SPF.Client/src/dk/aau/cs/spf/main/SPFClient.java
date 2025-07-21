package dk.aau.cs.spf.main;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import dk.aau.cs.spf.model.PathPattern;
import dk.aau.cs.spf.model.SinkPattern;
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
    private static List<ProjectionElem> projectionElemList; // the elements in the SELECT clause are called projectionElemList 
    private static QueryInput input;
    private static QueryProcessingMethod qpMethod = QueryProcessingMethod.SPF;
    private static ArrayList<StarPattern> starPatterns;
    private static ArrayList<SinkPattern> sinkPatterns;

    private static String queryStr = "";
    private static boolean tests = false;

    public static void main(String[] args) {
        try {
            initializeInput(args);
            if(tests) {
                Experiment.main(Arrays.copyOfRange(args, 2, args.length-1));
                return;
            }

            // First: method to be called //
            initializeQueryAndConfig();
            
            // Second: The object sqp to be created by calling this constructor
            SparqlQueryProcessor sqp =
            new SparqlQueryProcessor(starPatterns, projectionElemList, input, qpMethod, false, true, true);
            
            // Third: 
            sqp.processQuery();
            
            sqp.printBindings();

            
            System.out.println("NoOfRequests: "+ SparqlQueryProcessor.SERVER_REQUESTS.get() + "  " + "TransferredBytes: "+ SparqlQueryProcessor.TRANSFERRED_BYTES.get() + " " + "ResponseTime: "+SparqlQueryProcessor.RESPONSE_TIME);
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
        System.out.println("\n initializeQueryAndConfig() called");
        String queryString =
                FileUtils.readFileToString(new File(input.getQueryFile()), StandardCharsets.UTF_8);
        queryStr = queryString;
        System.out.println("\nThe queryStr = "+ queryStr);
    
        //List<List<String>> allCombinations = QueryShapeDetector.detectFormattedShapes(queryStr);
        //QueryShapeSingleProcessor.displayCombinations(allCombinations);
        /////////////////////////////////////////////////////
        
        SPARQLParserFactory factory = new SPARQLParserFactory();
        QueryParser parser = factory.getParser();
        ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
        System.out.println("\nParsed query is = "+ parsedQuery);
        TupleExpr query = parsedQuery.getTupleExpr();
        if (query instanceof Projection) {
            Projection proj = (Projection) query;
            projectionElemList = proj.getProjectionElemList().getElements();
            // We can display the projection elements here:
            System.out.println("nProjection Elements are:");
            for (ProjectionElem elem : projectionElemList) {
            System.out.println("- " + elem.getSourceName());  // or elem.toString()
        } } else {
            throw new IllegalArgumentException("The given query should be a select query!!!");
        }
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);
        System.out.println("\n No. of Collected Statement Patterns = " + statementPatterns.size() + "\n");
        //System.out.println("\n The collected statement patterns are  = " + statementPatterns.toString() + "\n"); 

        
        /////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////@@ Star Map @@////////////////////////////////////////
        /// /////////////////////////////////////////////////////////////////////////////

        Map<String, List<StatementPattern>> patterns = new HashMap<>();

        for (StatementPattern statementPattern : statementPatterns) {
            TriplePattern tp = new TriplePattern(statementPattern);
            triplePatterns.add(tp);
            String subj = tp.getSubjectVarName();
            
            System.out.println("Subject Variable: " + subj + "\n\n");

            //Logic for extracting star patterns from the given query

            if(patterns.containsKey(subj)) {
                patterns.get(subj).add(statementPattern);
                //System.out.println("the patterns are: "+ patterns.toString());
            } else {
                List<StatementPattern> lst = new ArrayList<>();
                lst.add(statementPattern);
                patterns.put(subj, lst);
                //System.out.println("In the else, the patterns are: "+ patterns.toString());
            }
        }

        //System.out.println("\n****Overall patterns are as below: " + patterns.toString() + "*************************\n");

        starPatterns = new ArrayList<>();
        Collection<List<StatementPattern>> lst = patterns.values();
        //System.out.println("\n\n --- lst = patterns.values() --- so lst == "+ lst+"\n");
        for(List<StatementPattern> stps : lst) {
            //System.out.println("---- stps = "+stps+" : lst = "+lst );
            StarPattern sp = new StarPattern(stps);
            starPatterns.add(sp);
            //System.out.println("------- The star Patterns = "+ starPatterns);
        }

        int starPatternIndex = 1;
        System.out.println("\n**** Star Patterns Grouped by Subject Variable (1 or more times) ****\n");

        for (StarPattern sp : starPatterns) {
        // Print the pattern index
        System.out.println("Star Pattern " + starPatternIndex++ + ":");
    
        // Get the list of statement patterns for this star pattern
        List<StatementPattern> statements = sp.getStatementPatterns(); // Assuming you have a getter for statement patterns

       // Print the associated statement patterns
       System.out.println("  Associated Statement Patterns:");
    
        for (StatementPattern stmt : statements) {
          System.out.println("    " + stmt);  // Assuming StatementPattern has a meaningful toString() method
        }

        System.out.println("---------------------------------------------------");
}
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////
    /// ////////////////////////////////////////////////////////////////////////////////////////////

    private static void initializeInput(String[] args)
            throws ParseException, IllegalArgumentException {
                System.out.println("\n** initializeInput() ** of the SPFClient.java class");
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

        //String s = commandLine.getOptionValue("t");
        //if(s.equals("true")) {
        //    tests = true;
        //    return;
       // }

        input = new QueryInput();
        input.setStartFragment(commandLine.getOptionValue("f"));
        input.setQueryFile(commandLine.getOptionValue("q"));
    }
}
