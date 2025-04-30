package dk.aau.cs.spf.main;

import org.eclipse.rdf4j.query.algebra.*;
import java.util.*;

public class QueryShapeExtractor {

    public static void extractPathsAndSinks(TupleExpr expr) {
        // Collect statement patterns
        List<StatementPattern> patterns = new ArrayList<>();
        collectStatementPatterns(expr, patterns);

        // Track appearances
        Set<String> subjectVars = new HashSet<>();
        Set<String> objectVars = new HashSet<>();
        Map<String, List<StatementPattern>> subjectToPatterns = new HashMap<>();

        // Analyze patterns
        for (StatementPattern sp : patterns) {
            String subj = sp.getSubjectVar().getName();
            String obj = sp.getObjectVar().getName();

            subjectVars.add(subj);
            objectVars.add(obj);

            subjectToPatterns.computeIfAbsent(subj, k -> new ArrayList<>()).add(sp);
        }

        // Extract path queries (naive DFS from each subject)
        System.out.println("=== Path Queries ===");
        for (String subj : subjectToPatterns.keySet()) {
            List<String> visited = new ArrayList<>();
            dfsPaths(subj, subjectToPatterns, visited);
        }

        // Identify sinks
        System.out.println("\n=== Sink Variables ===");
        Set<String> sinks = new HashSet<>(objectVars);
        sinks.removeAll(subjectVars);
        for (String sink : sinks) {
            System.out.println("?"+sink);
        }
    }

    private static void collectStatementPatterns(TupleExpr expr, List<StatementPattern> patterns) {
        if (expr instanceof StatementPattern) {
            patterns.add((StatementPattern) expr);
        } else if (expr instanceof UnaryTupleOperator) {
            collectStatementPatterns(((UnaryTupleOperator) expr).getArg(), patterns);
        } else if (expr instanceof BinaryTupleOperator) {
            collectStatementPatterns(((BinaryTupleOperator) expr).getLeftArg(), patterns);
            collectStatementPatterns(((BinaryTupleOperator) expr).getRightArg(), patterns);
        }
    }

    private static void dfsPaths(String currentVar, Map<String, List<StatementPattern>> subjectToPatterns, List<String> visited) {
        if (visited.contains(currentVar)) return;

        visited.add(currentVar);
        List<StatementPattern> patterns = subjectToPatterns.get(currentVar);
        if (patterns != null) {
            for (StatementPattern sp : patterns) {
                String obj = sp.getObjectVar().getName();
                System.out.println(sp);
                dfsPaths(obj, subjectToPatterns, visited);
            }
        }
    }
}
