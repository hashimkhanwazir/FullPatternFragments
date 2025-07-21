package dk.aau.cs.spf.model;

import dk.aau.cs.spf.util.Tuple;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.ArrayList;
import java.util.List;

public class PathPattern {
    private List<StatementPattern> statementPatterns;
    private ArrayList<String> listOfVars;
    private String startVarName;
    private String endVarName;
    private List<Tuple<String, String>> varNames; // Stores (predicate, object) pairs for each triple
    private int triplesCount;
    private List<StatementPattern> firstStep;
    private List<StatementPattern> secondStep;

    // 🔹 Existing constructor
    public PathPattern(List<StatementPattern> statementPatterns) {
        initialize(statementPatterns);
    }

    // 🔹 New constructor accepting two steps
    public PathPattern(List<StatementPattern> firstStep, List<StatementPattern> secondStep) {
        List<StatementPattern> combined = new ArrayList<>();
        combined.addAll(firstStep);
        combined.addAll(secondStep);
        initialize(combined);
    }

    // 🔹 Shared logic for both constructors
    private void initialize(List<StatementPattern> statementPatterns) {
        this.statementPatterns = statementPatterns;
        this.listOfVars = new ArrayList<>();
        this.varNames = new ArrayList<>();

        if (!statementPatterns.isEmpty()) {
            Var startVar = statementPatterns.get(0).getSubjectVar();
            if (!startVar.isAnonymous() && !startVar.isConstant()) {
                startVarName = "?" + startVar.getName();
                listOfVars.add(startVarName);
            }

            Var endVar = statementPatterns.get(statementPatterns.size() - 1).getObjectVar();
            if (!endVar.isAnonymous() && !endVar.isConstant()) {
                endVarName = "?" + endVar.getName();
                listOfVars.add(endVarName);
            }

            for (StatementPattern pattern : statementPatterns) {
                Tuple<String, String> tpl = new Tuple<>(null, null);

                String pVarName = null;
                Var predicateVar = pattern.getPredicateVar();
                if (!predicateVar.isAnonymous() && !predicateVar.isConstant()) {
                    pVarName = "?" + predicateVar.getName();
                    listOfVars.add(pVarName);
                }
                tpl.x = pVarName;

                String oVarName = null;
                Var objectVar = pattern.getObjectVar();
                if (!objectVar.isAnonymous() && !objectVar.isConstant()) {
                    oVarName = "?" + objectVar.getName();
                    listOfVars.add(oVarName);
                }
                tpl.y = oVarName;

                varNames.add(tpl);
            }
        }
    }

    // (All your other methods remain unchanged below...)

    public StatementPattern getStatement(int index) {
        return statementPatterns.get(index);
    }

    public String getVarString(String varName) {
        if (varName.equals(startVarName)) return "start";
        if (varName.equals(endVarName)) return "end";

        for (int i = 0; i < varNames.size(); i++) {
            int j = i + 1;
            Tuple<String, String> tpl = varNames.get(i);
            if (varName.equals(tpl.x)) return "p" + j;
            if (varName.equals(tpl.y)) return "o" + j;
        }
        return "";
    }

    public boolean containsVar(String varName) {
        return listOfVars.contains(varName);
    }

    public List<StatementPattern> getStatementPatterns() {
        return statementPatterns;
    }

    public int getNumberOfBoundVariables(ArrayList<String> boundVars) {
        int count = 0;
        for (String var : boundVars) {
            if (containsVar(var)) count++;
        }
        return count;
    }

    public ArrayList<String> getListOfVars() {
        return listOfVars;
    }

    public Var getStartVar() {
        return statementPatterns.get(0).getSubjectVar();
    }

    public Var getEndVar() {
        return statementPatterns.get(statementPatterns.size() - 1).getObjectVar();
    }

    public String getStartVarName() {
        return startVarName;
    }

    public String getEndVarName() {
        return endVarName;
    }

    public int getNumberOfTriplePatterns() {
        return statementPatterns.size();
    }

    public int getTriplesCount() {
        return triplesCount;
    }

    public Var getPredicateVar(int index) {
        return statementPatterns.get(index).getPredicateVar();
    }

    public Var getObjectVar(int index) {
        return statementPatterns.get(index).getObjectVar();
    }

    public String getPredicateVarName(int index) {
        return varNames.get(index).x;
    }

    public String getObjectVarName(int index) {
        return varNames.get(index).y;
    }

    public void setTriplesCount(int triplesCount) {
        this.triplesCount = triplesCount;
    }

    public List<StatementPattern> getFirstStep() {
        return firstStep != null ? firstStep : statementPatterns;
    }
    
    public List<StatementPattern> getSecondStep() {
        return secondStep != null ? secondStep : new ArrayList<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PathPattern:\n");
        for (StatementPattern sp : statementPatterns) {
            sb.append("  ").append(sp).append("\n");
        }
        return sb.toString();
    }
}
