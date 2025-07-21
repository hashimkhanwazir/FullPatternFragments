package dk.aau.cs.spf.model;

import dk.aau.cs.spf.util.Tuple;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.ArrayList;
import java.util.List;

public class SinkPattern {
    private List<StatementPattern> statementPatterns;
    private ArrayList<String> listOfVars;
    private String objectVarName;
    private List<Tuple<String, String>> varNames;
    private int triplesCount;

    public SinkPattern(List<StatementPattern> statementPatterns) {
        this.statementPatterns = statementPatterns;
        this.listOfVars = new ArrayList<String>();
        objectVarName = null;

        Var objectVar = statementPatterns.get(0).getObjectVar();
        if (!objectVar.isAnonymous() && !objectVar.isConstant()) {
            objectVarName = "?" + objectVar.getName();
            listOfVars.add(objectVarName);
        }

        varNames = new ArrayList<>();

        for (StatementPattern pattern : statementPatterns) {
            Tuple<String, String> tpl = new Tuple<>(null, null);

            String sVarName = null;
            Var subjectVar = pattern.getSubjectVar();
            if (!subjectVar.isAnonymous() && !subjectVar.isConstant()) {
                sVarName = "?" + subjectVar.getName();
                listOfVars.add(sVarName);
            }
            if (sVarName != null)
                tpl.x = sVarName;

            String pVarName = null;
            Var predicateVar = pattern.getPredicateVar();
            if (!predicateVar.isAnonymous() && !predicateVar.isConstant()) {
                pVarName = "?" + predicateVar.getName();
                listOfVars.add(pVarName);
            }
            if (pVarName != null)
                tpl.y = pVarName;

            varNames.add(tpl);
        }
    }

    public StatementPattern getStatement(int index) {
        return statementPatterns.get(index);
    }

    public String getVarString(String varName) {
        if(varName.equals(objectVarName))
            return "object";
        for (int i = 0; i < varNames.size(); i++) {
            int j = i+1;
            Tuple<String, String> tpl = varNames.get(i);
            if (varName.equals(tpl.x)) {
                return "s"+j;
            } else if (varName.equals(tpl.y)) {
                return "p"+j;
            }
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
        int numberOfBV = 0;
        for (String boundVar : boundVars) {
            if (containsVar(boundVar)) {
                numberOfBV++;
            }
        }
        return numberOfBV;
    }

    public ArrayList<String> getListOfVars() {
        return listOfVars;
    }

    public Var getObjectVar() {
        return statementPatterns.get(0).getObjectVar();
    }

    public String getObjectVarName() {
        return objectVarName;
    }

    public int getNumberOfTriplePatterns() {
        return statementPatterns.size();
    }

    public int getTriplesCount() {
        return triplesCount;
    }

    public Var getSubjectVar(int index) {
        return statementPatterns.get(index).getSubjectVar();
    }

    public Var getPredicateVar(int index) {
        return statementPatterns.get(index).getPredicateVar();
    }

    public String getSubjectVarName(int index) {
        return varNames.get(index).x;
    }

    public String getPredicateVarName(int index) {
        return varNames.get(index).y;
    }

    public void setTriplesCount(int triplesCount) {
        this.triplesCount = triplesCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // Uncomment if you want to see the patterns
         for (StatementPattern sp : statementPatterns) {
             sb.append("  ").append(sp).append("\n");
         }
        return sb.toString();
    }
}
