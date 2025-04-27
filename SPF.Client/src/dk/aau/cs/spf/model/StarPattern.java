package dk.aau.cs.spf.model;

import dk.aau.cs.spf.util.Tuple;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.ArrayList;
import java.util.List;

public class StarPattern {
    private List<StatementPattern> statementPatterns;
    private ArrayList<String> listOfVars;
    private String subjectVarName;
    private List<Tuple<String, String>> varNames;
    private int triplesCount;

    public StarPattern(List<StatementPattern> statementPatterns) {
        this.statementPatterns = statementPatterns;
        this.listOfVars = new ArrayList<String>();
        subjectVarName = null;
        Var subjectVar = statementPatterns.get(0).getSubjectVar();
        if (!subjectVar.isAnonymous() && !subjectVar.isConstant()) {
            subjectVarName = "?" + subjectVar.getName();
            listOfVars.add(subjectVarName);
        }

        varNames = new ArrayList<>();

        for (StatementPattern pattern : statementPatterns) {
            Tuple<String, String> tpl = new Tuple<>(null, null);

            String pVarName = null;
            Var predicateVar = pattern.getPredicateVar();
            if (!predicateVar.isAnonymous() && !predicateVar.isConstant()) {
                pVarName = "?" + predicateVar.getName();
                listOfVars.add(pVarName);
            }
            if (pVarName != null)
                tpl.x = pVarName;

            String oVarName = null;
            Var objectVar = pattern.getObjectVar();
            if (!objectVar.isAnonymous() && !objectVar.isConstant()) {
                oVarName = "?" + objectVar.getName();
                listOfVars.add(oVarName);
            }
            if (oVarName != null)
                tpl.y = oVarName;

            varNames.add(tpl);
        }
    }

    public StatementPattern getStatement(int index) {
        return statementPatterns.get(index);
    }

    public String getVarString(String varName) {
        if(varName.equals(subjectVarName))
            return "subject";
        for (int i = 0; i < varNames.size(); i++) {
            int j = i+1;
            Tuple<String, String> tpl = varNames.get(i);
            if (varName.equals(tpl.x)) {
                return "p"+j;
            } else if (varName.equals(tpl.y)) {
                return "o"+j;
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
        System.out.println("\n** Inside getNumberOfBoundVariables() **");
        System.out.println("Bound Variables List: " + boundVars);
        for (String boundVar : boundVars) {
            if (containsVar(boundVar)) {
                System.out.println(" Checking if StarPattern contains bound variable " + boundVar );
                numberOfBV++;
            }
        }
        System.out.println("Total Bound Variables Found: " + numberOfBV);
        return numberOfBV;
    }

    public ArrayList<String> getListOfVars() {
        return listOfVars;
    }

    public Var getSubjectVar() {
        return statementPatterns.get(0).getSubjectVar();
    }

    public String getSubjectVarName() {
        return subjectVarName;
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

    @Override
public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("StarPattern:\n");
    for (StatementPattern sp : statementPatterns) {
        sb.append("  ").append(sp).append("\n");
    }
    return sb.toString();
}
}
