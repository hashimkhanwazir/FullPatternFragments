package dk.aau.cs.spf.model;

import java.util.ArrayList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

public class TriplePattern {
  private StatementPattern statementPattern;
  private ArrayList<String> listOfVars;
  private String subjectVarName;
  private String predicateVarName;
  private String objectVarName;
  private int triplesCount;


  public TriplePattern(StatementPattern statementPattern) {
    this.statementPattern = statementPattern;
    this.listOfVars = new ArrayList<String>();
    subjectVarName = null;
    Var subjectVar = statementPattern.getSubjectVar();
    if (!subjectVar.isAnonymous() && !subjectVar.isConstant()) {
      subjectVarName = "?" + subjectVar.getName();
      listOfVars.add(subjectVarName);
    }
    predicateVarName = null;
    Var predicateVar = statementPattern.getPredicateVar();
    if (!predicateVar.isAnonymous() && !predicateVar.isConstant()) {
      predicateVarName = "?" + predicateVar.getName();
      listOfVars.add(predicateVarName);
    }
    objectVarName = null;
    Var objectVar = statementPattern.getObjectVar();
    if (!objectVar.isAnonymous() && !objectVar.isConstant()) {
      objectVarName = "?" + objectVar.getName();
      listOfVars.add(objectVarName);
    }
  }

  public boolean containsVar(String varName) {
    return listOfVars.contains(varName);
  }

  public StatementPattern getStatementPattern() {
    return statementPattern;
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
    return statementPattern.getObjectVar();
  }

  public Var getSubjectVar() {
    return statementPattern.getSubjectVar();
  }

  public Var getPredicateVar() {
    return statementPattern.getPredicateVar();
  }

  public String getSubjectVarName() {
    return subjectVarName;
  }

  public String getObjectVarName() {
    return objectVarName;
  }

  public String getPredicateVarName() {
    return predicateVarName;
  }

  public int getTriplesCount() {
    return triplesCount;
  }

  public void setTriplesCount(int triplesCount) {
    this.triplesCount = triplesCount;
  }
}
