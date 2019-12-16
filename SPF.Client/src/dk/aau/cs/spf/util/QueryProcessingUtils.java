package dk.aau.cs.spf.util;

import java.util.*;

import dk.aau.cs.spf.model.*;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import com.github.jsonldjava.shaded.com.google.common.collect.Sets;
import dk.aau.cs.spf.model.VarBinding.VarBindingType;

public class QueryProcessingUtils {

    private static URLCodec urlCodec = new URLCodec("utf8");

    public static ArrayList<String> getBoundVariables(ArrayList<TriplePattern> triplePatterns) {
        ArrayList<String> boundedVariables = new ArrayList<String>();
        for (TriplePattern triplePattern : triplePatterns) {
            boundedVariables.addAll(triplePattern.getListOfVars());
        }
        return boundedVariables;
    }

    public static ArrayList<String> getBoundVariablesSP(ArrayList<StarPattern> starPatterns) {
        ArrayList<String> boundedVariables = new ArrayList<String>();
        for (StarPattern starPattern : starPatterns) {
            boundedVariables.addAll(starPattern.getListOfVars());
        }
        return boundedVariables;
    }

    public static TriplePattern findAndRemoveNextWithMaxNumberOfBV(
            ArrayList<TriplePattern> triplePatterns, ArrayList<String> boundVariables) {
        if (triplePatterns.isEmpty()) {
            return null;
        } else if (triplePatterns.size() == 1) {
            return triplePatterns.remove(0);
        }
        int maxNoOfBV = 0;
        int indexOfNextTP = 0;
        for (int i = 0; i < triplePatterns.size(); i++) {
            TriplePattern currTP = triplePatterns.get(i);
            int noOfBV = currTP.getNumberOfBoundVariables(boundVariables);
            if (noOfBV > maxNoOfBV) {
                maxNoOfBV = noOfBV;
                indexOfNextTP = i;
            }
        }
        return triplePatterns.remove(indexOfNextTP);
    }

    public static StarPattern findAndRemoveNextWithMaxNumberOfBVSP(
            ArrayList<StarPattern> starPatterns, ArrayList<String> boundVariables) {
        if (starPatterns.isEmpty()) {
            return null;
        } else if (starPatterns.size() == 1) {
            return starPatterns.remove(0);
        }
        int maxNoOfBV = 0;
        int indexOfNextTP = 0;
        for (int i = 0; i < starPatterns.size(); i++) {
            StarPattern currTP = starPatterns.get(i);
            int noOfBV = currTP.getNumberOfBoundVariables(boundVariables);
            if (noOfBV > maxNoOfBV) {
                maxNoOfBV = noOfBV;
                indexOfNextTP = i;
            }
        }
        return starPatterns.remove(indexOfNextTP);
    }

    public static String constructFragmentURL(String startingFragment, TriplePattern tp)
            throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();
        isQuestionMarkAdded = appendUrlParam(sb, tp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendUrlParam(sb, tp.getPredicateVar(),
                HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
        isQuestionMarkAdded =
                appendUrlParam(sb, tp.getObjectVar(), HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);
        return startingFragment + sb.toString();
    }

    public static String constructFragmentURL(String startingFragment, StarPattern sp)
            throws EncoderException {
        if(sp.getNumberOfTriplePatterns() == 1) {
            return constructFragmentURL(startingFragment, new TriplePattern(sp.getStatement(0)));
        }

        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        int num = sp.getNumberOfTriplePatterns();
        isQuestionMarkAdded = appendUrlParamStar(sb, sp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendTripleParam(sb, num, HttpRequestConfig.TRIPLES_PARAM,
                isQuestionMarkAdded);

        for (int i = 0; i < num; i++) {
            int j = i + 1;

            isQuestionMarkAdded = appendUrlParamStar(sb, sp.getPredicateVar(i),
                    "p" + j, isQuestionMarkAdded);
            isQuestionMarkAdded = appendUrlParamStar(sb, sp.getObjectVar(i),
                    "o" + j, isQuestionMarkAdded);
        }
        return startingFragment + sb.toString();
    }

    private static boolean appendTripleParam(StringBuilder sb, int num, String paramName,
                                             Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(num);
        return isQuestionMarkAdded;
    }

    private static boolean appendUrlParam(StringBuilder sb, Var var, String paramName,
                                          Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (!var.isAnonymous()) {
                sb.append("&").append(paramName).append("=?").append(var.getName());
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("&").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
            }
        } else {
            if (!var.isAnonymous()) {
                sb.append("?").append(paramName).append("=?").append(var.getName());
                return true;
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("?").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private static boolean appendUrlParamStar(StringBuilder sb, Var var, String paramName,
                                              Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (!var.isAnonymous()) {
                sb.append("&").append(paramName).append("=");
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("&").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
            }
        } else {
            if (!var.isAnonymous()) {
                sb.append("?").append(paramName).append("=");
                return true;
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("?").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private static boolean matchesWithBinding(TriplePattern tp, Statement triple,
                                              BindingHashMap binding) {
        String subjectVarName = tp.getSubjectVarName();
        if (binding.containsKey(subjectVarName)) {
            if (!binding.get(subjectVarName).getValue().equals(triple.getSubject().toString())) {
                return false;
            }
        }

        String predicateVarName = tp.getPredicateVarName();
        if (binding.containsKey(predicateVarName)) {
            if (!binding.get(predicateVarName).getValue().equals(triple.getPredicate().toString())) {
                return false;
            }
        }
        String objectVarName = tp.getObjectVarName();
        if (binding.containsKey(objectVarName)) {
            if (!binding.get(objectVarName).getValue().equals(triple.getObject().toString())) {
                return false;
            }

        }
        return true;

    }

    public static void extendBinding(TriplePattern tp, BindingHashMap binding, Statement triple) {
        String subjectVarName = tp.getSubjectVarName();
        if (subjectVarName != null && !binding.containsKey(subjectVarName)) {
            binding.put(subjectVarName,
                    new VarBinding(triple.getSubject().toString(), VarBindingType.IRI));
        }
        String predicateVarName = tp.getPredicateVarName();
        if (predicateVarName != null && !binding.containsKey(predicateVarName)) {
            binding.put(predicateVarName,
                    new VarBinding(triple.getPredicate().toString(), VarBindingType.IRI));
        }
        String objectVarName = tp.getObjectVarName();
        if (objectVarName != null && !binding.containsKey(objectVarName)) {
            if (triple.getObject() instanceof Literal) {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBindingType.LITERAL));
            } else {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBindingType.IRI));
            }
        }
    }

    public static BindingHashMap createBinding(TriplePattern tp, Statement triple) {
        BindingHashMap binding = new BindingHashMap();
        String subjectVarName = tp.getSubjectVarName();
        if (subjectVarName != null) {
            binding.put(subjectVarName,
                    new VarBinding(triple.getSubject().toString(), VarBindingType.IRI));
        }
        String predicateVarName = tp.getPredicateVarName();
        if (predicateVarName != null) {
            binding.put(predicateVarName,
                    new VarBinding(triple.getPredicate().toString(), VarBindingType.IRI));
        }
        String objectVarName = tp.getObjectVarName();
        if (objectVarName != null) {
            if (triple.getObject() instanceof Literal) {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBindingType.LITERAL));
            } else {
                binding.put(objectVarName,
                        new VarBinding(triple.getObject().toString(), VarBindingType.IRI));
            }
        }
        return binding;
    }

    public static void extendBinding(BindingHashMap firstBHM, BindingHashMap secondBHM) {
        Set<String> secondVarNames = secondBHM.keySet();
        Set<String> firstVarNames = firstBHM.keySet();
        Set<String> differentVarNames = Sets.difference(secondVarNames, firstVarNames);
        for (String differentVarName : differentVarNames) {
            firstBHM.put(differentVarName, secondBHM.get(differentVarName));
        }
    }

    public static ArrayList<BindingHashMap> extendBindings(ArrayList<BindingHashMap> bindings,
                                                           TriplePattern tp, Collection<Statement> triples) {
        ArrayList<BindingHashMap> extendedBindings = new ArrayList<BindingHashMap>();
        if (bindings.isEmpty()) {
            for (Statement triple : triples) {
                BindingHashMap binding = new BindingHashMap();
                extendBinding(tp, binding, triple);
                extendedBindings.add(binding);
            }
        } else {
            for (BindingHashMap currentBinding : bindings) {
                for (Statement triple : triples) {
                    if (matchesWithBinding(tp, triple, currentBinding)) {
                        BindingHashMap newBinding = new BindingHashMap(currentBinding);
                        extendBinding(tp, newBinding, triple);
                        extendedBindings.add(newBinding);
                        //break;
                    }
                }
            }

        }
        return extendedBindings;
    }

    private static boolean matchesWithBinding(StarPattern tp, List<Statement> triple,
                                              BindingHashMap binding) {
        String subjectVarName = tp.getSubjectVarName();
        if (binding.containsKey(subjectVarName)) {
            if (!binding.get(subjectVarName).getValue().equals(triple.get(0).getSubject().toString())) {
                return false;
            }
        }

        int cnt = tp.getNumberOfTriplePatterns();
        for (int i = 0; i < cnt; i++) {
            StatementPattern stp = tp.getStatement(i);
            String predVal = stp.getPredicateVar().getValue().stringValue();
            Statement stmt = null;
            for (Statement s : triple) {
                if (predVal.equals(s.getPredicate().toString())) {
                    stmt = s;
                    break;
                }
            }
            if (stmt == null) continue;

            String predicateVarName = tp.getPredicateVarName(i);
            if (binding.containsKey(predicateVarName)) {
                if (!binding.get(predicateVarName).getValue().equals(stmt.getPredicate().toString())) {
                    return false;
                }
            }
            String objectVarName = tp.getObjectVarName(i);
            if (binding.containsKey(objectVarName)) {
                if (!binding.get(objectVarName).getValue().equals(stmt.getObject().toString())) {
                    return false;
                }

            }
        }
        return true;

    }

    public static void extendBinding(StarPattern sp, BindingHashMap binding, List<Statement> stars) {
        String subjectVarName = sp.getSubjectVarName();
        if (subjectVarName != null && !binding.containsKey(subjectVarName)) {
            binding.put(subjectVarName,
                    new VarBinding(stars.get(0).getSubject().toString(), VarBindingType.IRI));
        }

        int cnt = sp.getNumberOfTriplePatterns();
        for (int i = 0; i < cnt; i++) {
            StatementPattern stp = sp.getStatement(i);
            Statement stmt = null;
            for (Statement s : stars) {
                if (stp.getPredicateVar().getValue().stringValue().equals(s.getPredicate().toString())) {
                    stmt = s;
                    break;
                }
            }
            if (stmt == null) continue;
            String predicateVarName = sp.getPredicateVarName(i);
            if (predicateVarName != null && !binding.containsKey(predicateVarName)) {
                binding.put(predicateVarName,
                        new VarBinding(stmt.getPredicate().toString(), VarBindingType.IRI));
            }
            String objectVarName = sp.getObjectVarName(i);
            if (objectVarName != null && !binding.containsKey(objectVarName)) {
                if (stmt.getObject() instanceof Literal) {
                    binding.put(objectVarName,
                            new VarBinding(stmt.getObject().toString(), VarBindingType.LITERAL));
                } else {
                    binding.put(objectVarName,
                            new VarBinding(stmt.getObject().toString(), VarBindingType.IRI));
                }
            }
        }
    }

    public static ArrayList<BindingHashMap> extendBindings(ArrayList<BindingHashMap> bindings,
                                                           StarPattern sp, List<List<Statement>> stars) {
        ArrayList<BindingHashMap> extendedBindings = new ArrayList<>();
        if (bindings.isEmpty()) {
            for (List<Statement> triple : stars) {
                BindingHashMap binding = new BindingHashMap();
                extendBinding(sp, binding, triple);
                extendedBindings.add(binding);
            }
        } else {
            for (BindingHashMap currentBinding : bindings) {
                for (List<Statement> triple : stars) {
                    if (matchesWithBinding(sp, triple, currentBinding)) {
                        BindingHashMap newBinding = new BindingHashMap(currentBinding);
                        extendBinding(sp, newBinding, triple);
                        extendedBindings.add(newBinding);
                    }
                }
            }

        }
        return extendedBindings;
    }

    public static BindingHashMap extendBindingWithSingleTriple(BindingHashMap currentBinding,
                                                               TriplePattern tp, Statement triple) {
        if (currentBinding == null) {
            BindingHashMap extendedBinding = new BindingHashMap();
            extendBinding(tp, extendedBinding, triple);
            return extendedBinding;
        } else {
            if (matchesWithBinding(tp, triple, currentBinding)) {
                BindingHashMap newBinding = new BindingHashMap(currentBinding);
                extendBinding(tp, newBinding, triple);
                return newBinding;
            } else {
                return null;
            }
        }
    }
}
