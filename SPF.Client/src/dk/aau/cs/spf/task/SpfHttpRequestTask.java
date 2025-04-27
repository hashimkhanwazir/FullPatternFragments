package dk.aau.cs.spf.task;

import com.github.jsonldjava.shaded.com.google.common.collect.Sets;
import dk.aau.cs.spf.model.BindingHashMap;
import dk.aau.cs.spf.model.HttpRequestConfig;
import dk.aau.cs.spf.model.StarPattern;
import dk.aau.cs.spf.model.TriplePattern;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.eclipse.rdf4j.query.algebra.Var;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class SpfHttpRequestTask {
    private ArrayList<StarPattern> spOrder;
    private String startingFragment;
    private ArrayList<BindingHashMap> bindings;
    private int tpIdx;
    private String fragmentURL;
    private ConcurrentLinkedQueue<BindingHashMap> outputBindings;
    private static URLCodec urlCodec = new URLCodec("utf8");
    private boolean single = false;


    public SpfHttpRequestTask(ArrayList<StarPattern> spOrder, 
                              String startingFragment,
                              ArrayList<BindingHashMap> bindings, 
                              int tpIdx,
                              ConcurrentLinkedQueue<BindingHashMap> outputBindings) {
        this.spOrder = spOrder;
        this.startingFragment = startingFragment;
        this.bindings = bindings;
        this.tpIdx = tpIdx;
        if(spOrder.get(tpIdx).getNumberOfTriplePatterns() == 1) single = true;
        this.outputBindings = outputBindings;
        try {
            this.fragmentURL = constructURL();
        } catch (EncoderException e) {
            e.printStackTrace();
        }
        System.out.println(" ********* I am inside the SpfHttpRequestTask() constructor ****** ");
    }


    public SpfHttpRequestTask(ArrayList<StarPattern> spOrder, 
                              ArrayList<BindingHashMap> bindings,
                              int tpIdx,
                              String fragmentURL, 
                              ConcurrentLinkedQueue<BindingHashMap> outputBindings) {
        this.spOrder = spOrder;
        this.bindings = bindings;
        this.tpIdx = tpIdx;
        this.fragmentURL = fragmentURL;
        this.outputBindings = outputBindings;
        if(spOrder.get(tpIdx).getNumberOfTriplePatterns() == 1) single = true;
    }

    public boolean isSingle() { return single; }

    public String getStartingFragment() {
        return startingFragment;
    }

    public ArrayList<BindingHashMap> getBindings() {
        return bindings;
    }




    private String constructURLSingle() throws EncoderException {
        TriplePattern tp = new TriplePattern(spOrder.get(tpIdx).getStatement(0));
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();
        isQuestionMarkAdded = appendUrlParamSingle(sb, tp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendUrlParamSingle(sb, tp.getPredicateVar(),
                HttpRequestConfig.PREDICATE_PARAM, isQuestionMarkAdded);
        isQuestionMarkAdded =
                appendUrlParamSingle(sb, tp.getObjectVar(), HttpRequestConfig.OBJECT_PARAM, isQuestionMarkAdded);
        if (!bindings.isEmpty()) {
            appendBindingsSingle(sb);
        }
        return startingFragment + sb.toString();
    }

    private void appendBindingsSingle(StringBuilder sb) throws EncoderException {
        if (!bindings.isEmpty()) {
            TriplePattern tp = new TriplePattern(spOrder.get(tpIdx).getStatement(0));
            Set<String> varsInTP = tp.getListOfVars().stream().collect(Collectors.toSet());
            StringBuilder valuesSb = new StringBuilder();
            Set<String> boundVars = bindings.get(0).keySet();
            ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInTP, boundVars));
            List<String> vars = new ArrayList<>();
            for(String str : varsInURL) {
                vars.add("?"+tp.getVarString(str));
            }
            valuesSb.append("(");
            valuesSb.append(String.join(" ", vars));
            valuesSb.append("){");

            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                for (int j = 0; j < varsInURL.size(); j++) {
                    bindingsStrList.add(bindings.get(i).get(varsInURL.get(j)).toString());
                }
                if(set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");
            sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
                    .append(urlCodec.encode(valuesSb.toString()));
        }

    }

    private boolean appendUrlParamSingle(StringBuilder sb, Var var, String paramName,
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



    private String constructURL() throws EncoderException {
        if(single) return constructURLSingle();

        StarPattern sp = spOrder.get(tpIdx);
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();

        int num = sp.getNumberOfTriplePatterns();
        isQuestionMarkAdded = appendUrlParam(sb, sp.getSubjectVar(), HttpRequestConfig.SUBJECT_PARAM,
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendTripleParam(sb, num, HttpRequestConfig.TRIPLES_PARAM,
                isQuestionMarkAdded);

        String str = "[";
        for (int i = 0; i < num; i++) {
            int j = i + 1;
            if(sp.getPredicateVar(i).getValue() != null)
                str = str + "p"+j+"," + sp.getPredicateVar(i).getValue() + ";";
            else
                str = str + "p"+j+"," + sp.getPredicateVarName(i) + ";";
            if(sp.getObjectVar(i).getValue() != null)
                str = str + "o"+j+"," + sp.getObjectVar(i).getValue() + ";";
            else
                str = str + "o"+j+"," + sp.getObjectVarName(i) + ";";
        }

        str = str.substring(0, str.length()-1) + "]";
        isQuestionMarkAdded = appendStringParam(sb, str, "star", isQuestionMarkAdded);
        if (!bindings.isEmpty()) {
            appendBindings(sb);
        }
        return startingFragment + sb.toString();
    }

    private void appendBindings(StringBuilder sb) throws EncoderException {
        if (!bindings.isEmpty()) {
            StarPattern tp = spOrder.get(tpIdx);
            Set<String> varsInTP = tp.getListOfVars().stream().collect(Collectors.toSet());
            StringBuilder valuesSb = new StringBuilder();
            Set<String> boundVars = bindings.get(0).keySet();
            ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInTP, boundVars));
            List<String> vars = new ArrayList<>();
            for(String str : varsInURL) {
                vars.add("?"+tp.getVarString(str));
            }
            valuesSb.append("(");
            valuesSb.append(String.join(" ", vars));
            valuesSb.append("){");

            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                for (int j = 0; j < varsInURL.size(); j++) {
                    bindingsStrList.add(bindings.get(i).get(varsInURL.get(j)).toString());
                }
                if(set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");
            sb.append("&").append(HttpRequestConfig.BINDINGS_PARAM).append("=")
                    .append(urlCodec.encode(valuesSb.toString()));
        }

    }

    private boolean appendUrlParam(StringBuilder sb, Var var, String paramName,
                                   Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (!var.isAnonymous()) {
                sb.append("&").append(paramName).append("=")
                        .append(urlCodec.encode("?" + var.getName()));
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("&").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
            }
        } else {
            if (!var.isAnonymous()) {
                sb.append("?").append(paramName).append("=")
                        .append(urlCodec.encode("?" + var.getName()));
                return true;
            } else if (var.isAnonymous() && var.isConstant()) {
                sb.append("?").append(paramName).append("=")
                        .append(urlCodec.encode(var.getValue().stringValue()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private boolean appendTripleParam(StringBuilder sb, int num, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(num);
        return isQuestionMarkAdded;
    }

    private boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        return isQuestionMarkAdded;
    }

    public int getTpIdx() {
        return tpIdx;
    }

    public StarPattern getStarPattern() {
        return spOrder.get(tpIdx);
    }

    public String getFragmentURL() {
        return fragmentURL;
    }

    public ArrayList<StarPattern> getSpOrder() {
        return spOrder;
    }

    public ConcurrentLinkedQueue<BindingHashMap> getOutputBindings() {
        return outputBindings;
    }

    /**
     * @param startingFragment the startingFragment to set
     */
    public void setStartingFragment(String startingFragment) {
        this.startingFragment = startingFragment;
    }
}
