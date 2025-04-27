package org.linkeddatafragments.util;

import org.apache.jena.base.Sys;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StarString {
    private CharSequence subject;
    private List<Tuple<CharSequence, CharSequence>> triples = new ArrayList<>();

    /**
     * Basic constructor
     */
    public StarString() {
        super();
    }

    public StarString(CharSequence subject, List<Tuple<CharSequence, CharSequence>> triples) {
        this.subject = subject;
        this.triples = triples;
    }

    public StarString(CharSequence subject) {
        this.subject = subject;
    }

    /**
     * Build a TripleID as a copy of another one.
     * @param other
     */
    public StarString(StarString other) {
        super();
        subject = other.subject;
        triples = other.triples;
    }

    public TripleString getTriple(int pos) {
        Tuple<CharSequence, CharSequence> t = triples.get(pos);
        System.out.println("\n- Method getTriple(int pos) - returns TripleString");
        System.out.println("_____ pos = "+ pos);
        System.out.println("_____ t.x = "+ t.x);
        System.out.println("_____ t.y = "+ t.y);
        System.out.println("_____ subject = "+ subject);
        
        return new TripleString(subject, t.x, t.y);
    }

    public TripleString getTripleString(int pos) {
        Tuple<CharSequence, CharSequence> t = triples.get(pos);
        return new TripleString(subject.toString().startsWith("?")? "" : subject,
                t.x.toString().startsWith("?")? "" : t.x,
                t.y.toString().startsWith("?")? "" : t.y);
    }

    public ArrayList<TripleString> toTripleStrings() {
        ArrayList<TripleString> ret = new ArrayList<>();
        for (Tuple<CharSequence, CharSequence> t : triples) {
            ret.add(new TripleString(subject, t.x, t.y));
        }
        return ret;
    }

    public List<CharSequence> getPredicates() {
        List<CharSequence> ret = new ArrayList<>();

        for(Tuple<CharSequence, CharSequence> t : triples) {
            ret.add(t.x);
        }

        return ret;
    }

    public int size() {
        return triples.size();
    }

    public CharSequence getSubject() {
        return subject;
    }

    public void setSubject(CharSequence subject) {
        this.subject = subject;
    }

    public List<Tuple<CharSequence, CharSequence>> getTriples() {
        return triples;
    }

    public void setTriples(List<Tuple<CharSequence, CharSequence>> triples) {
        this.triples = triples;
    }

    public void addTriple(Tuple<CharSequence, CharSequence> triple) {
        triples.add(triple);
    }

    public StarID toStarID(Dictionary dictionary) {

        System.out.println("\nClass StarString.java - Method toStarID(Dict dict) returns StarID");
        int subj = (subject.equals("") || subject.charAt(0) == '?')? 0 : (int)dictionary.stringToId(subject, TripleComponentRole.SUBJECT);
        
        boolean sub_empty = subject.equals("");
        System.out.println("subject_is_empty: " + sub_empty);

        boolean sub_starts_with_question_mark = subject.charAt(0) == '?';
        System.out.println("sub_starts_with_question_mark: " +sub_starts_with_question_mark);
        
        System.out.println("subj = "+subj);
        
        String subjVar = (subject.equals("") || subject.charAt(0) == '?')? subject.toString() : "";
        System.out.println("subVar = "+subjVar);

        List<Tuple<String, String>> vars = new ArrayList<>();
        List<Tuple<Integer, Integer>> lst = new ArrayList<>();
        int size = size();
        System.out.println("int size = size() -- size = "+size);
        for(int i = 0; i < size; i++) {
            TripleString tpl = getTriple(i);
            // Displaying the TripleString object
            System.out.println("\nIteration " + i + " : tpl = getTriple(i) --- tpl = " + tpl);
            
            
            Tuple<Integer, Integer> t = new Tuple<>(
                    (tpl.getPredicate().equals("") || tpl.getPredicate().charAt(0) == '?')? 0 : (int)dictionary.stringToId(tpl.getPredicate(), TripleComponentRole.PREDICATE),
                    (tpl.getObject().equals("") || tpl.getObject().charAt(0) == '?')? 0 : (int)dictionary.stringToId(tpl.getObject(), TripleComponentRole.OBJECT)
            );
            lst.add(t);


            Tuple<String, String> t1 = new Tuple<>(
                    tpl.getPredicate().charAt(0) == '?'? tpl.getPredicate().toString() : "",
                    tpl.getObject().charAt(0) == '?'? tpl.getObject().toString() : ""
            );
            vars.add(t1);

            // Printing debug information
    System.out.println("\n  Predicate String: " + tpl.getPredicate() + "  -- Predicate ID: " + t.x);
    System.out.println("  Object String: " + tpl.getObject() + " -- Object ID: " + t.y);
    System.out.println("  Tuple t (IDs): " + t);
    System.out.println("  Tuple t1 (Variables): " + t1);
    System.out.println("return new StarID(subj, lst, subVar, vars)");
    System.out.println("----------------------------------");
        }

        return new StarID(subj, lst, subjVar, vars);
    }

    public void updateField(String name, String val) {
        if(name.equals("subject")) {
            subject = val;
            return;
        }

        String so = name.substring(0,1);
        int num = Integer.parseInt(name.substring(1));

        if(so.equals("p"))
            triples.get(num-1).x = val;
        else
            triples.get(num-1).y = val;
    }

    @Override
    public String toString() {
        String str = subject.toString();

        for(Tuple<CharSequence, CharSequence> t : triples) {
            str += "\n    " + t.x + " " + t.y;
        }

        return str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarString starID = (StarString) o;
        return subject == starID.subject &&
                Objects.equals(triples, starID.triples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, triples);
    }

    /**
     * Set all components to zero.
     */
    public void clear() {
        triples.clear();
        subject = "";
    }

    public List<String> getVariables() {
        List<String> vars = new ArrayList<>();
        if(subject.toString().startsWith("?"))
            vars.add(subject.toString());

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String pred = tpl.x.toString(), obj = tpl.y.toString();
            if(pred.startsWith("?")) vars.add(pred);
            if(obj.startsWith("?")) vars.add(obj);
        }

        return vars;
    }

    public int numBoundVars(List<String> bound) {
        int bv = 0;
        if(bound.contains(subject.toString())) bv++;

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String pred = tpl.x.toString(), obj = tpl.y.toString();
            if(bound.contains(pred)) bv++;
            if(bound.contains(obj)) bv++;
        }

        return bv;
    }

    public int numBoundSO(List<String> bound) {
        int bso = 0;
        if(bound.contains(subject.toString()) || !subject.toString().startsWith("?")) bso++;

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String obj = tpl.y.toString();
            if(bound.contains(obj) || !obj.startsWith("?")) bso++;
        }

        return bso;
    }
}
