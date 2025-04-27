package org.rdfhdt.hdt.triples.impl;

import org.apache.jena.base.Sys;
import org.linkeddatafragments.util.StarID;
import org.linkeddatafragments.util.Tuple;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class NestedStarIterator {
    private Triples triples;
    private StarID originalStar;
    private Map<String, Integer> currentBindings;
    protected List<TripleID> currentTriples;
    private StarID star;
    private TripleID selected = null;
    private NestedStarIterator iterator = null;
    private Tuple<String, String> vars = null;
    private IteratorTripleID it = null;
    private List<TripleID> next = null;


    public NestedStarIterator() {
    }



    public NestedStarIterator(Triples triples,
                              StarID originalStar,
                              Map<String, Integer> currentBindings,
                              List<TripleID> currentTriples) {
        this.triples = triples;
        this.originalStar = originalStar;
        this.currentBindings = currentBindings;
        this.currentTriples = currentTriples;
        System.out.println("\nClass NestedStarIterator.java - Constructor (triples, originalStar, currentBindings, currentTriples)");

        System.out.println("this.triples = "+this.triples);
        System.out.println("this.originalStar = "+this.originalStar);
        System.out.println("this.currentBindings = "+this.currentBindings);
        System.out.println("this.currentTriples = "+this.currentTriples);

        star = new StarID(originalStar);
        System.out.println("star = new StarID(originalStar) -- star = "+star);

        selectNextTriple();
        iterator = new EmptyNestedIterator(currentTriples);
        it = triples.search(selected);
    }



    public NestedStarIterator(Triples triples,
                              StarID originalStar) {
        this(triples, originalStar, new HashMap<>(), new ArrayList<>());
    System.out.println("\nClass NesterStarIterator - Constructor (Triples, originalStar) - Iterator obj created");
    System.out.println("Triples = "+triples.size());
    System.out.println("Original Star = "+originalStar.toString());
    }


    public List<TripleID> getNext() {
        if(next == null) bufferNext();
        List<TripleID> ret = next;
        next = null;
        return ret;
    }


    public boolean hasNext() {
        if(next == null)
            bufferNext();
        return next != null;
    }


    private void bufferNext() {
        while(true) {
            if (iterator.hasNext()) {
                next = iterator.getNext();
                return;
            }

            if (!it.hasNext()) {
                next = null;
                return;
            }

            TripleID tid = it.next();

            Map<String, Integer> bindings = new HashMap<>(currentBindings);
            if(!currentBindings.containsKey(star.getSubjVar()))
                bindings.put(star.getSubjVar(), (int)tid.getSubject());
            if (!vars.x.equals(""))
                bindings.put(vars.x, (int) tid.getPredicate());
            if (!vars.y.equals(""))
                bindings.put(vars.y, (int) tid.getObject());

            List<TripleID> triples = new ArrayList<>(currentTriples);
            triples.add(tid);
            if(star.size() == 0) {
                next = triples;
                return;
            }
            iterator = new NestedStarIterator(this.triples, star, bindings, triples);
        }
    }



    private void selectNextTriple() {
        System.out.println("-- inside the selectNextTriple() and star.size() = "+star.size());
        if (star.size() == 0) { 
            System.out.println("if (star.size() == 0) then Return");
            return;
        }
        if (star.size() == 1) {
            System.out.println("-- if (star.size() == 1)");
            selected = star.getTriple(0);
            System.out.println("-- selected = star.getTriple(0) -- selected = "+selected);
            vars = star.getVar(0);
            System.out.println("-- vars = star.getVar(0) -- vars = "+vars);
            star.remove(0);
            setNextSelected();
            return;
        }

        System.out.println("-- if (star.size() > 1) then start from here:");
        int currIndex = 0;
        long currBest = Long.MAX_VALUE;
        System.out.println("-- currIndex = "+currIndex+" and currBest = "+currBest);

        for (int i = 0; i < star.size(); i++) {

            System.out.println("\n-- star.getTriple(i) at i="+i+" is "+star.getTriple(i));
            System.out.println("-- star.getVar(i) at i="+i+" is "+star.getVar(i));
            
            TripleID triple = replace(star.getTriple(i), star.getVar(i));
       
            System.out.println("--- triples class is: " + triples.getClass().getName());
            IteratorTripleID tmpIt = triples.search(triple);
            System.out.println("\n**** Class NestedStarIterator.java - tmpIt returned here --->> " +tmpIt.toString());

            long cnt = tmpIt.estimatedNumResults();

            System.out.println("long cnt = tmpIt.estimatedNumResults(); -- so cnt = "+cnt);
            if (cnt < currBest) {
                System.out.println("if (cnt < currBest) - ( "+cnt+" < "+currBest+" )");
                currIndex = i;
                System.out.println("currIndex = i; i.e., i = "+i+" and currIndex = "+currIndex);
                currBest = cnt;
                System.out.println("currBest = cnt; i.e., cnt = "+cnt+" currBest = "+currBest);
            }
        }

        selected = star.getTriple(currIndex);
        System.out.println("selected = star.getTriple(currIndex);  So selected = "+selected);
        vars = star.getVar(currIndex);
        System.out.println("vars = star.getVar(currIndex);  So vars = "+vars);
        star.remove(currIndex);
        System.out.println("star.remove(currIndex);  Since currIndex = "+currIndex+" , star.remove(currIndex) worked !!");
        setNextSelected();
        System.out.println("setNextSelected(); called !!");
    }

    private void setNextSelected() {
        if (selected.getSubject() == 0 && currentBindings.containsKey(star.getSubjVar()))
            selected.setSubject(currentBindings.get(star.getSubjVar()));
        if (selected.getPredicate() == 0 && currentBindings.containsKey(vars.x))
            selected.setPredicate(currentBindings.get(vars.x));
        if (selected.getObject() == 0 && currentBindings.containsKey(vars.y))
            selected.setObject(currentBindings.get(vars.y));
    }

    private TripleID replace(TripleID triple, Tuple<String, String> vars) {

        System.out.println("\n----------- inside replace(tripleID, Tuple<String, String> vars) -------------------------");

        System.out.println("\nThe triples to be sent is = "+triple);
        System.out.println("The variables to be sent is = "+vars);

        System.out.println("\n triple.getSubject() == "+triple.getSubject());
        System.out.println(" triple.getPredicate() == "+triple.getPredicate());
        System.out.println(" triple.getObject() == "+triple.getObject());
        
        System.out.println("\nstar.getSubjVar() == "+star.getSubjVar());
        System.out.println("vars.x == "+vars.x);
        System.out.println("vars.y == "+vars.y);

    

        if (triple.getSubject() == 0 && currentBindings.containsKey(star.getSubjVar()))
        {   System.out.println("IF triple.getSubject() == 0: " +triple.getSubject() == 0+ "AND currentBindings.containsKey(star.getSubjVar()): "+currentBindings.containsKey(star.getSubjVar()));
            triple.setSubject(currentBindings.get(star.getSubjVar()));}

        if (triple.getPredicate() == 0 && currentBindings.containsKey(vars.x)){
            System.out.println("IF triple.getPredicate() == 0: "+triple.getPredicate()+ " AND currentBindings.containsKey(vars.x): "+currentBindings.containsKey(vars.x));
            triple.setPredicate(currentBindings.get(vars.x));}

        if (triple.getObject() == 0 && currentBindings.containsKey(vars.y)){
            System.out.println("IF triple.getObject() == 0: "+triple.getObject() == 0+" AND currentBindings.containsKey(vars.y): "+currentBindings.containsKey(vars.y));
            triple.setObject(currentBindings.get(vars.y));}

            System.out.println("---------- The returned triples is = "+triple+"--------------\n");
        
        return triple;
    }
}
