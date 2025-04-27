package org.linkeddatafragments.characteristicset;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.util.StarString;
import org.linkeddatafragments.util.Tuple;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.*;

public class CharacteristicSetImpl extends CharacteristicSetBase {
    private final Map<String, Tuple<Integer, Integer>> predicateMap;

    public CharacteristicSetImpl(int distinct, Map<String, Tuple<Integer, Integer>> predicateMap) {
        super(distinct);
        this.predicateMap = predicateMap;
    }

    public CharacteristicSetImpl(Map<String, Tuple<Integer, Integer>> predicateMap) {
        super(0);
        this.predicateMap = predicateMap;
    }

    public CharacteristicSetImpl() {
        this(new HashMap<>());
    }

    @Override
    public int countPredicate(String predicate) {
        return predicateMap.get(predicate).x;
    }

    @Override
    public boolean matches(StarString starPattern) {
        int size = starPattern.size();
        for(int i = 0; i < size; i++) {
            String pred = starPattern.getTriple(i).getPredicate().toString();
            if(!predicateMap.containsKey(pred)) return false;
        }
        return true;
    }

    @Override
    public boolean containsPredicate(String predicate) {
        return predicateMap.containsKey(predicate);
    }

    @Override
    public void addDistinct(Map<String, Tuple<Integer, Integer>> element) {
        distinct++;

        for(Map.Entry<String, Tuple<Integer, Integer>> e : element.entrySet()) {
            if( predicateMap.containsKey(e.getKey()))
                predicateMap.put(e.getKey(), new Tuple<>(e.getValue().x + predicateMap.get(e.getKey()).x,
                        e.getValue().y + predicateMap.get(e.getKey()).y));
            else
                predicateMap.put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void setObjectCount(String predicate, int count) {
        predicateMap.get(predicate).y = count;
    }

    @Override
    public double count(StarString starPattern) {
        if(distinct == 1) return 0;
        int starSize = starPattern.size();
        double m = 1, o = 1;
        boolean subjBound = (!starPattern.getSubject().equals("") && !starPattern.getSubject().toString().startsWith("?"));
        for(int i = 0; i < starSize; i++) {
            TripleString triple = starPattern.getTriple(i);

            Integer count = predicateMap.get(triple.getPredicate().toString()).x;
            Integer objects = predicateMap.get(triple.getPredicate().toString()).y;
            
            if(count == null) continue;

            double multiplicity = (double) count / (double) distinct;
            if(!triple.getObject().equals("") && !triple.getObject().toString().startsWith("?")) {
                o = Double.min(o, 1.0/(double)objects);
            } else
                m = m * multiplicity;
        }

        double card = distinct * m * o;
        return subjBound? card / distinct : card;
    }

    public static long estimateNumResults(StarString star, List<Binding> bindings, List<ICharacteristicSet> characteristicSets) {
        long numResultEstimation = 0L;
        StarStringIterator it = new StarStringIterator(bindings, star);
        List<ICharacteristicSet> css = new ArrayList();
        Iterator var7 = characteristicSets.iterator();

        while(var7.hasNext()) {
            ICharacteristicSet cs = (ICharacteristicSet)var7.next();
            if (cs.matches(star)) {
                css.add(cs);
            }
        }

        while(it.hasNext()) {
            StarString st = it.next();
            double size = 0.0D;

            ICharacteristicSet cs;
            for(Iterator var10 = css.iterator(); var10.hasNext(); size += cs.count(st)) {
                cs = (ICharacteristicSet)var10.next();
            }

            numResultEstimation += (long)size;
        }

        return numResultEstimation;
    }

    private static class StarStringIterator implements Iterator<StarString> {
        private final List<Binding> bindings;
        private final StarString star;
        private int current = 0;
        private StarString next = null;

        StarStringIterator(List<Binding> bindings, StarString star) {
            this.bindings = bindings;
            this.star = star;
        }

        private void bufferNext() {
            if (bindings == null && current == 0) {
                next = star;
                current++;
                return;
            }
            if (bindings == null) {
                next = null;
                return;
            }
            if (current >= bindings.size()) {
                next = null;
                return;
            }
            Binding binding = bindings.get(current);
            current++;
            StarString s = new StarString(star.getSubject(), star.getTriples());

            Iterator<Var> vars = binding.vars();
            while (vars.hasNext()) {
                Var var = vars.next();
                Node node = binding.get(var);

                String val = "";
                if (node.isLiteral())
                    val = node.getLiteral().toString();
                else if (node.isURI())
                    val = node.getURI();

                s.updateField(var.getVarName(), val);
            }

            next = s;
        }

        public void reset() {
            current = 0;
        }

        @Override
        public boolean hasNext() {
            if (next == null)
                bufferNext();
            return next != null;
        }

        @Override
        public StarString next() {
            if(next == null) bufferNext();
            StarString n = next;
            next = null;
            return n;
        }
    }
}
