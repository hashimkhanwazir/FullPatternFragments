package org.linkeddatafragments.characteristicset;

import org.linkeddatafragments.util.StarString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.HashMap;
import java.util.Map;

public class CharacteristicSetImpl extends CharacteristicSetBase {
    private final Map<String, Integer> predicateMap;

    public CharacteristicSetImpl(int distinct, Map<String, Integer> predicateMap) {
        super(distinct);
        this.predicateMap = predicateMap;
    }

    public CharacteristicSetImpl(Map<String, Integer> predicateMap) {
        super(0);
        this.predicateMap = predicateMap;
    }

    public CharacteristicSetImpl() {
        this(new HashMap<>());
    }

    @Override
    public int countPredicate(String predicate) {
        return predicateMap.get(predicate);
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
    public void addDistinct(Map<String, Integer> element) {
        distinct++;

        for(Map.Entry<String, Integer> e : element.entrySet()) {
            if( predicateMap.containsKey(e.getKey()))
                predicateMap.put(e.getKey(), e.getValue() + predicateMap.get(e.getKey()));
            else
                predicateMap.put(e.getKey(), e.getValue());
        }
    }

    @Override
    public double count(StarString starPattern) {
        int starSize = starPattern.size();
        double m = 1, o = 1;
        boolean subjBound = (!starPattern.getSubject().equals(""));
        for(int i = 0; i < starSize; i++) {
            TripleString triple = starPattern.getTriple(i);

            Integer count = predicateMap.get(triple.getPredicate().toString());
            if(count == null) continue;

            double multiplicity = (double) count / (double) distinct;
            if(!triple.getObject().equals("")) {
                o = Double.min(o, 1/multiplicity);
            } else
                m = m * multiplicity;
        }

        double card = distinct * m * o;
        return subjBound? card / distinct : card;
    }
}
