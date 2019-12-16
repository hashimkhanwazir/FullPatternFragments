package dk.aau.cs.spf.model;

import java.util.Comparator;

public class StarPatternComparator implements Comparator<StarPattern> {
    @Override
    public int compare(StarPattern o1, StarPattern o2) {
        return Integer.compare(o1.getTriplesCount(), o2.getTriplesCount());
    }
}
