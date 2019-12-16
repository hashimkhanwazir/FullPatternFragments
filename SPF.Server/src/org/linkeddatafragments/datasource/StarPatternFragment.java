package org.linkeddatafragments.datasource;

import org.apache.jena.rdf.model.Model;

import java.util.ArrayList;
import java.util.List;

public interface StarPatternFragment {
    public long getTotalSize();
    List<Model> getStarPatterns();
}
