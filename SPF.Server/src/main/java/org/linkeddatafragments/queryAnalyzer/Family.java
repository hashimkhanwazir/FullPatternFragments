/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatafragments.queryAnalyzer;

import java.util.List;
import org.apache.jena.graph.Node;

 
public class Family {
    
    private int index;
    private String name;
    private boolean originalFamily;
    private int numSubjects;
    private int numTriples;
    private List<Integer> sourceSet;
    private boolean grouped;
    private List<String> predicateSet;
   
    
    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNumSubjects() {
        return numSubjects;
    }

    public void setNumSubjects(int numSubjects) {
        this.numSubjects = numSubjects;
    }

    public int getNumTriples() {
        return numTriples;
    }

    public void setNumTriples(int numTriples) {
        this.numTriples = numTriples;
    }

    public List<String> getPredicateSet() {
        return predicateSet;
    }

    public void setPredicateSet(List<String> predicateSet) {
        this.predicateSet = predicateSet;
    }

    /**
     * @return the grouped
     */
    public boolean isGrouped() {
        return grouped;
    }

    /**
     * @param grouped the grouped to set
     */
    public void setGrouped(boolean grouped) {
        this.grouped = grouped;
    }
    
    
    public boolean isOriginalFamily() {
        return originalFamily;
    }

    public void setOriginalFamily(boolean originalFamily) {
        this.originalFamily = originalFamily;
    }

    public List<Integer> getSourceSet() {
        return sourceSet;
    }

    public void setSourceSet(List<Integer> sourceSet) {
        this.sourceSet = sourceSet;
    }
    
    
    
    
}

