/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatafragments.queryAnalyzer;

/**
 *
 * @author azzam
 */

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;


public class TripleWrapper extends Triple {

    public TripleWrapper(Node s, Node p, Node o) {
        super(s, p, o);
    }

    @Override
    public int hashCode() {
        return this.getPredicate().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        /* if (getClass() != obj.getClass()) {
            return false;
        }*/
        
        Node subject = ((TripleWrapper)obj).getSubject();
        Node predicate = ((TripleWrapper)obj).getPredicate();
        Node object = ((TripleWrapper)obj).getObject();
        
        if (subject.toString().startsWith("?"))
            subject = Node.ANY;
        if (object.toString().startsWith("?"))
            object = Node.ANY;
        if (predicate.toString().startsWith("?"))
            predicate = Node.ANY;
        
       final TripleWrapper other = new TripleWrapper(subject, predicate, object);
     //   System.out.println("OBJ: " + this.hashCode());
      //  System.out.println("OBJ: " + this);
       // System.out.println("Other: " + other.hashCode());
       // System.out.println("Other: " + other);
       // System.out.println("Matches: " + this.matches(other));
       // System.out.println("Matches: " + other.matches(this));
        
        return other.matches(this);

    }

}
