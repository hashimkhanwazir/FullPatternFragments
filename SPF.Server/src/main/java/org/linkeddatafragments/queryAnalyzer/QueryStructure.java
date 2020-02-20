/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatafragments.queryAnalyzer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Var;

/**
 *
 * @author azzam
 */
class QueryStructure {
    
     private List<Triple> queryBGP;
    private List<OpJoin> queryJoins;
    private List<OpLeftJoin> queryLeftJoins;
    private List<OpUnion> queryUnions;
    //query modifiers
    private List<OpOrder> queryOrders;
    private List<OpFilter> queryFilters;
    private List<Var> queryProjectVars;
    // Star Star ID , List of TP in each Star
    private Map<String, List<Triple>> queryStars;
    private HashMap< TripleWrapper, String> TripleByStar;

    static QueryStructure getInstance() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    void parseQuery(Query query) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Map<String, List<Triple>> getQueryStars() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    List<Triple> getQueryBGP() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public HashMap<TripleWrapper, String> getTripleByStar() {
        return TripleByStar;
    }
    
    
    
}
