package org.linkeddatafragments.fragments.spf;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.StarPatternElementParser;
import org.linkeddatafragments.util.Tuple;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

public class SPFRequestParser<ConstantTermType,NamedVarType,AnonVarType>
        extends FragmentRequestParserBase {
    public final StarPatternElementParser<ConstantTermType,NamedVarType,AnonVarType> elmtParser;

    /**
     *
     * @param elmtParser
     */
    public SPFRequestParser(
            final StarPatternElementParser<ConstantTermType,NamedVarType,AnonVarType> elmtParser )
    {
        this.elmtParser = elmtParser;
    }

    /**
     *
     * @param httpRequest
     * @param config
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getWorker(final HttpServletRequest httpRequest,
                                                final ConfigReader config )
            throws IllegalArgumentException
    {
        return new Worker( httpRequest, config );
    }

    /**
     *
     */
    protected class Worker extends FragmentRequestParserBase.Worker
    {

        /**
         *
         * @param request
         * @param config
         */
        public Worker( final HttpServletRequest request,
                       final ConfigReader config )
        {
            super( request, config );
        }

        /**
         *
         * @return
         * @throws IllegalArgumentException
         */
        @Override
        public ILinkedDataFragmentRequest createFragmentRequest()
                throws IllegalArgumentException
        {
            // System.out.println("Create Fragment Request :)");
            return new StarPatternFragmentRequestImpl<ConstantTermType,NamedVarType,AnonVarType>(
                    getFragmentURL(),
                    getDatasetURL(),
                    pageNumberWasRequested,
                    pageNumber,
                    getSubject(),
                    getStars(),
                    getBindings(),
                    getNumTriples());
        }

        /**
         *
         * @return
         */
        public IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType> getSubject() {
            return getParameterAsStarPatternElement("s");
        }

        /**
         *
         * @return
         */
        public List<Tuple<IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>,
                IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>>> getStars() {
            List<Tuple<IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>,
                    IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>>> lst = new ArrayList<>();

            int triples = getNumTriples();
            for(int i = 0; i < triples; i++) {
                int n = i+1;
                lst.add(new Tuple<>(getParameterAsStarPatternElement("p"+n),
                        getParameterAsStarPatternElement("o"+n)));
            }

            return lst;
        }

        public List<Binding> getBindings() {
            final List<Var> foundVariables = new ArrayList<Var>();
            return parseAsSetOfBindings(
                    request.getParameter("values"),
                    foundVariables);
        }

        /**
         *
         * @param paramName
         * @return
         */
        public IStarPatternElement<ConstantTermType,NamedVarType,AnonVarType>
        getParameterAsStarPatternElement(final String paramName )
        {
            final String parameter = request.getParameter( paramName );
            return elmtParser.parseIntoStarPatternElement( parameter );
        }

        private int getNumTriples() {
            return Integer.parseInt(request.getParameter("triples"));
        }

        /**
         * Parses the given value as set of bindings.
         *
         * @param value          containing the SPARQL bindings
         * @param foundVariables a list with variables found in the VALUES clause
         * @return a list with solution mappings found in the VALUES clause
         */
        private List<Binding> parseAsSetOfBindings(final String value, final List<Var> foundVariables) {
            if (value == null) {
                return null;
            }
            String newString = "select * where {} VALUES " + value;
            Query q = QueryFactory.create(newString);
            foundVariables.addAll(q.getValuesVariables());
            return q.getValuesData();
        }

    } // end of class Worker
}
