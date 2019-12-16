package dk.aau.cs.spf.callable;

import dk.aau.cs.spf.main.QueryInput;
import dk.aau.cs.spf.main.SparqlQueryProcessor;
import dk.aau.cs.spf.task.StarHandler;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.turtle.TurtleParser;
import org.eclipse.rdf4j.rio.turtle.TurtleUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TurtleParserStar extends TurtleParser {
    private List<Statement> currStar = new ArrayList<>();

    public TurtleParserStar() {}
    public TurtleParserStar(ValueFactory valueFactory) {
        super(valueFactory);
    }

    @Override
    protected void parseStatement() throws IOException, RDFParseException, RDFHandlerException {
        StringBuilder sb = new StringBuilder(8);

        do {
            int codePoint = this.readCodePoint();
            if (codePoint == -1 || TurtleUtil.isWhitespace(codePoint)) {
                this.unread(codePoint);
                break;
            }

            appendCodepoint(sb, codePoint);
        } while(sb.length() < 8);

        String directive = sb.toString();
        if (!directive.startsWith("@") && !directive.equalsIgnoreCase("prefix") && !directive.equalsIgnoreCase("base")) {
            this.unread(directive);
            if(SparqlQueryProcessor.method == QueryInput.QueryProcessingMethod.SPF)
                parseStar();
            else
                this.parseTriples();
            this.skipWSC();
            this.verifyCharacterOrFail(this.readCodePoint(), ".");
        } else {
            this.parseDirective(directive);
            this.skipWSC();
            if (directive.startsWith("@")) {
                this.verifyCharacterOrFail(this.readCodePoint(), ".");
            }
        }
    }

    protected void parseStar() throws IOException, RDFParseException, RDFHandlerException {
        int c = this.peekCodePoint();
        if (c == 91) {
            c = this.readCodePoint();
            this.skipWSC();
            c = this.peekCodePoint();
            if (c == 93) {
                c = this.readCodePoint();
                this.subject = this.createNode();
                this.skipWSC();
                this.parsePredicateObjectListStar();
            } else {
                this.unread(91);
                this.subject = this.parseImplicitBlank();
            }

            this.skipWSC();
            c = this.peekCodePoint();
            if (c != 46) {
                this.parsePredicateObjectListStar();
            }
        } else {
            this.parseSubject();
            this.skipWSC();
            this.parsePredicateObjectListStar();
        }

        this.subject = null;
        this.predicate = null;
        this.object = null;

        if(rdfHandler != null && currStar.size() > 0) {
            ((StarHandler) rdfHandler).handleStatements(currStar);
        }

        currStar = new ArrayList<>();
    }

    protected void parsePredicateObjectListStar() throws IOException, RDFParseException, RDFHandlerException {
        this.predicate = this.parsePredicate();
        this.skipWSC();
        this.parseObjectListStar();

        while(this.skipWSC() == 59) {
            this.readCodePoint();
            int c = this.skipWSC();
            if (c == 46 || c == 93 || c == 125) {
                break;
            }

            if (c != 59) {
                this.predicate = this.parsePredicate();
                this.skipWSC();
                this.parseObjectListStar();
            }
        }
    }

    protected void parseObjectListStar() throws IOException, RDFParseException, RDFHandlerException {
        this.parseObjectStar();
        while(this.skipWSC() == 44) {
            this.readCodePoint();
            this.skipWSC();
            this.parseObjectStar();
        }
    }

    protected void parseObjectStar() throws IOException, RDFParseException, RDFHandlerException {
        int c = this.peekCodePoint();
        if (c == 40) {
            this.object = this.parseCollection();
        } else if (c == 91) {
            this.object = this.parseImplicitBlank();
        } else {
            this.object = this.parseValue();
            if (this.subject != null && this.predicate != null && this.object != null) {
                Statement st = this.createStatement(this.subject, this.predicate, this.object);
                currStar.add(st);
            }
        }

    }

    private static void appendCodepoint(StringBuilder dst, int codePoint) {
        if (Character.isBmpCodePoint(codePoint)) {
            dst.append((char)codePoint);
        } else {
            if (!Character.isValidCodePoint(codePoint)) {
                throw new IllegalArgumentException("Invalid codepoint " + codePoint);
            }

            dst.append(Character.highSurrogate(codePoint));
            dst.append(Character.lowSurrogate(codePoint));
        }
    }
}
