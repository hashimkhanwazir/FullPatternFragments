package dk.aau.cs.spf.main;

import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class QuerySelector {
    static String outDir = "clients";
    static int selected = 0;
    static final int num = 50;
    static final int num_clients = 128;
    static final String[] dirs = {"paths", "1_stars", "2_stars", "3_stars"};


    public static void main(String[] args) throws IOException {
        String queryFolder = args[0];

        /*String d = "clients";
        File[] ds = new File(d).listFiles();

        for(File dd : ds) {
            File dir = new File(dd.getPath() + "/3_stars");
            File[] qfs = dir.listFiles();

            for(File qf : qfs) {

                qf.delete();
            }
        }*/

        /*for(int i = 0; i < num_clients; i++) {
            String dir = outDir + "/client" + i;
            new File(dir).mkdirs();

            for(String m : dirs) {
                File d = new File(dir + "/" + m);
                int count = d.listFiles().length;
                if(count < num) {
                    for(int j = d.listFiles().length; j < num; j++) {
                        File d1 = new File(queryFolder + "/" + m);
                        File[] files = d1.listFiles();
                        Random rand = new Random();
                        File file = files[rand.nextInt(files.length)];

                        File od = new File(dir + "/" + m);
                        od.mkdirs();

                        Files.move(file.toPath(), Paths.get(dir + "/" + m + "/1" + file.getName()));
                    }
                } else if(count > num) {
                    while (d.listFiles().length > num) {
                        File[] files = d.listFiles();
                        Random rand = new Random();
                        File file = files[rand.nextInt(files.length)];
                        file.delete();
                    }
                }
            }
        }*/

        int max = 0;
        int count = 0;
        int num = 0;
        for (int i = 0; i < num_clients; i++) {
            String dir = outDir + "/client" + i + "/paths";
            File d = new File(dir);
            File[] files = d.listFiles();

            for(File qf : files) {
                String queryString = new String(Files.readAllBytes(qf.toPath()), StandardCharsets.UTF_8);
                SPARQLParserFactory factory = new SPARQLParserFactory();
                QueryParser parser = factory.getParser();
                ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
                TupleExpr query = parsedQuery.getTupleExpr();
                List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);

                int size = statementPatterns.size();

                if(size > max) max = size;
                count += size;
                num++;
            }
        }

        double avg = ((double) count) / ((double) num);

        System.out.println(max);
        System.out.println(avg);
    }

    private static void applySelector(Path p) throws IOException {
        String queryString = FileUtils.readFileToString(new File(p.toString()), StandardCharsets.UTF_8);

        SPARQLParserFactory factory = new SPARQLParserFactory();
        QueryParser parser = factory.getParser();
        ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
        TupleExpr query = parsedQuery.getTupleExpr();
        List<StatementPattern> statementPatterns = StatementPatternCollector.process(query);

        Set<String> subjSet = new HashSet<>();
        for (StatementPattern pattern : statementPatterns) {
            subjSet.add(pattern.getSubjectVar().getName());
        }

        int size = subjSet.size();

        if (size <= 3) {
            String dirPath = outDir + "/" + size + "_stars";

            File directory = new File(dirPath);
            if (!directory.exists()) {
                directory.mkdirs();
            }

            Files.move(p, Paths.get(dirPath + "/" + "0" + p.getFileName()));
            selected++;
            return;
        }

        subjSet = new HashSet<>();
        for (StatementPattern pattern : statementPatterns) {
            if (subjSet.contains(pattern.getSubjectVar().getName())) return;
            subjSet.add(pattern.getSubjectVar().getName());
        }

        String dirPath = outDir + "/" + "paths";

        File directory = new File(dirPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        Files.move(p, Paths.get(dirPath + "/" + p.getFileName()));
        selected++;
    }
}
