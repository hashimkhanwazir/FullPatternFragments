package org.linkeddatafragments.datasource.hdt;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.linkeddatafragments.characteristicset.CharacteristicSetBase;
import org.linkeddatafragments.characteristicset.CharacteristicSetImpl;
import org.linkeddatafragments.characteristicset.ICharacteristicSet;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.spf.SPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.BRTPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.linkeddatafragments.queryAnalyzer.Config;
import org.linkeddatafragments.servlet.LinkedDataFragmentServlet;
import org.linkeddatafragments.util.LRUCache;
import org.linkeddatafragments.util.StarString;
import org.linkeddatafragments.util.Tuple;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.NodeDictionary;


public class HdtDataSource extends DataSourceBase {

    private final String hdtFile;

    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    /**
     * The Characteristic Sets
     */
    protected List<ICharacteristicSet> characteristicSets = new ArrayList<>();

    protected LRUCache<Tuple<Long, Long>, IteratorStarString> pageCache;

    /**
     * Creates a new HdtDataSource.
     *
     * @param title       title of the datasource
     * @param description datasource description
     * @param hdtFile     the HDT datafile
     * @throws IOException if the file cannot be loaded
     */
    public HdtDataSource(String title, String description, String hdtFile) throws IOException {
        super(title, description);
        this.hdtFile = hdtFile;

        if(!description.contains("Molecule"))
            datasource = HDTManager.mapIndexedHDT(hdtFile, null);
        else
            datasource = HDTManager.mapIndexedHDT(hdtFile, null);
        dictionary = new NodeDictionary(datasource.getDictionary());

        if(!description.contains("Molecule")) {
            pageCache = new LRUCache<>(500);
        } else {
            pageCache = new LRUCache<>(2);
        }

        /*String csFileName = hdtFile + ".cs";

        File csFile = new File(csFileName);
        if (!csFile.exists()) {
            System.out.println("Creating Characteristics sets..");
            createCharacteristicSets();

            System.out.println("Found " + characteristicSets.size() + " characteristic sets");
            System.out.println("Saving " + csFileName);
            try {
                Gson gson = new Gson();
                FileWriter writer = new FileWriter(csFileName);

                int size = characteristicSets.size();
                int i = 1;
                for(ICharacteristicSet cs : characteristicSets) {
                    if(i%100 == 0)
                        System.out.print("\rSaving " + i + "/" + size);
                    i++;
                    String jsonStr = gson.toJson(cs);
                    writer.write(jsonStr);
                    writer.write("\n");
                }
                writer.close();
            } catch (Exception e) {
                System.out.println("Could not save file. Continuing in memory.");
                e.printStackTrace();
            }

            System.out.println("Done");
        } else {
            Gson gson = new Gson();
            File f = new File(csFileName);
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);

            String line;
            while((line=br.readLine())!=null) {
                if(line.equals("")) continue;
                Type collectionType = new TypeToken<List<CharacteristicSetImpl>>() {
                }.getType();
                this.characteristicSets = gson.fromJson(line, collectionType);
            }
        }*/
    }

    private void createCharacteristicSets() {
        Map<Set<String>, CharacteristicSetBase> sets = new HashMap<>();
        Map<Set<String>, Map<String, Set<String>>> preds = new HashMap<>();
        Map<String, Map<String, Tuple<Integer, Integer>>> subjs = new HashMap<>();
        Map<String, Map<String, Set<String>>> predMap = new HashMap<>();
        int tpls = 0;

        IteratorTripleString it;
        try {
            it = datasource.search("", "", "");
        } catch (NotFoundException e) {
            return;
        }

        int i = 1;
        long size = it.estimatedNumResults();
        while (it.hasNext()) {
            TripleString triple = it.next();

            if(i%100000 == 0)
                System.out.print("\rNo. " + i + " / " + size);
            i++;

            String subj = triple.getSubject().toString();
            String pred = triple.getPredicate().toString();
            String obj = triple.getObject().toString();

            if(predMap.containsKey(subj)) {
                Map<String, Set<String>> map = predMap.get(subj);
                if(map.containsKey(pred)) map.get(pred).add(obj);
                else {
                    Set<String> set = new HashSet<>();
                    set.add(obj);
                    map.put(pred, set);
                }
            } else {
                Map<String, Set<String>> map = new HashMap<>();
                Set<String> set = new HashSet<>();
                set.add(obj);
                map.put(pred, set);
                predMap.put(subj, map);
            }

            if (subjs.containsKey(subj)) {
                Map<String, Tuple<Integer, Integer>> sMap = subjs.get(subj);
                if (sMap.containsKey(pred)) {
                    sMap.put(pred, new Tuple<>(sMap.get(pred).x + 1,0));
                }
                else sMap.put(pred, new Tuple<>(1,0));
                subjs.put(subj, sMap);
            } else {
                Map<String, Tuple<Integer,Integer>> sMap = new HashMap<>();
                sMap.put(pred, new Tuple<>(1,0));
                subjs.put(subj, sMap);
            }
            tpls++;
        }

        System.out.print("\n");
        System.out.println("Found " + subjs.size() + " subjects");
        System.out.println("Converting to characteristic sets...");

        size = subjs.size();
        i = 1;
        for (Map.Entry<String, Map<String, Tuple<Integer,Integer>>> entry : subjs.entrySet()) {
            if(i%100000 == 0)
                System.out.print("\rNo. " + i + " / " + size);
            i++;

            Set<String> family = entry.getValue().keySet();
            if (sets.containsKey(family)) sets.get(family).addDistinct(entry.getValue());
            else sets.put(family, new CharacteristicSetImpl(1, entry.getValue()));
        }

        System.out.println("Finding object values...");

        size = predMap.size();
        i = 1;
        for (Map.Entry<String, Map<String, Set<String>>> entry : predMap.entrySet()) {
            if(i%100000 == 0)
                System.out.print("\rNo. " + i + " / " + size);
            i++;

            Set<String> family = entry.getValue().keySet();
            if(preds.containsKey(family)) {
                Map<String, Set<String>> set = preds.get(family);
                for (Map.Entry<String, Set<String>> ent : entry.getValue().entrySet()) {
                    if(set.containsKey(ent.getKey())) set.get(ent.getKey()).addAll(ent.getValue());
                    else set.put(ent.getKey(), ent.getValue());
                }
            } else {
                Map<String, Set<String>> set = new HashMap<>();
                for (Map.Entry<String, Set<String>> ent : entry.getValue().entrySet()) {
                    set.put(ent.getKey(), ent.getValue());
                }
                preds.put(family, set);
            }
        }

        System.out.println("Setting no. object values.");
        i = 1;
        size = preds.size();

        for(Map.Entry<Set<String>, Map<String, Set<String>>> entry : preds.entrySet()) {
            if(i%100000 == 0)
                System.out.print("\rNo. " + i + " / " + size);
            i++;

            CharacteristicSetBase charSet = sets.get(entry.getKey());
            for(Map.Entry<String, Set<String>> ent : entry.getValue().entrySet()) {
                charSet.setObjectCount(ent.getKey(), ent.getValue().size());
            }
        }

        System.out.print("\n");

        this.characteristicSets.addAll(sets.values());
    }

    @Override
    public IFragmentRequestParser getRequestParser(ProcessorType processor) {
        if (processor == ProcessorType.TPF)
            return TPFRequestParserForJenaBackends.getInstance();
        else if (processor == ProcessorType.BRTPF)
            return BRTPFRequestParserForJenaBackends.getInstance();
        return SPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(ProcessorType processor) {
        if (processor == ProcessorType.TPF)
            return new HdtBasedRequestProcessorForTPFs(datasource, dictionary);
        if (processor == ProcessorType.BRTPF)
            return new HdtBasedRequestProcessorForBRTPFs(datasource, dictionary);
        return new HdtBasedRequestProcessorForSPFs(datasource, dictionary, characteristicSets, pageCache);
    }
}
