package org.linkeddatafragments.datasource.hdt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.linkeddatafragments.characteristicset.CharacteristicSetBase;
import org.linkeddatafragments.characteristicset.CharacteristicSetImpl;
import org.linkeddatafragments.characteristicset.ICharacteristicSet;
import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.spf.SPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.BRTPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.NodeDictionary;


public class HdtDataSource extends DataSourceBase {

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

        datasource = HDTManager.mapIndexedHDT(hdtFile, null);
        dictionary = new NodeDictionary(datasource.getDictionary());

        String csFileName = hdtFile + ".cs";

        File csFile = new File(csFileName);
        if (!csFile.exists()) {
            System.out.println("Creating Characteristics sets..");
            createCharacteristicSets();

            System.out.println("Saving " + csFileName);
            String jsonStr = new Gson().toJson(this.characteristicSets);
            try {
                FileWriter writer = new FileWriter(csFileName);
                writer.write(jsonStr);
                writer.close();
            } catch (IOException e) {
                System.out.println("Could not save file. Continuing in memory.");
                e.printStackTrace();
            }

            System.out.println("Done");
        } else {
            String str = readFile(csFileName);
            Gson gson = new Gson();

            Type collectionType = new TypeToken<List<CharacteristicSetImpl>>(){}.getType();
            this.characteristicSets = gson.fromJson(str, collectionType);
        }
    }

    private static String readFile(String filePath) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    private void createCharacteristicSets() {
        Map<Set<String>, CharacteristicSetBase> sets = new HashMap<>();
        Map<String, Map<String, Integer>> subjs = new HashMap<>();
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

            System.out.print("\rNo. " + i + " / " + size);
            i++;

            String subj = triple.getSubject().toString();
            String pred = triple.getPredicate().toString();

            if (subjs.containsKey(subj)) {
                Map<String, Integer> sMap = subjs.get(subj);
                if (sMap.containsKey(pred)) sMap.put(pred, sMap.get(pred) + 1);
                else sMap.put(pred, 1);
                subjs.put(subj, sMap);
            } else {
                Map<String, Integer> sMap = new HashMap<>();
                sMap.put(pred, 1);
                subjs.put(subj, sMap);
            }
            tpls++;
        }

        System.out.print("\n");
        System.out.println("Found " + subjs.size() + " subjects");
        System.out.println("Converting to characteristic sets...");

        size = subjs.size();
        i = 1;
        for (Map.Entry<String, Map<String, Integer>> entry : subjs.entrySet()) {
            System.out.print("\rNo. " + i + " / " + size);
            i++;

            Set<String> family = entry.getValue().keySet();
            if (sets.containsKey(family)) sets.get(family).addDistinct(entry.getValue());
            else sets.put(family, new CharacteristicSetImpl(1, entry.getValue()));
        }

        System.out.print("\n");

        this.characteristicSets.addAll(sets.values());
    }

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor) {
        if (processor == ProcessorType.TPF)
            return TPFRequestParserForJenaBackends.getInstance();
        else if (processor == ProcessorType.BRTPF)
            return BRTPFRequestParserForJenaBackends.getInstance();
        return SPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor) {
        if (processor == ProcessorType.TPF)
            return new HdtBasedRequestProcessorForTPFs(datasource, dictionary);
        if (processor == ProcessorType.BRTPF)
            return new HdtBasedRequestProcessorForBRTPFs(datasource, dictionary);
        return new HdtBasedRequestProcessorForSPFs(datasource, dictionary, characteristicSets);
    }

}
