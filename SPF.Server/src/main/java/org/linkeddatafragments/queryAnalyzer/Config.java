package org.linkeddatafragments.queryAnalyzer;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
  
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Config {

    private static Config INSTANCE;
    private String datasource;
    private boolean cacheReuse;
    private String server;
    private String metadatapath;
    private String downloadedpartitions;
    private String queriespath;
    private String ExperimentName;
    private int TimeInMin;
    private Map<String, String> prefixes;

    

    private Config() {

    }

    public Config(String configFileName) {
        JsonReader configReader = null;
        if (INSTANCE == null) {
            File configFile;

            prefixes = Maps.newHashMap();
            configFile = new File(configFileName);
            try {
                configReader = new JsonReader(new FileReader(configFile));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Config.class.getName()).log(Level.SEVERE, null, ex);
            }
           
            INSTANCE = new Gson().fromJson(configReader, Config.class);
        }

    }

    public static Config getInstance() {
        return INSTANCE;
    }

    
     public String getExperimentName() {
        return ExperimentName;
    }
    
   
    /**
     * @return the datasource
     */
    public String getDatasource() {
        return datasource;
    }

    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    /**
     * @return the server
     */
    public String getServer() {
        return server;
    }

    /**
     * @return the metadatapath
     */
    public String getMetadatapath() {
        return metadatapath;
    }

    /**
     * @return the queriespath
     */
    public String getQueriespath() {
        return queriespath;
    }
    
    /**
     * @return the downloadedpartitions
     */
     public String getDownloadedpartitions() {
        return downloadedpartitions;
    }

    /**
     * @return the cacheEnabled
     */
    public boolean isCacheResued() {
        return cacheReuse;
    }

    /**
     * @return the TimeInMin
     */
    public int getTimeInMin() {
        return TimeInMin;
    }

}
