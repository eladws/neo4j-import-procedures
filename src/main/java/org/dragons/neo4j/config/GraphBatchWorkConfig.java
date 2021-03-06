package org.dragons.neo4j.config;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Map;

/**
 * Created by Elad on 3/28/2017.
 */
public abstract class GraphBatchWorkConfig {

    private BaseImportConfig baseImportConfig;
    protected int batchSize;
    protected Map<String, String> propertiesMap;
    protected GraphDatabaseAPI graphDatabaseAPI;
    protected Log log;

    public BaseImportConfig getBaseImportConfig() {
        return baseImportConfig;
    }

    public void setBaseImportConfig(BaseImportConfig baseImportConfig) {
        this.baseImportConfig = baseImportConfig;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public GraphDatabaseAPI getGraphDatabaseAPI() {
        return graphDatabaseAPI;
    }

    public void setGraphDatabaseAPI(GraphDatabaseAPI graphDatabaseAPI) {
        this.graphDatabaseAPI = graphDatabaseAPI;
    }

    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public void setPropertiesMap(Map<String, String> propertiesMap) {
        this.propertiesMap = propertiesMap;
    }

    public String printPropMap() {
        StringBuilder sb = new StringBuilder();

        propertiesMap.forEach((k,v)-> sb.append(String.format("%s: %s\n",k,v)));

        return sb.toString();
    }

}
