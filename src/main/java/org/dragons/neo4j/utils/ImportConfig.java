package org.dragons.neo4j.utils;

import java.util.List;

/**
 * Created by User on 23/03/2017.
 */
public class ImportConfig {
    public String nodesParallelLevel;
    public String relsParallelLevel;
    public String nodeIdsCache;
    public int maxThreads;
    public List<NodeImportConfig> nodes;
    public List<RelationshipImportConfig> relationships;
}
