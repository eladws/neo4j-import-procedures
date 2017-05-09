package org.dragons.neo4j.utils;

/**
 * Created by Elad on 5/9/2017.
 */
public interface NodesIndexAPI {
    void addNodeToIndex(String label, Object idPropertyValue, long id);
    long getNodeId(String label, Object idPropertyValue);
}
