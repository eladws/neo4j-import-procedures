package org.dragons.neo4j.index;

/**
 * Created by Elad on 5/9/2017.
 */
public interface NodesIndexAPI {
    void prepareIndex(String label);
    void addNodeToIndex(String label, Object idPropertyValue, long id);
    long getNodeId(String label, Object idPropertyValue);
    void persist();
}
