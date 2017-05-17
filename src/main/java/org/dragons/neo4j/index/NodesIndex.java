package org.dragons.neo4j.index;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Elad on 5/9/2017.
 */
public class NodesIndex implements NodesIndexAPI{
    Map<String, Map<Object, Long>> index = new ConcurrentHashMap<>();

    public void addNodeToIndex(String label, Object property, long id) {
        if(!index.containsKey(label)) {
            index.put(label, new HashMap<>());
        }
        if (!index.get(label).containsKey(property)) {
            index.get(label).put(property, id);
        }
    }

    public long getNodeId(String label, Object property) {
        if(!index.containsKey(label) || !index.get(label).containsKey(property)) {
            return -1;
        }
        return index.get(label).get(property);
    }

    @Override
    public void persist() {
        //nothing is needed here
    }
}
