package org.dragons.neo4j.index;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

/**
 * Created by Elad on 5/11/2017.
 */
public class IgniteNodesIndex implements NodesIndexAPI {

    Ignite ignite;

    public IgniteNodesIndex() {
        ignite = Ignition.start();
    }

    @Override
    public void addNodeToIndex(String label, Object idPropertyValue, long id) {
        IgniteCache<String, Long> labelCache = ignite.getOrCreateCache(label);
        labelCache.put(idPropertyValue.toString(), id);
    }

    @Override
    public long getNodeId(String label, Object idPropertyValue) {
        IgniteCache<String, Long> labelCache = ignite.getOrCreateCache(label);
        return labelCache.get(idPropertyValue.toString());
    }
}
