package org.dragons.neo4j.index;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;

import java.util.HashMap;
import java.util.Map;

public class IgniteComplexIndex implements NodesIndexAPI {

    private String weakToStrongCache = "STRONG_TO_WEAK_STREAMER)";
    enum NODE_TYPE{STRONG_ID, WEAK_ID};
    Ignite ignite;
    IgniteDataStreamer<String, Long> strongToWeakStreamer;
    Map<String, IgniteDataStreamer<String, Long>> streamers;

    public IgniteComplexIndex() {
        ignite = Ignition.start();
        streamers = new HashMap<>();
        strongToWeakStreamer = ignite.dataStreamer(weakToStrongCache);
        strongToWeakStreamer.perNodeBufferSize(10000);
    }

    @Override
    public void prepareIndex(String label) {
        if(!streamers.containsKey(label)) {
            ignite.getOrCreateCache(label);
            IgniteDataStreamer<String, Long> streamer = ignite.dataStreamer(label);
            streamer.perNodeBufferSize(10000);
            streamers.put(label, streamer);
        }
    }

    public void addStrongToWeakMapping(String strongId, long weakId) {
        strongToWeakStreamer.addData(strongId, weakId);
    }

    @Override
    public void addNodeToIndex(String label, Object idPropertyValue, long id) {
        //add regular id record for this node
        streamers.get(label).addData(idPropertyValue.toString(), id);
    }

    public long getComplexNodeId(String label, String weakId, String strongIdLabel, String strongId) {
        //check if id exists in its label's cache

        //check if the strong id exists in the weak2string id

        return 0;
    }

    @Override
    public long getNodeId(String label, Object idPropertyValue) {
        IgniteCache<String, Long> labelCache = ignite.getOrCreateCache(label);
        if(labelCache == null) {
            return -1;
        }
        Long nodeId = labelCache.get(idPropertyValue.toString());
        if(nodeId == null) {
            return -1;
        }
        return nodeId.longValue();
    }

    @Override
    public void persist() {
        for (IgniteDataStreamer s :
                streamers.values()) {
            s.flush();
        }
    }
}
