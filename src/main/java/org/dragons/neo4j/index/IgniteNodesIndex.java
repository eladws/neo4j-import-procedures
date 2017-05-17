package org.dragons.neo4j.index;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by Elad on 5/11/2017.
 */
public class IgniteNodesIndex implements NodesIndexAPI {

    Ignite ignite;
    Map<String, IgniteDataStreamer<String, Long>> streamers;

    public IgniteNodesIndex() {
        ignite = Ignition.start();
        streamers = new HashMap<>();
    }

    @Override
    public void addNodeToIndex(String label, Object idPropertyValue, long id) {
        if(!streamers.containsKey(label)) {
            //create new streamer
            ignite.getOrCreateCache(label);
            IgniteDataStreamer<String, Long> streamer = ignite.dataStreamer(label);
            streamer.perNodeBufferSize(10000);
            streamers.put(label, streamer);
        }
        //add data to streamer
        streamers.get(label).addData(idPropertyValue.toString(), id);
    }

    @Override
    public long getNodeId(String label, Object idPropertyValue) {
        IgniteCache<String, Long> labelCache = ignite.getOrCreateCache(label);
        return labelCache.get(idPropertyValue.toString());
    }

    @Override
    public void persist() {
        for (IgniteDataStreamer s :
                streamers.values()) {
            s.close();
        }
    }
}
