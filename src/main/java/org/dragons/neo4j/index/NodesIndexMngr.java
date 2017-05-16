package org.dragons.neo4j.index;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Elad on 5/14/2017.
 */
public class NodesIndexMngr {

    private static NodesIndexAPI index;

    public static void initNodesIndex(String indexConfig) {
        switch(indexConfig) {
            case "redis":
                index = new RedisNodesIndex();
            case "ignite":
                index = new IgniteNodesIndex();
            case "internal":
                index = new NodesIndex();
            default:
                //no indexing
                index = null;
        }
    }

    public static NodesIndexAPI getNodesIndex() {
        return index;
    }

}
