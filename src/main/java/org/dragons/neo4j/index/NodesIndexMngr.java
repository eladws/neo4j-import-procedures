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
                break;
            case "ignite":
                index = new IgniteNodesIndex();
                break;
            case "internal":
                index = new NodesIndex();
                break;
            default:
                //no indexing
                index = null;
                break;
        }
    }

    public static NodesIndexAPI getNodesIndex() {
        return index;
    }

}
