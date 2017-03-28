package org.dragons.neo4j.utils;

import java.util.Map;

/**
 * Created by Elad on 3/28/2017.
 */
public class RelationshipBatchWorkConfig extends GraphBatchWorkConfig {

    private int startMatchPropCol = -1;
    private int endMatchPropCol = -1;

    public void setPropertiesMap(Map<String, String> propMap) {
        propertiesMap = propMap;
        for (int i=0; i<propMap.size(); i++) {
            if (propMap.keySet().toArray()[i].equals("start")) {
                setStartMatchPropCol(i);
            } else if (propMap.keySet().toArray()[i].equals("end")) {
                setEndMatchPropCol(i);
            }
        }
        if(getStartMatchPropCol() < 0 || getEndMatchPropCol() < 0) {
            throw new RuntimeException("Invalid properties map for relationship. missing start+end columns.");
        }
    }

    public int getStartMatchPropCol() {
        return startMatchPropCol;
    }

    public void setStartMatchPropCol(int startMatchPropCol) {
        this.startMatchPropCol = startMatchPropCol;
    }

    public int getEndMatchPropCol() {
        return endMatchPropCol;
    }

    public void setEndMatchPropCol(int endMatchPropCol) {
        this.endMatchPropCol = endMatchPropCol;
    }
}
