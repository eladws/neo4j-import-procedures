package org.dragons.neo4j.utils;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Elad on 3/28/2017.
 */
public class WorkFunctions {

    @FunctionalInterface
    public interface WorkFunc<A, B, C> {
        C apply (A a, B b);
    }

    public enum FunctionResult{FAIL, SUCCESS}

    public static Map<Class, WorkFunc<String, GraphBatchWorkConfig, FunctionResult>> functionMap;

    public WorkFunctions() {

        functionMap = new HashMap<>();
        functionMap.put(NodeBatchWorkConfig.class, (line, config) -> createNode(line, config));
        functionMap.put(RelationshipBatchWorkConfig.class,(line, config)->createRelationShip(line, config));
    }

    public WorkFunc<String, GraphBatchWorkConfig, FunctionResult> getFunction(Class cls) {
        return functionMap.get(cls);
    }

    private FunctionResult createRelationShip(String line, GraphBatchWorkConfig config) {


        RelationshipBatchWorkConfig relWorkConf = (RelationshipBatchWorkConfig) config;
        RelationshipImportConfig relImportConf = (RelationshipImportConfig) config.getBaseImportConfig();

        String[] rowTokens = line.split(",");

        //Find endpoints and create the relationship
        Node startNode = config.getGraphDatabaseAPI().findNode(Label.label(relImportConf.startNodeLabel), // node label
                                                               relImportConf.startNodeMatchPropName, // the relevant property name
                                                               config.getPropertiesMap().get("start").equals("int") ? // the property value that identifies the specific node
                                                                            Integer.valueOf(rowTokens[relWorkConf.getStartMatchPropCol()]) :
                                                                            rowTokens[relWorkConf.getStartMatchPropCol()]);

        if (startNode == null) {
            config.getLog().warn("Failed creating relationship. Start node [:%s {%s=%s}] could not be found.",
                    relImportConf.startNodeLabel,
                    relImportConf.startNodeMatchPropName,
                    rowTokens[relWorkConf.getStartMatchPropCol()]);
            return FunctionResult.FAIL;
        }

        Node endNode = config.getGraphDatabaseAPI().findNode(Label.label(relImportConf.endNodeLabel),
                                                             relImportConf.endNodeMatchPropName,
                                                             config.getPropertiesMap().get("end").equals("int") ?
                                                                            Integer.valueOf(rowTokens[relWorkConf.getEndMatchPropCol()]) :
                                                                            rowTokens[relWorkConf.getEndMatchPropCol()]);


        if (endNode == null) {
            config.getLog().warn("Failed creating relationship. Start node [:%s {%s=%s}] could not be found.",
                    relImportConf.endNodeLabel,
                    relImportConf.endNodeMatchPropName,
                    rowTokens[relWorkConf.getEndMatchPropCol()]);
            return FunctionResult.FAIL;
        }

        Relationship rel = startNode.createRelationshipTo(endNode, RelationshipType.withName(relImportConf.label));

        int idx = 0;
        for (String propName :
                relWorkConf.getPropertiesMap().keySet()
                ) {
            if (!propName.equals("start") && !propName.equals("end")) {
                if (relWorkConf.getPropertiesMap().get(propName).equals("int")) {
                    rel.setProperty(propName, Integer.valueOf(rowTokens[idx]));
                } else {
                    rel.setProperty(propName, rowTokens[idx]);
                }
            }
            idx++;
        }

        return FunctionResult.SUCCESS;
    }

    private FunctionResult createNode(String line, GraphBatchWorkConfig config) {

        String[] rowTokens = line.split(",");

        //create the node, and set all properties
        Node node = config.getGraphDatabaseAPI().createNode(Label.label(config.baseImportConfig.label));

        int idx = 0;

        for (String propName :
                config.getPropertiesMap().keySet()
                ) {
            if (config.getPropertiesMap().get(propName).equals("int")) {
                node.setProperty(propName, Integer.valueOf(rowTokens[idx]));
            } else {
                node.setProperty(propName, rowTokens[idx]);
            }
            idx++;
        }

        return FunctionResult.SUCCESS;

    }

}
