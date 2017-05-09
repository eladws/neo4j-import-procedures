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
    public interface WorkFunc<IN1, IN2, OUT> {
        OUT apply (IN1 a, IN2 b);
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
        Node startNode = null;

        Object startIdProperty = config.getPropertiesMap().get("start").equals("int") ? // the property value that identifies the specific node
                Integer.valueOf(rowTokens[relWorkConf.getStartMatchPropCol()]) :
                rowTokens[relWorkConf.getStartMatchPropCol()];

        //first, try to seek internal index
        if (config.getNodesIndex() != null) {
            long id = config.getNodesIndex().getNodeId(relImportConf.startNodeLabel, startIdProperty);
            if (id >= 0) {
                startNode = config.getGraphDatabaseAPI().getNodeById(id);
                if (startNode == null) {
                    //indexed node not found in db
                    config.getLog().warn("Node (:%s {%s: %s}) not found in database. searched id: %d",
                            relImportConf.startNodeLabel,
                            relImportConf.startNodeMatchPropName,
                            rowTokens[relWorkConf.getStartMatchPropCol()],
                            id);
                }
            } else {
                //node not found in index
                config.getLog().warn("Node (:%s {%s: %s}) not found in index.",
                        relImportConf.startNodeLabel,
                        relImportConf.startNodeMatchPropName,
                        rowTokens[relWorkConf.getStartMatchPropCol()]);
            }
        }

        if (startNode == null) {
            //find the node using database api
            startNode = config.getGraphDatabaseAPI().findNode(Label.label(relImportConf.startNodeLabel), // node label
                                                                relImportConf.startNodeMatchPropName, // the relevant property name
                                                                startIdProperty // the property value that identifies the specific node
                                                               );
            if (startNode == null) {
                config.getLog().warn("Failed creating relationship. Start node (:%s {%s: %s}) could not be found.",
                        relImportConf.startNodeLabel,
                        relImportConf.startNodeMatchPropName,
                        rowTokens[relWorkConf.getStartMatchPropCol()]);
                return FunctionResult.FAIL;
            }
        }

        Node endNode = null;
        Object endIdProperty = config.getPropertiesMap().get("end").equals("int") ?
                                        Integer.valueOf(rowTokens[relWorkConf.getEndMatchPropCol()]) :
                                        rowTokens[relWorkConf.getEndMatchPropCol()];
        if (config.getNodesIndex() != null) {
            long id = config.getNodesIndex().getNodeId(relImportConf.endNodeLabel, endIdProperty);
            if (id >= 0) {
                endNode = config.getGraphDatabaseAPI().getNodeById(id);
                if (endNode == null) {
                    //indexed node not found in db
                    config.getLog().warn("Node (:%s {%s: %s}) not found in database. searched id: %d",
                            relImportConf.endNodeLabel,
                            relImportConf.endNodeMatchPropName,
                            rowTokens[relWorkConf.getEndMatchPropCol()],
                            id);
                }
            } else {
                //node not found in index
                config.getLog().warn("Node (:%s {%s: %s}) not found in index.",
                        relImportConf.endNodeLabel,
                        relImportConf.endNodeMatchPropName,
                        rowTokens[relWorkConf.getEndMatchPropCol()]);
            }
        }

        if (endNode == null) {
            //find the node using database api
            endNode = config.getGraphDatabaseAPI().findNode(Label.label(relImportConf.endNodeLabel), // node label
                    relImportConf.endNodeMatchPropName, // the relevant property name
                    endIdProperty // the property value that identifies the specific node
                    );

            if (endNode == null) {
                config.getLog().warn("Failed creating relationship. End node (:%s {%s: %s}) could not be found.",
                        relImportConf.endNodeLabel,
                        relImportConf.endNodeMatchPropName,
                        rowTokens[relWorkConf.getEndMatchPropCol()]);
                return FunctionResult.FAIL;
            }
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
            if(config.getNodesIndex() != null && propName.equals("id")) {
                //TODO: currently the only indexed property is the "id" property
                config.getNodesIndex().addNodeToIndex(config.baseImportConfig.label, node.getProperty(propName), node.getId());
            }
            idx++;
        }

        return FunctionResult.SUCCESS;

    }

}
