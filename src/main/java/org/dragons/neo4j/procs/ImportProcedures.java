package org.dragons.neo4j.procs;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by eladw on 09/03/2017.
 */
public class ImportProcedures {

    private static final String POISON = "POISON";

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public Log log;

    @Procedure(mode = Mode.SCHEMA)
    public void loadNodesFile(@Name("File path") String file,
                              @Name("Node label") String label,
                              @Name("Properties names and types (e.g. 'name1:type1,name2:type2'...) in the same order as they appear in the file." +
                                    "If this value is null, the first row will be parsed as the header.") String header,
                              @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                              @Name("batch size for a single transaction") long batchSize,
                              @Name("array of property names to index on")List<String> indexedProps) {

        try {

            log.info("Importing nodes of type %s from %s started.", label, file);

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;

            BlockingQueue<String> workerQueue = new ArrayBlockingQueue<>((int) (batchSize * 2));

            Map<String, String> propertyTypeMap;

            int rowCount = 0;

            while ((line = br.readLine()) != null) {

                ++rowCount;

                if(rowCount == 1) {

                    if (header == null) {

                        //first row: build property map, and spawn a worker
                        propertyTypeMap = buildPropertyTypeMap(line);
                        skipFirst = true;

                    } else {
                        propertyTypeMap = buildPropertyTypeMap(header);
                    }

                    log.info("Using Properties map:\n %s", printPropMap(propertyTypeMap));

                    spawnNodesBatchWorker(graphDatabaseAPI, workerQueue, batchSize, label, propertyTypeMap);

                    if(skipFirst) {
                        continue;
                    }
                }

                workerQueue.put(line);

            }

            workerQueue.put(POISON);

            log.info("Finished loading %s lines.",String.valueOf(rowCount));

            if(indexedProps != null && indexedProps.size() > 0) {
                log.info("Starting indexing process...");
                indexedProps.forEach(s -> graphDatabaseAPI.execute(String.format("CREATE INDEX ON :%s(%s)",label,s)));
                log.info("Indexing process finished.");
            }

        } catch (Exception e) {
            log.error("Failed with exception: %s",e.getMessage());
        }

    }


    @Procedure(mode = Mode.SCHEMA)
    public void loadRelationshipFile(@Name("File path") String file,
                                     @Name("Relationship label") String label,
                                     @Name("Start node label") String startLabel,
                                     @Name("End node label") String endNLabel,
                                     @Name("Start node property for matching") String startNodeProp,
                                     @Name("End node property for matching") String endNodeProp,
                                     @Name("Properties names and types (e.g. 'name1:type1,name2:type2'...) in the same order as they appear in the file." +
                                           "If this value is null, the first row will be parsed as the header." +
                                           "The start and end constraints must be named 'start' and 'end' exactly.") String header,
                                     @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                                     @Name("batch size for a single transaction") long batchSize,
                                     @Name("Whether to create indices on nodes properties to speed-up relationship creation") Boolean index) {

        try {

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;

            BlockingQueue<String> workerQueue = new ArrayBlockingQueue<>((int) (batchSize * 2));

            Map<String, String> propertyTypeMap;

            int rowCount = 0;

            while ((line = br.readLine()) != null) {

                ++rowCount;

                if(rowCount == 1) {

                    if (header == null) {

                        //first row: build property map
                        propertyTypeMap = buildPropertyTypeMap(line);
                        skipFirst = true;

                    } else {

                        propertyTypeMap = buildPropertyTypeMap(header);

                    }

                    log.info("Using Properties map:\n %s", printPropMap(propertyTypeMap));

                    if(index) {

                        log.info("Starting index creation...\n");

                        graphDatabaseAPI.execute(String.format("CREATE INDEX ON :%s(%s)",startLabel,startNodeProp));

                        log.info("Created index :%s(%s)\n",startLabel,startNodeProp);

                        graphDatabaseAPI.execute(String.format("CREATE INDEX ON :%s(%s)",endNLabel,endNodeProp));

                        log.info("Created index :%s(%s)\n",endNLabel,endNodeProp);

                    }

                    spawnRelsBatchWorker(graphDatabaseAPI,
                            workerQueue,
                            batchSize,
                            label,
                            startLabel,
                            endNLabel,
                            startNodeProp,
                            endNodeProp,
                            propertyTypeMap);

                    if(skipFirst) {
                        continue;
                    }
                }

                workerQueue.put(line);

            }

            workerQueue.put(POISON);

            log.info("Finished loading %s lines.",String.valueOf(rowCount));

        } catch (Exception e) {
            log.error("Failed with exception: %s",e.getMessage());
        }

    }

    @Procedure(mode=Mode.SCHEMA)
    public void loadNodesFolder(@Name("Directory path") String dir,
                                @Name("File prefix") String prefix,
                                @Name("Node label") String label,
                                @Name("Properties names and types (e.g. 'name1:type1,name2:type2'...) in the same order as they appear in the file." +
                                        "If this value is null, the first row will be parsed as the header.") String header,
                                @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                                @Name("batch size for a single transaction") long batchSize,
                                @Name("array of property names to index on")List<String> indexedProps) {

        File directory = new File(dir);
        File[] files = directory.listFiles((dir1, name) -> name.startsWith(prefix));
        for (File file :
                files) {
            loadNodesFile(file.getPath(), label, header, skipFirst, batchSize,indexedProps);
        }
    }

    @Procedure(mode=Mode.SCHEMA)
    public void loadRelationshipsFolder(@Name("Directory path") String dir,
                                        @Name("File prefix") String prefix,
                                        @Name("Node label") String label,
                                        @Name("Start node label") String startLabel,
                                        @Name("End node label") String endNLabel,
                                        @Name("Start node property for matching") String startNodeProp,
                                        @Name("End node property for matching") String endNodeProp,
                                        @Name("Properties names and types (e.g. 'name1:type1,name2:type2'...) in the same order as they appear in the file." +
                                                "If this value is null, the first row will be parsed as the header." +
                                                "The start and end constraints must be named start and end.") String header,
                                        @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                                        @Name("batch size for a single transaction") long batchSize,
                                        @Name("Whether to create indices on nodes properties to speed-up relationship creation") Boolean index) {

        File directory = new File(dir);
        File[] files = directory.listFiles((dir1, name) -> name.startsWith(prefix));
        for (File file :
                files) {
            loadRelationshipFile(file.getPath(), label, startLabel, endNLabel, startNodeProp, endNodeProp, header, skipFirst, batchSize, index);
        }

    }

    private Map<String, String> buildPropertyTypeMap(String header) {

        LinkedHashMap<String, String> map = new LinkedHashMap<>();

        for (String token :
                header.split(",")) {
            if (token.contains(":")) {
                String[] split = token.split(":");
                map.put(split[0], split[1]);
            } else {
                map.put(token, "string");
            }
        }

        return map;

    }

    private String printPropMap(Map<String, String> propertiesMap) {
        StringBuilder sb = new StringBuilder();

        propertiesMap.forEach((k,v)-> sb.append(String.format("%s: %s\n",k,v)));

        return sb.toString();
    }

    private void spawnNodesBatchWorker(GraphDatabaseAPI api,
                                       BlockingQueue<String> queue,
                                       long batchSize,
                                       String label,
                                       Map<String, String> propMap) {

        new Thread(() -> {

            Transaction tx = null;
            String nextRow;
            long opsCount = 0;

            try {

                while(!(nextRow = queue.take()).equals(POISON)) {

                    if (opsCount % batchSize == 0) {

                        if (tx!=null) {
                            tx.success();
                            tx.close();
                        }

                        tx = api.beginTx();

                        log.info("Rolling over transaction at ops count " + opsCount);

                    }

                    String[] rowTokens = nextRow.split(",");

                    try {

                        //create the node, and set all properties
                        Node node = api.createNode(Label.label(label));

                        int idx = 0;

                        for (String propName :
                                propMap.keySet()
                                ) {
                            if (propMap.get(propName).equals("int")) {
                                node.setProperty(propName, Integer.valueOf(rowTokens[idx]));
                            } else {
                                node.setProperty(propName, rowTokens[idx]);
                            }
                            idx++;
                        }

                        opsCount++;

                    } catch (Exception ex) {
                        log.warn("Failed parsing row of node type %s. " +
                                "\n\t\trow: %s", label, nextRow);
                    }
                }

            } catch (Exception e) {

                log.error("Failed parsing node type %s: ", label);

            } finally {
                if (tx != null) {
                    tx.success();
                    tx.close();
                }

            }

        }).start();

    }

    private void spawnRelsBatchWorker(GraphDatabaseAPI api,
                                      BlockingQueue<String> queue,
                                      long batchSize,
                                      String label,
                                      String startNodeLabel,
                                      String endNodeLabel,
                                      String startNodeProp,
                                      String endNodeProp,
                                      Map<String, String> propMap) {

        new Thread(() -> {

            Transaction tx = null;
            String nextRow;
            long opsCount = 0;

            int startMatchPropCol = -1;
            int endMatchPropCol = -1;

            for (int i=0; i<propMap.size(); i++) {
                if (propMap.keySet().toArray()[i].equals("start")) {
                    startMatchPropCol = i;
                } else if (propMap.keySet().toArray()[i].equals("end")) {
                    endMatchPropCol = i;
                }
            }

            if(startMatchPropCol < 0 || endMatchPropCol < 0) {
                log.error("Start or End definitions are missing. aborting...");
                return;
            }

            try {

                while(!(nextRow = queue.take()).equals(POISON)) {

                    if (opsCount % batchSize == 0) {

                        if (tx!=null) {
                            tx.success();
                            tx.close();
                        }

                        tx = api.beginTx();

                        log.info("Rolling over transaction at ops count " + opsCount);

                    }

                    String[] rowTokens = nextRow.split(",");

                    try {

                        //Find endpoints and create the relationship
                        Node startNode = api.findNode(Label.label(startNodeLabel), // node label
                                                     startNodeProp, // the relevant property name
                                                     propMap.get("start").equals("int") ? // the property value that identifies the specific node
                                                                Integer.valueOf(rowTokens[startMatchPropCol]) :
                                                                rowTokens[startMatchPropCol]);

                        if (startNode == null) {
                            log.warn("Failed creating relationship. Start node [:%s {%s=%s}] could not be found.",
                                    startNodeLabel,
                                    startNodeProp,
                                    rowTokens[startMatchPropCol]);
                            continue;
                        }

                        Node endNode = api.findNode(Label.label(endNodeLabel),
                                                    endNodeProp,
                                                    propMap.get("end").equals("int") ?
                                                                Integer.valueOf(rowTokens[startMatchPropCol]) :
                                                                rowTokens[endMatchPropCol]);

                        if (endNode == null) {
                            log.warn("Failed creating relationship. Start node [:%s {%s=%s}] could not be found.",
                                    endNodeLabel,
                                    endNodeProp,
                                    rowTokens[endMatchPropCol]);
                            continue;
                        }

                        Relationship rel = startNode.createRelationshipTo(endNode, RelationshipType.withName(label));

                        int idx = 0;
                        for (String propName :
                                propMap.keySet()
                                ) {
                            if (!propName.equals("start") && !propName.equals("end")) {
                                if (propMap.get(propName).equals("int")) {
                                    rel.setProperty(propName, Integer.valueOf(rowTokens[idx]));
                                } else {
                                    rel.setProperty(propName, rowTokens[idx]);
                                }
                            }
                            idx++;
                        }

                        opsCount++;
                    }
                    catch (MultipleFoundException mfe) {
                        log.warn("Failed parsing row of relationship type %s. " +
                                 "\n\t\tMultiple nodes found for relationship endpoints." +
                                 "\n\t\trow: %s", label, nextRow);
                    }
                }

            } catch (Exception e) {

                log.error("Failed parsing relationship type %s: ", label);

            } finally {
                if (tx != null) {
                    tx.success();
                    tx.close();
                }

            }

        }).start();

    }
}
