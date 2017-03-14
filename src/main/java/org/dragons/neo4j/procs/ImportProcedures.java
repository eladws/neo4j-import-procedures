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

    @Procedure(mode = Mode.WRITE)
    public void loadNodesFile(@Name("File path") String file,
                              @Name("Node label") String label,
                              @Name("Properties names and types in the same order as they appear in the file." +
                                    "If this value is null, the first row will be parsed as the header.") Map<String, String> propertiesMap,
                              @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                              @Name("batch size for a single transaction") long batchSize) {

        try {

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;

            BlockingQueue<String[]> workerQueue = new ArrayBlockingQueue<>((int) (batchSize * 2));

            while ((line = br.readLine()) != null) {

                if (propertiesMap == null) {

                    //first row: build property map, and spawn a worker
                    propertiesMap = buildPropertyTypeMap(line.split(","));

                    spawnNodesBatchWorker(graphDatabaseAPI, workerQueue, batchSize, label, propertiesMap);

                    continue;

                } else {
                    if (skipFirst) {
                        skipFirst = false;
                        continue;
                    }
                }

                workerQueue.put(line.split(","));

            }

            workerQueue.put(new String[]{POISON});

        } catch (Exception e) {

        }

    }

    @Procedure(mode = Mode.WRITE)
    public void loadRelationshipFile(@Name("File path") String file,
                                     @Name("Relationship label") String label,
                                     @Name("Start node label") String startLabel,
                                     @Name("End node label") String endNLabel,
                                     @Name("Start node match property") String startMatchProp,
                                     @Name("End node match property") String endMatchProp,
                                     @Name("Start node match property column index in the file or the property map") long startMatchPropColIdx,
                                     @Name("End node match property column index in the file or the property map") long endMatchPropColIdx,
                                     @Name("Properties names and types in the same order as they appear in the file." +
                                           "If this value is null, the first row will be parsed as the header.") Map<String, String> propertiesMap,
                                     @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                                     @Name("batch size for a single transaction") long batchSize) {

        try {

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;

            BlockingQueue<String[]> workerQueue = new ArrayBlockingQueue<>((int) (batchSize * 2));

            while ((line = br.readLine()) != null) {

                if (propertiesMap == null) {

                    //first row: build property map, and spawn a worker
                    propertiesMap = buildPropertyTypeMap(line.split(","));

                    spawnRelsBatchWorker(graphDatabaseAPI,
                                            workerQueue,
                                            batchSize,
                                            label,
                                            startLabel,
                                            endNLabel,
                                            startMatchProp,
                                            endMatchProp,
                                            (int)startMatchPropColIdx,
                                            (int)endMatchPropColIdx,
                                            propertiesMap);

                    continue;

                } else {
                    if (skipFirst) {
                        skipFirst = false;
                        continue;
                    }
                }

                workerQueue.put(line.split(","));

            }

            workerQueue.put(new String[]{POISON});

        } catch (Exception e) {

        }

    }

    @Procedure(mode = Mode.WRITE)
    public void importDataDirectory(@Name("data directory") String dataDir,
                                    @Name("batch size for a single transaction") long batchSize) {

        log.info("Starting CSV import process. from: %s, starting at: %d, number of rows: %d, batch size: %d", dataDir, batchSize);

        File dir = new File(dataDir);

        if (!dir.exists()) {
            log.error("CSV import failed. Could not find data directory: %s", dataDir);
            return;
        }

        //1. name of nodes files must start with [node]_[label]
        //2. name of relationship files must start with [rel]_[label]_[fromLabel]_[toLabel]_[matchOnPropertyFrom]_[matchOnPropertyTo]

        String[] nodeFiles = dir.list((dir1, name) -> name.startsWith("node"));

        String[] relFiles = dir.list((dir12, name) -> name.startsWith("rel"));

        for (String nodeFile :
                nodeFiles) {

            try {

                //extract node label
                String[] details = nodeFile.split("_");

                String label = details[1].replace(".csv","");

                loadNodesFile(dataDir + "/" + nodeFile, label, null, false,batchSize);

            } catch (Exception e) {
                log.error("Error while parsing node file %s: %s",nodeFile, e.getMessage());
            }

        }

        for (String relFile :
                relFiles) {
            //TODO: spawn a new worker to enter these relationships
        }

    }

    private Map<String, String> buildPropertyTypeMap(String[] header) {

        LinkedHashMap<String, String> map = new LinkedHashMap<>();

        for (String token :
                header) {
            if (token.contains(":")) {
                String[] split = token.split(":");
                map.put(split[0], split[1]);
            } else {
                map.put(token, "string");
            }
        }

        return map;

    }

    private void spawnNodesBatchWorker(GraphDatabaseAPI api, BlockingQueue<String[]> queue, long batchSize, String label, Map<String, String> propMap) {

        new Thread(() -> {

            Transaction tx = null;
            String[] nextRow;
            long opsCount = 0;

            try {

                while(!(nextRow = queue.take())[0].equals(POISON)) {

                    if (opsCount % batchSize == 0) {

                        if (tx!=null) {
                            tx.success();
                            tx.close();
                        }

                        tx = api.beginTx();

                        log.warn("Rolling over transaction at opscount " + opsCount);

                    }

                    //create the node, and set all properties
                    Node node = api.createNode(Label.label(label));
                    int idx = 0;
                    for (String propName :
                            propMap.keySet()
                            ) {
                        if(propMap.get(propName).equals("int")) {
                            node.setProperty(propName, Integer.valueOf(nextRow[idx]));
                        } else {
                            node.setProperty(propName, nextRow[idx]);
                        }
                        idx++;
                    }

                    opsCount ++;
                }

            } catch (InterruptedException e) {

                log.error("Failed parsing row for node type %s: ", label);

            } finally {
                if (tx != null) {
                    tx.success();
                    tx.close();
                }

            }

        }).start();

    }

    private void spawnRelsBatchWorker(GraphDatabaseAPI api,
                                      BlockingQueue<String[]> queue,
                                      long batchSize,
                                      String label,
                                      String startNodeLabel,
                                      String endNodeLabel,
                                      String startMatchProp,
                                      String endMatchProp,
                                      int startMatchPropCol,
                                      int endMatchPropCol,
                                      Map<String, String> propMap) {

        new Thread(() -> {

            Transaction tx = null;
            String[] nextRow;
            long opsCount = 0;

            try {

                Object[] propsArr = propMap.keySet().toArray();

                while(!(nextRow = queue.take())[0].equals(POISON)) {

                    if (opsCount % batchSize == 0) {

                        if (tx!=null) {
                            tx.success();
                            tx.close();
                        }

                        tx = api.beginTx();

                        log.warn("Rolling over transaction at opscount ",opsCount);

                    }

                    //Find endpoints and create the relationship
                    Node startNode = api.findNode(Label.label(startNodeLabel),
                                                    startMatchProp,
                                                    propMap.get(propsArr[startMatchPropCol]).equals("int") ?
                                                                        Integer.valueOf(nextRow[startMatchPropCol]) :
                                                                        nextRow[startMatchPropCol]);

                    if(startNode == null) {
                        log.warn("Failed creating relationship. Start node [:%s {%s=%s}] could not be found.",
                                startNodeLabel,startMatchProp,nextRow[startMatchPropCol]);
                        continue;
                    }

                    Node endNode = api.findNode(Label.label(endNodeLabel), endMatchProp,
                                                propMap.get(propsArr[endMatchPropCol]).equals("int") ?
                                                        Integer.valueOf(nextRow[startMatchPropCol]) :
                                                        nextRow[endMatchPropCol]);

                    if(endNode == null) {
                        log.warn("Failed creating relationship. Start node [:%s {%s=%s}] could not be found.",
                                endNodeLabel,endMatchProp,nextRow[endMatchPropCol]);
                        continue;
                    }

                    Relationship rel = startNode.createRelationshipTo(endNode, RelationshipType.withName(label));
                    int idx = 0;
                    for (String propName :
                            propMap.keySet()
                            ) {
                        if(idx != startMatchPropCol && idx != endMatchPropCol) {
                            if (propMap.get(propName).equals("int")) {
                                rel.setProperty(propName, Integer.valueOf(nextRow[idx]));
                            } else {
                                rel.setProperty(propName, nextRow[idx]);
                            }
                        }
                        idx++;
                    }

                    opsCount ++;
                }

            } catch (InterruptedException e) {

                log.error("Failed parsing row for relationship type %s: ", label);

            } finally {
                if (tx != null) {
                    tx.success();
                    tx.close();
                }

            }

        }).start();

    }
}
