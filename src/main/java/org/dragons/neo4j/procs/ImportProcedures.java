package org.dragons.neo4j.procs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tools.ant.DirectoryScanner;
import org.dragons.neo4j.utils.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by eladw on 09/03/2017.
 */
public class ImportProcedures {

    private static int totalNodesCount = 0;
    private static int totalEdgesCount = 0;
    private static NodesIndex nodesIndex;

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
                              @Name("Batch size for a single transaction") long batchSize,
                              @Name("Array of property names to create indexes on") List<String> indexedProps) {

        if(indexedProps != null) {
            // Causes deadlocks in Neo4j. Uncomment when solved.
            log.info("ImportProcedures: Creating indices is not supported in this version. Create the indices manually before starting the import.");
            // createIndexes(label, indexedProps);
        }

        NodeImportConfig importConf = new NodeImportConfig();
        importConf.label = label;
        importConf.skipFirst = skipFirst;
        importConf.indexedProps = indexedProps;
        importConf.header = header;

        batchLoadNodes(file, importConf, (int) batchSize);

    }


    @Procedure(mode = Mode.SCHEMA)
    public void loadRelationshipsFile(@Name("File path") String file,
                                      @Name("Relationship label") String label,
                                      @Name("Start node label") String startLabel,
                                      @Name("End node label") String endNLabel,
                                      @Name("Start node property for matching (must be unique!)") String startNodeProp,
                                      @Name("End node property for matching (must be unique!)") String endNodeProp,
                                      @Name("Properties names and types (e.g. 'name1:type1,name2:type2'...) in the same order as they appear in the file." +
                                              "If this value is null, the first row will be parsed as the header." +
                                              "The properties identifying the start and end nodes must be named 'start' and 'end' exactly.") String header,
                                      @Name("Whether the first line of the file should be ignored") Boolean skipFirst,
                                      @Name("batch size for a single transaction") long batchSize) {

        RelationshipImportConfig importConf = new RelationshipImportConfig();
        importConf.label = label;
        importConf.skipFirst = skipFirst;
        importConf.startNodeLabel = startLabel;
        importConf.endNodeLabel = endNLabel;
        importConf.startNodeMatchPropName = startNodeProp;
        importConf.endNodeMatchPropName = endNodeProp;
        importConf.header = header;

        batchLoadRelationships(file, importConf, (int) batchSize);

    }

    @Procedure(mode = Mode.SCHEMA)
    public void loadWithConfiguration(@Name("Configuration file") String configFilePath,
                                      @Name("Batch size for a single transaction") long batchSize) {

        long startTime = System.nanoTime();

        try {

            List<Thread> nodesThreads = new ArrayList<>();

            byte[] jsonData = Files.readAllBytes(Paths.get(configFilePath));

            ObjectMapper om = new ObjectMapper();

            ImportConfig importConfig = om.readValue(jsonData, ImportConfig.class);

            if(importConfig.indexNodeIds) {
                nodesIndex = new NodesIndex();
            }

            for (NodeImportConfig nic : importConfig.nodes) {

                if(importConfig.nodesParallelLevel.equals("group")) {
                    //spawn new thread for this nodes group. files inside this group will be imported sequentially.
                   Thread groupThread = new Thread(() -> loadNodesGroup(nic, false, (int) batchSize, nodesThreads));
                   groupThread.start();
                   nodesThreads.add(groupThread);
                } else {
                    //groups will be ran sequentially.
                    //parallelism inside each group will occur if the configuration does not specify "none".
                    boolean isParallel = importConfig.nodesParallelLevel.equals("in-group") ||
                                         importConfig.nodesParallelLevel.equals("all");

                    loadNodesGroup(nic, isParallel, (int) batchSize, nodesThreads);

                    if(!importConfig.nodesParallelLevel.equals("all")) {
                        //wait for threads of this group to terminate before we continue to the next group
                        for(Thread thread : nodesThreads) {
                            thread.join();
                        }
                        nodesThreads.clear();
                    }
                }

            }

            //Wait for all nodes to complete before starting edges import
            for(Thread thread : nodesThreads) {
                thread.join();
            }

            log.info("Finished importing nodes: %d nodes (approx.) were successfully imported in %d ms.",totalNodesCount, (int)((System.nanoTime() - startTime) / 1000000));

            log.info("Starting relationships import...");

            long edgesStartTime = System.nanoTime();

            List<Thread> relsThreads = new ArrayList<>();

            for (RelationshipImportConfig ric : importConfig.relationships) {

                if(importConfig.relsParallelLevel.equals("group")) {
                    //spawn new thread for this nodes group. files inside this group will be imported sequentially.
                    Thread groupThread = new Thread(() -> loadRelsGroup(ric, false, (int) batchSize, relsThreads));
                    groupThread.start();
                    relsThreads.add(groupThread);
                } else {
                    //groups will be ran sequentially.
                    //parallelism inside each group will occur if the configuration does not specify "none".
                    boolean isParallel = importConfig.relsParallelLevel.equals("in-group") ||
                                         importConfig.relsParallelLevel.equals("all");
                    loadRelsGroup(ric, isParallel, (int) batchSize, relsThreads);

                    if(!importConfig.relsParallelLevel.equals("all")) {
                        //wait for threads of this group to terminate before we continue to the next group
                        for(Thread thread : relsThreads) {
                            thread.join();
                        }
                        relsThreads.clear();
                    }
                }
            }

            //Wait for all threads to complete
            for(Thread thread : relsThreads) {
                thread.join();
            }

            log.info("Finished importing edges: %d edges (approx.) were successfully imported in %d ms.",totalEdgesCount, (int)((System.nanoTime() - edgesStartTime) / 1000000));

        } catch (Exception e) {
            log.error("Failed importing with configuration file " + configFilePath);
            log.error("Failed with exception %s: %s", e, e.getMessage());
        }

        long endTime = System.nanoTime();
        long totalTime = (endTime - startTime) / 1000000;
        log.info("Import summary: %d nodes, %d edges, total time: %d ms.", totalNodesCount, totalEdgesCount, totalTime);
    }

    private void loadRelsGroup(RelationshipImportConfig ric, boolean isParallel, int batchSize, List<Thread> relsThreads) {

        String[] files = getMatchingFiles(ric.rootDir, ric.namePattern);

        log.info("Starting to load %d files...",files.length);

        Arrays.stream(files).forEach(f -> log.info("Will load file %s", Paths.get(ric.rootDir, f).toString()));

        for (String file :
                files) {

            if (isParallel) {
                Thread fileThread = new Thread(() -> batchLoadRelationships(Paths.get(ric.rootDir, file).toString(), ric, batchSize));
                fileThread.start();
                relsThreads.add(fileThread);
            } else {
                batchLoadRelationships(Paths.get(ric.rootDir, file).toString(), ric, batchSize);
            }
        }
    }

    private void loadNodesGroup(NodeImportConfig nic, boolean isParallel, int batchSize, List<Thread> nodesThreads) {

        if(nic.indexedProps != null) {
            // Causes deadlocks in Neo4j. Uncomment when solved.
            log.info("ImportProcedures: Creating indices is not supported in this version. Create the indices manually before starting the import.");
            // createIndexes(nic.label, nic.indexedProps);
        }

        String[] files = getMatchingFiles(nic.rootDir, nic.namePattern);

        log.info("Starting to load %d files...",files.length);

        Arrays.stream(files).forEach(f -> log.info("Will load file %s", Paths.get(nic.rootDir, f).toString()));

        for (String file :
                files) {
            if (isParallel) {
                Thread fileThread = new Thread(() -> batchLoadNodes(Paths.get(nic.rootDir, file).toString(), nic, batchSize));
                fileThread.start();
                nodesThreads.add(fileThread);
            } else {
                batchLoadNodes(Paths.get(nic.rootDir, file).toString(), nic, batchSize);
            }
        }
    }

    private void createIndexes(String label, List<String> props) {
        props.forEach(propName -> {
            graphDatabaseAPI.execute(String.format("CREATE INDEX ON :%s(%S)", label, propName));
            log.info("Created index: %s(%s)", label, propName);
        });
    }

    private void batchLoadNodes(String file, NodeImportConfig nodesImportConfig, int batchSize) {
        GraphBatchWorkConfig workConfig = new NodeBatchWorkConfig();
        workConfig.setBaseImportConfig(nodesImportConfig);
        workConfig.setBatchSize(batchSize);
        workConfig.setGraphDatabaseAPI(graphDatabaseAPI);
        workConfig.setLog(log);
        workConfig.setNodesIndex(nodesIndex);
        batchLoadWithConfig(file, workConfig);
    }

    private void batchLoadRelationships(String file, RelationshipImportConfig relsImportConfig, int batchSize) {
        GraphBatchWorkConfig workConfig = new RelationshipBatchWorkConfig();
        workConfig.setBaseImportConfig(relsImportConfig);
        workConfig.setBatchSize(batchSize);
        workConfig.setGraphDatabaseAPI(graphDatabaseAPI);
        workConfig.setLog(log);
        workConfig.setNodesIndex(nodesIndex);
        batchLoadWithConfig(file, workConfig);
    }

    private void batchLoadWithConfig(String file, GraphBatchWorkConfig config) {

        String line;
        int rowCount = 0;
        int opsCount = 0;
        Transaction tx = null;
        WorkFunctions wf = new WorkFunctions();

        log.info("Importing elements of type %s from file %s started.", config.getBaseImportConfig().label, file);

        try {

            BufferedReader br = new BufferedReader(new FileReader(file));

            while ((line = br.readLine()) != null) {

                ++rowCount;

                if (rowCount == 1) {

                    //first row: parse the header
                    if (config.getBaseImportConfig().header == null) {

                        //if there is no header supplied, the first row must be parsed as a header
                        config.setPropertiesMap(buildPropertyTypeMap(line));
                        config.getBaseImportConfig().skipFirst = true;

                    } else {

                        //build property map based on the given header
                        config.setPropertiesMap(buildPropertyTypeMap(config.getBaseImportConfig().header));
                    }

                    if (config.getBaseImportConfig().skipFirst) {
                        continue;
                    }

                }

                if (opsCount % config.getBatchSize() == 0) {

                    if (tx != null) {
                        tx.success();
                        tx.close();
                    }

                    tx = graphDatabaseAPI.beginTx();

                }

                try {

                    //apply function to current element
                    WorkFunctions.FunctionResult result = wf.getFunction(config.getClass()).apply(line, config);

                    if(result == WorkFunctions.FunctionResult.SUCCESS) {
                        opsCount++;
                        if(config.getBaseImportConfig().getClass().equals(NodeImportConfig.class)) {
                            totalNodesCount++;
                        } else {
                            totalEdgesCount++;
                        }
                        if (opsCount % 1000000 == 0) {
                            log.info("Loaded %d elements of type %s from file %s.", opsCount, config.getBaseImportConfig().label, file);
                            log.info("Total count (approx.): %d nodes, %d edges.", totalNodesCount, totalEdgesCount);
                        }
                    }

                } catch (Exception ex) {
                    log.warn("Exception in file: %s%nFailed processing %s record: %s%nException: %s%n%s",
                            file,
                            config.getBaseImportConfig().label,
                            line,
                            ex,
                            ex.getMessage());
                }

            }

        } catch (Exception e) {
            log.error("Exception in file: %s%nException: %s%n%s", file, e, e.getMessage());
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }
    }

    private String[] getMatchingFiles(String baseDir, String pattern) {
        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setBasedir(baseDir);
        scanner.setCaseSensitive(false);
        scanner.setIncludes(new String[]{pattern});
        scanner.scan();
        return scanner.getIncludedFiles();
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

}
