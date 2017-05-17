package org.dragons.neo4j.procs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tools.ant.DirectoryScanner;
import org.dragons.neo4j.config.*;
import org.dragons.neo4j.index.*;
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
import java.util.concurrent.ExecutorService;

/**
 * Created by eladw on 09/03/2017.
 */
public class ImportProcedures {

    private static long totalNodesCount = 0;
    private static long totalEdgesCount = 0;
    private static long globalStartTime = 0;
    private static long edgesStartTime = 0;

    @SuppressWarnings("WeakerAccess")
    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @SuppressWarnings("WeakerAccess")
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

        globalStartTime = System.nanoTime();

        try {

            byte[] jsonData = Files.readAllBytes(Paths.get(configFilePath));

            ObjectMapper om = new ObjectMapper();

            ImportConfig importConfig = om.readValue(jsonData, ImportConfig.class);

            int maxThreads = importConfig.maxThreads == 0 ? (Runtime.getRuntime().availableProcessors() / 2) : importConfig.maxThreads;

            log.info("Max threads number set to: %d", maxThreads);

            ThreadsExecutionType nodesExecutionType = getExecutionType(importConfig.nodesParallelLevel);

            //initialize nodes index
            if(importConfig.nodeIdsCache != null) {
                try {
                     NodesIndexMngr.initNodesIndex(importConfig.nodeIdsCache);
                } catch (Exception e) {
                    log.warn("Failed initializing cache of type %s.%n%s%n%s%n%s",
                                                    importConfig.nodeIdsCache,
                                                    e,
                                                    e.getMessage(),
                                                    e.getStackTrace());
                }
            }

            ThreadPoolService nodesThreadsPool = new ThreadPoolService(maxThreads);

            for (NodeImportConfig nic : importConfig.nodes) {

                if (nodesExecutionType == ThreadsExecutionType.GROUP) {
                    //spawn new thread for this nodes group. files inside this group will be imported sequentially.
                    nodesThreadsPool.addTask(ThreadsExecutionType.GROUP, () -> loadNodesGroup(nic, (int) batchSize, null));
                } else {
                    //groups will be ran sequentially.
                    //parallelism inside each group will occur if the configuration does not specify "none".
                    loadNodesGroup(nic, (int) batchSize, nodesThreadsPool.getExecutor(nodesExecutionType));

                    //if the execution type is in-group, wait for all threads to complete
                    if (nodesExecutionType == ThreadsExecutionType.IN_GROUP) {
                        nodesThreadsPool.shutdownExecutor(ThreadsExecutionType.IN_GROUP);
                    }
                }
            }

            //wait for all nodes threads to finish
            nodesThreadsPool.shutdownExecutor(nodesExecutionType);

            log.info("Finished importing nodes: %d nodes (approx.) were successfully imported in %d ms.",totalNodesCount, getElapsedTimeSeconds());

            if(NodesIndexMngr.getNodesIndex() != null) {
                log.info("Flushing nodes index...");
                try {
                    NodesIndexMngr.getNodesIndex().persist();
                    log.info("Flushing nodes index completed successfully.");
                } catch (Exception e) {
                    log.info("Flushing nodes index failed with exception %s%n%s%n%s%n",e,e.getMessage(),e.getStackTrace());
                }

            }

            log.info("Starting relationships import...");

            edgesStartTime = System.nanoTime();

            ThreadsExecutionType relsExecutionType = getExecutionType(importConfig.relsParallelLevel);

            ThreadPoolService relsThreadsPool = new ThreadPoolService(maxThreads);

            for (RelationshipImportConfig ric : importConfig.relationships) {

                if(relsExecutionType == ThreadsExecutionType.GROUP) {
                    //spawn new thread for this nodes group. files inside this group will be imported sequentially.
                    relsThreadsPool.addTask(ThreadsExecutionType.GROUP,() -> loadRelsGroup(ric, (int) batchSize, null));
                } else {
                    //groups will be ran sequentially.
                    //parallelism inside each group will occur if the configuration does not specify "none".
                    loadRelsGroup(ric, (int) batchSize, relsThreadsPool.getExecutor(relsExecutionType));

                    if(relsExecutionType == ThreadsExecutionType.IN_GROUP) {
                        //if the execution type is in-group, wait for all threads to complete
                        relsThreadsPool.shutdownExecutor(relsExecutionType);
                    }
                }
            }

            //Wait for all threads to complete
            relsThreadsPool.shutdownExecutor(relsExecutionType);

            log.info("Finished importing edges: %d edges (approx.) were successfully imported in %d ms.",totalEdgesCount, getEdgesElapsedTimeSeconds());

        } catch (Exception e) {
            log.warn("Failed importing with configuration file " + configFilePath);
            log.warn("Failed with exception %s: %s%n%s",
                                                        e,
                                                        e.getMessage(),
                                                        Arrays.toString(e.getStackTrace()));
        }

        log.info("Import summary: %d nodes, %d edges, total time: %d ms.", totalNodesCount, totalEdgesCount, getElapsedTimeSeconds());
        log.info("Nodes import rate: %d nodes per second", getNodesRate());
        log.info("Edges import rate: %d edges per second", getEdgesRate());
    }

    private void loadRelsGroup(RelationshipImportConfig ric, int batchSize, ExecutorService executor) {

        String[] files = getMatchingFiles(ric.rootDir, ric.namePattern);

        log.info("Starting to load %d files...",files.length);

        Arrays.stream(files).forEach(f -> log.info("Will load file %s", Paths.get(ric.rootDir, f).toString()));

        for (String file :
                files) {

            if (executor != null) {
                executor.execute(() -> batchLoadRelationships(Paths.get(ric.rootDir, file).toString(), ric, batchSize));
            } else {
                batchLoadRelationships(Paths.get(ric.rootDir, file).toString(), ric, batchSize);
            }
        }
    }

    private void loadNodesGroup(NodeImportConfig nic, int batchSize, ExecutorService executor) {

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
            if (executor != null) {
                executor.execute(() -> batchLoadNodes(Paths.get(nic.rootDir, file).toString(), nic, batchSize));
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
        batchLoadWithConfig(file, workConfig);
    }

    private void batchLoadRelationships(String file, RelationshipImportConfig relsImportConfig, int batchSize) {
        GraphBatchWorkConfig workConfig = new RelationshipBatchWorkConfig();
        workConfig.setBaseImportConfig(relsImportConfig);
        workConfig.setBatchSize(batchSize);
        workConfig.setGraphDatabaseAPI(graphDatabaseAPI);
        workConfig.setLog(log);
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

                    if (result == WorkFunctions.FunctionResult.SUCCESS) {
                        opsCount++;
                        if (config.getBaseImportConfig().getClass().equals(NodeImportConfig.class)) {
                            totalNodesCount++;
                        } else {
                            totalEdgesCount++;
                        }
                        if (opsCount % 10000000 == 0) {
                            log.info("Loaded %d elements of type %s from file %s.", opsCount, config.getBaseImportConfig().label, file);
                            log.info("Total count (approx.): %d nodes, %d edges.", totalNodesCount, totalEdgesCount);
                            log.info("Current rate: %d nodes per second, %d edges per second.",getNodesRate(),getEdgesRate());
                        }
                    }

                } catch (Exception ex) {
                    log.debug("Exception in file: %s%nFailed processing %s record: %s%n: %s%n%s%n%s",
                                file,
                                config.getBaseImportConfig().label,
                                line,
                                ex,
                                ex.getMessage(),
                                Arrays.toString(ex.getStackTrace()));
                }

            }

        } catch (Exception e) {
            log.warn("Exception in file: %s%n: %s%n%s%n%s", file, e, e.getMessage(),
                                                            Arrays.toString(e.getStackTrace()));
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

    private ThreadsExecutionType getExecutionType(String value) {
        switch (value) {
            case "none":
                return ThreadsExecutionType.NONE;
            case "all":
                return ThreadsExecutionType.ALL;
            case "group":
                return ThreadsExecutionType.GROUP;
            case "in-group":
                return ThreadsExecutionType.IN_GROUP;
            default:
                return ThreadsExecutionType.NONE;
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

    private static long getElapsedTimeSeconds() {
        return (System.nanoTime() - globalStartTime) / 1000000000;
    }

    private static long getEdgesElapsedTimeSeconds() {
        if(edgesStartTime == 0) {
            return 0;
        } else {
            return (System.nanoTime() - edgesStartTime) / 1000000000;
        }
    }

    private static long getNodesRate() {
        return totalNodesCount / (getElapsedTimeSeconds() - getEdgesElapsedTimeSeconds());
    }

    private static long getEdgesRate() {
        if (getEdgesElapsedTimeSeconds() == 0) {
            return 0;
        } else {
            return totalEdgesCount / getEdgesElapsedTimeSeconds();
        }
    }
}
