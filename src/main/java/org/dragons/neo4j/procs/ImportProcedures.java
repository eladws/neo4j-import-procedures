package org.dragons.neo4j.procs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tools.ant.DirectoryScanner;
import org.dragons.neo4j.utils.*;
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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by eladw on 09/03/2017.
 */
public class ImportProcedures {

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
                              @Name("array of property names to index on") List<String> indexedProps) {

        if(indexedProps != null) {
            createIndexes(label, indexedProps);
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
                                      @Name("Start node property for matching") String startNodeProp,
                                      @Name("End node property for matching") String endNodeProp,
                                      @Name("Properties names and types (e.g. 'name1:type1,name2:type2'...) in the same order as they appear in the file." +
                                              "If this value is null, the first row will be parsed as the header." +
                                              "The start and end constraints must be named 'start' and 'end' exactly.") String header,
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

        try {

            byte[] jsonData = Files.readAllBytes(Paths.get(configFilePath));

            ObjectMapper om = new ObjectMapper();

            ImportConfig importConfig = om.readValue(jsonData, ImportConfig.class);

            for (NodeImportConfig nic : importConfig.nodes) {

                if(nic.indexedProps != null) {
                    createIndexes(nic.label, nic.indexedProps);
                }

                String[] files = getMatchingFiles(nic.rootDir, nic.namePattern);

                log.info("Starting to load %d files...",files.length);

                Arrays.stream(files).forEach(f -> log.info("Will load file %s",Paths.get(nic.rootDir, f).toString()));

                for (String file :
                        files) {
                    if (importConfig.parallelLevel.equals("nodes")) {
                        new Thread(() -> batchLoadNodes(Paths.get(nic.rootDir, file).toString(), nic, (int) batchSize)).start();
                    } else {
                        batchLoadNodes(Paths.get(nic.rootDir, file).toString(), nic, (int) batchSize);
                    }
                }
            }

            //TODO: If parallelism is only at element level - wait for nodes to finish...
            for (RelationshipImportConfig ric : importConfig.relationships) {

                String[] files = getMatchingFiles(ric.rootDir, ric.namePattern);

                log.info("Starting to load %d files...",files.length);

                Arrays.stream(files).forEach(f -> log.info("Will load file %s",Paths.get(ric.rootDir, f).toString()));

                for (String file :
                        files) {

                    if (importConfig.parallelLevel.equals("rels")) {
                        new Thread(() -> batchLoadRelationships(Paths.get(ric.rootDir, file).toString(), ric, (int) batchSize)).start();
                    } else {
                        batchLoadRelationships(Paths.get(ric.rootDir, file).toString(), ric, (int) batchSize);
                    }
                }
            }

        } catch (Exception e) {
            log.error("Failed importing with configuration file " + configFilePath);
            log.error("Failed with exception %s: %s", e, e.getMessage());
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

        try {

            log.info("Importing elements of type %s from file %s started.", config.getBaseImportConfig().label, file);

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line;

            BlockingQueue<String> workerQueue = new ArrayBlockingQueue<>(config.getBatchSize() * 2);

            config.setBlockingQueue(workerQueue);

            int rowCount = 0;

            while ((line = br.readLine()) != null) {

                ++rowCount;

                if (rowCount == 1) {

                    //first row: parse header and start the queue

                    if (config.getBaseImportConfig().header == null) {

                        //if there is no header supplied, the first row must be parsed as a header
                        config.setPropertiesMap(buildPropertyTypeMap(line));
                        config.getBaseImportConfig().skipFirst = true;

                    } else {

                        //build property mao based on the given header
                        config.setPropertiesMap(buildPropertyTypeMap(config.getBaseImportConfig().header));
                    }

                    new Thread(new GraphBatchWorker(config)).start();

                    if (config.getBaseImportConfig().skipFirst) {
                        continue;
                    }

                }

                workerQueue.put(line);

                if (rowCount % 1000000 == 0) {
                    log.info("Processed %d elements of type %s", rowCount, config.getBaseImportConfig().label);
                }

            }

            workerQueue.put(GraphBatchWorker.BATCH_WORKER_POISON);

            log.info("Finished loading %d lines from file %s.", rowCount, file);

        } catch (Exception e) {

            log.error("Failed with exception %s: %s", e, e.getMessage());

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
