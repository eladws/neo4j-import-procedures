package org.dragons.neo4j.utils;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Elad on 3/28/2017.
 */
public class GraphBatchWorker implements Runnable {

    public static String BATCH_WORKER_POISON = "POISON";
    GraphBatchWorkConfig workConfig;

    public GraphBatchWorker(GraphBatchWorkConfig config) {
        workConfig = config;
    }

    @Override
    public void run() {

        WorkFunctions wf = new WorkFunctions();

        BlockingQueue<String> queue = workConfig.getBlockingQueue();
        GraphDatabaseAPI api = workConfig.getGraphDatabaseAPI();
        Log log = workConfig.getLog();
        int batchSize = workConfig.batchSize;
        Transaction tx = null;
        String nextRow;
        long opsCount = 0;

        try {

            while (!(nextRow = queue.take()).equals(BATCH_WORKER_POISON)) {

                if (opsCount % batchSize == 0) {

                    if (tx != null) {
                        tx.success();
                        tx.close();
                    }

                    tx = api.beginTx();

                }

                try {

                    //apply function to current element
                    WorkFunctions.FunctionResult result = wf.getFunction(workConfig.getClass()).apply(nextRow, workConfig);

                    if(result == WorkFunctions.FunctionResult.SUCCESS) {
                        opsCount++;
                    }

                } catch (Exception ex) {
                    log.warn("Failed processing record: %s", nextRow);
                    log.warn("Failed with exception %s: %s", ex, ex.getMessage());
                }

            }

        } catch (Exception ex) {
            log.warn("Work failed with exception: %s", ex.getMessage());
            log.warn("Work failed for type %s, in %s", workConfig.baseImportConfig.label, workConfig.baseImportConfig.namePattern);
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }

        }

    }

}
