package org.dragons.neo4j.utils;

import org.dragons.neo4j.config.ThreadsExecutionType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Elad on 5/11/2017.
 */
public class ThreadPoolService {

    private Map<ThreadsExecutionType, ExecutorService> executors;
    private int maxThreads;

    public ThreadPoolService(int maxThread) {
        this.maxThreads = maxThread;
        executors = new HashMap<>();
    }

    public ExecutorService getExecutor(ThreadsExecutionType executorType) {
        if(executorType == ThreadsExecutionType.NONE) {
            return null;
        }
        if(!executors.containsKey(executorType)) {
            executors.put(executorType, Executors.newFixedThreadPool(maxThreads));
        }
        return executors.get(executorType);
    }

    public void addTask(ThreadsExecutionType executorType, Runnable task) {
        if(!executors.containsKey(executorType)) {
            executors.put(executorType, Executors.newFixedThreadPool(maxThreads));
        }
        executors.get(executorType).execute(task);
    }

    public void shutdownExecutor(ThreadsExecutionType executorType) throws InterruptedException {
        if(executors.containsKey(executorType)) {
            ExecutorService executor = executors.remove(executorType);
            executor.shutdown();
            executor.awaitTermination(7, TimeUnit.DAYS);
        }
    }
}
