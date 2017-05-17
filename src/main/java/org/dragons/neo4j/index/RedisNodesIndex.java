package org.dragons.neo4j.index;

import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;

/**
 * Created by Elad on 5/10/2017.
 */
public class RedisNodesIndex implements NodesIndexAPI {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;
    private final String COMBINED_KEY_FORMAT = "%s_%s";

    public RedisNodesIndex() {
        redisClient = RedisClient.create(RedisURI.create("redis://127.0.0.1:6379"));
        redisConnection = redisClient.connect();
    }

    @Override
    public void addNodeToIndex(String label, Object idPropertyValue, long id) {
        if(redisConnection.isOpen()) {
            redisConnection.sync().set(getCombinedKey(label, idPropertyValue), getValue(idPropertyValue));
        }
    }

    @Override
    public long getNodeId(String label, Object idPropertyValue) {
        if(!redisConnection.isOpen()) {
            return -1;
        }
        String nodeId = redisConnection.sync().get(getCombinedKey(label, idPropertyValue));
        if(nodeId != null) {
            return Long.parseLong(nodeId);
        }
        return -1;
    }

    @Override
    public void persist() {
        //TODO: wait for all data to flush
    }

    private String getValue(Object value) {
        return value.toString();
    }

    private String getCombinedKey(String label, Object idPropertyValue) {
        return String.format(COMBINED_KEY_FORMAT,label,idPropertyValue.toString());
    }

    public boolean isOpen() {
        return redisConnection.isOpen();
    }

    public void close() {
        redisConnection.close();
        redisClient.shutdown();
    }
}
