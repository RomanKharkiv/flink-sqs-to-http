package com.sage.flink.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FlinkTableExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableExecutor.class);
    private final StreamTableEnvironment tEnv;

    public FlinkTableExecutor(StreamTableEnvironment tEnv) {
        this.tEnv = tEnv;
    }

    public Table sqlQuery(String sql) {
        LOG.info("Executing SQL query: {}", sql);
        return tEnv.sqlQuery(sql);
    }

    public DataStream<Row> streamQuery(Table result) {
        LOG.info("Executing streaming SQL query!");
        return tEnv.toDataStream(result);
    }

    // Add this method for synchronous result collection
    public CloseableIterator<Row> collectQuery(Table result) {
        LOG.info("Executing SQL query and collecting results synchronously");
        try {
            return result.execute().collect();
        } catch (Exception e) {
            LOG.error("Error collecting query results: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to collect query results", e);
        }
    }

    // Alternative method that returns a list (for smaller result sets)
    public List<Row> collectQueryAsList(Table result) {
        LOG.info("Executing SQL query and collecting results as list");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.execute().collect()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
        } catch (Exception e) {
            LOG.error("Error collecting query results: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to collect query results", e);
        }
        return rows;
    }
}
