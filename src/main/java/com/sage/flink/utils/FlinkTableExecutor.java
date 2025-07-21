package com.sage.flink.utils;

import com.sage.flink.FlinkJob;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlinkTableExecutor implements TableExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableExecutor.class);

    private final StreamTableEnvironment tEnv;

    public FlinkTableExecutor(StreamTableEnvironment tEnv) {
        this.tEnv = tEnv;
    }

    @Override
    public Table sqlQuery(String sql) {
        LOG.info("FlinkTableExecutor attempt to execute query - {}", sql);
        return tEnv.sqlQuery(sql);
    }

    @Override
    public CloseableIterator<Row> collect(Table table) throws IOException {

        try {
            LOG.info("FlinkTableExecutor result - {}", table);
            return tEnv.toDataStream(table).executeAndCollect();
        } catch (Exception e) {
            throw new IOException("Failed to collect rows", e);
        }
    }
}
