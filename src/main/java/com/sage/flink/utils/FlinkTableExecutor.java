package com.sage.flink.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

public class FlinkTableExecutor implements TableExecutor {

    private final StreamTableEnvironment tEnv;

    public FlinkTableExecutor(StreamTableEnvironment tEnv) {
        this.tEnv = tEnv;
    }

    @Override
    public Table sqlQuery(String sql) {
        return tEnv.sqlQuery(sql);
    }

    @Override
    public CloseableIterator<Row> collect(Table table) throws IOException {
        try {
            return tEnv.toDataStream(table).executeAndCollect();
        } catch (Exception e) {
            throw new IOException("Failed to collect rows", e);
        }
    }
}
