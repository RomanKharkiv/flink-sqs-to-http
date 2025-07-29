package com.sage.flink.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public DataStream<Row> streamQuery(String sql) {
        LOG.info("Executing streaming SQL query: {}", sql);
        Table result = sqlQuery(sql);
        return tEnv.toDataStream(result);
    }
}
