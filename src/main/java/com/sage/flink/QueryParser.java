package com.sage.flink;

import com.sage.flink.utils.FlinkTableExecutor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.json.JSONObject;

public class QueryParser implements MapFunction<String, Tuple2<String, Table>> {

    private final FlinkTableExecutor executor;

    public QueryParser(FlinkTableExecutor executor) {
        this.executor = executor;
    }

    @Override
    public Tuple2<String, Table> map(String message) {
        JSONObject json = new JSONObject(message);
//        String tenantId = json.getString("tenant_id");
        String rawQuery = json.getString("query").trim();

        Table result = executor.sqlQuery(rawQuery);
        return Tuple2.of("tenantId", result);
    }
}

