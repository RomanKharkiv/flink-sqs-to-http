package com.sage.flink;

import com.sage.flink.utils.RowToJsonConverter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.json.JSONObject;

public class TenantLookupQuery extends RichMapFunction<String, QueryExecutor.LabeledRow> {

    private final StreamTableEnvironment tEnv;

    public TenantLookupQuery(StreamTableEnvironment tEnv) {
        this.tEnv = tEnv;
    }

    @Override
    public QueryExecutor.LabeledRow map(String jsonStr) throws Exception {
        JSONObject obj = new JSONObject(jsonStr);
        String tenantId = obj.getString("tenantId");

        String query = String.format("SELECT * FROM businesses WHERE tenant_id = '%s' LIMIT 100", tenantId);

        Table result = tEnv.sqlQuery(query);
        String[] fields = RowToJsonConverter.extractFieldNames(result);
        return tEnv.toDataStream(result)
//                map(row -> new QueryExecutor.LabeledRow(row, fields))
                .map(new RowWithFieldNamesMapper(fields))
                .executeAndCollect().next();
    }
}