package com.sage.flink.util;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.json.JSONObject;

import java.util.List;

public class RowToJsonConverter {

    private final String[] fieldNames;

    public RowToJsonConverter(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public JSONObject convert(Row row) {
        JSONObject json = new JSONObject();

        for (int i = 0; i < row.getArity(); i++) {
            String key = (fieldNames != null && i < fieldNames.length)
                    ? fieldNames[i]
                    : "col" + i;

            Object value = row.getField(i);
            json.put(key, value);
        }

        return json;
    }

    /**
     * Creates a converter from a Flink Table's resolved schema.
     */
    public static RowToJsonConverter fromTable(Table table) {
        List<String> names = table.getResolvedSchema().getColumnNames();
        return new RowToJsonConverter(names.toArray(new String[0]));
    }

    /**
     * Fallback for unknown schema (e.g., dynamic query results).
     */
    public static RowToJsonConverter unnamed() {
        return new RowToJsonConverter(null);
    }
}

