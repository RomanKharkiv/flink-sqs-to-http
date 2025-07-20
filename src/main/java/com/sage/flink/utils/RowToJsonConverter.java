package com.sage.flink.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.json.JSONObject;

import java.util.List;

public final class RowToJsonConverter {

    private RowToJsonConverter() {
    }

    public static JSONObject convert(Row row, String[] fieldNames) {
        JSONObject json = new JSONObject();

        for (int i = 0; i < row.getArity(); i++) {
            String key = (fieldNames != null && i < fieldNames.length && fieldNames[i] != null)
                    ? fieldNames[i]
                    : "col" + i;

            Object value = row.getField(i);
            json.put(key, value);
        }

        return json;
    }

    public static JSONObject convertUnnamed(Row row) {
        return convert(row, null);
    }

    public static String[] extractFieldNames(Table table) {
        List<String> names = table.getResolvedSchema().getColumnNames();
        return names.toArray(new String[0]);
    }
}
