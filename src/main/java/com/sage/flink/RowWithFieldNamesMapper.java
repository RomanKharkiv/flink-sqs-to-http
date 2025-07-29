package com.sage.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class RowWithFieldNamesMapper implements MapFunction<Row, QueryExecutor.LabeledRow> {
    private final String[] fieldNames;

    public RowWithFieldNamesMapper(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public QueryExecutor.LabeledRow map(Row row) {
        return new QueryExecutor.LabeledRow(row, fieldNames);
    }
}
