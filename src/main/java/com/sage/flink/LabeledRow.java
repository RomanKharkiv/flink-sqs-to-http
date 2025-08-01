package com.sage.flink;

import org.apache.flink.types.Row;

public class LabeledRow {
    private Row row;
    private String[] fieldNames;

    public LabeledRow (){}
    public LabeledRow(Row row, String[] fieldNames) {
        this.row = row;
        this.fieldNames = fieldNames;
    }

    public Row getRow() {
        return row;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setRow(Row row) {
        this.row = row;
    }
    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }
}