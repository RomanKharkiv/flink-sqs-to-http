package com.sage.flink.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface TableExecutor {
    Table sqlQuery(String sql);
    CloseableIterator<Row> collect(Table table) throws IOException;
}