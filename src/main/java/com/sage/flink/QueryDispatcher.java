package com.sage.flink;

import com.sage.flink.utils.FlinkTableExecutor;
import com.sage.flink.utils.RowToJsonConverter;
import com.sage.flink.utils.TableExecutor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryDispatcher extends RichFlatMapFunction<String, QueryDispatcher.LabeledRow> {
    private static final Logger LOG = LoggerFactory.getLogger(QueryDispatcher.class);

    private TableExecutor executor;

    public QueryDispatcher() {
        LOG.info("Constructor QueryDispatcher!");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("QueryDispatcher open()!");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        executor = new FlinkTableExecutor(tEnv);
        LOG.info("Created FlinkTableExecutor!");
        super.open(parameters);
    }

    public static class LabeledRow {
        private final Row row;
        private final String[] fieldNames;

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
    }

    private static final Pattern TENANT_LOOKUP_PATTERN = Pattern.compile(
            "SELECT \\* FROM sbca_bronze\\.businesses WHERE tenant_id = '([a-zA-Z0-9]+)'",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern RECENT_ACTIVITY_PATTERN = Pattern.compile(
            "SELECT .* FROM sbca_bronze\\.businesses WHERE \\(updated_at >= CURRENT_TIMESTAMP - INTERVAL .* DAY .*\\) .*;",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    @Override
    public void flatMap(String message, Collector<LabeledRow> out) {
        try {
            LOG.info("QueryDispatcher FlatMap message: {}", message);

            JSONObject json = new JSONObject(message);
            String rawQuery = json.getString("query").trim();

            QueryMatch match = matchQueryType(rawQuery);

            if (match == null) {
                System.err.println("Unsupported or unsafe query: " + rawQuery);
                return;
            }

            String type = match.getType();
            if ("tenant_lookup".equals(type)) {
                handleTenantLookup(match.getMatcher(), out);
            } else if ("recent_changes".equals(type)) {
                handleRecentBusinessActivity(out);
            } else {
                System.err.println("No handler for query type: " + type);
            }

        } catch (Exception e) {
            System.err.println("Query dispatch failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleTenantLookup(Matcher matcher, Collector<LabeledRow> out) {
        String tenantId = matcher.group(1);
        String safeQuery = String.format(
                "SELECT * FROM sbca_bronze.businesses WHERE tenant_id = '%s'",
                tenantId
        );
        try {
            executeQuery(safeQuery, out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleRecentBusinessActivity(Collector<LabeledRow> out) {
        String query =
                "SELECT id, name, updated_at, created_at, website, owner_id, voided_at, " +
                "_airbyte_extracted_at, dms_timestamp, source, tenant_id " +
                "FROM sbca_bronze.businesses " +
                "WHERE (updated_at >= CURRENT_TIMESTAMP - INTERVAL '2' DAY " +
                "OR (website IS NULL OR TRIM(website) = '') OR owner_id IS NULL) " +
                "AND voided_at IS NULL " +
                "ORDER BY updated_at DESC " +
                "LIMIT 100";
        try {
            executeQuery(query, out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void executeQuery(String query, Collector<LabeledRow> out) throws Exception {
        Table result = executor.sqlQuery(query);
        String[] fieldNames = RowToJsonConverter.extractFieldNames(result);

        try (AutoCloseableIterator<Row> iterator = new AutoCloseableIterator<>(executor.collect(result))) {
            while (iterator.hasNext()) {
                out.collect(new LabeledRow(iterator.next(), fieldNames));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to collect query results", e);
        }
    }

    private QueryMatch matchQueryType(String query) {
        Matcher tenant = TENANT_LOOKUP_PATTERN.matcher(query);
        if (tenant.matches()) return new QueryMatch("tenant_lookup", tenant);

        Matcher recent = RECENT_ACTIVITY_PATTERN.matcher(query);
        if (recent.matches()) return new QueryMatch("recent_changes", recent);

        return null;
    }

    private static class QueryMatch {
        private final String type;
        private final Matcher matcher;

        public QueryMatch(String type, Matcher matcher) {
            this.type = type;
            this.matcher = matcher;
        }

        public String getType() {
            return type;
        }

        public Matcher getMatcher() {
            return matcher;
        }
    }

    // Helper to auto-close iterator for Java 11 try-with-resources
    private static class AutoCloseableIterator<T> implements AutoCloseable {
        private final java.util.Iterator<T> iterator;

        public AutoCloseableIterator(java.util.Iterator<T> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public T next() {
            return iterator.next();
        }

        @Override
        public void close() {
            // no-op since the underlying collect() result is not AutoCloseable in Java 11
        }
    }
}
