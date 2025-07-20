package com.sage.flink;

import com.sage.flink.utils.RowToJsonConverter;
import com.sage.flink.utils.TableExecutor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryDispatcher implements FlatMapFunction<String, QueryDispatcher.LabeledRow> {

    private final TableExecutor executor;

    public QueryDispatcher(TableExecutor executor) {
        this.executor = executor;
    }
    public record LabeledRow(Row row, String[] fieldNames) {}


    // Query Type 1 — Tenant Lookup
    private static final Pattern TENANT_LOOKUP_PATTERN = Pattern.compile(
            "SELECT \\* FROM sbca_bronze\\.businesses WHERE tenant_id = '([a-zA-Z0-9]+)'",
            Pattern.CASE_INSENSITIVE
    );

    // Query Type 2 — Recent business activity with filters + limit
    private static final Pattern RECENT_ACTIVITY_PATTERN = Pattern.compile(
            "SELECT .* FROM sbca_bronze\\.businesses WHERE \\(updated_at >= CURRENT_TIMESTAMP - INTERVAL .* DAY .*\\) .*;",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    @Override
    public void flatMap(String message, Collector<LabeledRow> out) throws Exception {
        try {
            JSONObject json = new JSONObject(message);
            String rawQuery = json.getString("query").trim();

            QueryMatch match = matchQueryType(rawQuery);

            if (match == null) {
                System.err.println("Unsupported or unsafe query: " + rawQuery);
                return;
            }

            switch (match.type()) {
                case "tenant_lookup" -> handleTenantLookup(match.matcher(), out);
                case "recent_changes" -> handleRecentBusinessActivity(out);
                default -> System.err.println("No handler for query type: " + match.type());
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

        try (var iterator = executor.collect(result)) {
            iterator.forEachRemaining(row -> out.collect(new LabeledRow(row, fieldNames)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to collect query results", e);
        }
    }


    /**
     * Try to match the query string to a known pattern.
     */
    private QueryMatch matchQueryType(String query) {
        Matcher tenant = TENANT_LOOKUP_PATTERN.matcher(query);
        if (tenant.matches()) return new QueryMatch("tenant_lookup", tenant);

        Matcher recent = RECENT_ACTIVITY_PATTERN.matcher(query);
        if (recent.matches()) return new QueryMatch("recent_changes", recent);

        return null;
    }

    /**
     * Simple record to carry matched query type and its Matcher.
     */
    private record QueryMatch(String type, Matcher matcher) {}
}
