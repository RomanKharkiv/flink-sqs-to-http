package com.sage.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.sage.flink.utils.FlinkTableExecutor;
import com.sage.flink.utils.RowToJsonConverter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class QueryExecutor extends RichFlatMapFunction<String, QueryExecutor.LabeledRow> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    private transient FlinkTableExecutor executor;

    private static final Pattern TENANT_LOOKUP_PATTERN = Pattern.compile(
            "SELECT\\s+([^\\s]+|\\*|[^\\s]+(\\s*,\\s*[^\\s]+)*)\\s+FROM\\s+sbca_bronze\\.businesses\\s+WHERE\\s+tenant_id\\s*=\\s*'([a-zA-Z0-9\\-]+)'\\s*" +
            "(?:\\s+ORDER\\s+BY\\s+([^\\s;]+(?:\\s+(?:ASC|DESC))?(?:\\s*,\\s*[^\\s;]+(?:\\s+(?:ASC|DESC))?)*))?\\s*" +
            "(?:\\s+LIMIT\\s+(\\d+))?\\s*;?\\s*",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern RECENT_ACTIVITY_PATTERN = Pattern.compile(
            "SELECT\\s+([^\\s]+|\\*|[^\\s]+(\\s*,\\s*[^\\s]+)*)\\s+FROM sbca_bronze\\.businesses WHERE \\(updated_at >= CURRENT_TIMESTAMP - INTERVAL .* DAY .*\\) .*;",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Initializing QueryExecutor");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        executor = new FlinkTableExecutor(tEnv);

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties flinkProperties = applicationProperties.getOrDefault("FlinkApplicationProperties", new Properties());

        String region = flinkProperties.getProperty("aws.region", "eu-west-1");
        String warehousePath = flinkProperties.getProperty("warehouse.path", "s3://sbca-bronze");
        String dataCatalog = flinkProperties.getProperty("data.catalog", "iceberg_catalog");

        LOG.info("Using Glue catalog at {} in {}", warehousePath, region);

        String createCatalogSQL =
                "CREATE CATALOG " + dataCatalog + " WITH (" +
                "'type' = 'iceberg', " +
                "'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog', " +
                "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', " +
                "'warehouse' = '" + warehousePath + "', " +
                "'aws.region' = '" + region + "'" +
                ")";
        tEnv.executeSql(createCatalogSQL);
        tEnv.useCatalog(dataCatalog);
    }

    @Override
    public void flatMap(String message, Collector<LabeledRow> out) {
        try {
            JSONObject json = new JSONObject(message);
            String rawQuery = json.getString("query").trim();

            String type = detectQueryType(rawQuery);
            if (type == null) {
                LOG.warn("Unsupported query: {}", rawQuery);
                return;
            }

            Table table = executor.sqlQuery(rawQuery);
            String[] fieldNames = RowToJsonConverter.extractFieldNames(table);

            try (CloseableIterator<Row> iterator = executor.collectQuery(table)) {
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    LabeledRow labeledRow = new LabeledRow(row, fieldNames);
                    out.collect(labeledRow);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to execute query: {}", e.getMessage(), e);
        }
    }

    private String detectQueryType(String sql) {
        if (TENANT_LOOKUP_PATTERN.matcher(sql).matches()) return "tenant_lookup";
        if (RECENT_ACTIVITY_PATTERN.matcher(sql).matches()) return "recent_changes";
        return null;
    }

    public static class LabeledRow {
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
}
