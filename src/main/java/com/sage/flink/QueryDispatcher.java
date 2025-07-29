//package com.sage.flink;
//
//import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
//import com.sage.flink.utils.FlinkTableExecutor;
//import com.sage.flink.utils.RowToJsonConverter;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.json.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.Map;
//import java.util.Properties;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class QueryDispatcher extends RichFlatMapFunction<String, QueryDispatcher.LabeledRow> {
//    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = LoggerFactory.getLogger(QueryDispatcher.class);
//
//    private transient FlinkTableExecutor executor;
//
//    public QueryDispatcher() {
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        LOG.info("Running Flink job version: 1.4-flink-desc");
//        String classPath = System.getProperty("java.class.path");
//        LOG.info("Classpath: {} ", classPath);
//        try {
//            Class<?> hdfsConfigClass = Class.forName("org.apache.hadoop.hdfs.HdfsConfiguration");
//            LOG.info("HdfsConfiguration loaded successfully: {}", hdfsConfigClass.getName());
//        } catch (ClassNotFoundException e) {
//            LOG.error("HdfsConfiguration not found in classpath", e);
//        }
//        LOG.info("Classloader: {}", this.getClass().getClassLoader());
//
////        try {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        executor = new FlinkTableExecutor(tEnv);
//
//        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
//        Properties flinkProperties = applicationProperties.getOrDefault("FlinkApplicationProperties", new Properties());
//
//        String region = flinkProperties.getProperty("aws.region", "eu-west-1");
//        String warehousePath = flinkProperties.getProperty("warehouse.path", "s3://sbca-bronze");
//        String dataCatalog = flinkProperties.getProperty("data.catalog", "iceberg_catalog");
//
//        LOG.info("Using region: {} and warehouse path: {}", region, warehousePath);
//
//
//        String createCatalogSQL =
//                "CREATE CATALOG " + dataCatalog + " WITH (" +
//                "'type' = 'iceberg', " +
//                "'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog', " +
//                "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', " +
//                "'warehouse' = '" + warehousePath + "', " +
//                "'aws.region' = '" + region + "'" +
//                ")";
//
//        LOG.info("Create catalog SQL: {}", createCatalogSQL);
//        tEnv.executeSql(createCatalogSQL);
//        LOG.info("Successfully created catalog: {}", dataCatalog);
//
//        // Use the catalog
//        tEnv.executeSql("USE CATALOG " + dataCatalog);
//        LOG.info("Using catalog: {}", dataCatalog);
//
////                Configuration conf = new Configuration();
////                conf.setString("type", "iceberg");
////                conf.setString("catalog-name", dataCatalog);
////                conf.setString("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
////                conf.setString("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
////                conf.setString("warehouse", warehousePath); // base path
////                conf.setString("aws.region", region);
////                CatalogDescriptor descriptor = CatalogDescriptor.of(dataCatalog, conf);
////
////                LOG.info("Creating flink catalog using CatalogDescriptor");
////
////                tEnv.createCatalog(dataCatalog, descriptor);
////                LOG.info("Successfully registered catalog: {}", dataCatalog);
////
////                tEnv.useCatalog(dataCatalog);
////                LOG.info("Using catalog: {}", dataCatalog);
//
//
////                tEnv.executeSql(String.format(
////                        "CREATE CATALOG %s WITH (\n" +
////                        "  'type' = 'iceberg',\n" +
////                        "  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',\n" +
////                        "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
////                        "  'warehouse' = '%s',\n" +
////                        "  'aws.region' = '%s'\n" +
////                        ")",
////                        dataCatalog,
////                        warehousePath,
////                        region
////                ));
////
////                tEnv.useCatalog(dataCatalog);
//
//
//        // List databases in the catalog
////                try {
////                    LOG.info("===== LISTING DATABASES IN CATALOG =====");
////                    TableResult databasesResult = tEnv.executeSql("SHOW DATABASES");
////                    Iterator<Row> dbIterator = databasesResult.collect();
////                    int dbCount = 0;
////
////                    while (dbIterator.hasNext()) {
////                        Row row = dbIterator.next();
////                        String dbName = row.getField(0).toString();
////                        LOG.info("Found database: {}", dbName);
////                        dbCount++;
////
////                        // If we find sbca_bronze, check its tables
////                        if (dbName.equalsIgnoreCase("sbca_bronze")) {
////                            try {
////                                LOG.info("===== LISTING TABLES IN SBCA_BRONZE =====");
////                                tEnv.useDatabase(dbName);
////                                TableResult tablesResult = tEnv.executeSql("SHOW TABLES");
////                                Iterator<Row> tableIterator = tablesResult.collect();
////                                int tableCount = 0;
////
////                                while (tableIterator.hasNext()) {
////                                    Row tableRow = tableIterator.next();
////                                    String tableName = tableRow.getField(0).toString();
////                                    LOG.info("Found table: {}", tableName);
////                                    tableCount++;
////
////                                    // If we find the businesses table, test it
////                                    if (tableName.equalsIgnoreCase("businesses")) {
////                                        try {
////                                            LOG.info("===== TESTING BUSINESSES TABLE =====");
////                                            TableResult countResult = tEnv.executeSql("SELECT COUNT(*) FROM businesses");
////                                            Iterator<Row> countIterator = countResult.collect();
////                                            if (countIterator.hasNext()) {
////                                                Row countRow = countIterator.next();
////                                                LOG.info("Count of records in businesses table: {}", countRow.getField(0));
////                                            }
////                                        } catch (Exception e) {
////                                            LOG.error("Error testing businesses table: {}", e.getMessage(), e);
////                                        }
////                                    }
////                                }
////
////                                if (tableCount == 0) {
////                                    LOG.warn("No tables found in sbca_bronze database!");
////                                } else {
////                                    LOG.info("Total tables found in sbca_bronze: {}", tableCount);
////                                }
////                            } catch (Exception e) {
////                                LOG.error("Error listing tables in sbca_bronze: {}", e.getMessage(), e);
////                            }
////                        }
////                    }
////
////                    if (dbCount == 0) {
////                        LOG.warn("No databases found in catalog!");
////                    } else {
////                        LOG.info("Total databases found: {}", dbCount);
////                    }
////                } catch (Exception e) {
////                    LOG.error("Error listing databases: {}", e.getMessage(), e);
////                }
////
////            } catch (Exception e) {
////                LOG.error("Error registering AWS Glue catalog: {}", e.getMessage(), e);
////            }
////
////        } catch (Exception e) {
////            LOG.error("Error in QueryDispatcher.open(): {}", e.getMessage(), e);
////            throw e;
////        }
//
//        super.open(parameters);
//    }
//
//
//    public static class LabeledRow {
//        private final Row row;
//        private final String[] fieldNames;
//
//        public LabeledRow(Row row, String[] fieldNames) {
//            this.row = row;
//            this.fieldNames = fieldNames;
//        }
//
//        public Row getRow() {
//            return row;
//        }
//
//        public String[] getFieldNames() {
//            return fieldNames;
//        }
//    }
//
//    private static final Pattern TENANT_LOOKUP_PATTERN = Pattern.compile(
//            "SELECT\\s+([^\\s]+|\\*|[^\\s]+(\\s*,\\s*[^\\s]+)*)\\s+FROM\\s+sbca_bronze\\.businesses\\s+WHERE\\s+tenant_id\\s*=\\s*'([a-zA-Z0-9\\-]+)'\\s*" +
//            "(?:\\s+ORDER\\s+BY\\s+([^\\s;]+(?:\\s+(?:ASC|DESC))?(?:\\s*,\\s*[^\\s;]+(?:\\s+(?:ASC|DESC))?)*))?\\s*" +
//            "(?:\\s+LIMIT\\s+(\\d+))?\\s*;?\\s*",
//            Pattern.CASE_INSENSITIVE
//    );
//
//    private static final Pattern RECENT_ACTIVITY_PATTERN = Pattern.compile(
//            "SELECT\\s+([^\\s]+|\\*|[^\\s]+(\\s*,\\s*[^\\s]+)*)\\s+FROM sbca_bronze\\.businesses WHERE \\(updated_at >= CURRENT_TIMESTAMP - INTERVAL .* DAY .*\\) .*;",
//            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
//    );
//
//    @Override
//    public void flatMap(String message, Collector<LabeledRow> out) {
//        try {
//            LOG.info("QueryDispatcher FlatMap message: {}", message);
//
//            JSONObject json = new JSONObject(message);
//            String rawQuery = json.getString("query").trim();
//
//            QueryMatch match = matchQueryType(rawQuery);
//
//            if (match == null) {
//                LOG.info("Unsupported or unsafe query: {}", rawQuery);
//                return;
//            }
//
//            String type = match.getType();
//            if ("tenant_lookup".equals(type)) {
//                LOG.info("tenant_lookup query type:  {}", type);
//                handleTenantLookup(match.getMatcher(), out);
//            } else if ("recent_changes".equals(type)) {
//                LOG.info("recent_changes query type:  {}", type);
//                handleRecentBusinessActivity(out);
//            } else {
//                System.err.println("No handler for query type: " + type);
//            }
//
//        } catch (Exception e) {
//            LOG.info("Query dispatch failed: {}", e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    private void handleTenantLookup(Matcher matcher, Collector<LabeledRow> out) {
//        LOG.info("HandleTenantLookup, tenant_id: {}", matcher.group(3));
//        try {
//            executeQuery(matcher.group(), out);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private void handleRecentBusinessActivity(Collector<LabeledRow> out) {
//        LOG.info("handleRecentBusinessActivity!");
//        String query =
//                "SELECT id, name, updated_at, created_at, website, owner_id, voided_at, " +
//                "_airbyte_extracted_at, dms_timestamp, source, tenant_id " +
//                "FROM sbca_bronze.businesses " +
//                "WHERE (updated_at >= CURRENT_TIMESTAMP - INTERVAL '2' DAY " +
//                "OR (website IS NULL OR TRIM(website) = '') OR owner_id IS NULL) " +
//                "AND voided_at IS NULL " +
//                "ORDER BY updated_at DESC " +
//                "LIMIT 100";
//        try {
//            executeQuery(query, out);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private void executeQuery(String query, Collector<LabeledRow> out) throws Exception {
//        LOG.info("ExecuteQuery: {}", query);
//        Table result = executor.sqlQuery(query);
//        LOG.info("Query result: {}", result);
//        String[] fieldNames = RowToJsonConverter.extractFieldNames(result);
//        Arrays.stream(fieldNames).forEach(name -> LOG.info("Query result fields names: {}", name));
//
//        try (AutoCloseableIterator<Row> iterator = new AutoCloseableIterator<>(executor.collect(result))) {
//            while (iterator.hasNext()) {
//                out.collect(new LabeledRow(iterator.next(), fieldNames));
//            }
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to collect query results", e);
//        }
//    }
//
//    private QueryMatch matchQueryType(String query) {
//        Matcher tenant = TENANT_LOOKUP_PATTERN.matcher(query);
//        if (tenant.matches()) return new QueryMatch("tenant_lookup", tenant);
//
//        Matcher recent = RECENT_ACTIVITY_PATTERN.matcher(query);
//        if (recent.matches()) return new QueryMatch("recent_changes", recent);
//
//        return null;
//    }
//
//    private static class QueryMatch {
//        private final String type;
//        private final Matcher matcher;
//
//        public QueryMatch(String type, Matcher matcher) {
//            this.type = type;
//            this.matcher = matcher;
//        }
//
//        public String getType() {
//            return type;
//        }
//
//        public Matcher getMatcher() {
//            return matcher;
//        }
//    }
//
//    // Helper to auto-close iterator for Java 11 try-with-resources
//    private static class AutoCloseableIterator<T> implements AutoCloseable {
//        private final java.util.Iterator<T> iterator;
//
//        public AutoCloseableIterator(java.util.Iterator<T> iterator) {
//            this.iterator = iterator;
//        }
//
//        public boolean hasNext() {
//            return iterator.hasNext();
//        }
//
//        public T next() {
//            return iterator.next();
//        }
//
//        @Override
//        public void close() {
//            // no-op since the underlying collect() result is not AutoCloseable in Java 11
//        }
//    }
//}
