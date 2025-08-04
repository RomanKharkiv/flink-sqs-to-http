package com.sage.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.api.common.typeinfo.Types.*;

public class FlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);
    private static final Pattern TENANT_LOOKUP_PATTERN = Pattern.compile(
            "SELECT\\s+([^\\s]+|\\*|[^\\s]+(\\s*,\\s*[^\\s]+)*)\\s+FROM\\s+sbca_bronze\\.businesses\\s+WHERE\\s+tenant_id\\s*=\\s*'([a-zA-Z0-9\\-]+)'\\s*" +
            "(?:\\s+ORDER\\s+BY\\s+([^\\s;]+(?:\\s+(?:ASC|DESC))?(?:\\s*,\\s*[^\\s;]+(?:\\s+(?:ASC|DESC))?)*))?\\s*" +
            "(?:\\s+LIMIT\\s+(\\d+))?\\s*;?\\s*",
            Pattern.CASE_INSENSITIVE
    );
    public static void main(String[] args) throws Exception {
        LOG.info("Starting SQS source Flink job");
        Class.forName("org.apache.iceberg.flink.source.FlinkInputFormat");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties appConfigProperties;
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        appConfigProperties = applicationProperties.get("dev");
        LOG.info("Successfully loaded application properties from KinesisAnalyticsRuntime");

        String sqsQueueUrl = appConfigProperties.getProperty("aws.sqs.queue.url");
        String awsRegion = appConfigProperties.getProperty("aws.region");
        String endPointUrl = appConfigProperties.getProperty(
                "api.endpoint.url",
                "https://webhook.site/f9a2e949-bd82-40b7-8f36-8d57063bdec5"
        );

        String warehousePath = appConfigProperties.getProperty("warehouse.path", "s3://sbca-bronze");
        String dataCatalog = appConfigProperties.getProperty("data.catalog", "iceberg_catalog");
        String database = appConfigProperties.getProperty("database", "sbca_bronze");


        String createCatalogSQL =
                "CREATE CATALOG " + dataCatalog + " WITH (" +
                "'type' = 'iceberg', " +
                "'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog', " +
                "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', " +
                "'warehouse' = '" + warehousePath + "', " +
                "'aws.region' = '" + awsRegion + "'" +
                ")";

        tEnv.executeSql(createCatalogSQL);
        tEnv.useCatalog(dataCatalog);
        tEnv.useDatabase(database);

//        MapStateDescriptor<String, String> broadcastStateDescriptor =
//                new MapStateDescriptor<>("TenantBroadcastState", Types.STRING, Types.STRING);
//        Table allData = tEnv.from("businesses");
        Table allData = tEnv.sqlQuery(
                "SELECT * FROM sbca_bronze.businesses WHERE tenant_id IS NOT NULL /*+ OPTIONS(" +
                "'monitor-interval'='1s'," +
                "'scan.incremental.snapshot.enabled'='true'," +
                "'streaming'='true'" +
                ") */"
        );


        DataType dataType = allData.getResolvedSchema().toPhysicalRowDataType();
        RowType rowType = (RowType) dataType.getLogicalType();

        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        LOG.info("FieldNames: - {}", Arrays.toString(fieldNames));
        DataStream<Row> allRows = tEnv.toDataStream(allData);

//        DataStream<TenantRows> tenantRowMapStream = allRows
//                .keyBy(row -> (String) row.getField("tenant_id"))
//                .process(new KeyedProcessFunction<String, Row, TenantRows>() {
//
//                    private transient ListState<Row> tenantRows;
//
//                    @Override
//                    public void open(Configuration parameters) {
//                        ListStateDescriptor<Row> descriptor = new ListStateDescriptor<>(
//                                "tenantRowsState", Types.ROW());
//                        tenantRows = getRuntimeContext().getListState(descriptor);
//                    }
//
//                    @Override
//                    public void processElement(Row row, Context ctx, Collector<TenantRows> out) throws Exception {
//                        tenantRows.add(row);
//                        List<Row> current = new ArrayList<>();
//                        for (Row r : tenantRows.get()) {
//                            current.add(r);
//                        }
//                        out.collect(new TenantRows(ctx.getCurrentKey(), current));
//                    }
//                });



//        MapStateDescriptor<String, List<Row>> broadcastStateDescriptor =
//                new MapStateDescriptor<>("tenantRows", Types.STRING, Types.LIST(Types.ROW()));

        TypeInformation<Row> rowTypeInfo = allRows.getType();
        MapStateDescriptor<String, List<Row>> broadcastStateDescriptor =
                new MapStateDescriptor<>("tenantRows", STRING, LIST(rowTypeInfo));

        BroadcastStream<Row> broadcastedIcebergRows = allRows.broadcast(broadcastStateDescriptor);

        CustomSqsSource<String> sqsSource = CustomSqsSource.<String>builder()
                .queueUrl(sqsQueueUrl)
                .region(awsRegion)
                .deserializationSchema(new SimpleStringSchema())
                .batchSize(10)
                .pollingIntervalMillis(1000)
                .build();


        DataStream<String> sqsMessages = env.fromSource(
                sqsSource,
                WatermarkStrategy.noWatermarks(),
                sqsQueueUrl.substring(sqsQueueUrl.lastIndexOf('/') + 1),
                TypeInformation.of(String.class)
        );

        LOG.info("Created DataStream from SQS!");
        DataStream<String> tenantIdStream = sqsMessages
                .map(message -> {
                    JSONObject json = new JSONObject(message);
                    String query = json.getString("query").trim();
                    Matcher matcher = TENANT_LOOKUP_PATTERN.matcher(query);
                    if (matcher.matches()) {
                        return matcher.group(3);
                    } else {
                        LOG.warn("Invalid query format: {}", query);
                        return null;
                    }
                })
                .returns(Types.STRING)
                .name("Extract tenant_id");


        DataStream<LabeledRow> labeledFilteredRows = tenantIdStream
                .connect(broadcastedIcebergRows)
                .process(new TenantRowLookupFunction(broadcastStateDescriptor, fieldNames))
                .returns(Types.POJO(LabeledRow.class))
                .name("TenantRowFilter with LabeledRow");


//        BroadcastStream<String> tenantBroadcast = tenantStream.broadcast(broadcastStateDescriptor);


//        DataStream<LabeledRow> labeledFilteredRows = allRows
//                .connect(tenantBroadcast)
//                .process(new TenantRowFilterFunction(broadcastStateDescriptor))
//                .map(row -> new LabeledRow(row, fieldNames))
//                .returns(Types.POJO(LabeledRow.class))
//                .name("TenantRowFilter with LabeledRow");

        labeledFilteredRows
                .keyBy(row -> row.getRow().getField("tenant_id").toString())
                .process(new BatchingRowToJsonFunction(100, 1000)) // 10 rows or 5 sec
                .addSink(new ApiSinkFunction(endPointUrl))
                .name("HTTP Row Batch Sink");

        env.execute("Flink Iceberg Query to external API");
    }
}
