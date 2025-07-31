package com.sage.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class FlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting SQS source Flink job");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Properties appConfigProperties;
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        appConfigProperties = applicationProperties.get("dev");
        LOG.info("Successfully loaded application properties from KinesisAnalyticsRuntime");

        String sqsQueueUrl = appConfigProperties.getProperty("aws.sqs.queue.url");
        String awsRegion = appConfigProperties.getProperty("aws.region");
        String endPointUrl = appConfigProperties.getProperty("api.endpoint.url");

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

//        DataStream<QueryExecutor.LabeledRow> queryResults = sqsMessages
//                .flatMap(new QueryExecutor())
//                .name("Iceberg query Executor")
//                .returns(TypeInformation.of(QueryExecutor.LabeledRow.class));
//
//        queryResults
//                .addSink(new ApiSinkFunction(endPointUrl))
//                .name("HTTP Sink");

        DataStream<QueryExecutor.LabeledRow> tenantStream = sqsMessages
                .map(new TenantLookupQuery(tEnv))
                .returns(TypeInformation.of(QueryExecutor.LabeledRow.class))
                .name("Tenant Lookup");

        tenantStream
                .addSink(new ApiSinkFunction(endPointUrl))
                .name("HTTP-Sink");


        env.execute("Flink Iceberg Query to external API");
    }
}
