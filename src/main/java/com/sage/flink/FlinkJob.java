package com.sage.flink;

import com.sage.flink.utils.FlinkTableExecutor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.http.impl.client.CloseableHttpClient;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static org.apache.http.impl.client.HttpClients.createDefault;

public class FlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting SQS source Flink job");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        env.getConfig().setGlobalJobParameters(config);



        Properties appConfigProperties;
        try {
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            appConfigProperties = applicationProperties.get("dev");
            LOG.info("Successfully loaded application properties from KinesisAnalyticsRuntime");
        } catch (Exception e) {
            LOG.warn("Failed to load properties from KinesisAnalyticsRuntime. Using default properties.", e);
            appConfigProperties = getDefaultProperties();
        }


        String sqsQueueUrl = appConfigProperties.getProperty("aws.sqs.queue.url");
        String awsRegion = appConfigProperties.getProperty("aws.region");
        String endPointUrl = appConfigProperties.getProperty("api.endpoint.url");

        appConfigProperties.forEach((k, v) ->
                LOG.info("Starting Job properties: {} - {}", k, v)
        );


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
        sqsMessages
                .flatMap(new QueryDispatcher())
                .returns(TypeInformation.of(QueryDispatcher.LabeledRow.class))
                .addSink(new ApiSinkFunction(endPointUrl))
                .name("HTTP-Response-Sink");

        env.execute("Flink Iceberg Query to external API");
    }

    private static Properties getDefaultProperties() {
        Properties defaultProps = new Properties();
        defaultProps.setProperty("input.topic", "default-input-topic");
        defaultProps.setProperty("output.topic", "default-output-topic");
        defaultProps.setProperty("kafka.bootstrap.servers", "localhost:9092");
        return defaultProps;
    }
}
