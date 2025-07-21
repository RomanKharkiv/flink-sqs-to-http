package com.sage.flink;

import com.sage.flink.utils.FlinkTableExecutor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.http.impl.client.CloseableHttpClient;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import java.util.Map;
import java.util.Properties;

import static org.apache.http.impl.client.HttpClients.createDefault;

public class FlinkJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties appConfigProperties = applicationProperties.get("dev");


        String sqsQueueUrl = appConfigProperties.getProperty("aws.sqs.queue.url");
        String awsRegion = appConfigProperties.getProperty("aws.region");
        String glueDatabase = appConfigProperties.getProperty("aws.glue.database");
        String endPointUrl = appConfigProperties.getProperty("api.endpoint.url");

        System.out.println("SQS Queue URL: " + sqsQueueUrl);
        System.out.println("AWS Region: " + awsRegion);
        System.out.println("Post Http url: " + endPointUrl);
        System.out.println("AWS Glue database: " + glueDatabase);


        tEnv.executeSql("USE CATALOG glue_catalog");
        tEnv.executeSql("USE " + glueDatabase);

        DataStream<String> sqsMessages = env.fromSource(
                new SqsSource(sqsQueueUrl),
                WatermarkStrategy.noWatermarks(),
                sqsQueueUrl.substring(sqsQueueUrl.lastIndexOf('/') + 1)
        );

        try (CloseableHttpClient httpClient = createDefault()) {
            sqsMessages
                    .flatMap(new QueryDispatcher(new FlinkTableExecutor(tEnv)))
                    .addSink(new ApiSinkFunction(httpClient, endPointUrl));
        }

        env.execute("Flink Iceberg Query to external API");
    }
}
