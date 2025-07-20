package com.sage.flink;

import com.sage.flink.utils.FlinkTableExecutor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import software.amazon.awssdk.services.sqs.endpoints.internal.Value;

import static org.apache.http.impl.client.HttpClients.createDefault;

public class FlinkJob {
    private static final String SQS_QUEUE_URL = Config.sqsQueueUrl();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql("USE CATALOG glue_catalog");
        tEnv.executeSql("USE " + Config.awsGlueDatabase());

        DataStream<String> sqsMessages = env.fromSource(
                new SqsSource(SQS_QUEUE_URL),
                WatermarkStrategy.noWatermarks(),
                SQS_QUEUE_URL.substring(SQS_QUEUE_URL.lastIndexOf('/') + 1)
        );

        try (CloseableHttpClient httpClient = createDefault()) {
            sqsMessages
                    .flatMap(new QueryDispatcher(new FlinkTableExecutor(tEnv)))
                    .addSink(new ApiSinkFunction(httpClient));
        }

        env.execute("Flink Iceberg Query Dispatcher");
    }
}
