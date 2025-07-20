package com.sage.flink;

import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static final Properties props = new Properties();

    static {
        try (InputStream in = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in == null) throw new RuntimeException("config.properties not found");
            props.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config.properties", e);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }

    // Convenience helpers
    public static String apiEndpointUrl() {
        return get("api.endpoint.url");
    }

    public static String sqsQueueUrl() {
        return get("aws.sqs.queue.url");
    }

    public static String awsRegion() {
        return get("aws.region");
    }

    public static String awsGlueDatabase() {
        return get("aws.glue.database");
    }
}
