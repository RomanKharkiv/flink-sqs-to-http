package com.sage.flink;

public class Config {

    public static String sqsQueueUrl() {
        return getEnvOrThrow("SQS_QUEUE_URL");
    }

    public static String apiEndpointUrl() {
        return getEnvOrThrow("API_ENDPOINT_URL");
    }

    public static String awsRegion() {
        return getEnv("AWS_REGION", "eu-west-1");
    }

    public static String awsAccessKeyId() {
        return getEnvOrThrow("AWS_ACCESS_KEY_ID");
    }

    public static String awsSecretAccessKey() {
        return getEnvOrThrow("AWS_SECRET_ACCESS_KEY");
    }

    public static String flinkAppName() {
        return getEnv("FLINK_APP_NAME", "flink-bank-matching");
    }

    public static String awsGlueDatabase() {
        return getEnv("AWS_GLUE_DATABASE", "sbca_bronze");
    }

    private static String getEnvOrThrow(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return value;
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
