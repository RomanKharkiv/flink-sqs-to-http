package com.sage.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class ApiHttp2SinkFunction extends RichSinkFunction<Row> {

    private transient HttpClient httpClient;
    private final String endPointUrl;
    private static final Logger LOG = LoggerFactory.getLogger(ApiHttp2SinkFunction.class);

    public ApiHttp2SinkFunction(String endPointUrl) {
        this.endPointUrl = endPointUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        LOG.info("Initialized HTTP/2 client for endpoint: {}", endPointUrl);
    }

    @Override
    public void invoke(Row row, Context context) {
        try {
            String jsonPayload = rowToJson(row);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endPointUrl))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .exceptionally(e -> {
                        LOG.warn("Failed to send HTTP/2 request for row {}: {}", row, e.getMessage());
                        return null;
                    });
        } catch (Exception e) {
            LOG.error("Unexpected error while sending row: {}", row, e);
        }
    }

    private String rowToJson(Row row) {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < row.getArity(); i++) {
            if (i > 0) json.append(",");
            String key = "field" + i; // Optional: replace with real field names
            Object val = row.getField(i);
            json.append("\"").append(key).append("\":");
            json.append(val instanceof Number ? val : "\"" + String.valueOf(val).replace("\"", "\\\"") + "\"");
        }
        json.append("}");
        return json.toString();
    }
}
