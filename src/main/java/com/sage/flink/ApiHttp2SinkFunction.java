package com.sage.flink;

import com.sage.flink.utils.RowToJsonConverter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ApiHttp2SinkFunction extends RichSinkFunction<QueryExecutor.LabeledRow> {

    private transient HttpClient httpClient;
    private String endPointUrl;

    public ApiHttp2SinkFunction(HttpClient client, String endPointUrl) {
        this.httpClient = client;
        this.endPointUrl = endPointUrl;
    }

    public ApiHttp2SinkFunction() {}

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public void invoke(QueryExecutor.LabeledRow row, Context context) {
        JSONObject json = RowToJsonConverter.convert(row.getRow(), row.getFieldNames());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endPointUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json.toString()))
                .build();

        // Fire-and-forget async request (no blocking, no retry here)
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .exceptionally(e -> {
                    System.err.println("Failed to send HTTP/2 request: " + e.getMessage());
                    return null;
                });
    }
}
