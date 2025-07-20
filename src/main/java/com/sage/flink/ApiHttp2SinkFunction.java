package com.sage.flink;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.sage.flink.utils.RowToJsonConverter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.json.JSONObject;

public class ApiHttp2SinkFunction extends RichSinkFunction<QueryDispatcher.LabeledRow> {

    private transient HttpClient httpClient;

    public ApiHttp2SinkFunction(HttpClient client) {
        this.httpClient = client;
    }

    public ApiHttp2SinkFunction() {}

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public void invoke(QueryDispatcher.LabeledRow row, Context context) {
        JSONObject json = RowToJsonConverter.convert(row.row(), row.fieldNames());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(Config.apiEndpointUrl()))
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
