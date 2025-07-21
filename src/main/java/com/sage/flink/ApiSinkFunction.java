package com.sage.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import com.sage.flink.utils.RowToJsonConverter;

public class ApiSinkFunction extends RichSinkFunction<QueryDispatcher.LabeledRow> {

    private transient CloseableHttpClient httpClient;
    private final String endPointUrl;

    public ApiSinkFunction(CloseableHttpClient httpClient, String endPointUrl) {
        this.httpClient = httpClient;
        this.endPointUrl = endPointUrl;
    }


    @Override
    public void open(Configuration parameters) {
        this.httpClient = HttpClients.createDefault();
    }

    @Override
    public void invoke(QueryDispatcher.LabeledRow labeled, Context context) {
        try {
            JSONObject json = RowToJsonConverter.convert(labeled.getRow(), labeled.getFieldNames());

            HttpPost post = new HttpPost(endPointUrl);
            post.setHeader("Content-Type", "application/json");
            post.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

            httpClient.execute(post).close(); // discard response body

        } catch (Exception e) {
            System.err.println("Failed to POST row: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }

}
