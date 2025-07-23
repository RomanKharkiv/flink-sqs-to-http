package com.sage.flink;

import com.sage.flink.utils.RowToJsonConverter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ApiSinkFunction extends RichSinkFunction<QueryDispatcher.LabeledRow> {
    private static final Logger LOG = LoggerFactory.getLogger(ApiSinkFunction.class);

    private transient CloseableHttpClient httpClient;
    private final String endPointUrl;
    private final int maxRetries;
    private final long retryDelayMs;

    public ApiSinkFunction(String endPointUrl) {
        this(endPointUrl, 3, 1000);
    }

    public ApiSinkFunction(String endPointUrl, int maxRetries, long retryDelayMs) {
        LOG.info("ApiSinkFunction with endpoint: {}", endPointUrl);
        this.endPointUrl = endPointUrl;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    @Override
        public void open(Configuration parameters) throws Exception {
        LOG.info("ApiSinkFunction open with parameters: {}", parameters);
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(50);
        connectionManager.setDefaultMaxPerRoute(20);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(5000)
                .setSocketTimeout(30000)
                .setConnectionRequestTimeout(5000)
                .build();

        this.httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setDefaultRequestConfig(requestConfig)
                .build();

        LOG.info("HTTP client initialized with endpoint: {}", endPointUrl);
    }

    @Override
    public void invoke(QueryDispatcher.LabeledRow labeled, Context context) throws Exception {
        LOG.info("HTTP client invoke with labeled: {}", labeled);
        JSONObject json = RowToJsonConverter.convert(labeled.getRow(), labeled.getFieldNames());
        LOG.info("HTTP client invoke with json: {}", json);
        HttpPost post = new HttpPost(endPointUrl);
        post.setHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity(json.toString(), StandardCharsets.UTF_8));

        int attempts = 0;
        boolean success = false;
        Exception lastException = null;

        while (attempts < maxRetries && !success) {
            attempts++;
            try {
                LOG.debug("Sending HTTP request, attempt {}/{}", attempts, maxRetries);
                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode >= 200 && statusCode < 300) {
                        success = true;
                        LOG.debug("HTTP request successful, status code: {}", statusCode);
                    } else {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        LOG.warn("HTTP request failed with status code: {}, response: {}",
                                statusCode, responseBody);
                        if (statusCode >= 500) {
                            // Server error, retry
                            Thread.sleep(retryDelayMs * attempts);
                        } else {
                            // Client error, don't retry
                            throw new IOException("HTTP request failed with status code: " + statusCode);
                        }
                    }
                }
            } catch (IOException e) {
                lastException = e;
                LOG.warn("HTTP request failed, attempt {}/{}: {}", attempts, maxRetries, e.getMessage());
                if (attempts < maxRetries) {
                    Thread.sleep(retryDelayMs * attempts);
                }
            }
        }

        if (!success) {
            LOG.error("Failed to send HTTP request after {} attempts", maxRetries);
            throw new IOException("Failed to send HTTP request after " + maxRetries + " attempts", lastException);
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
            LOG.info("HTTP client closed");
        }
    }
}

