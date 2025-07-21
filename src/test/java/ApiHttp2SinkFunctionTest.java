import com.sage.flink.ApiHttp2SinkFunction;
import com.sage.flink.QueryDispatcher;
import com.sage.flink.Config;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ApiHttp2SinkFunctionTest {
    String endPointUrl = Config.apiEndpointUrl();

    @Test
    void testInvokeSendsHttpRequest() {
        // Arrange
        HttpClient mockClient = mock(HttpClient.class);

        @SuppressWarnings("unchecked")
        CompletableFuture<HttpResponse<Object>> dummyFuture =
                (CompletableFuture<HttpResponse<Object>>) (CompletableFuture<?>) CompletableFuture.completedFuture(mock(HttpResponse.class));

        when(mockClient.sendAsync(any(), any())).thenReturn(dummyFuture);

        ApiHttp2SinkFunction sink = new ApiHttp2SinkFunction(mockClient);

        Row row = Row.of(1, "test");
        String[] fieldNames = {"id", "name"};
        QueryDispatcher.LabeledRow labeledRow = new QueryDispatcher.LabeledRow(row, fieldNames);

        // Act
        sink.invoke(labeledRow, null);

        // Assert
        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(mockClient).sendAsync(requestCaptor.capture(), eq(HttpResponse.BodyHandlers.discarding()));

        HttpRequest sent = requestCaptor.getValue();
        assertEquals(endPointUrl, sent.uri().toString());
        assertEquals("application/json", sent.headers().firstValue("Content-Type").orElse(""));
        assertEquals("POST", sent.method());
        assertTrue(sent.bodyPublisher().isPresent());
    }

}

