import com.sage.flink.ApiSinkFunction;
import com.sage.flink.Config;
import com.sage.flink.QueryDispatcher.LabeledRow;
import org.apache.flink.types.Row;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;


import static org.mockito.Mockito.*;

class ApiSinkFunctionTest {
    String endPointUrl = Config.apiEndpointUrl();

    @Test
    void testInvokePostsJsonToHttpClient() throws Exception {
        // Arrange
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

        when(mockClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

        ApiSinkFunction sink = new ApiSinkFunction(mockClient, endPointUrl);

        Row row = Row.of(1, "Test");
        String[] fields = {"id", "name"};
        LabeledRow labeledRow = new LabeledRow(row, fields);

        // Act
        sink.invoke(labeledRow, null);

        // Assert
        verify(mockClient, times(1)).execute(argThat(post -> {
            try {
                return post instanceof HttpPost &&
                       ((HttpPost) post).getURI().toString().equals(endPointUrl) &&
                       ((HttpPost) post).getEntity().getContentLength() > 0;
            } catch (Exception e) {
                return false;
            }
        }));

        verify(mockResponse).close();
    }

    @Test
    void testCloseClosesHttpClient() throws Exception {
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        ApiSinkFunction sink = new ApiSinkFunction(mockClient, endPointUrl);

        sink.close();

        verify(mockClient, times(1)).close();
    }
}
