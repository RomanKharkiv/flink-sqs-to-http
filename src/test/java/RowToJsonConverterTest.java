import com.sage.flink.utils.RowToJsonConverter;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.json.JSONObject;

import static org.junit.jupiter.api.Assertions.*;

class RowToJsonConverterTest {

    @Test
    void testConvertWithFieldNames() {
        Row row = Row.of(123, "Alice", true);
        String[] fieldNames = {"id", "name", "active"};

        JSONObject json = RowToJsonConverter.convert(row, fieldNames);

        assertEquals(123, json.getInt("id"));
        assertEquals("Alice", json.getString("name"));
        assertTrue(json.getBoolean("active"));
    }

    @Test
    void testConvertWithoutFieldNames() {
        Row row = Row.of("X", "Y");

        JSONObject json = RowToJsonConverter.convert(row, null);

        assertEquals("X", json.getString("col0"));
        assertEquals("Y", json.getString("col1"));
    }
}
