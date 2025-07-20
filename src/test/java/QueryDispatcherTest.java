import com.sage.flink.QueryDispatcher;
import com.sage.flink.QueryDispatcher.LabeledRow;
import com.sage.flink.utils.TableExecutor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryDispatcherTest {

    @Test
    void testFlatMapReturnsExpectedRows() throws Exception {
        // Arrange
        TableExecutor executor = mock(TableExecutor.class);  // 🔄 Use interface
        Table mockedTable = mock(Table.class);

        // Stub SQL -> Table
        when(executor.sqlQuery(anyString())).thenReturn(mockedTable);

        // Stub schema
        when(mockedTable.getResolvedSchema()).thenReturn(
                ResolvedSchema.physical(
                        List.of("id", "name"),
                        List.of(DataTypes.INT(), DataTypes.STRING())
                )
        );

        // Stub result rows
        Row row = Row.of(42, "Hello");
        when(executor.collect(mockedTable)).thenReturn(
                new CloseableIterator<Row>() {
                    final Iterator<Row> delegate = List.of(row).iterator();
                    @Override
                    public boolean hasNext() {
                        return delegate.hasNext();
                    }
                    @Override
                    public Row next() {
                        return delegate.next();
                    }
                    @Override
                    public void close() {}
                }
        );


        // Build dispatcher and input
        QueryDispatcher dispatcher = new QueryDispatcher(executor);
        JSONObject msg = new JSONObject().put("query", "SELECT * FROM sbca_bronze.businesses WHERE tenant_id = 'a435h45hg1q34'");
        TestCollector<LabeledRow> collector = new TestCollector<>();

        // Act
        dispatcher.flatMap(msg.toString(), collector);

        // Assert
        List<LabeledRow> result = collector.getCollected();
        assertEquals(1, result.size());

        LabeledRow labeled = result.get(0);
        assertEquals(42, labeled.row().getField(0));
        assertEquals("Hello", labeled.row().getField(1));
        assertEquals("name", labeled.fieldNames()[1]);
    }

    @Test
    void testFlatMapReturnsRecentChangesRows() throws Exception {
        // Arrange
        TableExecutor executor = mock(TableExecutor.class);
        Table mockedTable = mock(Table.class);

        // Simulate schema
        when(executor.sqlQuery(anyString())).thenReturn(mockedTable);
        when(mockedTable.getResolvedSchema()).thenReturn(
                ResolvedSchema.physical(
                        List.of("id", "name", "updated_at"),
                        List.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING())
                )
        );

        // Simulate result rows
        Row row1 = Row.of(101, "Company A", "2024-07-01T12:00:00");
        Row row2 = Row.of(102, "Company B", "2024-07-02T09:00:00");
        when(executor.collect(mockedTable)).thenReturn(
                new CloseableIterator<Row>() {
                    final Iterator<Row> delegate = List.of(row1, row2).iterator();
                    @Override
                    public boolean hasNext() {
                        return delegate.hasNext();
                    }
                    @Override
                    public Row next() {
                        return delegate.next();
                    }
                    @Override
                    public void close() {}
                }
        );
        QueryDispatcher dispatcher = new QueryDispatcher(executor);

        String matchingQuery = "SELECT id, name, updated_at, created_at, website, owner_id, voided_at, _airbyte_extracted_at, dms_timestamp, source, tenant_id FROM sbca_bronze.businesses WHERE (updated_at >= CURRENT_TIMESTAMP - INTERVAL '2' DAY OR (website IS NULL OR TRIM(website) = '') OR owner_id IS NULL) AND voided_at IS NULL ORDER BY updated_at DESC;";

        JSONObject msg = new JSONObject().put("query", matchingQuery);
        TestCollector<LabeledRow> collector = new TestCollector<>();

        // Act
        dispatcher.flatMap(msg.toString(), collector);

        // Assert
        List<LabeledRow> result = collector.getCollected();

        assertEquals(2, result.size());

        QueryDispatcher.LabeledRow labeled1 = result.get(0);
        QueryDispatcher.LabeledRow labeled2 = result.get(1);

        assertEquals(101, labeled1.row().getField(0));
        assertEquals("Company A", labeled1.row().getField(1));

        assertEquals(102, labeled2.row().getField(0));
        assertEquals("Company B", labeled2.row().getField(1));

        assertEquals("id", labeled1.fieldNames()[0]);
        assertEquals("name", labeled1.fieldNames()[1]);
        assertEquals("updated_at", labeled1.fieldNames()[2]);
    }

}
