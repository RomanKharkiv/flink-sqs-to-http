import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TestCollector<T> implements Collector<T> {
    private final List<T> collected = new ArrayList<>();

    @Override
    public void collect(T record) {
        collected.add(record);
    }

    @Override
    public void close() {
        // no-op
    }

    public List<T> getCollected() {
        return collected;
    }
}
