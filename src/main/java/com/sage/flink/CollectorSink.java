package com.sage.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

public class CollectorSink<T> implements SinkFunction<T> {
    private final Collector<T> collector;

    public CollectorSink(Collector<T> collector) {
        this.collector = collector;
    }

    @Override
    public void invoke(T value, Context context) {
        collector.collect(value);
    }
}