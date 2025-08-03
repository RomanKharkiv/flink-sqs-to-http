package com.sage.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BatchingRowToJsonFunction extends KeyedProcessFunction<String, LabeledRow, String> {
    private static final Logger LOG = LoggerFactory.getLogger(BatchingRowToJsonFunction.class);

    private final int maxBatchSize;
    private final long flushIntervalMs;

    private transient ListState<LabeledRow> bufferState;

    public BatchingRowToJsonFunction(int maxBatchSize, long flushIntervalMs) {
        LOG.info("Creating BatchingRowToJsonFunction");
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("Opening BatchingRowToJsonFunction with params {}", parameters);
        ListStateDescriptor<LabeledRow> descriptor = new ListStateDescriptor<>(
                "labeledRowBuffer",
                Types.POJO(LabeledRow.class)
        );
        bufferState = getRuntimeContext().getListState(descriptor);
        LOG.info("Opening BatchingRowToJsonFunction with bufferState: {}", bufferState);
    }

    @Override
    public void processElement(LabeledRow value, Context ctx, Collector<String> out) throws Exception {
        LOG.info("Processing element with value: {}", value);
        bufferState.add(value);
        LOG.info("Processing element with bufferState: {}", bufferState);

        List<LabeledRow> current = new ArrayList<>();
        for (LabeledRow row : bufferState.get()) {
            current.add(row);
        }

        if (current.size() >= maxBatchSize) {
            flush(current, out);
            bufferState.clear();
        } else {
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + flushIntervalMs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        LOG.info("On Timer with timestamp: {}", timestamp);
        List<LabeledRow> current = new ArrayList<>();
        for (LabeledRow row : bufferState.get()) {
            current.add(row);
        }

        if (!current.isEmpty()) {
            flush(current, out);
            bufferState.clear();
        }
    }

    private void flush(List<LabeledRow> rows, Collector<String> out) {
        LOG.info("Flushing {} rows", rows.size());
        JSONArray array = new JSONArray();
        for (LabeledRow labeledRow : rows) {
            JSONObject obj = new JSONObject();
            Row row = labeledRow.getRow();
            String[] fieldNames = labeledRow.getFieldNames();

            for (int i = 0; i < row.getArity(); i++) {
                String name = i < fieldNames.length ? fieldNames[i] : "f" + i;
                Object val = row.getField(i);
                obj.put(name, val != null ? val : JSONObject.NULL);
            }

            array.put(obj);
        }
        LOG.info("Flushed {} rows", array.toString());
        out.collect(array.toString());
    }
}
