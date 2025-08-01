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

import java.util.ArrayList;
import java.util.List;

public class BatchingRowToJsonFunction extends KeyedProcessFunction<String, LabeledRow, String> {

    private final int maxBatchSize;
    private final long flushIntervalMs;

    private transient ListState<LabeledRow> bufferState;

    public BatchingRowToJsonFunction(int maxBatchSize, long flushIntervalMs) {
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<LabeledRow> descriptor = new ListStateDescriptor<>(
                "labeledRowBuffer",
                Types.POJO(LabeledRow.class)
        );
        bufferState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(LabeledRow value, Context ctx, Collector<String> out) throws Exception {
        bufferState.add(value);

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
        out.collect(array.toString());
    }
}
