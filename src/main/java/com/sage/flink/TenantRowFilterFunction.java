package com.sage.flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


public class TenantRowFilterFunction extends BroadcastProcessFunction<Row, String, Row> {

    private final MapStateDescriptor<String, String> stateDescriptor;

    public TenantRowFilterFunction(MapStateDescriptor<String, String> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void processBroadcastElement(
            String tenantId,
            Context ctx,
            Collector<Row> out) throws Exception {
        ctx.getBroadcastState(stateDescriptor).put(tenantId, "active");
    }

    @Override
    public void processElement(
            Row row,
            ReadOnlyContext ctx,
            Collector<Row> out) throws Exception {

        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
        String rowTenantId = (String) row.getField("tenant_id");

        if (state.contains(rowTenantId)) {
            out.collect(row);  // Only emit rows that match active tenant_id
        }
    }
}

