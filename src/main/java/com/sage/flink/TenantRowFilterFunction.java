package com.sage.flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TenantRowFilterFunction extends BroadcastProcessFunction<Row, String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(TenantRowFilterFunction.class);

    private final MapStateDescriptor<String, String> stateDescriptor;

    private final List<Row> bufferedRows = new ArrayList<>();
    private boolean broadcastReceived = false;

    public TenantRowFilterFunction(MapStateDescriptor<String, String> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
        LOG.info("TenantRowFilterFunction created");
    }

    @Override
    public void processBroadcastElement(
            String tenantId,
            Context ctx,
            Collector<Row> out) throws Exception {
        LOG.info("Received broadcast tenant ID: {}", tenantId);
        ctx.getBroadcastState(stateDescriptor).put(tenantId, "active");
        broadcastReceived = true;

        LOG.info("Flushing {} buffered rows", bufferedRows.size());
        bufferedRows.stream()
                .filter(row -> tenantId.equals(row.getField("tenant_id")))
                .peek(row -> LOG.info("Flushed buffered row for tenant {}", row.getField("tenant_id")))
                .forEach(out::collect);

        bufferedRows.clear();
    }

    @Override
    public void processElement(Row row, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
        String rowTenantId = (String) row.getField("tenant_id");
        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);

        if (state.contains(rowTenantId)) {
            LOG.info("Tenant match found. Emitting row.");
            out.collect(row);
        } else {
            LOG.info("No match for tenant {}, skipping", rowTenantId);
        }
    }

}