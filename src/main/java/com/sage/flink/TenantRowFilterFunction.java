package com.sage.flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantRowFilterFunction extends BroadcastProcessFunction<Row, String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(TenantRowFilterFunction.class);

    private final MapStateDescriptor<String, String> stateDescriptor;

    public TenantRowFilterFunction(MapStateDescriptor<String, String> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
        LOG.info("TenantRowFilterFunction created");
    }

    @Override
    public void processBroadcastElement(
            String tenantId,
            Context ctx,
            Collector<Row> out) throws Exception {
        LOG.info("TenantRowFilterFunction processBroadcastElement: {}.", tenantId);
        ctx.getBroadcastState(stateDescriptor).put(tenantId, "active");
    }

    @Override
    public void processElement(
            Row row,
            ReadOnlyContext ctx,
            Collector<Row> out) throws Exception {
        LOG.info("TenantRowFilterFunction processElement: {}.", row.toString());
        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
        LOG.info("TenantRowFilterFunction processElement state: {}.", state.toString());
        String rowTenantId = (String) row.getField("tenant_id");
        LOG.info("TenantRowFilterFunction processElement rowTenantId: {}.", rowTenantId);

        if (state.contains(rowTenantId)) {
            LOG.info("TenantRowFilterFunction processElement contains");
            out.collect(row);  // Only emit rows that match active tenant_id
        }
        out.collect(row);
    }
}

