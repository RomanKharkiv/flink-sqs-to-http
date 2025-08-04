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

public class TenantRowLookupFunction extends BroadcastProcessFunction<String, Row, LabeledRow> {
    private static final Logger LOG = LoggerFactory.getLogger(TenantRowLookupFunction.class);

    private final MapStateDescriptor<String, List<Row>> broadcastStateDescriptor;
    private final String[] fieldNames;

    public TenantRowLookupFunction(MapStateDescriptor<String, List<Row>> broadcastStateDescriptor, String[] fieldNames) {
        this.broadcastStateDescriptor = broadcastStateDescriptor;
        this.fieldNames = fieldNames;
    }

    @Override
    public void processBroadcastElement(Row row, Context ctx, Collector<LabeledRow> out) throws Exception {
        Object rowTenantId = row.getField("tenant_id");

        if (rowTenantId == null) {
//            LOG.warn("Skipping row with null tenant_id: {}", row);
            return;
        }

        String tenantId = rowTenantId.toString();

        List<Row> rowsForTenant = ctx.getBroadcastState(broadcastStateDescriptor).get(tenantId);
        if (rowsForTenant == null) {
            rowsForTenant = new ArrayList<>();
        }

        rowsForTenant.add(row);
        ctx.getBroadcastState(broadcastStateDescriptor).put(tenantId, rowsForTenant);

        LOG.debug("Added row to broadcast state for tenant: {}", tenantId);
    }

    @Override
    public void processElement(String tenantId, ReadOnlyContext ctx, Collector<LabeledRow> out) throws Exception {
        if (tenantId == null) {
            LOG.warn("Received null tenantId from SQS — skipping");
            return;
        }

        ReadOnlyBroadcastState<String, List<Row>> state = ctx.getBroadcastState(broadcastStateDescriptor);
        List<Row> tenantRows = state.get(tenantId);

        if (tenantRows == null || tenantRows.isEmpty()) {
            LOG.warn("No rows found in broadcast state for tenant: {}", tenantId);
            return;
        }

        for (Row row : tenantRows) {
            out.collect(new LabeledRow(row, fieldNames));
        }

        LOG.info("Emitted {} rows for tenant {}", tenantRows.size(), tenantId);
    }
}
