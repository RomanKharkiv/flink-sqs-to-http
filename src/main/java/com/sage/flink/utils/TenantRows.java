package com.sage.flink.utils;

import org.apache.flink.types.Row;

import java.util.List;

public class TenantRows {
    public String tenantId;
    public List<Row> rows;

    // Default constructor required for Flink POJO
    public TenantRows() {}

    public TenantRows(String tenantId, List<Row> rows) {
        this.tenantId = tenantId;
        this.rows = rows;
    }

    public String getTenantId() {
        return tenantId;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }
}
