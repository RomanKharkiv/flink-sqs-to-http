package com.sage.flink;

public class ParsedQuery {
    private final String query;
    private final String type;

    public ParsedQuery(String query, String type) {
        this.query = query;
        this.type = type;
    }

    public String getQuery() {
        return query;
    }

    public String getType() {
        return type;
    }
}