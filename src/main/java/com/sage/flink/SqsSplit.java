package com.sage.flink;

import org.apache.flink.api.connector.source.SourceSplit;
import java.util.Objects;

public class SqsSplit implements SourceSplit {

    private final String splitId;

    public SqsSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "SqsSplit{" + "splitId='" + splitId + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqsSplit)) return false;
        SqsSplit that = (SqsSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
