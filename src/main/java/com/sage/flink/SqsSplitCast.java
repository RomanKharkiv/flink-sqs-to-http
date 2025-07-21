package com.sage.flink;

import org.apache.flink.api.connector.source.SourceSplit;
import java.util.Objects;

public class SqsSplitCast implements SourceSplit {

    private final String splitId;

    public SqsSplitCast(String splitId) {
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
        if (!(o instanceof SqsSplitCast)) return false;
        SqsSplitCast that = (SqsSplitCast) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
