package com.sage.flink;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SqsSplitEnumerator implements SplitEnumerator<SqsSplit, Void> {

    private final SplitEnumeratorContext<SqsSplit> context;
    private final String queueUrl;
    private final Set<Integer> readersAwaitingAssignment = new HashSet<>();
    private boolean splitAssigned = false;

    public SqsSplitEnumerator(String queueUrl, SplitEnumeratorContext<SqsSplit> context) {
        this.queueUrl = queueUrl;
        this.context = context;
    }

    @Override
    public void start() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, String hostname) {
        readersAwaitingAssignment.add(subtaskId);
        assignSplitIfNeeded();
    }

    @Override
    public void addReader(int subtaskId) {
        readersAwaitingAssignment.add(subtaskId);
        assignSplitIfNeeded();
    }

    private void assignSplitIfNeeded() {
        if (!splitAssigned && !readersAwaitingAssignment.isEmpty()) {
            int readerId = readersAwaitingAssignment.iterator().next();
            SqsSplit split = new SqsSplit("sqs-queue-0");
            context.assignSplit(split, readerId);
            context.signalNoMoreSplits(readerId);
            splitAssigned = true;
            readersAwaitingAssignment.clear();
        }
    }

    @Override
    public void addSplitsBack(List<SqsSplit> splits, int subtaskId) {
    }

    @Override
    public Void snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
    }

    @Override
    public void handleSourceEvent(int subtaskId, org.apache.flink.api.connector.source.SourceEvent sourceEvent) {
    }

    @Override
    public void close() throws IOException {
    }
}
