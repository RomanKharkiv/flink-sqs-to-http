package com.sage.flink;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;


public class SqsSourceCast implements Source<String, SqsSplitCast, Void> {

    private final String queueUrl;

    public SqsSourceCast(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }


    @Override
    public SourceReader<String, SqsSplitCast> createReader(SourceReaderContext context) {
        return new SqsSourceReader(queueUrl);
    }

    @Override
    public SplitEnumerator<SqsSplitCast, Void> createEnumerator(SplitEnumeratorContext<SqsSplitCast> context) {
        return new SqsSplitEnumerator(queueUrl, context);
    }

    @Override
    public SplitEnumerator<SqsSplitCast, Void> restoreEnumerator(SplitEnumeratorContext<SqsSplitCast> context, Void checkpoint) {
        return new SqsSplitEnumerator(queueUrl, context);
    }

    @Override
    public SimpleVersionedSerializer<SqsSplitCast> getSplitSerializer() {
        return new SqsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SqsEnumeratorSerializer();
    }
}
