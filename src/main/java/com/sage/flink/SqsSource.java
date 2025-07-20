package com.sage.flink;

import org.apache.flink.api.connector.source.*;

import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Collections;
import java.util.List;

public class SqsSource implements Source<String, SqsSplit, Void> {

    private final String queueUrl;

    public SqsSource(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }


    @Override
    public SourceReader<String, SqsSplit> createReader(SourceReaderContext context) {
        return new SqsSourceReader(queueUrl);
    }

    @Override
    public SplitEnumerator<SqsSplit, Void> createEnumerator(SplitEnumeratorContext<SqsSplit> context) {
        return new SqsSplitEnumerator(queueUrl, context);
    }

    @Override
    public SplitEnumerator<SqsSplit, Void> restoreEnumerator(SplitEnumeratorContext<SqsSplit> context, Void checkpoint) {
        return new SqsSplitEnumerator(queueUrl, context);
    }

    @Override
    public SimpleVersionedSerializer<SqsSplit> getSplitSerializer() {
        return new SqsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new SqsEnumeratorSerializer();
    }
}
