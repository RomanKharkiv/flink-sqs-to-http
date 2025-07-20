package com.sage.flink;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SqsSourceReader implements SourceReader<String, SqsSplit> {

    private final SqsClient sqsClient;
    private final String queueUrl;
    private final SourceReaderContext context;

    private boolean running = true;

    public SqsSourceReader(String queueUrl) {
        this.queueUrl = queueUrl;
        this.sqsClient = SqsClient.create();
        this.context = null;
    }

    @Override
    public void start() {
        // No-op; polling is synchronous in pollNext()
    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        if (!running) {
            return InputStatus.END_OF_INPUT;
        }

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(10)
                .maxNumberOfMessages(10)
                .build();

        List<Message> messages = sqsClient.receiveMessage(request).messages();

        if (messages.isEmpty()) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        for (Message message : messages) {
            output.collect(message.body());

            // Delete after emit (simplified; in real code consider retries, idempotency)
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
        }

        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<SqsSplit> snapshotState(long checkpointId) {
        return Collections.emptyList();  // stateless
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<SqsSplit> splits) {
    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() {
        running = false;
        sqsClient.close();
    }

    @Override
    public void handleSourceEvents(org.apache.flink.api.connector.source.SourceEvent sourceEvent) {
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
    }

    @Override
    public void pauseOrResumeSplits(Collection<String> splitsToPause, Collection<String> splitsToResume) {
        SourceReader.super.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
    }
}
