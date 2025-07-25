package com.sage.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class CustomSqsSource<OUT> implements Source<OUT, CustomSqsSource.SqsSplit, CustomSqsSource.SqsEnumeratorState>, ResultTypeQueryable<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSqsSource.class);

    private final String queueUrl;
    private final String region;
    private final DeserializationSchema<OUT> deserializationSchema;
    private final int batchSize;
    private final int pollingIntervalMillis;

    private CustomSqsSource(
            String queueUrl,
            String region,
            DeserializationSchema<OUT> deserializationSchema,
            int batchSize,
            int pollingIntervalMillis) {
        this.queueUrl = queueUrl;
        this.region = region;
        this.deserializationSchema = deserializationSchema;
        this.batchSize = batchSize;
        this.pollingIntervalMillis = pollingIntervalMillis;
    }
    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, SqsSplit> createReader(SourceReaderContext readerContext) {
        return new SqsSourceReader<>(
                queueUrl,
                region,
                deserializationSchema,
                batchSize,
                pollingIntervalMillis,
                readerContext);
    }

    @Override
    public SplitEnumerator<SqsSplit, SqsEnumeratorState> createEnumerator(SplitEnumeratorContext<SqsSplit> enumContext) {
        return new SqsSplitEnumerator(enumContext, queueUrl, region);
    }

    @Override
    public SplitEnumerator<SqsSplit, SqsEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SqsSplit> enumContext, SqsEnumeratorState checkpoint) {
        return new SqsSplitEnumerator(enumContext, queueUrl, region);
    }

    @Override
    public SimpleVersionedSerializer<SqsSplit> getSplitSerializer() {
        return new SqsSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<SqsEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new SqsEnumeratorStateSerializer();
    }

    // Builder for the source
    public static <OUT> Builder<OUT> builder() {
        LOG.info("Starting builder...");
        return new Builder<>();
    }

    // Split class representing a partition of the source
    public static class SqsSplit implements SourceSplit {
        private final String splitId;



        public SqsSplit(String splitId) {
            this.splitId = splitId;
        }

        @Override
        public String splitId() {
            return splitId;
        }
    }

    // Enumerator state for checkpointing
    public static class SqsEnumeratorState {
        private String queueUrl;

        public SqsEnumeratorState(String queueUrl) {
            this.queueUrl = queueUrl;
        }

        public SqsEnumeratorState() {

        }

        public String getQueueUrl() {
            return queueUrl;
        }
    }

    // Split enumerator that assigns splits to readers
    private static class SqsSplitEnumerator implements SplitEnumerator<SqsSplit, SqsEnumeratorState> {
        private final SplitEnumeratorContext<SqsSplit> context;
        private final String queueUrl;
        private final String region;
        private final Map<Integer, List<SqsSplit>> pendingAssignments;

        public SqsSplitEnumerator(SplitEnumeratorContext<SqsSplit> context, String queueUrl, String region) {
            this.context = context;
            this.queueUrl = queueUrl;
            this.region = region;
            this.pendingAssignments = new ConcurrentHashMap<>();
        }

        public SqsSplitEnumerator(SplitEnumeratorContext<CustomSqsSource.SqsSplit> context,
                                  String queueUrl, String region,
                                  CustomSqsSource.SqsEnumeratorState state) {
            this(context, queueUrl, region);
            // Restore from checkpoint if needed
        }

        @Override
        public void start() {
            // No initialization needed
        }

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            // Create a split for the requesting subtask
            if (!context.registeredReaders().containsKey(subtaskId)) {
                LOG.warn("Subtask {} not registered in handleSplitRequest; skipping split assignment", subtaskId);
                return;
            }
            LOG.info("Handling split request for subtask {}", subtaskId);

            CustomSqsSource.SqsSplit split = new CustomSqsSource.SqsSplit(queueUrl + "-" + subtaskId);
            LOG.info("Assigning new split {} to subtask {}", split.splitId(), subtaskId);

            // Assign the split to the requesting subtask
            Map<Integer, List<CustomSqsSource.SqsSplit>> assignment = new HashMap<>();
            List<CustomSqsSource.SqsSplit> splits = new ArrayList<>();
            splits.add(split);
            assignment.put(subtaskId, splits);

            context.assignSplits(new SplitsAssignment<>(assignment));
        }

        @Override
        public void addSplitsBack(List<CustomSqsSource.SqsSplit> splits, int subtaskId) {
            // Store splits to be reassigned later
            pendingAssignments.computeIfAbsent(subtaskId, k -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {
            // When a new reader is added, check if there are pending splits to assign
            if (!context.registeredReaders().containsKey(subtaskId)) {
                LOG.warn("Subtask {} not registered; skipping split assignment", subtaskId);
                return;
            }

            if (pendingAssignments.containsKey(subtaskId)) {
                List<CustomSqsSource.SqsSplit> splits = pendingAssignments.remove(subtaskId);
                if (splits != null && !splits.isEmpty()) {
                    Map<Integer, List<CustomSqsSource.SqsSplit>> assignment = new HashMap<>();
                    assignment.put(subtaskId, splits);
                    context.assignSplits(new SplitsAssignment<>(assignment));
                }
            } else {
                // If no pending splits, create a new one for this subtask
                CustomSqsSource.SqsSplit split = new CustomSqsSource.SqsSplit(queueUrl + "-" + subtaskId);

                Map<Integer, List<CustomSqsSource.SqsSplit>> assignment = new HashMap<>();
                List<CustomSqsSource.SqsSplit> splits = new ArrayList<>();
                splits.add(split);
                assignment.put(subtaskId, splits);

                context.assignSplits(new SplitsAssignment<>(assignment));
            }
        }

        @Override
        public SqsEnumeratorState snapshotState(long checkpointId) throws Exception {
            return new SqsEnumeratorState(queueUrl);
        }

        @Override
        public void close() throws IOException {
            // No resources to clean up
        }
    }

    private static class SqsSourceReader<OUT> implements SourceReader<OUT, SqsSplit> {
        private final SourceReaderContext context;
        private final Queue<SqsSplit> assignedSplits;
        private final AtomicBoolean isRunning;
        private final String queueUrl;
        private final String region;
        private final DeserializationSchema<OUT> deserializationSchema;
        private final int batchSize;
        private final int pollingIntervalMillis;
        private SqsClient sqsClient;

        public SqsSourceReader(
                String queueUrl,
                String region,
                DeserializationSchema<OUT> deserializationSchema,
                int batchSize,
                int pollingIntervalMillis,
                SourceReaderContext context) {
            this.queueUrl = queueUrl;
            this.region = region;
            this.deserializationSchema = deserializationSchema;
            this.batchSize = batchSize;
            this.pollingIntervalMillis = pollingIntervalMillis;
            this.context = context;
            this.assignedSplits = new ConcurrentLinkedQueue<>();
            this.isRunning = new AtomicBoolean(true);
        }

        @Override
        public void start() {
            this.sqsClient = SqsClient.builder()
                    .region(Region.of(region))
                    .build();
        }

        @Override
        public List<SqsSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(assignedSplits);
        }

        @Override
        public void addSplits(List<SqsSplit> splits) {
            assignedSplits.addAll(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            // Nothing to do here
        }

        @Override
        public void close() throws Exception {
            isRunning.set(false);
            if (sqsClient != null) {
                sqsClient.close();
            }
            // No return value needed
        }


        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // Nothing to do here
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
            LOG.info("SQS SourceReader polling for messages on subtask {}", context.getIndexOfSubtask());
            if (sqsClient == null) {
                LOG.warn("SQS client is not initialized. Skipping poll.");
                return InputStatus.NOTHING_AVAILABLE;
            }

            if (!isRunning.get() || assignedSplits.isEmpty()) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            LOG.info("Poll for messages from SQS");
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(batchSize)
                    .waitTimeSeconds(20)
                    .build();

            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
            List<Message> messages = response.messages();

            if (messages.isEmpty()) {
                LOG.info("Raw SQS message is empty");
                Thread.sleep(pollingIntervalMillis);
                return InputStatus.NOTHING_AVAILABLE;
            }

            for (Message message : messages) {
                try {
                    processAndAck(message, output);
                } catch (Exception e) {
                    LOG.warn("First attempt failed for message: {}", message.messageId(), e);
                    try {
                        processAndAck(message, output);
                    } catch (Exception ex) {
                        LOG.error("Retry failed for message: {}", message.messageId(), ex);
                    }
                }
            }

            return messages.size() < batchSize ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
        }

        private void processAndAck(Message message, ReaderOutput<OUT> output) throws IOException {
            LOG.info("Raw SQS message: {}", message.body());
            OUT element = deserializationSchema.deserialize(message.body().getBytes());
            output.collect(element);

            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteRequest);
        }

    }



    private static class SqsSplitSerializer implements SimpleVersionedSerializer<SqsSplit> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(SqsSplit split) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeUTF(split.splitId());
            return out.getCopyOfBuffer();
        }

        @Override
        public SqsSplit deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            String id = in.readUTF();
            return new SqsSplit(id);
        }
    }

    private static class SqsEnumeratorStateSerializer implements SimpleVersionedSerializer<SqsEnumeratorState> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(SqsEnumeratorState state) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeUTF(state.getQueueUrl());
            return out.getCopyOfBuffer();
        }

        @Override
        public SqsEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            String queueUrl = in.readUTF();
            return new SqsEnumeratorState(queueUrl);
        }
    }

    public static class Builder<OUT> {
        private String queueUrl;
        private String region = "eu-west-1";
        private DeserializationSchema<OUT> deserializationSchema;
        private int batchSize = 10;
        private int pollingIntervalMillis = 1000;

        public Builder<OUT> queueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public Builder<OUT> batchSize(int size) {
            this.batchSize = size;
            return this;
        }

        public Builder<OUT> region(String region) {
            this.region = region;
            return this;
        }

        public Builder<OUT> deserializationSchema(DeserializationSchema<OUT> schema) {
            this.deserializationSchema = schema;
            return this;
        }

        public Builder<OUT> pollingIntervalMillis(int pollingIntervalMillis) {
            this.pollingIntervalMillis = pollingIntervalMillis;
            return this;
        }

        public CustomSqsSource<OUT> build() {
            if (queueUrl == null) {
                throw new IllegalArgumentException("Queue URL must be specified");
            }
            if (deserializationSchema == null) {
                throw new IllegalArgumentException("Deserialization schema must be specified");
            }
            return new CustomSqsSource<>(
                    queueUrl,
                    region,
                    deserializationSchema,
                    batchSize,
                    pollingIntervalMillis);
        }
    }
}
