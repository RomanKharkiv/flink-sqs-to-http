package com.sage.flink;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class SQSSourceFunction extends RichSourceFunction<String> {
    private volatile boolean running = true;

    private transient SqsClient sqsClient;
    private final String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/your-queue.fifo"; // <-- replace

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.sqsClient = SqsClient.create();
    }

    @Override
    public void run(SourceContext<String> ctx) {
        while (running) {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .waitTimeSeconds(10)      // long polling
                    .maxNumberOfMessages(10)
                    .build();

            List<Message> messages = sqsClient.receiveMessage(request).messages();

            for (Message message : messages) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(message.body());
                }

                // Delete message after processing (in real jobs add retry/error handling)
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
