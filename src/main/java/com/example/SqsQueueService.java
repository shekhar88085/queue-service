package com.example;

import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;

public class SqsQueueService implements QueueService {
    // The QueueService implementation intended for a production environment.
    private final AmazonSQS sqs;

    public SqsQueueService(AmazonSQS sqsClient) {
        if (sqsClient == null) {
            throw new IllegalArgumentException("AmazonSQS client cannot be null");
        }
        this.sqs = sqsClient;
    }

    @Override
    public void push(String queueUrl, String messageBody, int priority) {
        // SQS does not natively support message priority.
        // The priority parameter is ignored here, but custom implementations can encode priority in the message body or use dedicated queues.
        if (queueUrl == null || queueUrl.isEmpty()) {
            throw new IllegalArgumentException("Queue URL cannot be null or empty");
        }
        if (messageBody == null) {
            throw new IllegalArgumentException("Message body cannot be null");
        }
        sqs.sendMessage(queueUrl, messageBody);
    }

    @Override
    public com.example.Message pull(String queueUrl) {
        if (queueUrl == null || queueUrl.isEmpty()) {
            throw new IllegalArgumentException("Queue URL cannot be null or empty");
        }

        List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();

        if (messages == null || messages.isEmpty()) {
            return null; // No messages available
        }

        // Retrieve the first message
        Message sqsMsg = messages.get(0);

        return new com.example.Message(sqsMsg.getBody(), sqsMsg.getReceiptHandle());
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        if (queueUrl == null || queueUrl.isEmpty()) {
            throw new IllegalArgumentException("Queue URL cannot be null or empty");
        }
        if (receiptId == null || receiptId.isEmpty()) {
            throw new IllegalArgumentException("Receipt ID cannot be null or empty");
        }
        sqs.deleteMessage(queueUrl, receiptId);
    }
}