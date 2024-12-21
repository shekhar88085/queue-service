package com.example;

public interface QueueService {
    /**
     * Push a message onto a queue with a specified priority.
     *
     * @param queueUrl    the URL of the queue
     * @param messageBody the body of the message
     * @param priority    the priority of the message
     */
    public void push(String queueUrl, String messageBody, int priority);

    /**
     * Retrieves a single message from a queue.
     *
     * @param queueUrl the URL of the queue
     * @return the retrieved message
     */
    public Message pull(String queueUrl);

    /**
     * Deletes a message from the queue that was received by pull().
     *
     * @param queueUrl  the URL of the queue
     * @param receiptId the receipt ID of the message
     */
    public void delete(String queueUrl, String receiptId);
}