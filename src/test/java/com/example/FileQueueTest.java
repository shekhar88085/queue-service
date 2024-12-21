package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class FileQueueTest {
    private FileQueueService qs;
    private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";

    @Before
    public void setup() {
        qs = new FileQueueService();  // Ensure the service is instantiated.
        qs.purgeQueue(queueUrl);  // Clear the queue before each test
    }

    @Test
    public void testSendMessage() {
        String message = "Good message!";
        qs.push(queueUrl, message, 1);
        Message msg = qs.pull(queueUrl);

        // Check if the message is successfully pushed and pulled
        assertTrue(msg != null && msg.getBody().equals(message));
    }

    @Test
    public void testPullMessage() {
        String msgBody = "{\"name\":\"John\",\"age\":30,\"cars\": {\"car1\":\"Ford\",\"car2\":\"BMW\"}}";
        qs.push(queueUrl, msgBody, 1);
        Message msg = qs.pull(queueUrl);

        // Validate the body and receiptId of the message pulled
        assertEquals(msgBody, msg.getBody());
        assertTrue(msg.getReceiptId() != null && !msg.getReceiptId().isEmpty());
    }

    @Test
    public void testPullEmptyQueue() {
        // Test pulling from an empty queue
        Message msg = qs.pull(queueUrl);
        assertNull(msg);  // Ensure that no message is returned
    }

    @Test
    public void testDoublePull() {
        String message = "Message A.";
        qs.push(queueUrl, message, 1);
        qs.pull(queueUrl);  // First pull
        Message msg = qs.pull(queueUrl);  // Second pull

        assertNull(msg);  // Ensure no message is available after the first pull
    }

    @Test
    public void testDeleteMessage() {
        String msgBody = "Message A.";
        qs.push(queueUrl, msgBody, 1);
        Message msg = qs.pull(queueUrl);

        qs.delete(queueUrl, msg.getReceiptId());  // Delete the message using receiptId
        msg = qs.pull(queueUrl);

        assertNull(msg);  // Ensure that the message is deleted and cannot be pulled again
    }

    @Test
    public void testFIFO3Msgs() {
        String[] msgStrs = {"Test msg 1", "Test msg 2", "Test Message 3."};
        qs.push(queueUrl, msgStrs[0], 1);
        qs.push(queueUrl, msgStrs[1], 2);
        qs.push(queueUrl, msgStrs[2], 3);

        // Validate the order in which messages are pulled (FIFO)
        Message msg1 = qs.pull(queueUrl);
        Message msg2 = qs.pull(queueUrl);
        Message msg3 = qs.pull(queueUrl);

        assertTrue(msg1.getBody().equals(msgStrs[0]) &&
                   msg2.getBody().equals(msgStrs[1]) &&
                   msg3.getBody().equals(msgStrs[2]));
    }

    /**
     * Test delete/acknowledge timeout.
     */
    @Test
    public void testAckTimeout() {
        FileQueueService queueService = new FileQueueService();
        String message = "Message A.";
        queueService.push(queueUrl, message, 1);
        queueService.pull(queueUrl);  // Simulate message processing
        queueService.setTimeSupplier(() -> System.currentTimeMillis() + 1000 * 30 + 1);  // Set the time supplier for timeout

        // Pull the message after the timeout period
        Message msg = queueService.pull(queueUrl);
        assertTrue(msg != null && msg.getBody().equals(message));
    }
}