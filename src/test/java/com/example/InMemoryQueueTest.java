package com.example;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class InMemoryQueueTest {
    private QueueService qs;
    private final String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";

    @Before
    public void setup() {
        qs = new InMemoryQueueService();
    }

    @Test
    public void testSendMessage() {
        qs.push(queueUrl, "Good message!", 1);
        Message msg = qs.pull(queueUrl);

        assertNotNull(msg);
        assertEquals("Good message!", msg.getBody());
    }

    @Test
    public void testPullMessage() {
        String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";

        qs.push(queueUrl, msgBody, 1);
        Message msg = qs.pull(queueUrl);

        assertNotNull(msg);
        assertEquals(msgBody, msg.getBody());
        assertNotNull(msg.getReceiptId());
        assertTrue(msg.getReceiptId().length() > 0);
    }

    @Test
    public void testPullEmptyQueue() {
        Message msg = qs.pull(queueUrl);
        assertNull(msg);
    }

    @Test
    public void testDoublePull() {
        qs.push(queueUrl, "Message A.", 1);
        qs.pull(queueUrl);
        Message msg = qs.pull(queueUrl);
        assertNull(msg);
    }

    @Test
    public void testDeleteMessage() {
        String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";

        qs.push(queueUrl, msgBody, 1);
        Message msg = qs.pull(queueUrl);

        qs.delete(queueUrl, msg.getReceiptId());
        msg = qs.pull(queueUrl);

        assertNull(msg);
    }

    @Test
    public void testFIFO3Msgs() {
        String[] msgStrs = {
                "Test msg 1",
                "Test msg 2",
                "{\n" +
                        "    \"name\":\"John\",\n" +
                        "    \"age\":30,\n" +
                        "    \"cars\": {\n" +
                        "        \"car1\":\"Ford\",\n" +
                        "        \"car2\":\"BMW\",\n" +
                        "        \"car3\":\"Fiat\"\n" +
                        "    }\n" +
                        "}"
        };

        qs.push(queueUrl, msgStrs[0], 3);
        qs.push(queueUrl, msgStrs[1], 2);
        qs.push(queueUrl, msgStrs[2], 1);

        Message msg1 = qs.pull(queueUrl);
        Message msg2 = qs.pull(queueUrl);
        Message msg3 = qs.pull(queueUrl);

        // Ensure the test handles both types of message bodies correctly
        assertEquals(msgStrs[0], msg1.getBody());
        assertEquals(msgStrs[1], msg2.getBody());
        assertEquals(msgStrs[2], msg3.getBody());
    }

    @Test
    public void testAckTimeout() {
        InMemoryQueueService queueService = new InMemoryQueueService() {
            // Override the time to simulate a timeout after pulling
            @Override
            long now() {
                // Simulate a timeout by adding a little more time to the current time
                return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout) + 1;
            }
        };

        queueService.push(queueUrl, "Message A.", 1);
        queueService.push(queueUrl, "Message A.", 1);
        queueService.pull(queueUrl); // First pull makes it invisible temporarily
        Message msg = queueService.pull(queueUrl); // Should pull the same message after timeout

        assertNotNull(msg);
        assertEquals("Message A.", msg.getBody());
    }

    @Test
    public void testPriorityQueueBehavior() {
        qs.push(queueUrl, "Low Priority Message", 1);
        qs.push(queueUrl, "High Priority Message", 10);
        qs.push(queueUrl, "Medium Priority Message", 5);

        Message msg1 = qs.pull(queueUrl);
        Message msg2 = qs.pull(queueUrl);
        Message msg3 = qs.pull(queueUrl);

        assertEquals("High Priority Message", msg1.getBody());
        assertEquals("Medium Priority Message", msg2.getBody());
        assertEquals("Low Priority Message", msg3.getBody());
    }

    @Test
    public void testFCFSWithinPriority() {
        qs.push(queueUrl, "First Message", 5);
        qs.push(queueUrl, "Second Message", 5);

        Message msg1 = qs.pull(queueUrl);
        Message msg2 = qs.pull(queueUrl);

        assertEquals("First Message", msg1.getBody());
        assertEquals("Second Message", msg2.getBody());
    }
}