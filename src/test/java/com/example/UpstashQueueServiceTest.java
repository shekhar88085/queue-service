package com.example;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class UpstashQueueServiceTest {

    private UpstashQueueService queueService;

    @Before
    public void setup() {
        // Initialize the queue service with Redis URL and password
        // Remove "https://" from the URL, and specify the port and password.
        queueService = new UpstashQueueService("improved-elephant-28431.upstash.io",
                "AW8PAAIjcDEzMzAzZDA2NzMwMzg0YzllYjk2ZTExNWE1NTAwNTBkZXAxMA",
                6379);
    }

    @After
    public void cleanup() {
        // Close the Redis connection after tests
        queueService.close();
    }

    @Test
    public void testPushPullWithPriority() {
        queueService.push("High Priority", 1);
        queueService.push("Medium Priority", 5);
        queueService.push("Low Priority", 10);

        // Test that messages are pulled in priority order
        assertEquals("High Priority", queueService.pull()); // Should be dequeued first
        assertEquals("Medium Priority", queueService.pull());
        assertEquals("Low Priority", queueService.pull());
    }

    @Test
    public void testFCFSForSamePriority() {
        // Push two messages with the same priority
        queueService.push("Message A", 1);
        queueService.push("Message B", 1);

        // Test that the messages are pulled in the same order they were added (FCFS)
        assertEquals("Message A", queueService.pull()); // First message with same priority
        assertEquals("Message B", queueService.pull());
    }

    @Test
    public void testEmptyQueue() {
        // Pull from an empty queue should return null
        assertEquals(null, queueService.pull());
    }

    @Test
    public void testOrderWithMultiplePriorities() {
        // Push multiple messages with varying priorities
        queueService.push("Message 1", 3);
        queueService.push("Message 2", 1);
        queueService.push("Message 3", 2);

        // Pull messages in the correct order based on priority
        assertEquals("Message 2", queueService.pull()); // Priority 1 (lowest)
        assertEquals("Message 3", queueService.pull()); // Priority 2
        assertEquals("Message 1", queueService.pull()); // Priority 3 (highest)
    }
}