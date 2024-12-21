package com.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

public class UpstashQueueService {
    private Jedis jedis;
    private String queueKey = "priorityQueue"; // Redis key for the priority queue

    public UpstashQueueService(String redisUrl, String redisPassword, int port) {
        // Use Jedis with SSL enabled, no need for https://
        jedis = new Jedis(redisUrl, port, true);  // Ensure SSL is enabled by setting the third argument to true
        jedis.auth(redisPassword); // Authenticate with password if needed
        jedis.set("foo", "bar");
        String value = jedis.get("foo");
    }    

    // Push a message into the queue with a specific priority
    public void push(String message, int priority) {
        long timestamp = System.currentTimeMillis();
        String messageWithTimestamp = timestamp + ":" + message;  // Create a timestamped message
        // Use Redis Sorted Set where the score is the priority
        jedis.zadd(queueKey, priority, messageWithTimestamp);
    }

    // Pull a message from the queue based on priority (lowest score is highest priority)
    public String pull() {
        // Retrieve and remove the message with the highest priority (lowest score)
        Tuple tuple = jedis.zpopmin(queueKey);
        if (tuple != null) {
            // The message is stored as the value in the tuple
            String messageWithTimestamp = tuple.getElement();
            // Split the message by timestamp (assuming it's in the format "timestamp:message")
            String[] parts = messageWithTimestamp.split(":");
            if (parts.length > 1) {
                return parts[1]; // Return the message part (after the timestamp)
            } else {
                return null;  // Handle unexpected format
            }
        }
        return null; // Return null if no message is available
    }

    // Optionally, close the connection
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}