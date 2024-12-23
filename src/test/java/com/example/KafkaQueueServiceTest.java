package com.example;

public class KafkaQueueServiceTest {
    public static void main(String[] args) {
        // Kafka configuration - replace with your Kafka broker's address and consumer group
        String bootstrapServers = "localhost:9092";
        String groupId = "test-group";

        // Create an instance of KafkaQueueService
        KafkaQueueService queueService = new KafkaQueueService(bootstrapServers, groupId);

        // Push some messages with different priorities
        System.out.println("Pushing messages with varying priorities...");
        queueService.push("Message with priority 1", 1);
        queueService.push("Message with priority 5", 5);
        queueService.push("Message with priority 3", 3);
        queueService.push("Message with priority 10", 10);
        queueService.push("Message with priority 2", 2);

        System.out.println("Pushed messages, now wait for 2 seconds");
        // Wait a moment to ensure messages are sent before pulling
        try {
            Thread.sleep(2000);  // Sleep for 2 seconds to ensure messages are in the queue
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Wait over");

        // Pull and process the messages in priority order
        System.out.println("Pulling messages based on priority...");
        String message;
        while ((message = queueService.pull()) != null) {
            System.out.println("Pulled message: " + message);
        }

        // Close the queue service (producer and consumer)
        queueService.close();
    }
}
