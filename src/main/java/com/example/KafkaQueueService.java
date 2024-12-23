package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Collections;

public class KafkaQueueService {

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private String topic = "priorityQueue";  // Kafka topic for the priority queue

    public KafkaQueueService(String bootstrapServers, String groupId) {
        // Set up producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // Set up consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic)); // Subscribe to the queue topic
    }

    // Push a message into the Kafka queue with a priority
    public void push(String message, int priority) {
        long timestamp = System.currentTimeMillis();
        String messageWithTimestamp = timestamp + ":" + message;  // Create a timestamped message

        // Create a Kafka producer record with a key to ensure priority-based ordering
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(priority), messageWithTimestamp);

        // Send the message to Kafka
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
            } else {
                System.out.println("Sent message: " + messageWithTimestamp + " to partition: " + metadata.partition());
            }
        });
    }

    // Pull a message from the Kafka queue based on priority (lowest key = highest priority)
    public String pull() {
        // Poll the consumer to get messages
        while (true) {
            var records = consumer.poll(1000); // Poll for messages with a timeout of 1 second
            if (!records.isEmpty()) {
                // Process the record with the lowest priority (smallest key)
                var record = records.iterator().next();
                String messageWithTimestamp = record.value();

                // Split the message by timestamp (assuming it's in the format "timestamp:message")
                String[] parts = messageWithTimestamp.split(":");
                if (parts.length > 1) {
                    return parts[1]; // Return the message part (after the timestamp)
                } else {
                    return null;  // Handle unexpected format
                }
            }
        }
    }

    // Optionally, close the producer and consumer connections
    public void close() {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }
}