package com.example;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static java.nio.file.StandardCopyOption.*;

public class FileQueueService implements QueueService {
    private final String queueDir;
    private final String fieldDelimiter;
    private final int visibilityTimeout;
    private LongSupplier timeSupplier;

    public FileQueueService() {
        Properties confInfo = new Properties();
        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            confInfo.load(inStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load configuration", e);
        }

        this.queueDir = confInfo.getProperty("queueDirectory", "queue-service");
        this.fieldDelimiter = confInfo.getProperty("fieldDelimiter", ":");
        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    public void setTimeSupplier(LongSupplier timeSupplier) {
        this.timeSupplier = timeSupplier;
    }

    private void lock(File lock) throws InterruptedException {
        while (!lock.mkdir()) {
            Thread.sleep(50);
        }
    }

    private void unlock(File lock) {
        if (!lock.delete()) {
            System.err.println("Failed to delete lock file: " + lock.getAbsolutePath());
        }
    }

    @Override
    public void push(String queueUrl, String messageBody, int priority) {
        String queueName = fromUrl(queueUrl);
        File messagesFile = getMessagesFile(queueName);
        File lock = getLockFile(queueName);

        try {
            lock(lock);

            if (Files.notExists(messagesFile.toPath())) {
                Files.createFile(messagesFile.toPath());
            }

            try (PrintWriter writer = new PrintWriter(new FileWriter(messagesFile, true))) {
                writer.println(createRecord(0, priority, messageBody));
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to push message", e);
        } finally {
            unlock(lock);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        String queueName = fromUrl(queueUrl);
        File messagesFile = getMessagesFile(queueName);
        File lock = getLockFile(queueName);

        Path tempFilePath;
        Message message = null;

        try {
            lock(lock);
            tempFilePath = Files.createTempFile(Paths.get(queueDir), "temp", ".msg");

            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(tempFilePath.toFile(), true))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    if (message == null) {
                        message = getVisibleMessage(line);

                        if (message == null) {
                            writer.println(line);
                        } else {
                            writer.println(getDeliveredRecord(line, message.getReceiptId()));
                        }
                    } else {
                        writer.println(line);
                    }
                }
            }

            if (message != null) {
                Files.move(tempFilePath, messagesFile.toPath(), REPLACE_EXISTING);
            } else {
                Files.delete(tempFilePath);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to pull message", e);
        } finally {
            unlock(lock);
        }

        return message;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        String queueName = fromUrl(queueUrl);
        File messagesFile = getMessagesFile(queueName);
        File lock = getLockFile(queueName);

        Path tempFilePath;
        boolean deleted = false;

        try {
            lock(lock);
            tempFilePath = Files.createTempFile(Paths.get(queueDir), "temp", ".msg");

            try (BufferedReader reader = new BufferedReader(new FileReader(messagesFile));
                 PrintWriter writer = new PrintWriter(new FileWriter(tempFilePath.toFile(), true))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    if (isToDelete(line, receiptId)) {
                        deleted = true;
                    } else {
                        writer.println(line);
                    }
                }
            }

            if (deleted) {
                Files.move(tempFilePath, messagesFile.toPath(), REPLACE_EXISTING);
            } else {
                Files.delete(tempFilePath);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to delete message", e);
        } finally {
            unlock(lock);
        }
    }

    // Additional helper methods...

    private String fromUrl(String queueUrl) {
        String[] parts = queueUrl.split("/");
        return parts[parts.length - 1];
    }

    private File getMessagesFile(String queueName) {
        return Paths.get(queueDir, queueName, "messages").toFile();
    }

    private File getLockFile(String queueName) {
        Path queuePath = Paths.get(queueDir, queueName);
        try {
            Files.createDirectories(queuePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create queue directory", e);
        }
        return queuePath.resolve(".lock").toFile();
    }

    private String createRecord(int attempts, int priority, String messageBody) {
        return attempts + fieldDelimiter + now() + fieldDelimiter + UUID.randomUUID() + fieldDelimiter + priority + fieldDelimiter + messageBody;
    }

    private Message getVisibleMessage(String record) {
        String[] fields = record.split(fieldDelimiter, 5);
        if (fields.length < 5 || Long.parseLong(fields[1]) > now()) {
            return null;
        }
        return new Message(fields[4], fields[2]);
    }

    private String getDeliveredRecord(String record, String receiptId) {
        String[] fields = record.split(fieldDelimiter, 5);
        int attempts = Integer.parseInt(fields[0]) + 1;
        long visibleFrom = now() + TimeUnit.SECONDS.toMillis(visibilityTimeout);
        return attempts + fieldDelimiter + visibleFrom + fieldDelimiter + receiptId + fieldDelimiter + fields[3] + fieldDelimiter + fields[4];
    }

    private boolean isToDelete(String record, String receiptId) {
        String[] fields = record.split(fieldDelimiter, 5);
        return fields.length >= 5 && fields[2].equals(receiptId) && Long.parseLong(fields[1]) <= now();
    }

    private long now() {
        return timeSupplier == null ? System.currentTimeMillis() : timeSupplier.getAsLong();
    }

    /**
   * Deletes the messages in a queue specified by parameter queueUrl.
   *
   * @param queueUrl
   */
  protected void purgeQueue(String queueUrl) {
    String queueName = fromUrl(queueUrl);
    File messageFile = getMessagesFile(queueName);
    File lock = getLockFile(queueName);

    try {
      lock(lock);
    } catch (InterruptedException e) {
      e.printStackTrace();
      unlock(lock);
      return;
    }

    if (Files.exists(messageFile.toPath())) {
      try {
        Files.delete(messageFile.toPath());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      // Create the empty file with default permissions.
      Files.createFile(messageFile.toPath());
    } catch (IOException e) {
    }

    unlock(lock);
  }
}