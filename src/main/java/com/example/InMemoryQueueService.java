package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService implements QueueService {

  private final Map<String, PriorityQueue<Message>> queues;
  protected long visibilityTimeout;

  public InMemoryQueueService() {
    this.queues = new ConcurrentHashMap<>();
    String propFileName = "config.properties";
    Properties confInfo = new Properties();

    try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
      confInfo.load(inStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
  }

  @Override
  public void push(String queueUrl, String msgBody, int priority) {
    PriorityQueue<Message> queue = queues.get(queueUrl);
    if (queue == null) {
      queue = new PriorityQueue<>(new MessageComparator());
      queues.put(queueUrl, queue);
    }
    queue.add(new Message(msgBody, priority, System.currentTimeMillis()));
  }

  @Override
  public Message pull(String queueUrl) {
    PriorityQueue<Message> queue = queues.get(queueUrl);
    if (queue == null) {
      return null;
    }

    long nowTime = now();
    Optional<Message> msgOpt = queue.stream()
        .filter(m -> m.isVisibleAt(nowTime))
        .findFirst();

    if (msgOpt.isEmpty()) {
      return null;
    } else {
      Message msg = msgOpt.get();
      queue.remove(msg);
      msg.setReceiptId(UUID.randomUUID().toString());
      msg.incrementAttempts();
      msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));

      return new Message(msg.getBody(), msg.getReceiptId());
    }
  }

  @Override
  public void delete(String queueUrl, String receiptId) {
    PriorityQueue<Message> queue = queues.get(queueUrl);
    if (queue != null) {
      long nowTime = now();

      for (Iterator<Message> it = queue.iterator(); it.hasNext();) {
        Message msg = it.next();
        if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
          it.remove();
          break;
        }
      }
    }
  }

  long now() {
    return System.currentTimeMillis();
  }

  // Custom comparator for handling priorities and FCFS
  private static class MessageComparator implements Comparator<Message> {
    @Override
    public int compare(Message m1, Message m2) {
      if (m1.getPriority() != m2.getPriority()) {
        return Integer.compare(m2.getPriority(), m1.getPriority()); // Higher priority first
      }
      return Long.compare(m1.getTimestamp(), m2.getTimestamp()); // FCFS for equal priority
    }
  }
}