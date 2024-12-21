package com.example;

public class Message {
    private final String body;
    private final int priority;
    private final long timestamp;
    private String receiptId;
    private int attempts;
    private long visibleFrom;

    // Existing constructor
    public Message(String body, int priority, long timestamp) {
        this.body = body;
        this.priority = priority;
        this.timestamp = timestamp;
        this.attempts = 0;
        this.visibleFrom = 0;
    }

    // New constructor for body and receiptId
    public Message(String body, String receiptId) {
        this.body = body;
        this.priority = 0; // Default priority if not provided
        this.timestamp = System.currentTimeMillis(); // Default to current time
        this.receiptId = receiptId;
        this.attempts = 0;
        this.visibleFrom = 0;
    }

    public String getBody() {
        return body;
    }

    public int getPriority() {
        return priority;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getReceiptId() {
        return receiptId;
    }

    protected int getAttempts() {
      return attempts;
    }

    public void setReceiptId(String receiptId) {
        this.receiptId = receiptId;
    }

    public void incrementAttempts() {
        this.attempts++;
    }

    public void setVisibleFrom(long visibleFrom) {
        this.visibleFrom = visibleFrom;
    }

    public boolean isVisibleAt(long now) {
        return now >= visibleFrom;
    }
}