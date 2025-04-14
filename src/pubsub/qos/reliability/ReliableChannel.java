package src.pubsub.qos.reliability;

import src.pubsub.core.Channel;
import src.pubsub.core.Consumer;
import src.pubsub.core.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A channel implementation that handles message loss.
 * Used for R8: Dropped messages.
 */
public class ReliableChannel implements Channel {
    private final String name;
    private final Set<ReliableConsumer> subscribers = new HashSet<>();
    private final Map<String, TrackableMessage> pendingMessages = new ConcurrentHashMap<>();
    private final Map<String, TrackableMessage> sentMessages = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    
    // Configuration
    private final long messageTimeToLiveMs;
    private final long resendIntervalMs;
    private final int maxDeliveryAttempts;
    private boolean simulateMessageLoss;
    private double messageLossProbability;
    
    // Stats
    private int messagesSent = 0;
    private int messagesAcked = 0;
    private int messagesResent = 0;
    private int messagesFailed = 0;
    
    /**
     * Creates a new reliable channel.
     * 
     * @param name the channel name
     * @param messageTimeToLiveMs the message time-to-live in milliseconds
     * @param resendIntervalMs the interval to check and resend lost messages
     * @param maxDeliveryAttempts the maximum number of delivery attempts
     */
    public ReliableChannel(String name, long messageTimeToLiveMs, 
            long resendIntervalMs, int maxDeliveryAttempts) {
        this.name = name;
        this.messageTimeToLiveMs = messageTimeToLiveMs;
        this.resendIntervalMs = resendIntervalMs;
        this.maxDeliveryAttempts = maxDeliveryAttempts;
        this.simulateMessageLoss = false;
        this.messageLossProbability = 0.0;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start periodic tasks
        startResendTask();
        startCleanupTask();
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void addEvent(Event event) {
        TrackableMessage message = new TrackableMessage(event, name, messageTimeToLiveMs);
        
        // Register all current subscribers
        for (ReliableConsumer consumer : subscribers) {
            message.registerConsumer(consumer.getId());
        }
        
        // Add to pending queue
        pendingMessages.put(message.getMessageId(), message);
    }
    
    @Override
    public void subscribe(Consumer consumer) {
        if (consumer instanceof ReliableConsumer) {
            ReliableConsumer reliableConsumer = (ReliableConsumer) consumer;
            subscribers.add(reliableConsumer);
            
            // Register this consumer for all pending messages
            for (TrackableMessage message : pendingMessages.values()) {
                message.registerConsumer(reliableConsumer.getId());
            }
            
            // Register this consumer for all in-flight messages
            for (TrackableMessage message : sentMessages.values()) {
                message.registerConsumer(reliableConsumer.getId());
            }
        } else {
            throw new IllegalArgumentException(
                    "Consumer must be a ReliableConsumer for reliable message delivery");
        }
    }
    
    @Override
    public void unsubscribe(Consumer consumer) {
        if (consumer instanceof ReliableConsumer) {
            subscribers.remove(consumer);
        }
    }
    
    @Override
    public void dispatchEvents() {
        if (pendingMessages.isEmpty()) {
            return;
        }
        
        List<String> dispatchedMessageIds = new ArrayList<>();
        
        // Try to dispatch all pending messages
        for (TrackableMessage message : pendingMessages.values()) {
            deliverMessage(message);
            dispatchedMessageIds.add(message.getMessageId());
            messagesSent++;
        }
        
        // Move dispatched messages from pending to sent
        for (String messageId : dispatchedMessageIds) {
            TrackableMessage message = pendingMessages.remove(messageId);
            if (message != null) {
                sentMessages.put(messageId, message);
            }
        }
    }
    
    /**
     * Delivers a message to all subscribers.
     * 
     * @param message the message to deliver
     */
    private void deliverMessage(TrackableMessage message) {
        message.incrementDeliveryAttempts();
        message.setStatus(TrackableMessage.DeliveryStatus.SENT);
        
        for (ReliableConsumer consumer : subscribers) {
            String consumerId = consumer.getId();
            
            // Skip consumers that have already acknowledged this message
            if (message.getAcknowledgedConsumers().contains(consumerId)) {
                continue;
            }
            
            // Simulate message loss if enabled
            if (simulateMessageLoss && Math.random() < messageLossProbability) {
                System.out.println("Simulating message loss for " + message.getMessageId() + 
                        " to consumer " + consumerId);
                continue;
            }
            
            try {
                // Send the message with a reference to this channel for acknowledgment
                consumer.receiveMessage(message.getEvent(), message.getMessageId(), name);
            } catch (Exception e) {
                System.err.println("Error delivering message to consumer " + 
                        consumerId + ": " + e.getMessage());
            }
        }
    }
    
    @Override
    public List<Consumer> getSubscribers() {
        return new ArrayList<>(subscribers);
    }
    
    @Override
    public int getQueueSize() {
        return pendingMessages.size() + sentMessages.size();
    }
    
    /**
     * Processes an acknowledgment from a consumer.
     * 
     * @param ack the acknowledgment
     */
    public void processAcknowledgment(MessageAcknowledgment ack) {
        String messageId = ack.getMessageId();
        String consumerId = ack.getConsumerId();
        
        TrackableMessage message = sentMessages.get(messageId);
        if (message != null) {
            boolean newAck = message.acknowledgeConsumer(consumerId);
            
            if (newAck) {
                System.out.println("Received ACK from " + consumerId + 
                        " for message " + messageId);
                
                if (message.isFullyAcknowledged()) {
                    System.out.println("Message " + messageId + " fully acknowledged");
                    messagesAcked++;
                    sentMessages.remove(messageId);
                }
            }
        }
    }
    
    /**
     * Starts the periodic task to resend unacknowledged messages.
     */
    private void startResendTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                resendUnacknowledgedMessages();
            } catch (Exception e) {
                System.err.println("Error in resend task: " + e.getMessage());
            }
        }, resendIntervalMs, resendIntervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Resends messages that haven't been fully acknowledged.
     */
    private void resendUnacknowledgedMessages() {
        List<String> completedMessageIds = new ArrayList<>();
        List<String> failedMessageIds = new ArrayList<>();
        
        for (TrackableMessage message : sentMessages.values()) {
            // Skip fully acknowledged messages
            if (message.isFullyAcknowledged()) {
                completedMessageIds.add(message.getMessageId());
                continue;
            }
            
            // Skip expired messages
            if (message.isExpired()) {
                System.out.println("Message " + message.getMessageId() + " expired");
                message.setStatus(TrackableMessage.DeliveryStatus.EXPIRED);
                failedMessageIds.add(message.getMessageId());
                messagesFailed++;
                continue;
            }
            
            // Check if max delivery attempts reached
            if (message.getDeliveryAttempts() >= maxDeliveryAttempts) {
                System.out.println("Message " + message.getMessageId() + 
                        " failed after " + message.getDeliveryAttempts() + " attempts");
                message.setStatus(TrackableMessage.DeliveryStatus.FAILED);
                failedMessageIds.add(message.getMessageId());
                messagesFailed++;
                continue;
            }
            
            // Resend to consumers who haven't acknowledged
            System.out.println("Resending message " + message.getMessageId() + 
                    " (attempt " + (message.getDeliveryAttempts() + 1) + 
                    "/" + maxDeliveryAttempts + ")");
            
            deliverMessage(message);
            messagesResent++;
        }
        
        // Remove completed and failed messages
        for (String messageId : completedMessageIds) {
            sentMessages.remove(messageId);
        }
        
        for (String messageId : failedMessageIds) {
            sentMessages.remove(messageId);
        }
    }
    
    /**
     * Starts the periodic task to clean up expired messages.
     */
    private void startCleanupTask() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                cleanupExpiredMessages();
            } catch (Exception e) {
                System.err.println("Error in cleanup task: " + e.getMessage());
            }
        }, messageTimeToLiveMs / 2, messageTimeToLiveMs / 2, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Cleans up expired messages.
     */
    private void cleanupExpiredMessages() {
        List<String> expiredMessageIds = new ArrayList<>();
        
        // Check pending messages
        for (TrackableMessage message : pendingMessages.values()) {
            if (message.isExpired()) {
                expiredMessageIds.add(message.getMessageId());
                message.setStatus(TrackableMessage.DeliveryStatus.EXPIRED);
                messagesFailed++;
            }
        }
        
        // Remove expired pending messages
        for (String messageId : expiredMessageIds) {
            pendingMessages.remove(messageId);
        }
        
        expiredMessageIds.clear();
        
        // Check sent messages
        for (TrackableMessage message : sentMessages.values()) {
            if (message.isExpired()) {
                expiredMessageIds.add(message.getMessageId());
                message.setStatus(TrackableMessage.DeliveryStatus.EXPIRED);
                messagesFailed++;
            }
        }
        
        // Remove expired sent messages
        for (String messageId : expiredMessageIds) {
            sentMessages.remove(messageId);
        }
    }
    
    /**
     * Enables or disables message loss simulation.
     * 
     * @param simulate true to enable simulation, false to disable
     * @param lossProbability the probability of message loss (0.0 to 1.0)
     */
    public void simulateMessageLoss(boolean simulate, double lossProbability) {
        this.simulateMessageLoss = simulate;
        this.messageLossProbability = Math.max(0.0, Math.min(1.0, lossProbability));
    }
    
    /**
     * Gets the number of messages waiting for acknowledgment.
     * 
     * @return the count
     */
    public int getPendingAcknowledgmentCount() {
        return sentMessages.size();
    }
    
    /**
     * Gets the number of messages sent.
     * 
     * @return the count
     */
    public int getMessagesSent() {
        return messagesSent;
    }
    
    /**
     * Gets the number of messages fully acknowledged.
     * 
     * @return the count
     */
    public int getMessagesAcked() {
        return messagesAcked;
    }
    
    /**
     * Gets the number of messages resent.
     * 
     * @return the count
     */
    public int getMessagesResent() {
        return messagesResent;
    }
    
    /**
     * Gets the number of messages that failed delivery.
     * 
     * @return the count
     */
    public int getMessagesFailed() {
        return messagesFailed;
    }
    
    /**
     * Gets the delivery success rate.
     * 
     * @return the success rate as a percentage
     */
    public double getDeliverySuccessRate() {
        if (messagesSent == 0) {
            return 100.0;
        }
        return (double) messagesAcked / (messagesAcked + messagesFailed) * 100.0;
    }
    
    /**
     * Shuts down the channel's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
}