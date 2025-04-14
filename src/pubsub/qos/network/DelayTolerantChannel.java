package src.pubsub.qos.network;

import src.pubsub.core.Channel;
import src.pubsub.core.Consumer;
import src.pubsub.core.Event;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A channel implementation that can tolerate network delays.
 * Used for R7: Long delays in network traffic.
 */
public class DelayTolerantChannel implements Channel {
    private final String name;
    private final Set<Consumer> subscribers = new HashSet<>();
    private final DelayAwareQueue<TimestampedMessage> messageQueue;
    private final NetworkSimulator networkSimulator;
    private final ScheduledExecutorService scheduler;
    private final long timeoutMs;
    private final int maxRetries;
    private final int purgeIntervalSeconds;
    
    /**
     * Creates a new delay-tolerant channel.
     * 
     * @param name the name of the channel
     * @param timeoutMs the message timeout in milliseconds
     * @param maxRetries the maximum number of retry attempts
     * @param purgeIntervalSeconds the interval in seconds to purge expired messages
     */
    public DelayTolerantChannel(String name, long timeoutMs, int maxRetries, int purgeIntervalSeconds) {
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.maxRetries = maxRetries;
        this.purgeIntervalSeconds = purgeIntervalSeconds;
        this.messageQueue = new DelayAwareQueue<>(maxRetries);
        this.networkSimulator = new NetworkSimulator();
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start periodic purge task
        scheduler.scheduleAtFixedRate(this::purgeExpiredMessages, 
                purgeIntervalSeconds, purgeIntervalSeconds, TimeUnit.SECONDS);
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void addEvent(Event event) {
        TimestampedMessage message = new TimestampedMessage(event, name, timeoutMs);
        messageQueue.add(message);
    }
    
    @Override
    public void subscribe(Consumer consumer) {
        subscribers.add(consumer);
    }
    
    @Override
    public void unsubscribe(Consumer consumer) {
        subscribers.remove(consumer);
    }
    
    @Override
    public void dispatchEvents() {
        if (messageQueue.isEmpty()) {
            return;
        }
        
        // Process all messages in the queue
        while (!messageQueue.isEmpty()) {
            TimestampedMessage message = messageQueue.poll();
            if (message == null) {
                continue;
            }
            
            // Skip if message has expired
            if (message.isExpired()) {
                message.setStatus(TimestampedMessage.DeliveryStatus.EXPIRED);
                continue;
            }
            
            // Record that we're dispatching the message
            messageQueue.recordDispatched(message);
            
            // Dispatch to all subscribers
            List<Consumer> deliveryFailed = new ArrayList<>();
            
            for (Consumer consumer : subscribers) {
                // Simulate network delay
                boolean deliverySuccessful = networkSimulator.deliverWithPotentialDelay(
                        message, consumer, message.getRetryCount());
                
                if (!deliverySuccessful) {
                    deliveryFailed.add(consumer);
                }
            }
            
            // Handle message status
            if (deliveryFailed.isEmpty()) {
                // All subscribers received the message
                messageQueue.recordDelivered(message);
            } else {
                // Some subscribers didn't receive the message - retry
                if (!messageQueue.retry(message)) {
                    // Max retries exceeded or message expired
                    System.err.println("Message delivery failed after " + 
                            message.getRetryCount() + " retries: " + message);
                } else {
                    System.out.println("Message delivery partially failed, retrying: " + message);
                }
            }
        }
    }
    
    @Override
    public List<Consumer> getSubscribers() {
        return new ArrayList<>(subscribers);
    }
    
    @Override
    public int getQueueSize() {
        return messageQueue.size();
    }
    
    /**
     * Gets the delivery metrics for this channel.
     * 
     * @return the metrics
     */
    public DelayAwareQueue.DeliveryMetrics getMetrics() {
        return messageQueue.getMetrics();
    }
    
    /**
     * Purges expired messages from the queue.
     */
    private void purgeExpiredMessages() {
        int purged = messageQueue.purgeExpiredAndFailed();
        if (purged > 0) {
            System.out.println("Purged " + purged + " expired messages from channel " + name);
        }
    }
    
    /**
     * Simulates a network delay.
     * 
     * @param delayMs the delay in milliseconds
     */
    public void simulateNetworkDelay(int delayMs) {
        networkSimulator.setFixedDelayMs(delayMs);
    }
    
    /**
     * Simulates variable network delays.
     * 
     * @param minDelayMs the minimum delay in milliseconds
     * @param maxDelayMs the maximum delay in milliseconds
     */
    public void simulateVariableNetworkDelay(int minDelayMs, int maxDelayMs) {
        networkSimulator.setVariableDelay(minDelayMs, maxDelayMs);
    }
    
    /**
     * Simulates network jitter.
     * 
     * @param baseDelayMs the base delay in milliseconds
     * @param jitterMs the jitter in milliseconds
     */
    public void simulateNetworkJitter(int baseDelayMs, int jitterMs) {
        networkSimulator.setJitterDelay(baseDelayMs, jitterMs);
    }
    
    /**
     * Sets the probability of a message delivery failure.
     * 
     * @param failureProbability the probability (0.0 to 1.0)
     */
    public void setDeliveryFailureProbability(double failureProbability) {
        networkSimulator.setDeliveryFailureProbability(failureProbability);
    }
    
    /**
     * Shuts down the channel's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
}