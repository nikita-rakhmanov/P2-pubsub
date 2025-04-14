package src.pubsub.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import src.pubsub.qos.fault.PersistentQueue;
import src.pubsub.qos.network.NetworkSimulator;
import src.pubsub.qos.network.TimestampedMessage;

/**
 * Enhanced implementation of the Channel interface that includes QoS features.
 * Combines features from BasicChannel, RecoverableChannel, DelayTolerantChannel, and ReliableChannel.
 */
public class BasicChannel implements Channel {
    private final String name;
    private final Set<Consumer> subscribers = new HashSet<>();
    
    // Persistent queue for R5: Crashing queues
    private final PersistentQueue<BasicEvent> eventQueue;
    private boolean automaticRecovery = true;
    
    // Network delay simulation for R7: Long delays in network traffic
    private final NetworkSimulator networkSimulator;
    
    // Delivery tracking for R8: Dropped messages
    private int messagesSent = 0;
    private int messagesAcked = 0;
    private int messagesResent = 0;
    private int messagesFailed = 0;
    
    // Configuration
    private final long messageTimeToLiveMs;
    private final int maxRetries;
    private final int purgeIntervalSeconds;
    
    // Background tasks
    private final ScheduledExecutorService scheduler;
    
    /**
     * Creates a new channel with the specified name and default settings.
     * 
     * @param name the name of the channel
     */
    public BasicChannel(String name) {
        this(name, 30000, 3, 60); // Default: 30 second TTL, 3 retries, 60 second purge interval
    }
    
    /**
     * Creates a new channel with custom QoS settings.
     * 
     * @param name the name of the channel
     * @param messageTimeToLiveMs the message time-to-live in milliseconds
     * @param maxRetries the maximum number of retry attempts
     * @param purgeIntervalSeconds the interval in seconds to purge expired messages
     */
    public BasicChannel(String name, long messageTimeToLiveMs, int maxRetries, int purgeIntervalSeconds) {
        this.name = name;
        this.messageTimeToLiveMs = messageTimeToLiveMs;
        this.maxRetries = maxRetries;
        this.purgeIntervalSeconds = purgeIntervalSeconds;
        
        // Initialize components
        this.eventQueue = new PersistentQueue<>("channel-" + name, 10, 5);
        this.networkSimulator = new NetworkSimulator();
        this.scheduler = Executors.newScheduledThreadPool(2);
        
        // Start periodic tasks
        startPurgeTasks();
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void addEvent(Event event) {
        try {
            // Enrich the event with tracking information if it's a BasicEvent
            if (event instanceof BasicEvent) {
                BasicEvent enhancedEvent = (BasicEvent) event;
                enhancedEvent.setStatus(BasicEvent.DeliveryStatus.QUEUED);
                
                // Register subscribers for acknowledgments
                for (Consumer consumer : subscribers) {
                    if (consumer instanceof BasicConsumer) {
                        enhancedEvent.registerConsumer(((BasicConsumer) consumer).getId());
                    }
                }
                
                // Add to queue
                eventQueue.add(enhancedEvent);
            } else {
                // If it's a simple event, wrap it
                BasicEvent wrappedEvent = new BasicEvent(event.getType(), event.getTimestamp(), messageTimeToLiveMs);
                wrappedEvent.setStatus(BasicEvent.DeliveryStatus.QUEUED);
                
                // Register subscribers for acknowledgments
                for (Consumer consumer : subscribers) {
                    if (consumer instanceof BasicConsumer) {
                        wrappedEvent.registerConsumer(((BasicConsumer) consumer).getId());
                    }
                }
                
                // Add to queue
                eventQueue.add(wrappedEvent);
            }
        } catch (IllegalStateException e) {
            // Queue has crashed
            System.err.println("Channel " + name + " detected queue crash while adding event");
            if (automaticRecovery) {
                recoverQueue();
                // Try again recursively
                addEvent(event);
            } else {
                throw e; // Propagate the exception if automatic recovery is disabled
            }
        }
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
        try {
            List<BasicEvent> unacknowledgedEvents = new ArrayList<>();
            
            while (!eventQueue.isEmpty()) {
                BasicEvent event = eventQueue.poll();
                if (event == null) {
                    continue;
                }
                
                // Skip if event has expired
                if (event.isExpired()) {
                    event.setStatus(BasicEvent.DeliveryStatus.EXPIRED);
                    messagesFailed++;
                    continue;
                }
                
                // Mark that we're dispatching the event
                event.markDispatched();
                messagesSent++;
                
                // Dispatch to all subscribers with potential delays
                List<Consumer> deliveryFailed = new ArrayList<>();
                
                for (Consumer consumer : subscribers) {
                    // Get consumer ID if available
                    String consumerId = null;
                    if (consumer instanceof BasicConsumer) {
                        consumerId = ((BasicConsumer) consumer).getId();
                    }
                    
                    // Skip consumers that have already acknowledged this event
                    if (consumerId != null && event.getAcknowledgedConsumers().contains(consumerId)) {
                        continue;
                    }
                    
                    // Create a TimestampedMessage adapter for compatibility with NetworkSimulator
                    TimestampedMessage messageAdapter = createTimestampedAdapter(event, name);
                    
                    // Simulate network delay
                    boolean deliverySuccessful = networkSimulator.deliverWithPotentialDelay(
                            messageAdapter, consumer, event.getRetryCount());
                    
                    if (!deliverySuccessful) {
                        deliveryFailed.add(consumer);
                    }
                }
                
                if (!deliveryFailed.isEmpty() || !event.isFullyAcknowledged()) {
                    // Some subscribers didn't receive the message - retry later
                    event.incrementRetryCount();
                    
                    if (event.getRetryCount() <= maxRetries && !event.isExpired()) {
                        unacknowledgedEvents.add(event);
                    } else {
                        // Max retries exceeded or event expired
                        event.setStatus(BasicEvent.DeliveryStatus.FAILED);
                        messagesFailed++;
                        System.err.println("Event delivery failed after " + 
                                event.getRetryCount() + " retries: " + event);
                    }
                } else {
                    // All subscribers received and acknowledged the message
                    event.setStatus(BasicEvent.DeliveryStatus.DELIVERED);
                    event.markDelivered();
                    messagesAcked++;
                }
            }
            
            // Re-queue unacknowledged events for later retries
            for (BasicEvent event : unacknowledgedEvents) {
                eventQueue.add(event);
                messagesResent++;
            }
        } catch (IllegalStateException e) {
            // Queue has crashed
            System.err.println("Channel " + name + " detected queue crash while dispatching events");
            if (automaticRecovery) {
                recoverQueue();
                // Try again
                dispatchEvents();
            } else {
                throw e; // Propagate the exception if automatic recovery is disabled
            }
        }
    }
    
    @Override
    public List<Consumer> getSubscribers() {
        return new ArrayList<>(subscribers);
    }
    
    @Override
    public int getQueueSize() {
        try {
            return eventQueue.size();
        } catch (IllegalStateException e) {
            // Queue has crashed
            System.err.println("Channel " + name + " detected queue crash while getting size");
            if (automaticRecovery) {
                recoverQueue();
                return eventQueue.size();
            } else {
                return -1; // Indicate error if automatic recovery is disabled
            }
        }
    }
    
    /**
     * Recovers the event queue from a crash.
     * 
     * @return true if recovery was successful, false otherwise
     */
    public boolean recoverQueue() {
        System.out.println("Channel " + name + " attempting to recover event queue...");
        boolean recovered = eventQueue.recover();
        if (recovered) {
            System.out.println("Channel " + name + " successfully recovered event queue");
        } else {
            System.err.println("Channel " + name + " failed to recover event queue");
        }
        return recovered;
    }
    
    /**
     * Manually backs up the event queue.
     * 
     * @return true if the backup was successful, false otherwise
     */
    public boolean backupQueue() {
        return eventQueue.manualBackup();
    }
    
    /**
     * Simulates a queue crash.
     */
    public void simulateQueueCrash() {
        System.out.println("Simulating crash for channel " + name + "'s event queue");
        eventQueue.simulateCrash();
    }
    
    /**
     * Sets whether the channel should attempt automatic recovery from queue crashes.
     * 
     * @param automatic true to enable automatic recovery, false to disable
     */
    public void setAutomaticRecovery(boolean automatic) {
        this.automaticRecovery = automatic;
    }
    
    /**
     * Checks if the channel's queue has crashed.
     * 
     * @return true if the queue has crashed, false otherwise
     */
    public boolean isQueueCrashed() {
        return eventQueue.isCrashed();
    }
    
    /**
     * Starts the purge tasks for maintenance.
     */
    private void startPurgeTasks() {
        scheduler.scheduleAtFixedRate(this::purgeExpiredEvents, 
                purgeIntervalSeconds, purgeIntervalSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Purges expired events from the queue.
     */
    private void purgeExpiredEvents() {
        // In a real implementation, we would scan the queue for expired events
        // However, our PersistentQueue doesn't support scanning, so we handle expiration during dispatch
        // This method is kept for future extension
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
     * Creates a TimestampedMessage adapter for a BasicEvent.
     * This is needed for compatibility with the existing NetworkSimulator.
     * 
     * @param event the BasicEvent to adapt
     * @param channelName the channel name
     * @return a TimestampedMessage that wraps the event
     */
    private TimestampedMessage createTimestampedAdapter(BasicEvent event, String channelName) {
        // Create a TimestampedMessage that wraps the event
        TimestampedMessage adapter = new TimestampedMessage(event, channelName, messageTimeToLiveMs);
        
        // Sync the adapter's state with the event's state
        if (event.getRetryCount() > 0) {
            for (int i = 0; i < event.getRetryCount(); i++) {
                adapter.incrementRetryCount();
            }
        }
        
        // Map the status
        switch (event.getStatus()) {
            case QUEUED:
                adapter.setStatus(TimestampedMessage.DeliveryStatus.QUEUED);
                break;
            case DISPATCHED:
                adapter.setStatus(TimestampedMessage.DeliveryStatus.DISPATCHED);
                adapter.markDispatched();
                break;
            case DELIVERED:
            case PARTIAL_ACKS:
                adapter.setStatus(TimestampedMessage.DeliveryStatus.DELIVERED);
                adapter.markDelivered();
                break;
            case EXPIRED:
                adapter.setStatus(TimestampedMessage.DeliveryStatus.EXPIRED);
                break;
            case FAILED:
                adapter.setStatus(TimestampedMessage.DeliveryStatus.FAILED);
                break;
            default:
                adapter.setStatus(TimestampedMessage.DeliveryStatus.CREATED);
                break;
        }
        
        return adapter;
    }
    public void shutdown() {
        eventQueue.shutdown();
        scheduler.shutdownNow();
    }
    
    /**
     * Class to represent delivery metrics.
     */
    public static class DeliveryMetrics {
        private int queuedCount = 0;
        private int deliveredCount = 0;
        private int failedCount = 0;
        private int expiredCount = 0;
        private int retryCount = 0;
        private long totalDeliveryTime = 0;
        private long maxDeliveryTime = 0;
        private long totalDispatchTime = 0;
        
        public void recordQueued() { queuedCount++; }
        public void recordDelivered(long deliveryTime) {
            deliveredCount++;
            totalDeliveryTime += deliveryTime;
            maxDeliveryTime = Math.max(maxDeliveryTime, deliveryTime);
        }
        public void recordDispatched(long dispatchTime) { totalDispatchTime += dispatchTime; }
        public void recordFailed() { failedCount++; }
        public void recordExpired() { expiredCount++; }
        public void recordRetry() { retryCount++; }
        
        public int getQueuedCount() { return queuedCount; }
        public int getDeliveredCount() { return deliveredCount; }
        public int getFailedCount() { return failedCount; }
        public int getExpiredCount() { return expiredCount; }
        public int getRetryCount() { return retryCount; }
        
        public double getAverageDeliveryTime() {
            return deliveredCount > 0 ? (double) totalDeliveryTime / deliveredCount : 0;
        }
        
        public long getMaxDeliveryTime() {
            return maxDeliveryTime;
        }
        
        public double getAverageDispatchTime() {
            return deliveredCount > 0 ? (double) totalDispatchTime / deliveredCount : 0;
        }
        
        @Override
        public String toString() {
            return "Metrics{" +
                    "queued=" + queuedCount +
                    ", delivered=" + deliveredCount +
                    ", failed=" + failedCount +
                    ", expired=" + expiredCount +
                    ", retries=" + retryCount +
                    ", avgDeliveryTime=" + String.format("%.2f", getAverageDeliveryTime()) + "ms" +
                    ", maxDeliveryTime=" + maxDeliveryTime + "ms" +
                    ", avgDispatchTime=" + String.format("%.2f", getAverageDispatchTime()) + "ms" +
                    '}';
        }
    }
}