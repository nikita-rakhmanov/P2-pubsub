package src.pubsub.core;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced implementation of the Event interface that includes QoS features.
 * Combines features from BasicEvent, TimestampedMessage, and TrackableMessage.
 */
public class BasicEvent implements Event, Serializable {
    private static final long serialVersionUID = 1L;
    
    // Basic event properties
    private final String id;
    private final long timestamp;
    private final String type;
    
    // Time tracking for R7: Long delays in network traffic
    private final long creationTime;
    private final long expirationTime;
    private long dispatchTime = 0;
    private long deliveryTime = 0;
    
    // Delivery state for R8: Dropped messages
    private int retryCount = 0;
    private DeliveryStatus status = DeliveryStatus.CREATED;
    private final Map<String, Boolean> consumerAcknowledgments = new ConcurrentHashMap<>();
    
    /**
     * Event delivery status enum.
     */
    public enum DeliveryStatus {
        CREATED,        // Event created but not yet sent
        QUEUED,         // Event added to queue
        DISPATCHED,     // Event dispatched, waiting for delivery
        DELIVERED,      // Event delivered to all consumers
        PARTIAL_ACKS,   // Some consumers acknowledged
        EXPIRED,        // Event expired before full delivery
        FAILED          // Event delivery failed
    }
    
    /**
     * Creates a new event with the specified type and current timestamp.
     * 
     * @param type the event type
     */
    public BasicEvent(String type) {
        this(type, System.currentTimeMillis(), 30000); // Default 30 second expiration
    }
    
    /**
     * Creates a new event with specified type and timestamp.
     * 
     * @param type the event type
     * @param timestamp the timestamp
     */
    public BasicEvent(String type, long timestamp) {
        this(type, timestamp, 30000); // Default 30 second expiration
    }
    
    /**
     * Creates a new event with specified type, timestamp, and time-to-live.
     * 
     * @param type the event type
     * @param timestamp the timestamp
     * @param timeToLiveMs the time-to-live in milliseconds
     */
    public BasicEvent(String type, long timestamp, long timeToLiveMs) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = timestamp;
        this.type = type;
        this.creationTime = System.currentTimeMillis();
        this.expirationTime = this.creationTime + timeToLiveMs;
    }
    
    @Override
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String getType() {
        return type;
    }
    
    /**
     * Gets the event ID.
     * 
     * @return the event ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the creation time.
     * 
     * @return the creation time in milliseconds
     */
    public long getCreationTime() {
        return creationTime;
    }
    
    /**
     * Gets the expiration time.
     * 
     * @return the expiration time in milliseconds
     */
    public long getExpirationTime() {
        return expirationTime;
    }
    
    /**
     * Gets the dispatch time.
     * 
     * @return the dispatch time in milliseconds, or 0 if not yet dispatched
     */
    public long getDispatchTime() {
        return dispatchTime;
    }
    
    /**
     * Sets the dispatch time to the current time.
     */
    public void markDispatched() {
        this.dispatchTime = System.currentTimeMillis();
        this.status = DeliveryStatus.DISPATCHED;
    }
    
    /**
     * Gets the delivery time.
     * 
     * @return the delivery time in milliseconds, or 0 if not yet delivered
     */
    public long getDeliveryTime() {
        return deliveryTime;
    }
    
    /**
     * Sets the delivery time to the current time.
     */
    public void markDelivered() {
        this.deliveryTime = System.currentTimeMillis();
        this.status = DeliveryStatus.DELIVERED;
    }
    
    /**
     * Gets the retry count.
     * 
     * @return the number of delivery retries
     */
    public int getRetryCount() {
        return retryCount;
    }
    
    /**
     * Increments the retry count.
     */
    public void incrementRetryCount() {
        this.retryCount++;
    }
    
    /**
     * Gets the delivery status.
     * 
     * @return the delivery status
     */
    public DeliveryStatus getStatus() {
        return status;
    }
    
    /**
     * Sets the delivery status.
     * 
     * @param status the new status
     */
    public void setStatus(DeliveryStatus status) {
        this.status = status;
    }
    
    /**
     * Checks if the event has expired.
     * 
     * @return true if the event has expired, false otherwise
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }
    
    /**
     * Gets the age of the event in milliseconds.
     * 
     * @return the age
     */
    public long getAge() {
        return System.currentTimeMillis() - creationTime;
    }
    
    /**
     * Gets the delivery time in milliseconds.
     * 
     * @return the delivery time, or -1 if not yet delivered
     */
    public long getDeliveryDuration() {
        if (deliveryTime == 0) {
            return -1;
        }
        return deliveryTime - creationTime;
    }
    
    /**
     * Gets the dispatch time in milliseconds.
     * 
     * @return the dispatch time, or -1 if not yet dispatched
     */
    public long getDispatchDuration() {
        if (dispatchTime == 0) {
            return -1;
        }
        return dispatchTime - creationTime;
    }
    
    /**
     * Registers a consumer for this event.
     * 
     * @param consumerId the consumer ID
     */
    public void registerConsumer(String consumerId) {
        consumerAcknowledgments.put(consumerId, false);
    }
    
    /**
     * Records an acknowledgment from a consumer.
     * 
     * @param consumerId the consumer ID
     * @return true if this is a new acknowledgment, false if already acknowledged
     */
    public boolean acknowledgeConsumer(String consumerId) {
        if (consumerAcknowledgments.containsKey(consumerId) && !consumerAcknowledgments.get(consumerId)) {
            consumerAcknowledgments.put(consumerId, true);
            updateStatus();
            return true;
        }
        return false;
    }
    
    /**
     * Checks if all registered consumers have acknowledged the event.
     * 
     * @return true if fully acknowledged, false otherwise
     */
    public boolean isFullyAcknowledged() {
        if (consumerAcknowledgments.isEmpty()) {
            return false;
        }
        
        for (Boolean acked : consumerAcknowledgments.values()) {
            if (!acked) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Gets the set of consumer IDs that haven't acknowledged the event.
     * 
     * @return the set of consumer IDs
     */
    public Set<String> getUnacknowledgedConsumers() {
        Set<String> unacknowledged = ConcurrentHashMap.newKeySet();
        
        for (Map.Entry<String, Boolean> entry : consumerAcknowledgments.entrySet()) {
            if (!entry.getValue()) {
                unacknowledged.add(entry.getKey());
            }
        }
        
        return unacknowledged;
    }
    
    /**
     * Gets the set of consumer IDs that have acknowledged the event.
     * 
     * @return the set of consumer IDs
     */
    public Set<String> getAcknowledgedConsumers() {
        Set<String> acknowledged = ConcurrentHashMap.newKeySet();
        
        for (Map.Entry<String, Boolean> entry : consumerAcknowledgments.entrySet()) {
            if (entry.getValue()) {
                acknowledged.add(entry.getKey());
            }
        }
        
        return acknowledged;
    }
    
    /**
     * Updates the delivery status based on acknowledgments.
     */
    private void updateStatus() {
        if (isFullyAcknowledged()) {
            status = DeliveryStatus.DELIVERED;
        } else if (getAcknowledgedConsumerCount() > 0) {
            status = DeliveryStatus.PARTIAL_ACKS;
        }
    }
    
    /**
     * Gets the total number of consumers.
     * 
     * @return the count
     */
    public int getTotalConsumerCount() {
        return consumerAcknowledgments.size();
    }
    
    /**
     * Gets the number of consumers that have acknowledged the event.
     * 
     * @return the count
     */
    public int getAcknowledgedConsumerCount() {
        int count = 0;
        for (Boolean acked : consumerAcknowledgments.values()) {
            if (acked) {
                count++;
            }
        }
        return count;
    }
    
    @Override
    public String toString() {
        return "BasicEvent{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", timestamp=" + timestamp +
                ", status=" + status +
                ", age=" + getAge() + "ms" +
                ", acks=" + getAcknowledgedConsumerCount() + "/" + getTotalConsumerCount() +
                ", retries=" + retryCount +
                '}';
    }
}