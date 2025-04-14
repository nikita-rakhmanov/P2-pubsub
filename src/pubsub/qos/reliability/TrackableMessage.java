package src.pubsub.qos.reliability;

import src.pubsub.core.Event;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A message that can be tracked for delivery status.
 * Used for R8: Dropped messages.
 */
public class TrackableMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String messageId;
    private final Event event;
    private final String channelName;
    private final long creationTime;
    private final long expirationTime;
    private final Map<String, Boolean> consumerAcknowledgments = new ConcurrentHashMap<>();
    private int deliveryAttempts = 0;
    private DeliveryStatus status = DeliveryStatus.CREATED;
    
    /**
     * Message delivery status enum.
     */
    public enum DeliveryStatus {
        CREATED,        // Message created but not yet sent
        SENT,           // Message sent, waiting for acks
        PARTIAL_ACKS,   // Some consumers acknowledged
        FULLY_ACKED,    // All consumers acknowledged
        EXPIRED,        // Message expired before full acknowledgment
        FAILED          // Message delivery failed
    }
    
    /**
     * Creates a new trackable message.
     * 
     * @param event the event
     * @param channelName the channel name
     * @param timeToLiveMs the time-to-live in milliseconds
     */
    public TrackableMessage(Event event, String channelName, long timeToLiveMs) {
        this.messageId = UUID.randomUUID().toString();
        this.event = event;
        this.channelName = channelName;
        this.creationTime = System.currentTimeMillis();
        this.expirationTime = this.creationTime + timeToLiveMs;
    }
    
    /**
     * Gets the message ID.
     * 
     * @return the message ID
     */
    public String getMessageId() {
        return messageId;
    }
    
    /**
     * Gets the event.
     * 
     * @return the event
     */
    public Event getEvent() {
        return event;
    }
    
    /**
     * Gets the channel name.
     * 
     * @return the channel name
     */
    public String getChannelName() {
        return channelName;
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
     * Gets the current delivery status.
     * 
     * @return the status
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
     * Gets the number of delivery attempts.
     * 
     * @return the count
     */
    public int getDeliveryAttempts() {
        return deliveryAttempts;
    }
    
    /**
     * Increments the delivery attempts counter.
     */
    public void incrementDeliveryAttempts() {
        deliveryAttempts++;
    }
    
    /**
     * Registers a consumer for this message.
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
     * Checks if all registered consumers have acknowledged the message.
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
     * Gets the set of consumer IDs that haven't acknowledged the message.
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
     * Gets the set of consumer IDs that have acknowledged the message.
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
     * Gets the total number of consumers.
     * 
     * @return the count
     */
    public int getTotalConsumerCount() {
        return consumerAcknowledgments.size();
    }
    
    /**
     * Gets the number of consumers that have acknowledged the message.
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
    
    /**
     * Checks if the message has expired.
     * 
     * @return true if expired, false otherwise
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }
    
    /**
     * Updates the delivery status based on acknowledgments.
     */
    private void updateStatus() {
        if (isFullyAcknowledged()) {
            status = DeliveryStatus.FULLY_ACKED;
        } else if (getAcknowledgedConsumerCount() > 0) {
            status = DeliveryStatus.PARTIAL_ACKS;
        }
    }
    
    /**
     * Gets the age of the message in milliseconds.
     * 
     * @return the age
     */
    public long getAge() {
        return System.currentTimeMillis() - creationTime;
    }
    
    @Override
    public String toString() {
        return "TrackableMessage{" +
                "id='" + messageId + '\'' +
                ", event=" + event +
                ", channel='" + channelName + '\'' +
                ", status=" + status +
                ", age=" + getAge() + "ms" +
                ", acks=" + getAcknowledgedConsumerCount() + "/" + getTotalConsumerCount() +
                ", attempts=" + deliveryAttempts +
                '}';
    }
}