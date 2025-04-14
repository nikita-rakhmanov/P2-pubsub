package src.pubsub.qos.network;

import src.pubsub.core.Event;

import java.io.Serializable;
import java.util.UUID;

/**
 * A wrapper for events that adds timing information and delivery tracking.
 * Used to handle R7: Long delays in network traffic.
 */
public class TimestampedMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String messageId;
    private final Event event;
    private final String channelName;
    private final long creationTime;
    private final long expirationTime;
    private long dispatchTime = 0;
    private long deliveryTime = 0;
    private int retryCount = 0;
    private DeliveryStatus status = DeliveryStatus.CREATED;
    
    /**
     * Message delivery status enum.
     */
    public enum DeliveryStatus {
        CREATED,
        QUEUED,
        DISPATCHED,
        DELIVERED,
        EXPIRED,
        FAILED
    }
    
    /**
     * Creates a new timestamped message.
     * 
     * @param event the event
     * @param channelName the channel name
     * @param timeoutMs the timeout in milliseconds, after which the message is considered expired
     */
    public TimestampedMessage(Event event, String channelName, long timeoutMs) {
        this.messageId = UUID.randomUUID().toString();
        this.event = event;
        this.channelName = channelName;
        this.creationTime = System.currentTimeMillis();
        this.expirationTime = this.creationTime + timeoutMs;
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
     * Checks if the message has expired.
     * 
     * @return true if the message has expired, false otherwise
     */
    public boolean isExpired() {
        return System.currentTimeMillis() > expirationTime;
    }
    
    /**
     * Gets the age of the message in milliseconds.
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
    
    @Override
    public String toString() {
        return "TimestampedMessage{" +
                "messageId='" + messageId + '\'' +
                ", event=" + event +
                ", channelName='" + channelName + '\'' +
                ", status=" + status +
                ", age=" + getAge() + "ms" +
                ", retryCount=" + retryCount +
                '}';
    }
}