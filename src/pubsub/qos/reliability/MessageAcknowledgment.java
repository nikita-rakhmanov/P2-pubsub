package src.pubsub.qos.reliability;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents an acknowledgment for a message.
 * Used for R8: Dropped messages.
 */
public class MessageAcknowledgment implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final String messageId;
    private final String consumerId;
    private final String channelName;
    private final long receivedTimestamp;
    private final String ackId;
    
    /**
     * Creates a new message acknowledgment.
     * 
     * @param messageId the ID of the acknowledged message
     * @param consumerId the ID of the consumer acknowledging the message
     * @param channelName the name of the channel
     */
    public MessageAcknowledgment(String messageId, String consumerId, String channelName) {
        this.messageId = messageId;
        this.consumerId = consumerId;
        this.channelName = channelName;
        this.receivedTimestamp = System.currentTimeMillis();
        this.ackId = UUID.randomUUID().toString();
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
     * Gets the consumer ID.
     * 
     * @return the consumer ID
     */
    public String getConsumerId() {
        return consumerId;
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
     * Gets the timestamp when the message was received.
     * 
     * @return the timestamp in milliseconds
     */
    public long getReceivedTimestamp() {
        return receivedTimestamp;
    }
    
    /**
     * Gets the acknowledgment ID.
     * 
     * @return the acknowledgment ID
     */
    public String getAckId() {
        return ackId;
    }
    
    @Override
    public String toString() {
        return "Ack{messageId='" + messageId + "', consumerId='" + consumerId + 
                "', channel='" + channelName + "'}";
    }
}