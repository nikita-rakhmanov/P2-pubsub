package src.pubsub.qos.reliability;

import src.pubsub.core.Consumer;
import src.pubsub.core.Event;
import src.pubsub.core.Middleware;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * A consumer implementation that supports reliable delivery.
 * Used for R8: Dropped messages.
 */
public class ReliableConsumer implements Consumer {
    private final String id;
    private final Consumer delegate;
    private ReliableMiddleware middleware;
    private final Set<String> subscribedChannels = new HashSet<>();
    
    // Message loss simulation
    private boolean simulateMessageLoss = false;
    private double messageLossProbability = 0.0;
    
    /**
     * Creates a new reliable consumer with an auto-generated ID.
     * 
     * @param delegate the underlying consumer
     */
    public ReliableConsumer(Consumer delegate) {
        this(delegate, "consumer-" + UUID.randomUUID().toString());
    }
    
    /**
     * Creates a new reliable consumer with the specified ID.
     * 
     * @param delegate the underlying consumer
     * @param id the consumer ID
     */
    public ReliableConsumer(Consumer delegate, String id) {
        this.delegate = delegate;
        this.id = id;
    }
    
    @Override
    public void consume(Event event) {
        // This is a pass-through method - we use receiveMessage for reliable delivery
        delegate.consume(event);
    }
    
    /**
     * Receives a message with tracking information for reliable delivery.
     * 
     * @param event the event
     * @param messageId the message ID
     * @param channelName the channel name
     */
    public void receiveMessage(Event event, String messageId, String channelName) {
        // Check if we should simulate message loss
        if (simulateMessageLoss && Math.random() < messageLossProbability) {
            System.out.println("Consumer " + id + " simulating message loss for " + messageId);
            return;
        }
        
        try {
            // Process the message
            delegate.consume(event);
            
            // Send acknowledgment
            sendAcknowledgment(messageId, channelName);
        } catch (Exception e) {
            System.err.println("Error consuming message " + messageId + ": " + e.getMessage());
            // Do not send acknowledgment on error
        }
    }
    
    /**
     * Sends an acknowledgment for a message.
     * 
     * @param messageId the message ID
     * @param channelName the channel name
     */
    private void sendAcknowledgment(String messageId, String channelName) {
        if (middleware == null) {
            System.err.println("Cannot send acknowledgment: middleware not set");
            return;
        }
        
        MessageAcknowledgment ack = new MessageAcknowledgment(messageId, id, channelName);
        middleware.processAcknowledgment(ack);
    }
    
    @Override
    public void subscribe(String channelName) {
        if (middleware == null) {
            throw new IllegalStateException("Consumer not registered with middleware");
        }
        
        ReliableChannel channel = (ReliableChannel) middleware.lookupChannel(channelName);
        if (channel == null) {
            throw new IllegalArgumentException("Channel not found: " + channelName);
        }
        
        channel.subscribe(this);
        subscribedChannels.add(channelName);
    }
    
    @Override
    public void unsubscribe(String channelName) {
        if (middleware == null) {
            throw new IllegalStateException("Consumer not registered with middleware");
        }
        
        ReliableChannel channel = (ReliableChannel) middleware.lookupChannel(channelName);
        if (channel == null) {
            throw new IllegalArgumentException("Channel not found: " + channelName);
        }
        
        channel.unsubscribe(this);
        subscribedChannels.remove(channelName);
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        if (middleware instanceof ReliableMiddleware) {
            this.middleware = (ReliableMiddleware) middleware;
            this.middleware.registerConsumer(this);
        } else {
            throw new IllegalArgumentException(
                    "Middleware must be a ReliableMiddleware for reliable message delivery");
        }
    }
    
    /**
     * Gets the consumer ID.
     * 
     * @return the ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the set of channels this consumer is subscribed to.
     * 
     * @return the set of channel names
     */
    public Set<String> getSubscribedChannels() {
        return new HashSet<>(subscribedChannels);
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
}