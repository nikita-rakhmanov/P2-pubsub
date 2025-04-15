package src.pubsub.core;

/**
 * Interface for consumers (subscribers) in the pub-sub system.
 */
public interface Consumer {
    /**
     * Handles an event received from a channel.
     * 
     * @param event the event to consume
     */
    boolean consume(Event event);
    
    /**
     * Subscribes the consumer to a channel.
     * 
     * @param channelName the name of the channel to subscribe to
     */
    void subscribe(String channelName);
    
    /**
     * Unsubscribes the consumer from a channel.
     * 
     * @param channelName the name of the channel to unsubscribe from
     */
    void unsubscribe(String channelName);
    
    /**
     * Registers the consumer with the middleware.
     * 
     * @param middleware the middleware to register with
     */
    void registerWithMiddleware(Middleware middleware);
}