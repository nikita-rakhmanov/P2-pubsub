package src.pubsub.core;

/**
 * Interface for publishers in the pub-sub system.
 */
public interface Publisher {
    /**
     * Publishes an event to the specified channel.
     * 
     * @param channelName the name of the channel
     * @param event the event to publish
     */
    void publish(String channelName, Event event);
    
    /**
     * Registers the publisher with the middleware.
     * 
     * @param middleware the middleware to register with
     */
    void registerWithMiddleware(Middleware middleware);
}