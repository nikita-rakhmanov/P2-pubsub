package src.pubsub.core;

import java.util.List;

/**
 * Interface for middleware in the pub-sub system.
 * Handles R2: Lookup, discovery, and access of event channels.
 */
public interface Middleware {
    /**
     * Creates a new channel with the specified name.
     * 
     * @param channelName the name of the channel to create
     * @return the created channel
     */
    Channel createChannel(String channelName);
    
    /**
     * Looks up a channel by name.
     * 
     * @param channelName the name of the channel to look up
     * @return the channel, or null if not found
     */
    Channel lookupChannel(String channelName);
    
    /**
     * Lists all available channel names.
     * 
     * @return a list of channel names
     */
    List<String> listChannels();
    
    /**
     * Registers a publisher with the middleware.
     * 
     * @param publisher the publisher to register
     */
    void registerPublisher(Publisher publisher);
    
    /**
     * Registers a consumer with the middleware.
     * 
     * @param consumer the consumer to register
     */
    void registerConsumer(Consumer consumer);
}