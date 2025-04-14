package src.pubsub.core;

import java.util.List;

/**
 * Interface for channels in the pub-sub system.
 * Handles R1: Subscribe and publish events.
 */
public interface Channel {
    /**
     * Gets the name of the channel.
     * 
     * @return the channel name
     */
    String getName();
    
    /**
     * Adds an event to the channel's queue.
     * 
     * @param event the event to add
     */
    void addEvent(Event event);
    
    /**
     * Subscribes a consumer to the channel.
     * 
     * @param consumer the consumer to subscribe
     */
    void subscribe(Consumer consumer);
    
    /**
     * Unsubscribes a consumer from the channel.
     * 
     * @param consumer the consumer to unsubscribe
     */
    void unsubscribe(Consumer consumer);
    
    /**
     * Dispatches all events in the queue to subscribers.
     */
    void dispatchEvents();
    
    /**
     * Gets the list of consumers subscribed to this channel.
     * 
     * @return list of subscribers
     */
    List<Consumer> getSubscribers();
    
    /**
     * Gets the size of the event queue.
     * 
     * @return number of events in the queue
     */
    int getQueueSize();
}