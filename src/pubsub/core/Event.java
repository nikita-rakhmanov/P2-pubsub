package src.pubsub.core;

/**
 * Interface for events in the pub-sub system.
 * With timestamp and type for QoS.
 */
public interface Event {
    /**
     * @return timestamp when the event was created
     */
    long getTimestamp();
    
    /**
     * @return type of the event
     */
    String getType();
}