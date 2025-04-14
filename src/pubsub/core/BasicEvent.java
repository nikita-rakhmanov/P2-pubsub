package src.pubsub.core;

/**
 * Basic implementation of the Event interface.
 */
public class BasicEvent implements Event {
    private final long timestamp;
    private final String type;
    
    /**
     * Creates a new event with the specified type and current timestamp.
     * 
     * @param type the event type
     */
    public BasicEvent(String type) {
        this.timestamp = System.currentTimeMillis();
        this.type = type;
    }
    
    /**
     * Creates a new event with specified type and timestamp.
     * 
     * @param type the event type
     * @param timestamp the timestamp
     */
    public BasicEvent(String type, long timestamp) {
        this.timestamp = timestamp;
        this.type = type;
    }
    
    @Override
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String getType() {
        return type;
    }
    
    @Override
    public String toString() {
        return "BasicEvent{type='" + type + "', timestamp=" + timestamp + '}';
    }
}