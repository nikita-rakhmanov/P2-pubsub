package src.pubsub.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Basic implementation of the Channel interface.
 * Uses a dynamic queue for event management (R3).
 */
public class BasicChannel implements Channel {
    private final String name;
    private final Set<Consumer> subscribers = new HashSet<>();
    private final DynamicQueue<Event> eventQueue;
    
    /**
     * Creates a new channel with the specified name.
     * 
     * @param name the name of the channel
     */
    public BasicChannel(String name) {
        this.name = name;
        this.eventQueue = new DynamicQueue<>(10); // Start with a small capacity
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void addEvent(Event event) {
        eventQueue.add(event);
    }
    
    @Override
    public void subscribe(Consumer consumer) {
        subscribers.add(consumer);
    }
    
    @Override
    public void unsubscribe(Consumer consumer) {
        subscribers.remove(consumer);
    }
    
    @Override
    public void dispatchEvents() {
        while (!eventQueue.isEmpty()) {
            Event event = eventQueue.poll();
            for (Consumer consumer : subscribers) {
                try {
                    consumer.consume(event);
                } catch (Exception e) {
                    System.err.println("Error dispatching event to consumer: " + e.getMessage());
                    // In a real system, we would log this and possibly handle retry logic
                }
            }
        }
    }
    
    @Override
    public List<Consumer> getSubscribers() {
        return new ArrayList<>(subscribers);
    }
    
    @Override
    public int getQueueSize() {
        return eventQueue.size();
    }
}