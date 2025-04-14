package src. pubsub.qos.fault;

import src.pubsub.core.Channel;
import src.pubsub.core.Consumer;
import src.pubsub.core.Event;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A channel implementation that can recover from queue crashes.
 * Used to handle R5: Crashing queues.
 */
public class RecoverableChannel implements Channel {
    private final String name;
    private final Set<Consumer> subscribers = new HashSet<>();
    private final PersistentQueue<SerializableEventWrapper> eventQueue;
    private boolean automaticRecovery = true;
    
    /**
     * Creates a new recoverable channel.
     * 
     * @param name the name of the channel
     */
    public RecoverableChannel(String name) {
        this.name = name;
        this.eventQueue = new PersistentQueue<>(
                "channel-" + name, 
                10,  // Initial capacity
                5);  // Backup every 5 seconds
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public void addEvent(Event event) {
        try {
            eventQueue.add(new SerializableEventWrapper(event));
        } catch (IllegalStateException e) {
            // Queue has crashed
            System.err.println("Channel " + name + " detected queue crash while adding event");
            if (automaticRecovery) {
                recoverQueue();
                // Try again
                eventQueue.add(new SerializableEventWrapper(event));
            } else {
                throw e; // Propagate the exception if automatic recovery is disabled
            }
        }
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
        try {
            while (!eventQueue.isEmpty()) {
                SerializableEventWrapper wrapper = eventQueue.poll();
                if (wrapper != null) {
                    Event event = wrapper.getEvent();
                    for (Consumer consumer : subscribers) {
                        try {
                            consumer.consume(event);
                        } catch (Exception e) {
                            System.err.println("Error dispatching event to consumer: " + e.getMessage());
                        }
                    }
                }
            }
        } catch (IllegalStateException e) {
            // Queue has crashed
            System.err.println("Channel " + name + " detected queue crash while dispatching events");
            if (automaticRecovery) {
                recoverQueue();
                // Try again
                dispatchEvents();
            } else {
                throw e; // Propagate the exception if automatic recovery is disabled
            }
        }
    }
    
    @Override
    public List<Consumer> getSubscribers() {
        return new ArrayList<>(subscribers);
    }
    
    @Override
    public int getQueueSize() {
        try {
            return eventQueue.size();
        } catch (IllegalStateException e) {
            // Queue has crashed
            System.err.println("Channel " + name + " detected queue crash while getting size");
            if (automaticRecovery) {
                recoverQueue();
                return eventQueue.size();
            } else {
                return -1; // Indicate error if automatic recovery is disabled
            }
        }
    }
    
    /**
     * Recovers the event queue from a crash.
     * 
     * @return true if recovery was successful, false otherwise
     */
    public boolean recoverQueue() {
        System.out.println("Channel " + name + " attempting to recover event queue...");
        boolean recovered = eventQueue.recover();
        if (recovered) {
            System.out.println("Channel " + name + " successfully recovered event queue");
        } else {
            System.err.println("Channel " + name + " failed to recover event queue");
        }
        return recovered;
    }
    
    /**
     * Manually backs up the event queue.
     * 
     * @return true if the backup was successful, false otherwise
     */
    public boolean backupQueue() {
        return eventQueue.manualBackup();
    }
    
    /**
     * Simulates a queue crash.
     */
    public void simulateQueueCrash() {
        System.out.println("Simulating crash for channel " + name + "'s event queue");
        eventQueue.simulateCrash();
    }
    
    /**
     * Sets whether the channel should attempt automatic recovery from queue crashes.
     * 
     * @param automatic true to enable automatic recovery, false to disable
     */
    public void setAutomaticRecovery(boolean automatic) {
        this.automaticRecovery = automatic;
    }
    
    /**
     * Checks if the channel's queue has crashed.
     * 
     * @return true if the queue has crashed, false otherwise
     */
    public boolean isQueueCrashed() {
        return eventQueue.isCrashed();
    }
    
    /**
     * Shuts down the channel's background tasks.
     */
    public void shutdown() {
        eventQueue.shutdown();
    }
    
    /**
     * A wrapper class to make Events serializable for persistence.
     */
    private static class SerializableEventWrapper implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final long timestamp;
        private final String type;
        
        /**
         * Creates a new serializable event wrapper.
         * 
         * @param event the event to wrap
         */
        public SerializableEventWrapper(Event event) {
            this.timestamp = event.getTimestamp();
            this.type = event.getType();
        }
        
        /**
         * Gets the wrapped event.
         * 
         * @return the event
         */
        public Event getEvent() {
            return new SimpleEvent(type, timestamp);
        }
    }
    
    /**
     * A simple implementation of Event for recovery.
     */
    private static class SimpleEvent implements Event {
        private final String type;
        private final long timestamp;
        
        /**
         * Creates a new simple event.
         * 
         * @param type the event type
         * @param timestamp the event timestamp
         */
        public SimpleEvent(String type, long timestamp) {
            this.type = type;
            this.timestamp = timestamp;
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
            return "SimpleEvent{type='" + type + "', timestamp=" + timestamp + '}';
        }
    }
}