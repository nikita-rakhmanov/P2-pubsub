package src.pubsub.qos.fault;

import src.pubsub.core.DynamicQueue;
import src.pubsub.core.Event;

/**
 * Buffer for storing messages during connection interruptions.
 * Used to ensure messages aren't lost when temporary disconnections occur (R4).
 */
public class MessageBuffer {
    private final DynamicQueue<BufferedMessage> buffer;
    private final int maxBufferSize;
    
    /**
     * Creates a new message buffer with the specified capacity.
     * 
     * @param initialCapacity the initial buffer capacity
     * @param maxBufferSize the maximum size the buffer can grow to
     */
    public MessageBuffer(int initialCapacity, int maxBufferSize) {
        this.buffer = new DynamicQueue<>(initialCapacity);
        this.maxBufferSize = maxBufferSize;
    }
    
    /**
     * Adds a message to the buffer.
     * 
     * @param channelName the channel the message is for
     * @param event the event to buffer
     * @return true if the message was added, false if the buffer is full
     */
    public boolean bufferMessage(String channelName, Event event) {
        if (buffer.size() >= maxBufferSize) {
            return false; // Buffer full, can't add more messages
        }
        
        buffer.add(new BufferedMessage(channelName, event));
        return true;
    }
    
    /**
     * Gets the next buffered message.
     * 
     * @return the next buffered message, or null if the buffer is empty
     */
    public BufferedMessage getNextMessage() {
        return buffer.poll();
    }
    
    /**
     * Checks if the buffer is empty.
     * 
     * @return true if the buffer is empty, false otherwise
     */
    public boolean isEmpty() {
        return buffer.size() == 0;
    }
    
    /**
     * Gets the current buffer size.
     * 
     * @return number of messages in the buffer
     */
    public int size() {
        return buffer.size();
    }
    
    /**
     * Class representing a buffered message.
     */
    public static class BufferedMessage {
        private final String channelName;
        private final Event event;
        
        /**
         * Creates a new buffered message.
         * 
         * @param channelName the channel name
         * @param event the event
         */
        public BufferedMessage(String channelName, Event event) {
            this.channelName = channelName;
            this.event = event;
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
         * Gets the event.
         * 
         * @return the event
         */
        public Event getEvent() {
            return event;
        }
    }
}