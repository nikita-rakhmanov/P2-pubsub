package src.pubsub.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import src.pubsub.qos.fault.ConnectionManager;
import src.pubsub.qos.fault.MessageBuffer;

/**
 * Implementation of the Publisher interface that includes QoS features.
 */
public class BasicPublisher implements Publisher {
    private Middleware middleware;
    private final String id;
    
    // Connection management for R4: Temporary interruptions
    private final ConnectionManager connectionManager;
    private final MessageBuffer messageBuffer;
    private final ScheduledExecutorService scheduler;
    private final int reconnectInterval;
    
    /**
     * Creates a new publisher with the specified ID.
     * 
     * @param id the publisher ID
     */
    public BasicPublisher(String id) {
        this(id, 100, 5); // Default 100 max buffer size, 5 second reconnect interval
    }
    
    /**
     * Creates a new publisher with custom buffer and reconnect settings.
     * 
     * @param id the publisher ID
     * @param maxBufferSize the maximum buffer size for storing messages during disconnections
     * @param reconnectInterval the interval in seconds to retry reconnection
     */
    public BasicPublisher(String id, int maxBufferSize, int reconnectInterval) {
        this.id = id;
        this.messageBuffer = new MessageBuffer(10, maxBufferSize);
        this.connectionManager = ConnectionManager.getInstance();
        this.connectionManager.registerComponent(id);
        this.reconnectInterval = reconnectInterval;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start background task to retry buffered messages
        startRetryTask();
    }
    
    @Override
    public void publish(String channelName, Event event) {
        if (middleware == null) {
            throw new IllegalStateException("Publisher not registered with middleware");
        }
        
        if (connectionManager.isConnected(id)) {
            try {
                Channel channel = middleware.lookupChannel(channelName);
                if (channel != null) {
                    channel.addEvent(event);
                    System.out.println("Publisher " + id + " published event " + event.getType() + 
                            " to channel " + channelName);
                    
                    // If we have buffered messages, try to publish them too
                    processBufferedMessages();
                } else {
                    System.err.println("Channel not found: " + channelName);
                }
            } catch (Exception e) {
                // If publication fails, buffer the message
                connectionManager.simulateDisconnection(id);
                bufferMessage(channelName, event);
            }
        } else {
            // We're disconnected, buffer the message
            bufferMessage(channelName, event);
        }
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        this.middleware = middleware;
        middleware.registerPublisher(this);
        System.out.println("Publisher " + id + " registered with middleware");
    }
    
    /**
     * Gets the publisher ID.
     * 
     * @return the ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Buffers a message for later delivery.
     * 
     * @param channelName the channel name
     * @param event the event to buffer
     */
    private void bufferMessage(String channelName, Event event) {
        boolean added = messageBuffer.bufferMessage(channelName, event);
        if (added) {
            System.out.println("Publisher " + id + " buffered message for channel " + channelName);
        } else {
            System.err.println("Publisher " + id + " buffer full! Dropped message for channel " + channelName);
        }
    }
    
    /**
     * Processes buffered messages when connection is restored.
     */
    private void processBufferedMessages() {
        while (!messageBuffer.isEmpty() && connectionManager.isConnected(id)) {
            MessageBuffer.BufferedMessage message = messageBuffer.getNextMessage();
            try {
                Channel channel = middleware.lookupChannel(message.getChannelName());
                if (channel != null) {
                    channel.addEvent(message.getEvent());
                    System.out.println("Publisher " + id + " successfully published buffered message to " + 
                            message.getChannelName());
                } else {
                    System.err.println("Channel not found when publishing buffered message: " + 
                            message.getChannelName());
                }
            } catch (Exception e) {
                // If publishing fails, add back to the buffer and stop processing
                messageBuffer.bufferMessage(message.getChannelName(), message.getEvent());
                break;
            }
        }
    }
    
    /**
     * Starts the scheduled task that retries reconnection and processing buffered messages.
     */
    private void startRetryTask() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!connectionManager.isConnected(id)) {
                // Try to reconnect
                // In a real system, this would involve network operations
                // For simulation, we'll just randomly reconnect sometimes
                if (Math.random() < 0.3) { // 30% chance of reconnection each attempt
                    connectionManager.simulateReconnection(id);
                    processBufferedMessages();
                }
            }
        }, reconnectInterval, reconnectInterval, TimeUnit.SECONDS);
    }
    
    /**
     * Simulates a connection disruption.
     */
    public void simulateDisconnection() {
        connectionManager.simulateDisconnection(id);
    }
    
    /**
     * Simulates a connection restoration.
     */
    public void simulateReconnection() {
        connectionManager.simulateReconnection(id);
        // Process any buffered messages
        processBufferedMessages();
    }
    
    /**
     * Shuts down the publisher's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
    
    /**
     * Gets the number of messages in the buffer.
     * 
     * @return buffer size
     */
    public int getBufferSize() {
        return messageBuffer.size();
    }
    
    /**
     * Checks if the publisher is currently connected.
     * 
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return connectionManager.isConnected(id);
    }
}