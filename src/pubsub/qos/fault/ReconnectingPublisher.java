package src.pubsub.qos.fault;

import src.pubsub.core.Event;
import src.pubsub.core.Middleware;
import src.pubsub.core.Publisher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Publisher implementation that can handle temporary connection interruptions (R4).
 * Uses a MessageBuffer to store messages during disconnections and retries when the connection is restored.
 */
public class ReconnectingPublisher implements Publisher {
    private final Publisher delegate;
    private final String id;
    private final MessageBuffer messageBuffer;
    private final ConnectionManager connectionManager;
    private final ScheduledExecutorService scheduler;
    private final int reconnectInterval;
    private Middleware middleware;
    
    /**
     * Creates a new reconnecting publisher.
     * 
     * @param delegate the underlying publisher
     * @param id the publisher ID
     * @param maxBufferSize the maximum buffer size
     * @param reconnectInterval the interval in seconds to retry reconnection
     */
    public ReconnectingPublisher(Publisher delegate, String id, int maxBufferSize, int reconnectInterval) {
        this.delegate = delegate;
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
        if (connectionManager.isConnected(id)) {
            try {
                delegate.publish(channelName, event);
                
                // If we have buffered messages, try to publish them too
                processBufferedMessages();
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
        delegate.registerWithMiddleware(middleware);
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
                delegate.publish(message.getChannelName(), message.getEvent());
                System.out.println("Publisher " + id + " successfully published buffered message to " + 
                        message.getChannelName());
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
     * Gets the publisher ID.
     * 
     * @return the ID
     */
    public String getId() {
        return id;
    }
}