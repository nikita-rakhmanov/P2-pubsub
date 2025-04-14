package src.pubsub.qos.fault;

import src.pubsub.core.Consumer;
import src.pubsub.core.Event;
import src.pubsub.core.Middleware;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Consumer implementation that can handle temporary connection interruptions (R4).
 * Automatically resubscribes to channels when reconnected.
 */
public class ReconnectingConsumer implements Consumer {
    private final Consumer delegate;
    private final String id;
    private final ConnectionManager connectionManager;
    private final Set<String> subscribedChannels = new HashSet<>();
    private final ScheduledExecutorService scheduler;
    private final int reconnectInterval;
    private Middleware middleware;
    
    /**
     * Creates a new reconnecting consumer.
     * 
     * @param delegate the underlying consumer
     * @param id the consumer ID
     * @param reconnectInterval the interval in seconds to retry reconnection
     */
    public ReconnectingConsumer(Consumer delegate, String id, int reconnectInterval) {
        this.delegate = delegate;
        this.id = id;
        this.connectionManager = ConnectionManager.getInstance();
        this.connectionManager.registerComponent(id);
        this.reconnectInterval = reconnectInterval;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start background task for reconnection
        startReconnectionTask();
    }
    
    @Override
    public void consume(Event event) {
        if (connectionManager.isConnected(id)) {
            try {
                delegate.consume(event);
            } catch (Exception e) {
                // If consumption fails, mark as disconnected
                connectionManager.simulateDisconnection(id);
                System.err.println("Consumer " + id + " failed to consume event: " + e.getMessage());
            }
        } else {
            System.err.println("Consumer " + id + " is disconnected, cannot consume event");
        }
    }
    
    @Override
    public void subscribe(String channelName) {
        subscribedChannels.add(channelName);
        
        if (connectionManager.isConnected(id)) {
            try {
                delegate.subscribe(channelName);
            } catch (Exception e) {
                connectionManager.simulateDisconnection(id);
                System.err.println("Consumer " + id + " failed to subscribe to " + channelName + ": " + e.getMessage());
            }
        } else {
            System.out.println("Consumer " + id + " is disconnected, will subscribe to " + 
                    channelName + " when reconnected");
        }
    }
    
    @Override
    public void unsubscribe(String channelName) {
        subscribedChannels.remove(channelName);
        
        if (connectionManager.isConnected(id)) {
            try {
                delegate.unsubscribe(channelName);
            } catch (Exception e) {
                connectionManager.simulateDisconnection(id);
                System.err.println("Consumer " + id + " failed to unsubscribe from " + 
                        channelName + ": " + e.getMessage());
            }
        } else {
            System.out.println("Consumer " + id + " is disconnected, will unsubscribe from " + 
                    channelName + " when reconnected");
        }
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        this.middleware = middleware;
        delegate.registerWithMiddleware(middleware);
    }
    
    /**
     * Starts the scheduled task for reconnection attempts.
     */
    private void startReconnectionTask() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!connectionManager.isConnected(id)) {
                // Try to reconnect
                // In a real system, this would involve network operations
                // For simulation, we'll just randomly reconnect sometimes
                if (Math.random() < 0.3) { // 30% chance of reconnection each attempt
                    connectionManager.simulateReconnection(id);
                    resubscribeToChannels();
                }
            }
        }, reconnectInterval, reconnectInterval, TimeUnit.SECONDS);
    }
    
    /**
     * Resubscribes to all previously subscribed channels after reconnection.
     */
    private void resubscribeToChannels() {
        for (String channelName : subscribedChannels) {
            try {
                delegate.subscribe(channelName);
                System.out.println("Consumer " + id + " resubscribed to " + channelName);
            } catch (Exception e) {
                System.err.println("Consumer " + id + " failed to resubscribe to " + 
                        channelName + ": " + e.getMessage());
            }
        }
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
        // Resubscribe to channels
        resubscribeToChannels();
    }
    
    /**
     * Shuts down the consumer's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
    
    /**
     * Gets the consumer ID.
     * 
     * @return the ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * Gets the set of channels this consumer is subscribed to.
     * 
     * @return the set of channel names
     */
    public Set<String> getSubscribedChannels() {
        return new HashSet<>(subscribedChannels);
    }
}