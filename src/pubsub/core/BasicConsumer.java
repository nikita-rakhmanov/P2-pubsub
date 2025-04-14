package src.pubsub.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import src.pubsub.qos.fault.ConnectionManager;
import src.pubsub.qos.fault.ConsumerHealthMonitor;

/**
 * Enhanced implementation of the Consumer interface that includes QoS features.
 * Combines features from BasicConsumer, StatefulConsumer, ReconnectingConsumer, and ReliableConsumer.
 */
public class BasicConsumer implements Consumer, Serializable {
    private static final long serialVersionUID = 1L;
    
    private Middleware middleware;
    private final String id;
    private final Set<String> subscribedChannels = new HashSet<>();
    
    // Heartbeat mechanism for R6: Crashing consumers
    private final ScheduledExecutorService heartbeatScheduler;
    private final ConsumerHealthMonitor healthMonitor;
    private boolean crashed = false;
    
    // Connection state for R4: Temporary interruptions
    private final ConnectionManager connectionManager;
    private final ScheduledExecutorService reconnectScheduler;
    private final int reconnectIntervalSeconds;
    
    // For simulating failures in testing
    private boolean simulateMessageLoss = false;
    private double messageLossProbability = 0.0;
    
    /**
     * Creates a new consumer with the specified ID.
     * 
     * @param id the consumer ID
     */
    public BasicConsumer(String id) {
        this(id, 5); // Default 5 second reconnect interval
    }
    
    /**
     * Creates a new consumer with the specified ID and reconnect interval.
     * 
     * @param id the consumer ID
     * @param reconnectIntervalSeconds the interval in seconds to retry reconnection
     */
    public BasicConsumer(String id, int reconnectIntervalSeconds) {
        this.id = id;
        this.reconnectIntervalSeconds = reconnectIntervalSeconds;
        
        // Set up health monitoring (R6)
        this.healthMonitor = ConsumerHealthMonitor.getInstance();
        this.healthMonitor.registerConsumer(id);
        this.healthMonitor.registerHealthListener(id, this::handleHealthChange);
        this.heartbeatScheduler = Executors.newScheduledThreadPool(1);
        
        // Set up connection management (R4)
        this.connectionManager = ConnectionManager.getInstance();
        this.connectionManager.registerComponent(id);
        this.reconnectScheduler = Executors.newScheduledThreadPool(1);
        
        // Start sending heartbeats and reconnection attempts
        startHeartbeats();
        startReconnectionTask();
    }
    
    @Override
    public void consume(Event event) {
        checkCrashed();
        
        if (!connectionManager.isConnected(id)) {
            System.err.println("Consumer " + id + " is disconnected, cannot consume event");
            return;
        }
        
        // Simulate message loss if enabled
        if (simulateMessageLoss && Math.random() < messageLossProbability) {
            System.out.println("Consumer " + id + " simulating message loss for " + event.getType());
            return;
        }
        
        try {
            // Process the event
            System.out.println("Consumer " + id + " received event: " + event.getType() + 
                    " at " + event.getTimestamp());
            
            // If the event is a BasicEvent with enhanced features, acknowledge it
            if (event instanceof BasicEvent) {
                BasicEvent enhancedEvent = (BasicEvent) event;
                enhancedEvent.acknowledgeConsumer(id);
            }
        } catch (Exception e) {
            System.err.println("Error consuming event: " + e.getMessage());
            connectionManager.simulateDisconnection(id);
        }
    }
    
    @Override
    public void subscribe(String channelName) {
        checkCrashed();
        
        // Track subscription for recovery purposes
        subscribedChannels.add(channelName);
        
        if (!connectionManager.isConnected(id)) {
            System.out.println("Consumer " + id + " is disconnected, will subscribe to " + 
                    channelName + " when reconnected");
            return;
        }
        
        if (middleware == null) {
            throw new IllegalStateException("Consumer not registered with middleware");
        }
        
        try {
            Channel channel = middleware.lookupChannel(channelName);
            if (channel != null) {
                channel.subscribe(this);
                System.out.println("Consumer " + id + " subscribed to channel " + channelName);
            } else {
                System.err.println("Channel not found: " + channelName);
            }
        } catch (Exception e) {
            System.err.println("Error subscribing to channel: " + e.getMessage());
            connectionManager.simulateDisconnection(id);
        }
    }
    
    @Override
    public void unsubscribe(String channelName) {
        checkCrashed();
        
        // Track unsubscription for recovery purposes
        subscribedChannels.remove(channelName);
        
        if (!connectionManager.isConnected(id)) {
            System.out.println("Consumer " + id + " is disconnected, will unsubscribe from " + 
                    channelName + " when reconnected");
            return;
        }
        
        if (middleware == null) {
            throw new IllegalStateException("Consumer not registered with middleware");
        }
        
        try {
            Channel channel = middleware.lookupChannel(channelName);
            if (channel != null) {
                channel.unsubscribe(this);
                System.out.println("Consumer " + id + " unsubscribed from channel " + channelName);
            } else {
                System.err.println("Channel not found: " + channelName);
            }
        } catch (Exception e) {
            System.err.println("Error unsubscribing from channel: " + e.getMessage());
            connectionManager.simulateDisconnection(id);
        }
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        this.middleware = middleware;
        middleware.registerConsumer(this);
        System.out.println("Consumer " + id + " registered with middleware");
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
    
    /**
     * Checks if the consumer has crashed and throws an exception if so.
     */
    private void checkCrashed() {
        if (crashed) {
            throw new IllegalStateException("Consumer " + id + " has crashed and not yet recovered");
        }
    }
    
    /**
     * Starts the heartbeat task for crash detection.
     */
    private void startHeartbeats() {
        heartbeatScheduler.scheduleAtFixedRate(this::sendHeartbeat, 1, 1, TimeUnit.SECONDS);
    }
    
    /**
     * Sends a heartbeat to the health monitor.
     */
    private void sendHeartbeat() {
        if (!crashed && connectionManager.isConnected(id)) {
            healthMonitor.recordHeartbeat(id);
        }
    }
    
    /**
     * Handles changes in the consumer's health status.
     * 
     * @param consumerId the consumer ID
     * @param oldStatus the old status
     * @param newStatus the new status
     */
    private void handleHealthChange(String consumerId, 
            ConsumerHealthMonitor.ConsumerStatus oldStatus, 
            ConsumerHealthMonitor.ConsumerStatus newStatus) {
        
        if (newStatus == ConsumerHealthMonitor.ConsumerStatus.CONFIRMED_DEAD) {
            // Consumer is confirmed dead - trigger recovery process
            System.out.println("Consumer " + id + " is confirmed dead, preparing for recovery...");
        } else if (newStatus == ConsumerHealthMonitor.ConsumerStatus.RECOVERED) {
            // Consumer has recovered - restore subscriptions
            System.out.println("Consumer " + id + " has recovered, restoring subscriptions...");
            resubscribeToChannels();
        }
    }
    
    /**
     * Starts the scheduled task for reconnection attempts.
     */
    private void startReconnectionTask() {
        reconnectScheduler.scheduleAtFixedRate(() -> {
            if (!connectionManager.isConnected(id) && !crashed) {
                // Try to reconnect
                // In a real system, this would involve network operations
                // For simulation, we'll just randomly reconnect sometimes
                if (Math.random() < 0.3) { // 30% chance of reconnection each attempt
                    connectionManager.simulateReconnection(id);
                    resubscribeToChannels();
                }
            }
        }, reconnectIntervalSeconds, reconnectIntervalSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Resubscribes to all previously subscribed channels after reconnection.
     */
    private void resubscribeToChannels() {
        if (middleware == null) {
            return;
        }
        
        for (String channelName : subscribedChannels) {
            try {
                Channel channel = middleware.lookupChannel(channelName);
                if (channel != null) {
                    channel.subscribe(this);
                    System.out.println("Consumer " + id + " resubscribed to " + channelName);
                }
            } catch (Exception e) {
                System.err.println("Consumer " + id + " failed to resubscribe to " + 
                        channelName + ": " + e.getMessage());
            }
        }
    }
    
    /**
     * Simulates a consumer crash.
     */
    public void simulateCrash() {
        System.out.println("Simulating crash for consumer " + id);
        crashed = true;
        // Stop sending heartbeats - the health monitor will detect this
    }
    
    /**
     * Recovers the consumer from a crash.
     */
    public void recover() {
        if (!crashed) {
            return; // Not crashed, nothing to recover
        }
        
        System.out.println("Recovering consumer " + id);
        crashed = false;
        
        // Resume heartbeats
        sendHeartbeat();
        
        // Restore subscriptions
        resubscribeToChannels();
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
     * Enables or disables message loss simulation.
     * 
     * @param simulate true to enable simulation, false to disable
     * @param lossProbability the probability of message loss (0.0 to 1.0)
     */
    public void simulateMessageLoss(boolean simulate, double lossProbability) {
        this.simulateMessageLoss = simulate;
        this.messageLossProbability = Math.max(0.0, Math.min(1.0, lossProbability));
    }
    
    /**
     * Shuts down the consumer's background tasks.
     */
    public void shutdown() {
        heartbeatScheduler.shutdownNow();
        reconnectScheduler.shutdownNow();
        healthMonitor.unregisterHealthListener(id);
        healthMonitor.unregisterConsumer(id);
    }
    
    /**
     * Checks if the consumer is currently crashed.
     * 
     * @return true if crashed, false otherwise
     */
    public boolean isCrashed() {
        return crashed;
    }
    
    /**
     * Checks if the consumer is currently connected.
     * 
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return connectionManager.isConnected(id);
    }
}