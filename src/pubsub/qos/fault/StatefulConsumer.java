package src.pubsub.qos.fault;

import src.pubsub.core.Consumer;
import src.pubsub.core.Event;
import src.pubsub.core.Middleware;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A consumer implementation that can recover from crashes.
 * Used to handle R6: Crashing consumers.
 */
public class StatefulConsumer implements Consumer {
    private final String id;
    private final Consumer delegate;
    private final SubscriptionManager subscriptionManager;
    private final ConsumerHealthMonitor healthMonitor;
    private final ScheduledExecutorService heartbeatScheduler;
    private RecoverableMiddleware middleware;
    private boolean crashed = false;
    
    /**
     * Creates a new stateful consumer with auto-generated ID.
     * 
     * @param delegate the underlying consumer
     */
    public StatefulConsumer(Consumer delegate) {
        this(delegate, "consumer-" + UUID.randomUUID().toString());
    }
    
    /**
     * Creates a new stateful consumer with the specified ID.
     * 
     * @param delegate the underlying consumer
     * @param id the consumer ID
     */
    public StatefulConsumer(Consumer delegate, String id) {
        this.delegate = Objects.requireNonNull(delegate);
        this.id = id;
        this.subscriptionManager = SubscriptionManager.getInstance();
        this.healthMonitor = ConsumerHealthMonitor.getInstance();
        this.heartbeatScheduler = Executors.newScheduledThreadPool(1);
        
        // Register with health monitor
        healthMonitor.registerConsumer(id);
        
        // Register health change listener for recovery
        healthMonitor.registerHealthListener(id, this::handleHealthChange);
        
        // Start sending heartbeats
        startHeartbeats();
    }
    
    @Override
    public void consume(Event event) {
        checkCrashed();
        delegate.consume(event);
    }
    
    @Override
    public void subscribe(String channelName) {
        checkCrashed();
        delegate.subscribe(channelName);
        
        // Record subscription for recovery
        subscriptionManager.recordSubscription(id, channelName);
    }
    
    @Override
    public void unsubscribe(String channelName) {
        checkCrashed();
        delegate.unsubscribe(channelName);
        
        // Record unsubscription
        subscriptionManager.recordUnsubscription(id, channelName);
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        this.middleware = (RecoverableMiddleware) middleware;
        delegate.registerWithMiddleware(middleware);
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
     * Starts the heartbeat task.
     */
    private void startHeartbeats() {
        heartbeatScheduler.scheduleAtFixedRate(this::sendHeartbeat, 1, 1, TimeUnit.SECONDS);
    }
    
    /**
     * Sends a heartbeat to the health monitor.
     */
    private void sendHeartbeat() {
        if (!crashed) {
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
            if (middleware != null) {
                subscriptionManager.restoreSubscriptions(this, middleware);
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
        if (middleware != null) {
            subscriptionManager.restoreSubscriptions(this, middleware);
        }
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
     * Checks if the consumer is currently crashed.
     * 
     * @return true if crashed, false otherwise
     */
    public boolean isCrashed() {
        return crashed;
    }
    
    /**
     * Shuts down the consumer's background tasks.
     */
    public void shutdown() {
        heartbeatScheduler.shutdownNow();
        healthMonitor.unregisterHealthListener(id);
        healthMonitor.unregisterConsumer(id);
    }
}