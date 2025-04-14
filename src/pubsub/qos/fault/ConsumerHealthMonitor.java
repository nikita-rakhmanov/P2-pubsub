package src.pubsub.qos.fault;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Monitors the health of consumers in the system.
 * Used to handle R6: Crashing consumers.
 */
public class ConsumerHealthMonitor {
    private static final ConsumerHealthMonitor INSTANCE = new ConsumerHealthMonitor();
    
    // Maps consumer IDs to their last heartbeat time
    private final Map<String, Long> lastHeartbeats = new ConcurrentHashMap<>();
    
    // Maps consumer IDs to their health status
    private final Map<String, ConsumerStatus> consumerStatus = new ConcurrentHashMap<>();
    
    // Registered health change listeners
    private final Map<String, ConsumerHealthListener> healthListeners = new HashMap<>();
    
    private final ScheduledExecutorService scheduler;
    private final long heartbeatTimeoutMs;
    
    /**
     * Consumer health status enum.
     */
    public enum ConsumerStatus {
        HEALTHY, SUSPECTED_DEAD, CONFIRMED_DEAD, RECOVERED
    }
    
    /**
     * Interface for consumer health change listeners.
     */
    public interface ConsumerHealthListener {
        /**
         * Called when a consumer's health status changes.
         * 
         * @param consumerId the consumer ID
         * @param oldStatus the old status
         * @param newStatus the new status
         */
        void onConsumerHealthChange(String consumerId, ConsumerStatus oldStatus, ConsumerStatus newStatus);
    }
    
    private ConsumerHealthMonitor() {
        this.heartbeatTimeoutMs = 5000; // 5 seconds
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start health checking task
        scheduler.scheduleAtFixedRate(this::checkConsumerHealth, 
                heartbeatTimeoutMs / 2, heartbeatTimeoutMs / 2, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Gets the singleton instance of the ConsumerHealthMonitor.
     * 
     * @return the singleton instance
     */
    public static ConsumerHealthMonitor getInstance() {
        return INSTANCE;
    }
    
    /**
     * Registers a consumer for health monitoring.
     * 
     * @param consumerId the consumer ID
     */
    public void registerConsumer(String consumerId) {
        lastHeartbeats.put(consumerId, System.currentTimeMillis());
        updateStatus(consumerId, ConsumerStatus.HEALTHY);
    }
    
    /**
     * Records a heartbeat from a consumer.
     * 
     * @param consumerId the consumer ID
     */
    public void recordHeartbeat(String consumerId) {
        lastHeartbeats.put(consumerId, System.currentTimeMillis());
        
        // If the consumer was marked as dead, mark it as recovered
        ConsumerStatus currentStatus = consumerStatus.get(consumerId);
        if (currentStatus == ConsumerStatus.SUSPECTED_DEAD || 
            currentStatus == ConsumerStatus.CONFIRMED_DEAD) {
            updateStatus(consumerId, ConsumerStatus.RECOVERED);
        } else if (currentStatus != ConsumerStatus.HEALTHY) {
            updateStatus(consumerId, ConsumerStatus.HEALTHY);
        }
    }
    
    /**
     * Unregisters a consumer.
     * 
     * @param consumerId the consumer ID
     */
    public void unregisterConsumer(String consumerId) {
        lastHeartbeats.remove(consumerId);
        consumerStatus.remove(consumerId);
    }
    
    /**
     * Gets the current health status of a consumer.
     * 
     * @param consumerId the consumer ID
     * @return the health status, or null if the consumer is not registered
     */
    public ConsumerStatus getConsumerStatus(String consumerId) {
        return consumerStatus.get(consumerId);
    }
    
    /**
     * Manually marks a consumer as dead (for testing).
     * 
     * @param consumerId the consumer ID
     */
    public void markConsumerAsDead(String consumerId) {
        updateStatus(consumerId, ConsumerStatus.CONFIRMED_DEAD);
    }
    
    /**
     * Registers a listener for consumer health changes.
     * 
     * @param consumerId the consumer ID to listen for
     * @param listener the listener to register
     */
    public void registerHealthListener(String consumerId, ConsumerHealthListener listener) {
        healthListeners.put(consumerId, listener);
    }
    
    /**
     * Unregisters a health listener.
     * 
     * @param consumerId the consumer ID
     */
    public void unregisterHealthListener(String consumerId) {
        healthListeners.remove(consumerId);
    }
    
    /**
     * Periodic task to check the health of all registered consumers.
     */
    private void checkConsumerHealth() {
        long now = System.currentTimeMillis();
        
        for (Map.Entry<String, Long> entry : lastHeartbeats.entrySet()) {
            String consumerId = entry.getKey();
            long lastHeartbeat = entry.getValue();
            
            // Check if the consumer has timed out
            if (now - lastHeartbeat > heartbeatTimeoutMs) {
                ConsumerStatus currentStatus = consumerStatus.get(consumerId);
                
                if (currentStatus == ConsumerStatus.HEALTHY) {
                    // First timeout - mark as suspected dead
                    updateStatus(consumerId, ConsumerStatus.SUSPECTED_DEAD);
                    System.out.println("Consumer " + consumerId + " suspected dead (no heartbeat for " + 
                            (now - lastHeartbeat) / 1000.0 + " seconds)");
                } else if (currentStatus == ConsumerStatus.SUSPECTED_DEAD && 
                          now - lastHeartbeat > heartbeatTimeoutMs * 2) {
                    // Second timeout - mark as confirmed dead
                    updateStatus(consumerId, ConsumerStatus.CONFIRMED_DEAD);
                    System.out.println("Consumer " + consumerId + " confirmed dead (no heartbeat for " + 
                            (now - lastHeartbeat) / 1000.0 + " seconds)");
                }
            }
        }
    }
    
    /**
     * Updates a consumer's status and notifies listeners.
     * 
     * @param consumerId the consumer ID
     * @param newStatus the new status
     */
    private void updateStatus(String consumerId, ConsumerStatus newStatus) {
        ConsumerStatus oldStatus = consumerStatus.put(consumerId, newStatus);
        
        // Notify listener if registered
        ConsumerHealthListener listener = healthListeners.get(consumerId);
        if (listener != null && oldStatus != newStatus) {
            listener.onConsumerHealthChange(consumerId, oldStatus, newStatus);
        }
    }
    
    /**
     * Shuts down the health monitor's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
}