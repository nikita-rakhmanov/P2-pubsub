package src.pubsub.qos.network;

import src.pubsub.core.Consumer;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simulates various network conditions for testing.
 * Used for R7: Long delays in network traffic.
 */
public class NetworkSimulator {
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    
    // Delay configuration
    private NetworkDelayType delayType = NetworkDelayType.NONE;
    private int fixedDelayMs = 0;
    private int minVariableDelayMs = 0;
    private int maxVariableDelayMs = 0;
    private int baseJitterDelayMs = 0;
    private int jitterMs = 0;
    
    // Failure configuration
    private double deliveryFailureProbability = 0.0;
    private int deliveryFailureThresholdRetries = Integer.MAX_VALUE;
    
    /**
     * Network delay type enum.
     */
    public enum NetworkDelayType {
        NONE,
        FIXED,
        VARIABLE,
        JITTER
    }
    
    /**
     * Sets a fixed network delay.
     * 
     * @param delayMs the delay in milliseconds
     */
    public void setFixedDelayMs(int delayMs) {
        this.delayType = NetworkDelayType.FIXED;
        this.fixedDelayMs = delayMs;
    }
    
    /**
     * Sets a variable network delay.
     * 
     * @param minDelayMs the minimum delay in milliseconds
     * @param maxDelayMs the maximum delay in milliseconds
     */
    public void setVariableDelay(int minDelayMs, int maxDelayMs) {
        this.delayType = NetworkDelayType.VARIABLE;
        this.minVariableDelayMs = minDelayMs;
        this.maxVariableDelayMs = maxDelayMs;
    }
    
    /**
     * Sets a jittery network delay.
     * 
     * @param baseDelayMs the base delay in milliseconds
     * @param jitterMs the jitter in milliseconds (+/-)
     */
    public void setJitterDelay(int baseDelayMs, int jitterMs) {
        this.delayType = NetworkDelayType.JITTER;
        this.baseJitterDelayMs = baseDelayMs;
        this.jitterMs = jitterMs;
    }
    
    /**
     * Sets the probability of a message delivery failure.
     * 
     * @param probability the probability (0.0 to 1.0)
     */
    public void setDeliveryFailureProbability(double probability) {
        this.deliveryFailureProbability = Math.max(0.0, Math.min(1.0, probability));
    }
    
    /**
     * Sets the retry threshold after which messages will start succeeding.
     * 
     * @param retries the number of retries
     */
    public void setDeliveryFailureThresholdRetries(int retries) {
        this.deliveryFailureThresholdRetries = retries;
    }
    
    /**
     * Delivers a message to a consumer with potential network delay.
     * 
     * @param message the message to deliver
     * @param consumer the consumer to deliver to
     * @param retryCount the number of previous retry attempts
     * @return a future that completes when the message is delivered or fails
     */
    public boolean deliverWithPotentialDelay(TimestampedMessage message, 
            Consumer consumer, int retryCount) {
        try {
            // Check if this delivery should fail
            if (shouldDeliveryFail(retryCount)) {
                return false;
            }
            
            // Calculate delay based on configuration
            int delayMs = calculateDelay();
            
            if (delayMs <= 0) {
                // No delay, deliver immediately
                consumer.consume(message.getEvent());
                return true;
            } else {
                // Deliver after delay
                CompletableFuture.runAsync(() -> {
                    try {
                        // Simulate network delay
                        Thread.sleep(delayMs);
                        
                        // Check if message has expired during the delay
                        if (message.isExpired()) {
                            System.out.println("Message expired during network delay: " + message);
                            return;
                        }
                        
                        // Deliver the message
                        consumer.consume(message.getEvent());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("Error delivering message after delay: " + e.getMessage());
                    }
                }, scheduler);
                
                // We consider this a successful delivery even though it's delayed
                return true;
            }
        } catch (Exception e) {
            System.err.println("Error in message delivery: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Determines if a message delivery should fail based on configuration.
     * 
     * @param retryCount the number of previous retry attempts
     * @return true if the delivery should fail, false otherwise
     */
    private boolean shouldDeliveryFail(int retryCount) {
        // If we've exceeded the retry threshold, don't fail
        if (retryCount >= deliveryFailureThresholdRetries) {
            return false;
        }
        
        // Random failure based on probability
        return random.nextDouble() < deliveryFailureProbability;
    }
    
    /**
     * Calculates the network delay based on the current configuration.
     * 
     * @return the delay in milliseconds
     */
    private int calculateDelay() {
        switch (delayType) {
            case FIXED:
                return fixedDelayMs;
                
            case VARIABLE:
                return minVariableDelayMs + 
                        random.nextInt(maxVariableDelayMs - minVariableDelayMs + 1);
                
            case JITTER:
                int jitterRange = jitterMs * 2;  // +/- jitter
                int jitterOffset = random.nextInt(jitterRange) - jitterMs;
                return Math.max(0, baseJitterDelayMs + jitterOffset);
                
            case NONE:
            default:
                return 0;
        }
    }
    
    /**
     * Shuts down the simulator's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
}