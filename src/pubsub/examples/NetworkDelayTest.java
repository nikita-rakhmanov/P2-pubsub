package src.pubsub.examples;

import src.pubsub.core.BasicConsumer;
import src.pubsub.core.BasicEvent;
import src.pubsub.core.BasicMiddleware;
import src.pubsub.core.BasicPublisher;
import src.pubsub.core.Event;
import src.pubsub.core.Publisher;
import src.pubsub.core.Consumer;

/**
 * Test class to demonstrate handling network delays.
 * Tests R7: Long delays in network traffic.
 * Refactored to use BasicXXX implementation.
 */
public class NetworkDelayTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Network Delay Test...");
        
        // Create middleware with custom settings
        BasicMiddleware middleware = new BasicMiddleware(
                5000,   // 5 second timeout
                3,      // 3 retries
                30);    // 30 second purge interval
        
        // Create channels with different delay characteristics
        middleware.createChannel("fast-channel", 2000, 2, 30);   // Short timeout, few retries
        middleware.createChannel("medium-channel", 5000, 3, 30); // Medium timeout
        middleware.createChannel("slow-channel", 15000, 5, 30);  // Long timeout, many retries
        
        // Create publishers and consumers
        Publisher publisher = new BasicPublisher("MainPublisher");
        Consumer fastConsumer = new BasicConsumer("FastConsumer");
        Consumer mediumConsumer = new BasicConsumer("MediumConsumer");
        Consumer slowConsumer = new BasicConsumer("SlowConsumer");
        
        // Register with middleware
        publisher.registerWithMiddleware(middleware);
        fastConsumer.registerWithMiddleware(middleware);
        mediumConsumer.registerWithMiddleware(middleware);
        slowConsumer.registerWithMiddleware(middleware);
        
        // Subscribe consumers to channels
        fastConsumer.subscribe("fast-channel");
        mediumConsumer.subscribe("medium-channel");
        slowConsumer.subscribe("slow-channel");
        
        // Set up different network conditions for each channel
        System.out.println("\n--- Configuring network conditions ---");
        
        // Fast channel: low fixed delay
        middleware.simulateChannelNetworkDelay("fast-channel", 100);
        System.out.println("Fast channel: 100ms fixed delay");
        
        // Medium channel: variable delay
        middleware.simulateChannelVariableNetworkDelay("medium-channel", 500, 2000);
        System.out.println("Medium channel: 500-2000ms variable delay");
        
        // Slow channel: jittery delay + occasional failures
        middleware.simulateChannelNetworkJitter("slow-channel", 3000, 1000);
        middleware.setChannelDeliveryFailureProbability("slow-channel", 0.3);
        System.out.println("Slow channel: 3000±1000ms jittery delay with 30% failure rate");
        
        // Phase 1: Initial publishing
        System.out.println("\n--- Phase 1: Initial publishing ---");
        for (int i = 0; i < 5; i++) {
            Event event = new BasicEvent("Message " + i);
            publisher.publish("fast-channel", event);
            publisher.publish("medium-channel", event);
            publisher.publish("slow-channel", event);
        }
        
        // Dispatch and wait for delivery
        System.out.println("\nDispatching events...");
        middleware.dispatchAllEvents();
        
        // Wait for messages to be delivered (even with delays)
        System.out.println("Waiting for delayed message delivery...");
        Thread.sleep(3000);  // Wait for fast/medium channels
        
        // Check metrics
        printChannelMetrics(middleware, "fast-channel");
        printChannelMetrics(middleware, "medium-channel");
        
        // Phase 2: Heavy publishing with network congestion
        System.out.println("\n--- Phase 2: Heavy publishing with network congestion ---");
        
        // Increase delays due to "congestion"
        middleware.simulateChannelNetworkDelay("fast-channel", 500);
        middleware.simulateChannelVariableNetworkDelay("medium-channel", 1000, 3000);
        
        // Publish many messages
        for (int i = 0; i < 20; i++) {
            Event event = new BasicEvent("Burst Message " + i);
            publisher.publish("fast-channel", event);
            publisher.publish("medium-channel", event);
            publisher.publish("slow-channel", event);
        }
        
        // Dispatch and wait for delivery
        System.out.println("\nDispatching burst events...");
        middleware.dispatchAllEvents();
        
        // Wait for messages to be delivered (even with delays)
        System.out.println("Waiting for delayed burst message delivery...");
        Thread.sleep(5000);  // Wait longer for the burst of messages
        
        // Check metrics again
        printChannelMetrics(middleware, "fast-channel");
        printChannelMetrics(middleware, "medium-channel");
        
        // Wait longer for slow channel messages
        System.out.println("\nWaiting for slow channel messages...");
        Thread.sleep(10000);
        
        // Final metrics for all channels
        System.out.println("\n--- Final delivery metrics ---");
        printChannelMetrics(middleware, "fast-channel");
        printChannelMetrics(middleware, "medium-channel");
        printChannelMetrics(middleware, "slow-channel");
        
        // Clean up
        System.out.println("\nCleaning up...");
        middleware.shutdown();
        
        System.out.println("Network Delay Test Complete!");
    }
    
    /**
     * Prints the delivery metrics for a channel.
     * 
     * @param middleware the middleware
     * @param channelName the channel name
     */
    private static void printChannelMetrics(BasicMiddleware middleware, String channelName) {
        BasicMiddleware.DeliveryStats metrics = middleware.getDeliveryStats().get(channelName);
        if (metrics != null) {
            System.out.println(channelName + " metrics: " + metrics);
        } else {
            System.out.println(channelName + " metrics not available");
        }
    }
}