package src.pubsub.examples;

import src.pubsub.core.BasicConsumer;
import src.pubsub.core.BasicEvent;
import src.pubsub.core.BasicMiddleware;
import src.pubsub.core.BasicPublisher;
import src.pubsub.core.Consumer;
import src.pubsub.core.Publisher;

/**
 * Test class to demonstrate the queue crash recovery functionality.
 * Tests R5: Crashing queues.
 */
public class QueueCrashTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Queue Crash Recovery Test...");
        
        // Create middleware
        BasicMiddleware middleware = new BasicMiddleware();
        
        // Create channels
        middleware.createChannel("critical-data");
        middleware.createChannel("regular-data");
        
        // Set automatic recovery for critical channel only
        middleware.setChannelAutomaticRecovery("critical-data", true);
        middleware.setChannelAutomaticRecovery("regular-data", false);
        
        // Create publishers and consumers
        Publisher publisher = new BasicPublisher("DataProducer");
        Consumer consumer = new BasicConsumer("DataConsumer");
        
        // Register with middleware
        publisher.registerWithMiddleware(middleware);
        consumer.registerWithMiddleware(middleware);
        
        // Subscribe consumer to channels
        consumer.subscribe("critical-data");
        consumer.subscribe("regular-data");
        
        // Publish some initial messages
        System.out.println("\n--- Initial publishing ---");
        for (int i = 0; i < 5; i++) {
            publisher.publish("critical-data", new BasicEvent("Critical Message " + i));
            publisher.publish("regular-data", new BasicEvent("Regular Message " + i));
        }
        
        // Force backup of the current state
        System.out.println("\n--- Forcing backup before crash ---");
        Thread.sleep(6000); // Wait for automatic backup to happen
        
        // Crash the regular channel's queue
        System.out.println("\n--- Simulating queue crash for regular-data channel ---");
        middleware.simulateChannelQueueCrash("regular-data");
        
        // Try to publish to the crashed channel - should fail since automatic recovery is disabled
        System.out.println("\n--- Attempting to publish to crashed channel ---");
        try {
            publisher.publish("regular-data", new BasicEvent("Post-crash Regular Message"));
            System.out.println("Publish to regular-data succeeded (unexpected)");
        } catch (Exception e) {
            System.out.println("Publish to regular-data failed as expected: " + e.getMessage());
        }
        
        // Crash the critical channel's queue
        System.out.println("\n--- Simulating queue crash for critical-data channel ---");
        middleware.simulateChannelQueueCrash("critical-data");
        
        // Try to publish to the critical channel - should succeed due to automatic recovery
        System.out.println("\n--- Attempting to publish to critical channel ---");
        try {
            publisher.publish("critical-data", new BasicEvent("Post-crash Critical Message"));
            System.out.println("Publish to critical-data succeeded (expected with auto-recovery)");
        } catch (Exception e) {
            System.out.println("Publish to critical-data failed unexpectedly: " + e.getMessage());
        }
        
        // Manually recover the regular channel's queue
        System.out.println("\n--- Manually recovering regular-data channel queue ---");
        boolean recovered = middleware.recoverChannelQueue("regular-data");
        if (recovered) {
            System.out.println("Regular-data channel queue recovered successfully");
        } else {
            System.out.println("Failed to recover regular-data channel queue");
        }
        
        // Publish more messages after recovery
        System.out.println("\n--- Publishing after recovery ---");
        for (int i = 0; i < 3; i++) {
            publisher.publish("critical-data", new BasicEvent("Post-recovery Critical Message " + i));
            publisher.publish("regular-data", new BasicEvent("Post-recovery Regular Message " + i));
        }
        
        // Dispatch events
        System.out.println("\n--- Dispatching events ---");
        middleware.dispatchAllEvents();
        
        // Clean up
        System.out.println("\nCleaning up...");
        middleware.shutdown();
        
        System.out.println("Queue Crash Recovery Test Complete!");
    }
}