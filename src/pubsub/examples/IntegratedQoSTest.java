package src.pubsub.examples;

import src.pubsub.core.BasicConsumer;
import src.pubsub.core.BasicEvent;
import src.pubsub.core.BasicMiddleware;
import src.pubsub.core.BasicPublisher;
import src.pubsub.core.Consumer;
import src.pubsub.core.Event;
import src.pubsub.core.Publisher;

import java.util.Map;

/**
 * Integrated test class to demonstrate all QoS features working together.
 * Tests R4-R8 in a cohesive manner.
 */
public class IntegratedQoSTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Integrated QoS Test...");
        
        // Create middleware with custom settings
        BasicMiddleware middleware = new BasicMiddleware(
                10000,  // 10 second message TTL
                3,      // 3 max retries
                30);    // 30 second purge interval
        
        // Create channels with different QoS characteristics
        middleware.createChannel("reliable-channel", 10000, 3, 30);
        middleware.createChannel("delay-tolerant-channel", 30000, 5, 30);
        middleware.createChannel("critical-channel", 60000, 10, 30);
        
        // Set automatic recovery for critical channel only
        middleware.setChannelAutomaticRecovery("critical-channel", true);
        middleware.setChannelAutomaticRecovery("reliable-channel", false);
        middleware.setChannelAutomaticRecovery("delay-tolerant-channel", false);
        
        // Create publishers
        Publisher publisher1 = new BasicPublisher("MainPublisher", 100, 2);
        Publisher publisher2 = new BasicPublisher("BackupPublisher", 100, 2);
        
        // Register publishers with middleware
        publisher1.registerWithMiddleware(middleware);
        publisher2.registerWithMiddleware(middleware);
        
        // Create consumers
        Consumer consumer1 = new BasicConsumer("ReliableConsumer", 2);
        Consumer consumer2 = new BasicConsumer("DelayTolerantConsumer", 2);
        Consumer consumer3 = new BasicConsumer("CriticalConsumer", 2);
        
        // Register consumers with middleware
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        consumer3.registerWithMiddleware(middleware);
        
        // Subscribe consumers to channels
        consumer1.subscribe("reliable-channel");
        consumer2.subscribe("delay-tolerant-channel");
        consumer3.subscribe("critical-channel");
        consumer3.subscribe("reliable-channel"); // Critical consumer gets reliable messages too
        
        // Set up different network conditions for each channel
        System.out.println("\n--- Configuring network conditions ---");
        
        // Reliable channel: low fixed delay, low loss rate
        middleware.simulateChannelNetworkDelay("reliable-channel", 100);
        middleware.setChannelDeliveryFailureProbability("reliable-channel", 0.1);
        System.out.println("Reliable channel: 100ms fixed delay, 10% message loss");
        
        // Delay-tolerant channel: variable delay
        middleware.simulateChannelVariableNetworkDelay("delay-tolerant-channel", 500, 2000);
        middleware.setChannelDeliveryFailureProbability("delay-tolerant-channel", 0.2);
        System.out.println("Delay-tolerant channel: 500-2000ms variable delay, 20% message loss");
        
        // Critical channel: jittery delay + occasional failures
        middleware.simulateChannelNetworkJitter("critical-channel", 1000, 500);
        middleware.setChannelDeliveryFailureProbability("critical-channel", 0.3);
        System.out.println("Critical channel: 1000±500ms jittery delay, 30% message loss");
        
        // Configure consumer message loss simulation
        middleware.simulateConsumerMessageLoss("DelayTolerantConsumer", true, 0.1);
        System.out.println("DelayTolerantConsumer: 10% additional message loss");
        
        // Phase 1: Initial publishing
        System.out.println("\n--- Phase 1: Initial publishing ---");
        for (int i = 0; i < 5; i++) {
            Event reliableEvent = new BasicEvent("Reliable Message " + i);
            Event delayTolerantEvent = new BasicEvent("Delay-Tolerant Message " + i);
            Event criticalEvent = new BasicEvent("Critical Message " + i);
            
            publisher1.publish("reliable-channel", reliableEvent);
            publisher1.publish("delay-tolerant-channel", delayTolerantEvent);
            publisher1.publish("critical-channel", criticalEvent);
        }
        
        // Dispatch and wait for delivery
        System.out.println("\nDispatching events...");
        middleware.dispatchAllEvents();
        
        // Wait for messages to be delivered (even with delays)
        System.out.println("Waiting for delayed message delivery...");
        Thread.sleep(3000);
        
        // Show initial delivery stats
        System.out.println("\n--- Initial delivery stats ---");
        printDeliveryStats(middleware);
        
        // Phase 2: Simulate various failures
        System.out.println("\n--- Phase 2: Simulating failures ---");
        
        // Simulate publisher disconnection
        System.out.println("\n--- Simulating publisher disconnection ---");
        middleware.simulatePublisherDisconnection("MainPublisher");
        
        // Try to publish with disconnected publisher (should be buffered)
        for (int i = 5; i < 10; i++) {
            publisher1.publish("reliable-channel", new BasicEvent("Buffered Reliable Message " + i));
            publisher1.publish("critical-channel", new BasicEvent("Buffered Critical Message " + i));
        }
        
        // Use backup publisher
        for (int i = 5; i < 7; i++) {
            publisher2.publish("delay-tolerant-channel", new BasicEvent("Backup Delay-Tolerant Message " + i));
        }
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Simulate consumer crash
        System.out.println("\n--- Simulating consumer crash ---");
        middleware.simulateConsumerCrash("ReliableConsumer");
        
        // Simulate channel queue crash
        System.out.println("\n--- Simulating channel queue crash ---");
        middleware.simulateChannelQueueCrash("reliable-channel");
        
        // Try to publish to crashed channel
        try {
            publisher2.publish("reliable-channel", new BasicEvent("Post-crash Message"));
            System.out.println("Published to crashed channel (unexpected)");
        } catch (Exception e) {
            System.out.println("Publishing to crashed channel failed as expected: " + e.getMessage());
        }
        
        // Simulate extreme network delays
        System.out.println("\n--- Simulating extreme network delays ---");
        middleware.simulateChannelNetworkDelay("delay-tolerant-channel", 3000);
        
        // Publish more messages to test delay tolerance
        for (int i = 7; i < 10; i++) {
            publisher2.publish("delay-tolerant-channel", new BasicEvent("Delayed Message " + i));
        }
        
        // Recover crashed components
        System.out.println("\n--- Recovering crashed components ---");
        middleware.recoverChannelQueue("reliable-channel");
        middleware.recoverConsumer("ReliableConsumer");
        
        // Reconnect publisher
        ((BasicPublisher) publisher1).simulateReconnection();
        
        // Dispatch events again
        middleware.dispatchAllEvents();
        
        // Wait for recovery and delayed message delivery
        System.out.println("Waiting for recovery and delayed message delivery...");
        Thread.sleep(5000);
        
        // Show updated stats
        System.out.println("\n--- Updated delivery stats after recovery ---");
        printDeliveryStats(middleware);
        
        // Phase 3: Heavy load with all QoS features active
        System.out.println("\n--- Phase 3: Heavy load test ---");
        
        // Publish many messages across all channels
        for (int i = 0; i < 20; i++) {
            Event reliableEvent = new BasicEvent("Heavy Reliable Message " + i);
            Event delayTolerantEvent = new BasicEvent("Heavy Delay-Tolerant Message " + i);
            Event criticalEvent = new BasicEvent("Heavy Critical Message " + i);
            
            publisher1.publish("reliable-channel", reliableEvent);
            publisher1.publish("delay-tolerant-channel", delayTolerantEvent);
            publisher1.publish("critical-channel", criticalEvent);
        }
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Wait for processing
        System.out.println("Waiting for heavy load processing...");
        Thread.sleep(10000);
        
        // Final delivery stats
        System.out.println("\n--- Final delivery stats ---");
        printDeliveryStats(middleware);
        
        // Clean up
        System.out.println("\nCleaning up...");
        middleware.shutdown();
        
        System.out.println("Integrated QoS Test Complete!");
    }
    
    /**
     * Prints delivery statistics for all channels.
     * 
     * @param middleware the middleware
     */
    private static void printDeliveryStats(BasicMiddleware middleware) {
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        
        for (Map.Entry<String, BasicMiddleware.DeliveryStats> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}