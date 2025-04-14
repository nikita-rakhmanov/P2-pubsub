package src.pubsub.examples;

import src.pubsub.core.BasicConsumer;
import src.pubsub.core.BasicEvent;
import src.pubsub.core.BasicPublisher;
import src.pubsub.core.Event;
import src.pubsub.core.Publisher;
import src.pubsub.qos.reliability.ReliableConsumer;
import src.pubsub.qos.reliability.ReliableMiddleware;

import java.util.Map;

/**
 * Test class to demonstrate handling message loss.
 * Tests R8: Dropped messages.
 */
public class MessageLossTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Message Loss Test...");
        
        // Create middleware with quick timeouts for testing
        ReliableMiddleware middleware = new ReliableMiddleware(
                10000,  // 10 second message TTL
                2000,   // 2 second resend interval
                5);     // 5 max delivery attempts
        
        // Create channels with different reliability characteristics
        middleware.createChannel("reliable-channel");
        middleware.createChannel("lossy-channel");
        middleware.createChannel("very-lossy-channel");
        
        // Create publisher
        Publisher publisher = new BasicPublisher("TestPublisher");
        publisher.registerWithMiddleware(middleware);
        
        // Create reliable consumers
        ReliableConsumer consumer1 = new ReliableConsumer(
                new BasicConsumer("UnderlyingConsumer1"), "ReliableConsumer1");
        ReliableConsumer consumer2 = new ReliableConsumer(
                new BasicConsumer("UnderlyingConsumer2"), "ReliableConsumer2");
        ReliableConsumer consumer3 = new ReliableConsumer(
                new BasicConsumer("UnderlyingConsumer3"), "ReliableConsumer3");
        
        // Register consumers with middleware
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        consumer3.registerWithMiddleware(middleware);
        
        // Subscribe consumers to channels
        consumer1.subscribe("reliable-channel");
        consumer2.subscribe("lossy-channel");
        consumer3.subscribe("very-lossy-channel");
        
        // Configure message loss simulation
        System.out.println("\n--- Configuring message loss ---");
        
        // Channel-based loss
        middleware.simulateChannelMessageLoss("reliable-channel", false, 0.0);
        middleware.simulateChannelMessageLoss("lossy-channel", true, 0.3);
        middleware.simulateChannelMessageLoss("very-lossy-channel", true, 0.7);
        
        // Consumer-based loss (additional loss)
        consumer1.simulateMessageLoss(false, 0.0);
        consumer2.simulateMessageLoss(true, 0.2);
        consumer3.simulateMessageLoss(true, 0.3);
        
        System.out.println("reliable-channel: No message loss");
        System.out.println("lossy-channel: 30% channel loss + 20% consumer loss");
        System.out.println("very-lossy-channel: 70% channel loss + 30% consumer loss");
        
        // Phase 1: Publish initial messages
        System.out.println("\n--- Phase 1: Initial publishing ---");
        
        for (int i = 0; i < 10; i++) {
            Event event = new BasicEvent("Message " + i);
            publisher.publish("reliable-channel", event);
            publisher.publish("lossy-channel", event);
            publisher.publish("very-lossy-channel", event);
        }
        
        // Dispatch events
        System.out.println("\nDispatching initial events...");
        middleware.dispatchAllEvents();
        
        // Wait for initial delivery and acknowledgments
        Thread.sleep(1000);
        
        // Show initial stats
        System.out.println("\n--- Initial delivery stats ---");
        printDeliveryStats(middleware);
        
        // Wait for resend cycles to occur
        System.out.println("\n--- Waiting for message recovery ---");
        Thread.sleep(6000);  // Wait for 3 resend cycles
        
        // Show updated stats
        System.out.println("\n--- Stats after recovery attempts ---");
        printDeliveryStats(middleware);
        
        // Phase 2: Publish more messages with more extreme loss
        System.out.println("\n--- Phase 2: High message loss scenario ---");
        
        // Increase loss rates
        middleware.simulateChannelMessageLoss("very-lossy-channel", true, 0.9);
        consumer3.simulateMessageLoss(true, 0.5);
        
        System.out.println("very-lossy-channel now has 90% channel loss + 50% consumer loss");
        
        // Publish more messages
        for (int i = 0; i < 5; i++) {
            Event event = new BasicEvent("HighLossMessage " + i);
            publisher.publish("very-lossy-channel", event);
        }
        
        // Dispatch events
        System.out.println("\nDispatching high-loss events...");
        middleware.dispatchAllEvents();
        
        // Wait for multiple resend cycles
        System.out.println("\n--- Waiting for extended recovery ---");
        Thread.sleep(12000);  // Wait for 6 resend cycles
        
        // Show final stats
        System.out.println("\n--- Final delivery stats ---");
        printDeliveryStats(middleware);
        
        // Clean up
        System.out.println("\nCleaning up...");
        middleware.shutdown();
        
        System.out.println("Message Loss Test Complete!");
    }
    
    /**
     * Prints delivery statistics for all channels.
     * 
     * @param middleware the middleware
     */
    private static void printDeliveryStats(ReliableMiddleware middleware) {
        Map<String, ReliableMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        
        for (Map.Entry<String, ReliableMiddleware.DeliveryStats> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}