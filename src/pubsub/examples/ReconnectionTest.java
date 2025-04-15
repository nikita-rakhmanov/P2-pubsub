package src.pubsub.examples;

import src.pubsub.core.BasicConsumer;
import src.pubsub.core.BasicEvent;
import src.pubsub.core.BasicMiddleware;
import src.pubsub.core.BasicPublisher;

/**
 * Test class to demonstrate the reconnection functionality.
 * Tests R4: Temporary interruptions of connections.
 * Refactored to use BasicXXX implementation.
 */
public class ReconnectionTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Reconnection Test...");
        
        // Create middleware
        BasicMiddleware middleware = new BasicMiddleware();
        
        // Create channels
        middleware.createChannel("important-notifications");
        
        // Create publisher with reconnection capabilities
        BasicPublisher publisher = new BasicPublisher("PublisherA", 100, 2);
        
        // Register publisher with middleware
        publisher.registerWithMiddleware(middleware);
        
        // Create consumer with reconnection capabilities
        BasicConsumer consumer = new BasicConsumer("ConsumerA", 2);
        
        // Register consumer with middleware
        consumer.registerWithMiddleware(middleware);
        
        // Subscribe consumer to channel
        consumer.subscribe("important-notifications");
        
        // Publish a few messages normally
        System.out.println("\n--- Normal operation ---");
        for (int i = 0; i < 3; i++) {
            publisher.publish("important-notifications", 
                    new BasicEvent("Normal Message " + i));
        }
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Simulate publisher disconnection
        System.out.println("\n--- Publisher disconnection ---");
        publisher.simulateDisconnection();
        
        // Try to publish while disconnected (should be buffered)
        for (int i = 0; i < 5; i++) {
            publisher.publish("important-notifications", 
                    new BasicEvent("Buffered Message " + i));
        }
        
        // Check buffer size
        System.out.println("Publisher buffer size: " + publisher.getBufferSize());
        
        // Simulate publisher reconnection
        System.out.println("\n--- Publisher reconnection ---");
        publisher.simulateReconnection();
        
        // Dispatch events (buffered messages should now be published)
        middleware.dispatchAllEvents();
        
        // Simulate consumer disconnection
        System.out.println("\n--- Consumer disconnection ---");
        consumer.simulateDisconnection();
        
        // Publish more messages (consumer won't receive them now)
        for (int i = 0; i < 3; i++) {
            publisher.publish("important-notifications", 
                    new BasicEvent("Message during consumer disconnect " + i));
        }
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Simulate consumer reconnection
        System.out.println("\n--- Consumer reconnection ---");
        consumer.simulateReconnection();
        
        // Publish more messages after consumer reconnects
        for (int i = 0; i < 3; i++) {
            publisher.publish("important-notifications", 
                    new BasicEvent("Message after consumer reconnect " + i));
        }
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Clean up
        System.out.println("\nCleaning up...");
        publisher.shutdown();
        consumer.shutdown();
        middleware.shutdown();
        
        System.out.println("Reconnection Test Complete!");
    }
}