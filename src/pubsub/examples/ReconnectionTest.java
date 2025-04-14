package src.pubsub.examples;

import src.pubsub.core.*;
import src.pubsub.qos.fault.ReconnectingConsumer;
import src.pubsub.qos.fault.ReconnectingPublisher;

import java.util.concurrent.TimeUnit;

/**
 * Test class to demonstrate the reconnection functionality.
 * Tests R4: Temporary interruptions of connections.
 */
public class ReconnectionTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Reconnection Test...");
        
        // Create middleware
        Middleware middleware = new BasicMiddleware();
        
        // Create channels
        middleware.createChannel("important-notifications");
        
        // Create basic publisher and wrap with reconnecting publisher
        Publisher basePublisher = new BasicPublisher("PublisherA");
        ReconnectingPublisher publisher = new ReconnectingPublisher(
                basePublisher, "PublisherA", 100, 2);
        
        // Register publisher with middleware
        publisher.registerWithMiddleware(middleware);
        
        // Create basic consumer and wrap with reconnecting consumer
        Consumer baseConsumer = new BasicConsumer("ConsumerA");
        ReconnectingConsumer consumer = new ReconnectingConsumer(
                baseConsumer, "ConsumerA", 2);
        
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
        ((BasicMiddleware) middleware).dispatchAllEvents();
        
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
        ((BasicMiddleware) middleware).dispatchAllEvents();
        
        // Simulate consumer disconnection
        System.out.println("\n--- Consumer disconnection ---");
        consumer.simulateDisconnection();
        
        // Publish more messages (consumer won't receive them now)
        for (int i = 0; i < 3; i++) {
            publisher.publish("important-notifications", 
                    new BasicEvent("Message during consumer disconnect " + i));
        }
        
        // Dispatch events
        ((BasicMiddleware) middleware).dispatchAllEvents();
        
        // Simulate consumer reconnection
        System.out.println("\n--- Consumer reconnection ---");
        consumer.simulateReconnection();
        
        // Publish more messages after consumer reconnects
        for (int i = 0; i < 3; i++) {
            publisher.publish("important-notifications", 
                    new BasicEvent("Message after consumer reconnect " + i));
        }
        
        // Dispatch events
        ((BasicMiddleware) middleware).dispatchAllEvents();
        
        // Clean up
        System.out.println("\nCleaning up...");
        publisher.shutdown();
        consumer.shutdown();
        
        System.out.println("Reconnection Test Complete!");
    }
}