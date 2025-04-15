package src.pubsub.examples;

import src.pubsub.core.BasicConsumer;
import src.pubsub.core.BasicEvent;
import src.pubsub.core.BasicPublisher;
import src.pubsub.core.Channel;
import src.pubsub.core.Publisher;
import src.pubsub.qos.fault.ConsumerHealthMonitor;
import src.pubsub.qos.fault.RecoverableMiddleware;
import src.pubsub.qos.fault.StatefulConsumer;

/**
 * Test class to demonstrate the consumer crash recovery functionality.
 * Tests R6: Crashing consumers.
 */
public class ConsumerCrashTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Consumer Crash Test...");
        
        // Create middleware
        RecoverableMiddleware middleware = new RecoverableMiddleware();
        
        // Create channels
        middleware.createChannel("important-updates");
        middleware.createChannel("status-reports");
        
        // Create publisher
        Publisher publisher = new BasicPublisher("MainPublisher");
        publisher.registerWithMiddleware(middleware);
        
        // Create consumers
        BasicConsumer basicConsumer1 = new BasicConsumer("InternalConsumer1");
        BasicConsumer basicConsumer2 = new BasicConsumer("InternalConsumer2");
        
        // Wrap with stateful consumers for crash recovery
        StatefulConsumer consumer1 = new StatefulConsumer(basicConsumer1, "StatefulConsumer1");
        StatefulConsumer consumer2 = new StatefulConsumer(basicConsumer2, "StatefulConsumer2");
        
        // Register consumers with middleware
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        
        // Subscribe consumers to channels
        System.out.println("\n--- Initial subscriptions ---");
        consumer1.subscribe("important-updates");
        consumer1.subscribe("status-reports");
        consumer2.subscribe("important-updates");
        
        // Publish some initial events
        System.out.println("\n--- Initial publishing ---");
        publisher.publish("important-updates", new BasicEvent("Important Update 1"));
        publisher.publish("status-reports", new BasicEvent("Status Report 1"));
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Simulate consumer crash
        System.out.println("\n--- Simulating consumer crash ---");
        consumer1.simulateCrash();
        
        // Try to use the crashed consumer - should throw exception
        try {
            consumer1.consume(new BasicEvent("Test Event"));
            System.out.println("Consuming event on crashed consumer succeeded (unexpected)");
        } catch (Exception e) {
            System.out.println("Consuming event on crashed consumer failed as expected: " + e.getMessage());
        }
        
        // Wait for health monitor to detect crash (takes a few seconds)
        System.out.println("\n--- Waiting for health monitor to detect crash ---");
        for (int i = 0; i < 10; i++) {
            ConsumerHealthMonitor.ConsumerStatus status = 
                    ConsumerHealthMonitor.getInstance().getConsumerStatus("StatefulConsumer1");
            System.out.println("Consumer status: " + status);
            
            if (status == ConsumerHealthMonitor.ConsumerStatus.CONFIRMED_DEAD) {
                break;
            }
            
            Thread.sleep(1000);
        }
        
        // Publish more events while consumer is crashed
        System.out.println("\n--- Publishing while consumer is crashed ---");
        publisher.publish("important-updates", new BasicEvent("Important Update 2"));
        publisher.publish("status-reports", new BasicEvent("Status Report 2"));
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Recover the consumer
        System.out.println("\n--- Recovering consumer ---");
        consumer1.recover();
        
        // Wait for health monitor to detect recovery
        Thread.sleep(2000);
        
        // Check subscription status
        System.out.println("\n--- Verifying subscriptions after recovery ---");
        
        // Get channels to check subscriptions
        Channel importantChannel = middleware.lookupChannel("important-updates");
        Channel statusChannel = middleware.lookupChannel("status-reports");
        
        System.out.println("Consumer1 in important-updates subscribers: " + 
                importantChannel.getSubscribers().contains(consumer1));
        System.out.println("Consumer1 in status-reports subscribers: " + 
                statusChannel.getSubscribers().contains(consumer1));
        
        // Publish events after recovery
        System.out.println("\n--- Publishing after recovery ---");
        publisher.publish("important-updates", new BasicEvent("Important Update 3"));
        publisher.publish("status-reports", new BasicEvent("Status Report 3"));
        
        // Dispatch events
        middleware.dispatchAllEvents();
        
        // Clean up
        System.out.println("\nCleaning up...");
        consumer1.shutdown();
        consumer2.shutdown();
        ConsumerHealthMonitor.getInstance().shutdown();
        
        System.out.println("Consumer Crash Test Complete!");
    }
}