package src.pubsub.examples;

import src.pubsub.core.*;

import java.util.Map;

/**
 * Comprehensive test class for the enhanced implementation.
 * Tests requirements R1-R8 using only BasicEvent, BasicPublisher, 
 * BasicConsumer, BasicChannel, and BasicMiddleware.
 */
public class EnhancedImplementationTest {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Enhanced Implementation Test...");
        
        // Create middleware with custom settings
        BasicMiddleware middleware = new BasicMiddleware(
                10000,  // 10 second message TTL
                3,      // 3 max retries
                30);    // 30 second purge interval
        
        // ===== Test R1 & R2: Channel Creation, Publishing, and Subscription =====
        System.out.println("\n===== Testing R1 & R2: Channels, Publishing, Subscription =====");
        
        // Create channels (tests R2: channel creation)
        middleware.createChannel("test-channel-1");
        middleware.createChannel("test-channel-2");
        System.out.println("Available channels: " + middleware.listChannels());
        
        // Create publishers and consumers
        BasicPublisher publisher = new BasicPublisher("TestPublisher");
        BasicConsumer consumer1 = new BasicConsumer("TestConsumer1");
        BasicConsumer consumer2 = new BasicConsumer("TestConsumer2");
        
        // Register with middleware (tests R2: discovery and access)
        publisher.registerWithMiddleware(middleware);
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        
        // Subscribe consumers to channels (tests R1: subscription)
        consumer1.subscribe("test-channel-1");
        consumer2.subscribe("test-channel-1");
        consumer2.subscribe("test-channel-2");
        
        // Publish events (tests R1: publishing)
        System.out.println("Publishing events...");
        publisher.publish("test-channel-1", new BasicEvent("Event 1 for Channel 1"));
        publisher.publish("test-channel-2", new BasicEvent("Event 1 for Channel 2"));
        publisher.publish("test-channel-1", new BasicEvent("Event 2 for Channel 1"));
        
        // Dispatch events
        middleware.dispatchAllEvents();
        Thread.sleep(500);  // Allow time for processing
        
        // ===== Test R3: Dynamic Queue Management =====
        System.out.println("\n===== Testing R3: Dynamic Queue Management =====");
        
        // Publish many events to test queue growth
        System.out.println("Publishing 50 events to test queue growth...");
        for (int i = 0; i < 50; i++) {
            publisher.publish("test-channel-1", 
                    new BasicEvent("Bulk Event " + i + " for Channel 1"));
        }
        
        // Check queue size before dispatch
        Channel channel1 = middleware.lookupChannel("test-channel-1");
        System.out.println("Channel 1 queue size before dispatch: " + channel1.getQueueSize());
        
        // Dispatch and check again
        middleware.dispatchAllEvents();
        Thread.sleep(500);  // Allow time for processing
        System.out.println("Channel 1 queue size after dispatch: " + channel1.getQueueSize());
        
        // ===== Test R4: Temporary Interruptions =====
        System.out.println("\n===== Testing R4: Temporary Interruptions =====");
        
        // Simulate publisher disconnection
        System.out.println("Simulating publisher disconnection...");
        publisher.simulateDisconnection();
        
        // Try to publish while disconnected (should buffer)
        for (int i = 0; i < 5; i++) {
            publisher.publish("test-channel-1", 
                    new BasicEvent("Buffered Event " + i));
        }
        
        // Check buffer size
        System.out.println("Publisher buffer size: " + publisher.getBufferSize());
        
        // Simulate publisher reconnection
        System.out.println("Simulating publisher reconnection...");
        publisher.simulateReconnection();
        
        // Dispatch events (buffered events should be published)
        middleware.dispatchAllEvents();
        Thread.sleep(500);  // Allow time for processing
        
        // Now test consumer disconnection/reconnection
        System.out.println("Simulating consumer disconnection...");
        consumer1.simulateDisconnection();
        
        // Publish while consumer is disconnected
        publisher.publish("test-channel-1", new BasicEvent("Event during consumer disconnect"));
        middleware.dispatchAllEvents();
        
        // Reconnect consumer
        System.out.println("Simulating consumer reconnection...");
        consumer1.simulateReconnection();
        
        // ===== Test R5: Crashing Queues =====
        System.out.println("\n===== Testing R5: Crashing Queues =====");
        
        // Set different recovery settings for channels
        middleware.setChannelAutomaticRecovery("test-channel-1", true);
        middleware.setChannelAutomaticRecovery("test-channel-2", false);
        
        // Crash the automatically recovering channel
        System.out.println("Crashing channel 1 (auto-recovery enabled)...");
        middleware.simulateChannelQueueCrash("test-channel-1");
        
        // Try to publish to crashed channel (should auto-recover)
        publisher.publish("test-channel-1", new BasicEvent("Post-crash Event - Channel 1"));
        
        // Crash the manually recovering channel
        System.out.println("Crashing channel 2 (auto-recovery disabled)...");
        middleware.simulateChannelQueueCrash("test-channel-2");
        
        // Try to publish to crashed channel (should fail)
        try {
            publisher.publish("test-channel-2", new BasicEvent("Post-crash Event - Channel 2"));
            System.out.println("Published to crashed channel 2 (unexpected)");
        } catch (Exception e) {
            System.out.println("Failed to publish to crashed channel 2 as expected: " + e.getMessage());
        }
        
        // Manually recover the channel
        System.out.println("Manually recovering channel 2...");
        middleware.recoverChannelQueue("test-channel-2");
        
        // Now publish should succeed
        publisher.publish("test-channel-2", new BasicEvent("Post-recovery Event - Channel 2"));
        
        // ===== Test R6: Crashing Consumers =====
        System.out.println("\n===== Testing R6: Crashing Consumers =====");
        
        // Crash a consumer
        System.out.println("Simulating consumer crash...");
        consumer1.simulateCrash();
        
        // Try to use crashed consumer
        try {
            consumer1.consume(new BasicEvent("Test Event"));
            System.out.println("Consuming on crashed consumer succeeded (unexpected)");
        } catch (Exception e) {
            System.out.println("Consuming on crashed consumer failed as expected: " + e.getMessage());
        }
        
        // Recover the consumer
        System.out.println("Recovering crashed consumer...");
        consumer1.recover();
        
        // Verify recovered consumer works
        System.out.println("Publishing after consumer recovery...");
        publisher.publish("test-channel-1", new BasicEvent("Post-consumer-recovery Event"));
        middleware.dispatchAllEvents();
        
        // ===== Test R7: Long Delays in Network Traffic =====
        System.out.println("\n===== Testing R7: Long Delays in Network Traffic =====");
        
        // Configure different delay settings for channels
        middleware.simulateChannelNetworkDelay("test-channel-1", 100);  // Fixed delay
        middleware.simulateChannelVariableNetworkDelay("test-channel-2", 200, 500);  // Variable delay
        
        // Publish to channels with different delay settings
        System.out.println("Publishing to channels with network delays...");
        for (int i = 0; i < 3; i++) {
            publisher.publish("test-channel-1", new BasicEvent("Delayed Event " + i + " - Channel 1"));
            publisher.publish("test-channel-2", new BasicEvent("Delayed Event " + i + " - Channel 2"));
        }
        
        // Dispatch and wait for delayed delivery
        System.out.println("Dispatching and waiting for delayed delivery...");
        middleware.dispatchAllEvents();
        Thread.sleep(1000);  // Wait for delays
        
        // ===== Test R8: Dropped Messages =====
        System.out.println("\n===== Testing R8: Dropped Messages =====");
        
        // Configure message loss simulation
        middleware.setChannelDeliveryFailureProbability("test-channel-1", 0.3);  // 30% loss rate
        middleware.simulateConsumerMessageLoss("TestConsumer2", true, 0.2);  // 20% consumer loss
        
        // Publish with message loss simulation active
        System.out.println("Publishing with message loss simulation...");
        for (int i = 0; i < 10; i++) {
            publisher.publish("test-channel-1", new BasicEvent("Lossy Event " + i));
        }
        
        // Dispatch and allow time for retries
        System.out.println("Dispatching and waiting for delivery with retries...");
        middleware.dispatchAllEvents();
        Thread.sleep(3000);  // Wait for retries
        
        // Print delivery statistics
        System.out.println("\n===== Delivery Statistics =====");
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        for (Map.Entry<String, BasicMiddleware.DeliveryStats> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        
        // Clean up
        middleware.shutdown();
        System.out.println("\nEnhanced Implementation Test Complete!");
    }
}