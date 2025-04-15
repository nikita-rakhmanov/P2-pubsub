package src.pubsub;

import src.pubsub.core.*;

/**
 * Class to test the performance of the pub-sub system.
 * Tests if the system can handle 1000 messages per second as required.
 */
public class PerformanceTest {
    public static void main(String[] args) {
        System.out.println("Starting Performance Test...");
        
        // Create middleware
        Middleware middleware = new BasicMiddleware();
        
        // Create a single channel for testing
        Channel channel = middleware.createChannel("performance-test");
        
        // Create a publisher
        Publisher publisher = new BasicPublisher("PerfTestPublisher");
        publisher.registerWithMiddleware(middleware);
        
        // Create a consumer that counts messages
        final int[] messageCount = {0};
        Consumer consumer = new Consumer() {
            @Override
            public boolean consume(Event event) {
                messageCount[0]++;
                return true; // Indicate successful consumption
            }
            
            @Override
            public void subscribe(String channelName) {
                Channel ch = middleware.lookupChannel(channelName);
                if (ch != null) {
                    ch.subscribe(this);
                }
            }
            
            @Override
            public void unsubscribe(String channelName) {
                Channel ch = middleware.lookupChannel(channelName);
                if (ch != null) {
                    ch.unsubscribe(this);
                }
            }
            
            @Override
            public void registerWithMiddleware(Middleware mw) {
                mw.registerConsumer(this);
            }
        };
        
        consumer.registerWithMiddleware(middleware);
        consumer.subscribe("performance-test");
        
        // Prepare to publish 1000 messages
        final int messagesToPublish = 1000;
        
        // Record start time
        long startTime = System.currentTimeMillis();
        
        // Publish messages
        for (int i = 0; i < messagesToPublish; i++) {
            publisher.publish("performance-test", new BasicEvent("Message-" + i));
        }
        
        // Record time after publishing
        long afterPublishTime = System.currentTimeMillis();
        
        // Dispatch messages
        channel.dispatchEvents();
        
        // Record end time
        long endTime = System.currentTimeMillis();
        
        // Calculate metrics
        double publishTimeSeconds = (afterPublishTime - startTime) / 1000.0;
        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        
        System.out.println("Performance results:");
        System.out.println("Messages published: " + messagesToPublish);
        System.out.println("Messages received: " + messageCount[0]);
        System.out.println("Time to publish: " + publishTimeSeconds + " seconds");
        System.out.println("Publish rate: " + (messagesToPublish / publishTimeSeconds) + " messages/second");
        System.out.println("Total processing time: " + totalTimeSeconds + " seconds");
        System.out.println("Overall throughput: " + (messagesToPublish / totalTimeSeconds) + " messages/second");
        
        System.out.println("Performance Test Complete!");
    }
}