package src.pubsub;

import src.pubsub.core.*;

/**
 * Main class to test the pub-sub system.
 */
public class PubSubSystem {
    public static void main(String[] args) {
        System.out.println("Starting PubSub System...");
        
        // Create middleware
        Middleware middleware = new BasicMiddleware();
        
        // Create channels
        middleware.createChannel("news");
        middleware.createChannel("sports");
        middleware.createChannel("tech");
        
        System.out.println("Available channels: " + middleware.listChannels());
        
        // Create publishers
        Publisher publisher1 = new BasicPublisher("Publisher1");
        Publisher publisher2 = new BasicPublisher("Publisher2");
        
        // Register publishers with middleware
        publisher1.registerWithMiddleware(middleware);
        publisher2.registerWithMiddleware(middleware);
        
        // Create consumers
        Consumer consumer1 = new BasicConsumer("Consumer1");
        Consumer consumer2 = new BasicConsumer("Consumer2");
        Consumer consumer3 = new BasicConsumer("Consumer3");
        
        // Register consumers with middleware
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        consumer3.registerWithMiddleware(middleware);
        
        // Subscribe consumers to channels
        consumer1.subscribe("news");
        consumer1.subscribe("tech");
        consumer2.subscribe("news");
        consumer2.subscribe("sports");
        consumer3.subscribe("tech");
        
        // Publish events
        System.out.println("\nPublishing events...");
        publisher1.publish("news", new BasicEvent("Breaking News"));
        publisher1.publish("tech", new BasicEvent("New Tech Release"));
        publisher2.publish("sports", new BasicEvent("Match Results"));
        publisher2.publish("news", new BasicEvent("Weather Update"));
        
        // Simulate multiple messages for queue testing
        for (int i = 0; i < 20; i++) {
            publisher1.publish("tech", new BasicEvent("Tech Update " + i));
        }
        
        // Display channel information before dispatch
        Channel newsChannel = middleware.lookupChannel("news");
        Channel sportsChannel = middleware.lookupChannel("sports");
        Channel techChannel = middleware.lookupChannel("tech");
        
        System.out.println("\nChannel Queue Sizes Before Dispatch:");
        System.out.println("News channel queue size: " + newsChannel.getQueueSize());
        System.out.println("Sports channel queue size: " + sportsChannel.getQueueSize());
        System.out.println("Tech channel queue size: " + techChannel.getQueueSize());
        
        // Dispatch events
        System.out.println("\nDispatching events...");
        ((BasicMiddleware) middleware).dispatchAllEvents();
        
        // Display channel information after dispatch
        System.out.println("\nChannel Queue Sizes After Dispatch:");
        System.out.println("News channel queue size: " + newsChannel.getQueueSize());
        System.out.println("Sports channel queue size: " + sportsChannel.getQueueSize());
        System.out.println("Tech channel queue size: " + techChannel.getQueueSize());
        
        System.out.println("\nPubSub System Test Complete!");
    }
}