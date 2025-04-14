package src.pubsub.core;

/**
 * Basic implementation of the Consumer interface.
 */
public class BasicConsumer implements Consumer {
    private Middleware middleware;
    private final String id;
    
    /**
     * Creates a new consumer with the specified ID.
     * 
     * @param id the consumer ID
     */
    public BasicConsumer(String id) {
        this.id = id;
    }
    
    @Override
    public void consume(Event event) {
        System.out.println("Consumer " + id + " received event: " + event.getType() + 
                " at " + event.getTimestamp());
    }
    
    @Override
    public void subscribe(String channelName) {
        if (middleware == null) {
            throw new IllegalStateException("Consumer not registered with middleware");
        }
        
        Channel channel = middleware.lookupChannel(channelName);
        if (channel != null) {
            channel.subscribe(this);
            System.out.println("Consumer " + id + " subscribed to channel " + channelName);
        } else {
            System.err.println("Channel not found: " + channelName);
        }
    }
    
    @Override
    public void unsubscribe(String channelName) {
        if (middleware == null) {
            throw new IllegalStateException("Consumer not registered with middleware");
        }
        
        Channel channel = middleware.lookupChannel(channelName);
        if (channel != null) {
            channel.unsubscribe(this);
            System.out.println("Consumer " + id + " unsubscribed from channel " + channelName);
        } else {
            System.err.println("Channel not found: " + channelName);
        }
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        this.middleware = middleware;
        middleware.registerConsumer(this);
        System.out.println("Consumer " + id + " registered with middleware");
    }
    
    /**
     * Gets the consumer ID.
     * 
     * @return the ID
     */
    public String getId() {
        return id;
    }
}