package src.pubsub.core;

/**
 * Basic implementation of the Publisher interface.
 */
public class BasicPublisher implements Publisher {
    private Middleware middleware;
    private final String id;
    
    /**
     * Creates a new publisher with the specified ID.
     * 
     * @param id the publisher ID
     */
    public BasicPublisher(String id) {
        this.id = id;
    }
    
    @Override
    public void publish(String channelName, Event event) {
        if (middleware == null) {
            throw new IllegalStateException("Publisher not registered with middleware");
        }
        
        Channel channel = middleware.lookupChannel(channelName);
        if (channel != null) {
            channel.addEvent(event);
            System.out.println("Publisher " + id + " published event " + event.getType() + " to channel " + channelName);
        } else {
            System.err.println("Channel not found: " + channelName);
        }
    }
    
    @Override
    public void registerWithMiddleware(Middleware middleware) {
        this.middleware = middleware;
        middleware.registerPublisher(this);
        System.out.println("Publisher " + id + " registered with middleware");
    }
    
    /**
     * Gets the publisher ID.
     * 
     * @return the ID
     */
    public String getId() {
        return id;
    }
}