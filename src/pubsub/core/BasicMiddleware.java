package src.pubsub.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Basic implementation of the Middleware interface.
 */
public class BasicMiddleware implements Middleware {
    private final Map<String, Channel> channels = new HashMap<>();
    private final Set<Publisher> publishers = new HashSet<>();
    private final Set<Consumer> consumers = new HashSet<>();
    
    @Override
    public Channel createChannel(String channelName) {
        if (!channels.containsKey(channelName)) {
            Channel channel = new BasicChannel(channelName);
            channels.put(channelName, channel);
            return channel;
        }
        return channels.get(channelName);
    }
    
    @Override
    public Channel lookupChannel(String channelName) {
        return channels.get(channelName);
    }
    
    @Override
    public List<String> listChannels() {
        return new ArrayList<>(channels.keySet());
    }
    
    @Override
    public void registerPublisher(Publisher publisher) {
        publishers.add(publisher);
    }
    
    @Override
    public void registerConsumer(Consumer consumer) {
        consumers.add(consumer);
    }
    
    /**
     * Dispatches events for all channels.
     * In a real system, this would likely run in a separate thread.
     */
    public void dispatchAllEvents() {
        for (Channel channel : channels.values()) {
            channel.dispatchEvents();
        }
    }
    
    /**
     * Gets the number of registered publishers.
     * 
     * @return number of publishers
     */
    public int getPublisherCount() {
        return publishers.size();
    }
    
    /**
     * Gets the number of registered consumers.
     * 
     * @return number of consumers
     */
    public int getConsumerCount() {
        return consumers.size();
    }
}