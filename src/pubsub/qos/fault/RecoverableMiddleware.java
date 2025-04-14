package src.pubsub.qos.fault;

import src.pubsub.core.Channel;
import src.pubsub.core.Consumer;
import src.pubsub.core.Middleware;
import src.pubsub.core.Publisher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A middleware implementation that uses recoverable channels and can handle consumer crashes.
 * Used to handle R5: Crashing queues and R6: Crashing consumers.
 */
public class RecoverableMiddleware implements Middleware {
    private final Map<String, RecoverableChannel> channels = new HashMap<>();
    private final Set<Publisher> publishers = new HashSet<>();
    private final Set<Consumer> consumers = new HashSet<>();
    private final Map<String, StatefulConsumer> statefulConsumers = new HashMap<>();
    
    @Override
    public Channel createChannel(String channelName) {
        if (!channels.containsKey(channelName)) {
            RecoverableChannel channel = new RecoverableChannel(channelName);
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
        
        // Track stateful consumers for crash recovery
        if (consumer instanceof StatefulConsumer) {
            StatefulConsumer statefulConsumer = (StatefulConsumer) consumer;
            statefulConsumers.put(statefulConsumer.getId(), statefulConsumer);
        }
    }
    
    /**
     * Dispatches events for all channels.
     */
    public void dispatchAllEvents() {
        for (Channel channel : channels.values()) {
            try {
                channel.dispatchEvents();
            } catch (Exception e) {
                System.err.println("Error dispatching events for channel " + 
                        channel.getName() + ": " + e.getMessage());
            }
        }
    }
    
    /**
     * Simulates a crash for a specific channel's queue.
     * 
     * @param channelName the name of the channel
     * @return true if the channel was found and crashed, false otherwise
     */
    public boolean simulateChannelQueueCrash(String channelName) {
        RecoverableChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.simulateQueueCrash();
            return true;
        }
        return false;
    }
    
    /**
     * Manually recovers a channel's queue.
     * 
     * @param channelName the name of the channel
     * @return true if recovery was successful, false otherwise
     */
    public boolean recoverChannelQueue(String channelName) {
        RecoverableChannel channel = channels.get(channelName);
        if (channel != null) {
            return channel.recoverQueue();
        }
        return false;
    }
    
    /**
     * Checks if a channel's queue has crashed.
     * 
     * @param channelName the name of the channel
     * @return true if the queue has crashed, false if not crashed or the channel doesn't exist
     */
    public boolean isChannelQueueCrashed(String channelName) {
        RecoverableChannel channel = channels.get(channelName);
        return channel != null && channel.isQueueCrashed();
    }
    
    /**
     * Sets automatic recovery mode for a channel.
     * 
     * @param channelName the name of the channel
     * @param automatic true to enable automatic recovery, false to disable
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean setChannelAutomaticRecovery(String channelName, boolean automatic) {
        RecoverableChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.setAutomaticRecovery(automatic);
            return true;
        }
        return false;
    }
    
    /**
     * Gets a stateful consumer by ID.
     * 
     * @param consumerId the consumer ID
     * @return the consumer, or null if not found
     */
    public StatefulConsumer getStatefulConsumer(String consumerId) {
        return statefulConsumers.get(consumerId);
    }
    
    /**
     * Simulates a crash for a specific consumer.
     * 
     * @param consumerId the consumer ID
     * @return true if the consumer was found and crashed, false otherwise
     */
    public boolean simulateConsumerCrash(String consumerId) {
        StatefulConsumer consumer = statefulConsumers.get(consumerId);
        if (consumer != null) {
            consumer.simulateCrash();
            return true;
        }
        return false;
    }
    
    /**
     * Recovers a specific consumer from a crash.
     * 
     * @param consumerId the consumer ID
     * @return true if the consumer was found and recovered, false otherwise
     */
    public boolean recoverConsumer(String consumerId) {
        StatefulConsumer consumer = statefulConsumers.get(consumerId);
        if (consumer != null) {
            consumer.recover();
            return true;
        }
        return false;
    }
    
    /**
     * Shuts down all channels' and consumers' background tasks.
     */
    public void shutdown() {
        for (RecoverableChannel channel : channels.values()) {
            channel.shutdown();
        }
        
        for (StatefulConsumer consumer : statefulConsumers.values()) {
            consumer.shutdown();
        }
    }
}