package src.pubsub.qos.network;

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
 * A middleware implementation that uses delay-tolerant channels.
 * Used for R7: Long delays in network traffic.
 */
public class DelayTolerantMiddleware implements Middleware {
    private final Map<String, DelayTolerantChannel> channels = new HashMap<>();
    private final Set<Publisher> publishers = new HashSet<>();
    private final Set<Consumer> consumers = new HashSet<>();
    
    // Default configuration
    private final long defaultTimeoutMs;
    private final int defaultMaxRetries;
    private final int defaultPurgeIntervalSeconds;
    
    /**
     * Creates a new delay-tolerant middleware with default configuration.
     */
    public DelayTolerantMiddleware() {
        this(10000, 3, 60);  // 10 seconds timeout, 3 retries, 60-second purge interval
    }
    
    /**
     * Creates a new delay-tolerant middleware with custom configuration.
     * 
     * @param defaultTimeoutMs the default message timeout in milliseconds
     * @param defaultMaxRetries the default maximum number of retry attempts
     * @param defaultPurgeIntervalSeconds the default interval in seconds to purge expired messages
     */
    public DelayTolerantMiddleware(long defaultTimeoutMs, int defaultMaxRetries, int defaultPurgeIntervalSeconds) {
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.defaultMaxRetries = defaultMaxRetries;
        this.defaultPurgeIntervalSeconds = defaultPurgeIntervalSeconds;
    }
    
    @Override
    public Channel createChannel(String channelName) {
        if (!channels.containsKey(channelName)) {
            DelayTolerantChannel channel = new DelayTolerantChannel(
                    channelName, defaultTimeoutMs, defaultMaxRetries, defaultPurgeIntervalSeconds);
            channels.put(channelName, channel);
            return channel;
        }
        return channels.get(channelName);
    }
    
    /**
     * Creates a channel with custom timeout and retry settings.
     * 
     * @param channelName the name of the channel
     * @param timeoutMs the message timeout in milliseconds
     * @param maxRetries the maximum number of retry attempts
     * @return the created channel
     */
    public DelayTolerantChannel createChannel(String channelName, long timeoutMs, int maxRetries) {
        if (!channels.containsKey(channelName)) {
            DelayTolerantChannel channel = new DelayTolerantChannel(
                    channelName, timeoutMs, maxRetries, defaultPurgeIntervalSeconds);
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
     * Simulates a network delay for a specific channel.
     * 
     * @param channelName the name of the channel
     * @param delayMs the delay in milliseconds
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean simulateChannelNetworkDelay(String channelName, int delayMs) {
        DelayTolerantChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.simulateNetworkDelay(delayMs);
            return true;
        }
        return false;
    }
    
    /**
     * Simulates variable network delays for a specific channel.
     * 
     * @param channelName the name of the channel
     * @param minDelayMs the minimum delay in milliseconds
     * @param maxDelayMs the maximum delay in milliseconds
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean simulateChannelVariableNetworkDelay(
            String channelName, int minDelayMs, int maxDelayMs) {
        DelayTolerantChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.simulateVariableNetworkDelay(minDelayMs, maxDelayMs);
            return true;
        }
        return false;
    }
    
    /**
     * Simulates network jitter for a specific channel.
     * 
     * @param channelName the name of the channel
     * @param baseDelayMs the base delay in milliseconds
     * @param jitterMs the jitter in milliseconds
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean simulateChannelNetworkJitter(String channelName, int baseDelayMs, int jitterMs) {
        DelayTolerantChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.simulateNetworkJitter(baseDelayMs, jitterMs);
            return true;
        }
        return false;
    }
    
    /**
     * Sets the probability of message delivery failures for a specific channel.
     * 
     * @param channelName the name of the channel
     * @param failureProbability the probability (0.0 to 1.0)
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean setChannelDeliveryFailureProbability(
            String channelName, double failureProbability) {
        DelayTolerantChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.setDeliveryFailureProbability(failureProbability);
            return true;
        }
        return false;
    }
    
    /**
     * Gets the delivery metrics for a specific channel.
     * 
     * @param channelName the name of the channel
     * @return the metrics, or null if the channel doesn't exist
     */
    public DelayAwareQueue.DeliveryMetrics getChannelMetrics(String channelName) {
        DelayTolerantChannel channel = channels.get(channelName);
        if (channel != null) {
            return channel.getMetrics();
        }
        return null;
    }
    
    /**
     * Shuts down all channels' background tasks.
     */
    public void shutdown() {
        for (DelayTolerantChannel channel : channels.values()) {
            channel.shutdown();
        }
    }
}