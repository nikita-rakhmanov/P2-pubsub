package src.pubsub.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * implementation of the Middleware interface that includes QoS features.
 */
public class BasicMiddleware implements Middleware {
    private final Map<String, BasicChannel> channels = new HashMap<>();
    private final Set<Publisher> publishers = new HashSet<>();
    private final Set<Consumer> consumers = new HashSet<>();
    private final Map<String, BasicConsumer> trackedConsumers = new HashMap<>();
    
    // Default configuration
    private final long defaultMessageTimeToLiveMs;
    private final int defaultMaxRetries;
    private final int defaultPurgeIntervalSeconds;
    
    /**
     * Creates a new middleware with default configuration.
     */
    public BasicMiddleware() {
        this(30000, 3, 60);  // 30 seconds timeout, 3 retries, 60-second purge interval
    }
    
    /**
     * Creates a new middleware with custom configuration.
     * 
     * @param defaultMessageTimeToLiveMs the default message time-to-live in milliseconds
     * @param defaultMaxRetries the default maximum number of retry attempts
     * @param defaultPurgeIntervalSeconds the default interval in seconds to purge expired messages
     */
    public BasicMiddleware(long defaultMessageTimeToLiveMs, int defaultMaxRetries, int defaultPurgeIntervalSeconds) {
        this.defaultMessageTimeToLiveMs = defaultMessageTimeToLiveMs;
        this.defaultMaxRetries = defaultMaxRetries;
        this.defaultPurgeIntervalSeconds = defaultPurgeIntervalSeconds;
    }
    
    @Override
    public Channel createChannel(String channelName) {
        if (!channels.containsKey(channelName)) {
            BasicChannel channel = new BasicChannel(
                    channelName, 
                    defaultMessageTimeToLiveMs, 
                    defaultMaxRetries, 
                    defaultPurgeIntervalSeconds);
            channels.put(channelName, channel);
            return channel;
        }
        return channels.get(channelName);
    }
    
    /**
     * Creates a channel with custom QoS settings.
     * 
     * @param channelName the name of the channel
     * @param messageTimeToLiveMs the message time-to-live in milliseconds
     * @param maxRetries the maximum number of retry attempts
     * @param purgeIntervalSeconds the interval in seconds to purge expired messages
     * @return the created channel
     */
    public BasicChannel createChannel(String channelName, 
            long messageTimeToLiveMs, int maxRetries, int purgeIntervalSeconds) {
        
        if (!channels.containsKey(channelName)) {
            BasicChannel channel = new BasicChannel(
                    channelName, messageTimeToLiveMs, maxRetries, purgeIntervalSeconds);
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
        
        if (consumer instanceof BasicConsumer) {
            BasicConsumer trackedConsumer = (BasicConsumer) consumer;
            trackedConsumers.put(trackedConsumer.getId(), trackedConsumer);
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
    
    /**
     * Simulates a crash for a specific channel's queue.
     * 
     * @param channelName the name of the channel
     * @return true if the channel was found and crashed, false otherwise
     */
    public boolean simulateChannelQueueCrash(String channelName) {
        BasicChannel channel = channels.get(channelName);
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
        BasicChannel channel = channels.get(channelName);
        if (channel != null) {
            return channel.recoverQueue();
        }
        return false;
    }
    
    /**
     * Sets automatic recovery mode for a channel.
     * 
     * @param channelName the name of the channel
     * @param automatic true to enable automatic recovery, false to disable
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean setChannelAutomaticRecovery(String channelName, boolean automatic) {
        BasicChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.setAutomaticRecovery(automatic);
            return true;
        }
        return false;
    }
    
    /**
     * Simulates a crash for a specific consumer.
     * 
     * @param consumerId the consumer ID
     * @return true if the consumer was found and crashed, false otherwise
     */
    public boolean simulateConsumerCrash(String consumerId) {
        BasicConsumer consumer = trackedConsumers.get(consumerId);
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
        BasicConsumer consumer = trackedConsumers.get(consumerId);
        if (consumer != null) {
            consumer.recover();
            return true;
        }
        return false;
    }
    
    /**
     * Simulates a network delay for a specific channel.
     * 
     * @param channelName the name of the channel
     * @param delayMs the delay in milliseconds
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean simulateChannelNetworkDelay(String channelName, int delayMs) {
        BasicChannel channel = channels.get(channelName);
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
        BasicChannel channel = channels.get(channelName);
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
        BasicChannel channel = channels.get(channelName);
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
        BasicChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.setDeliveryFailureProbability(failureProbability);
            return true;
        }
        return false;
    }
    
    /**
     * Gets delivery statistics for all channels.
     * 
     * @return a map of channel names to delivery statistics
     */
    public Map<String, DeliveryStats> getDeliveryStats() {
        Map<String, DeliveryStats> stats = new HashMap<>();
        
        for (Map.Entry<String, BasicChannel> entry : channels.entrySet()) {
            String channelName = entry.getKey();
            BasicChannel channel = entry.getValue();
            
            DeliveryStats channelStats = new DeliveryStats(
                    channel.getMessagesSent(),
                    channel.getMessagesAcked(),
                    channel.getMessagesResent(),
                    channel.getMessagesFailed(),
                    channel.getQueueSize(),
                    channel.getDeliverySuccessRate());
            
            stats.put(channelName, channelStats);
        }
        
        return stats;
    }
    
    /**
     * Simulates message loss for a specific consumer.
     * 
     * @param consumerId the consumer ID
     * @param simulate true to enable simulation, false to disable
     * @param lossProbability the probability of message loss (0.0 to 1.0)
     * @return true if the consumer was found and updated, false otherwise
     */
    public boolean simulateConsumerMessageLoss(
            String consumerId, boolean simulate, double lossProbability) {
        
        BasicConsumer consumer = trackedConsumers.get(consumerId);
        if (consumer != null) {
            consumer.simulateMessageLoss(simulate, lossProbability);
            return true;
        }
        return false;
    }
    
    /**
     * Simulates a connection disruption for a consumer.
     * 
     * @param consumerId the consumer ID
     * @return true if the consumer was found and disconnected, false otherwise
     */
    public boolean simulateConsumerDisconnection(String consumerId) {
        BasicConsumer consumer = trackedConsumers.get(consumerId);
        if (consumer != null) {
            consumer.simulateDisconnection();
            return true;
        }
        return false;
    }
    
    /**
     * Simulates a connection disruption for a publisher.
     * 
     * @param publisherId the publisher ID
     * @return true if the publisher was found and disconnected, false otherwise
     */
    public boolean simulatePublisherDisconnection(String publisherId) {
        for (Publisher publisher : publishers) {
            if (publisher instanceof BasicPublisher) {
                BasicPublisher basicPublisher = (BasicPublisher) publisher;
                if (basicPublisher.getId().equals(publisherId)) {
                    basicPublisher.simulateDisconnection();
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Shuts down all channels' and consumers' background tasks.
     */
    public void shutdown() {
        for (BasicChannel channel : channels.values()) {
            channel.shutdown();
        }
        
        for (BasicConsumer consumer : trackedConsumers.values()) {
            consumer.shutdown();
        }
        
        for (Publisher publisher : publishers) {
            if (publisher instanceof BasicPublisher) {
                ((BasicPublisher) publisher).shutdown();
            }
        }
    }
    
    /**
     * Class representing delivery statistics.
     */
    public static class DeliveryStats {
        private final int messagesSent;
        private final int messagesAcked;
        private final int messagesResent;
        private final int messagesFailed;
        private final int pendingMessages;
        private final double successRate;
        
        /**
         * Creates new delivery stats.
         * 
         * @param messagesSent the number of messages sent
         * @param messagesAcked the number of messages acknowledged
         * @param messagesResent the number of messages resent
         * @param messagesFailed the number of messages that failed delivery
         * @param pendingMessages the number of messages waiting in queue
         * @param successRate the delivery success rate as a percentage
         */
        public DeliveryStats(int messagesSent, int messagesAcked, int messagesResent,
                int messagesFailed, int pendingMessages, double successRate) {
            this.messagesSent = messagesSent;
            this.messagesAcked = messagesAcked;
            this.messagesResent = messagesResent;
            this.messagesFailed = messagesFailed;
            this.pendingMessages = pendingMessages;
            this.successRate = successRate;
        }
        
        public int getMessagesSent() { return messagesSent; }
        public int getMessagesAcked() { return messagesAcked; }
        public int getMessagesResent() { return messagesResent; }
        public int getMessagesFailed() { return messagesFailed; }
        public int getPendingMessages() { return pendingMessages; }
        public double getSuccessRate() { return successRate; }
        
        @Override
        public String toString() {
            return String.format(
                    "Sent: %d, Acked: %d, Resent: %d, Failed: %d, Pending: %d, Success: %.2f%%",
                    messagesSent, messagesAcked, messagesResent, messagesFailed, 
                    pendingMessages, successRate);
        }
    }
}