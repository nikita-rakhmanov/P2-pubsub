package src.pubsub.qos.reliability;

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
 * A middleware implementation that supports reliable message delivery.
 * Used for R8: Dropped messages.
 */
public class ReliableMiddleware implements Middleware {
    private final Map<String, ReliableChannel> channels = new HashMap<>();
    private final Set<Publisher> publishers = new HashSet<>();
    private final Set<Consumer> consumers = new HashSet<>();
    private final Map<String, ReliableConsumer> reliableConsumers = new HashMap<>();
    
    // Default configuration
    private final long defaultMessageTimeToLiveMs;
    private final long defaultResendIntervalMs;
    private final int defaultMaxDeliveryAttempts;
    
    /**
     * Creates a new reliable middleware with default configuration.
     */
    public ReliableMiddleware() {
        this(30000, 5000, 5); // 30 sec TTL, 5 sec resend interval, 5 max attempts
    }
    
    /**
     * Creates a new reliable middleware with custom configuration.
     * 
     * @param defaultMessageTimeToLiveMs the default message time-to-live in milliseconds
     * @param defaultResendIntervalMs the default interval to check and resend lost messages
     * @param defaultMaxDeliveryAttempts the default maximum number of delivery attempts
     */
    public ReliableMiddleware(long defaultMessageTimeToLiveMs, 
            long defaultResendIntervalMs, int defaultMaxDeliveryAttempts) {
        this.defaultMessageTimeToLiveMs = defaultMessageTimeToLiveMs;
        this.defaultResendIntervalMs = defaultResendIntervalMs;
        this.defaultMaxDeliveryAttempts = defaultMaxDeliveryAttempts;
    }
    
    @Override
    public Channel createChannel(String channelName) {
        if (!channels.containsKey(channelName)) {
            ReliableChannel channel = new ReliableChannel(
                    channelName, 
                    defaultMessageTimeToLiveMs, 
                    defaultResendIntervalMs, 
                    defaultMaxDeliveryAttempts);
            
            channels.put(channelName, channel);
            return channel;
        }
        return channels.get(channelName);
    }
    
    /**
     * Creates a channel with custom reliability settings.
     * 
     * @param channelName the channel name
     * @param messageTimeToLiveMs the message time-to-live in milliseconds
     * @param resendIntervalMs the interval to check and resend lost messages
     * @param maxDeliveryAttempts the maximum number of delivery attempts
     * @return the created channel
     */
    public ReliableChannel createChannel(String channelName, 
            long messageTimeToLiveMs, long resendIntervalMs, int maxDeliveryAttempts) {
        
        if (!channels.containsKey(channelName)) {
            ReliableChannel channel = new ReliableChannel(
                    channelName, messageTimeToLiveMs, resendIntervalMs, maxDeliveryAttempts);
            
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
        
        if (consumer instanceof ReliableConsumer) {
            ReliableConsumer reliableConsumer = (ReliableConsumer) consumer;
            reliableConsumers.put(reliableConsumer.getId(), reliableConsumer);
        }
    }
    
    /**
     * Processes an acknowledgment for a message.
     * 
     * @param ack the acknowledgment
     */
    public void processAcknowledgment(MessageAcknowledgment ack) {
        String channelName = ack.getChannelName();
        ReliableChannel channel = channels.get(channelName);
        
        if (channel != null) {
            channel.processAcknowledgment(ack);
        } else {
            System.err.println("Cannot process acknowledgment: channel not found: " + channelName);
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
     * Gets a reliable consumer by ID.
     * 
     * @param consumerId the consumer ID
     * @return the consumer, or null if not found
     */
    public ReliableConsumer getReliableConsumer(String consumerId) {
        return reliableConsumers.get(consumerId);
    }
    
    /**
     * Simulates message loss for a specific channel.
     * 
     * @param channelName the channel name
     * @param simulate true to enable simulation, false to disable
     * @param lossProbability the probability of message loss (0.0 to 1.0)
     * @return true if the channel was found and updated, false otherwise
     */
    public boolean simulateChannelMessageLoss(
            String channelName, boolean simulate, double lossProbability) {
        
        ReliableChannel channel = channels.get(channelName);
        if (channel != null) {
            channel.simulateMessageLoss(simulate, lossProbability);
            return true;
        }
        return false;
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
        
        ReliableConsumer consumer = reliableConsumers.get(consumerId);
        if (consumer != null) {
            consumer.simulateMessageLoss(simulate, lossProbability);
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
        
        for (Map.Entry<String, ReliableChannel> entry : channels.entrySet()) {
            String channelName = entry.getKey();
            ReliableChannel channel = entry.getValue();
            
            DeliveryStats channelStats = new DeliveryStats(
                    channel.getMessagesSent(),
                    channel.getMessagesAcked(),
                    channel.getMessagesResent(),
                    channel.getMessagesFailed(),
                    channel.getPendingAcknowledgmentCount(),
                    channel.getDeliverySuccessRate());
            
            stats.put(channelName, channelStats);
        }
        
        return stats;
    }
    
    /**
     * Shuts down all channels' background tasks.
     */
    public void shutdown() {
        for (ReliableChannel channel : channels.values()) {
            channel.shutdown();
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
        private final int pendingAcks;
        private final double successRate;
        
        /**
         * Creates new delivery stats.
         * 
         * @param messagesSent the number of messages sent
         * @param messagesAcked the number of messages acknowledged
         * @param messagesResent the number of messages resent
         * @param messagesFailed the number of messages that failed delivery
         * @param pendingAcks the number of messages waiting for acknowledgment
         * @param successRate the delivery success rate as a percentage
         */
        public DeliveryStats(int messagesSent, int messagesAcked, int messagesResent,
                int messagesFailed, int pendingAcks, double successRate) {
            this.messagesSent = messagesSent;
            this.messagesAcked = messagesAcked;
            this.messagesResent = messagesResent;
            this.messagesFailed = messagesFailed;
            this.pendingAcks = pendingAcks;
            this.successRate = successRate;
        }
        
        public int getMessagesSent() {
            return messagesSent;
        }
        
        public int getMessagesAcked() {
            return messagesAcked;
        }
        
        public int getMessagesResent() {
            return messagesResent;
        }
        
        public int getMessagesFailed() {
            return messagesFailed;
        }
        
        public int getPendingAcks() {
            return pendingAcks;
        }
        
        public double getSuccessRate() {
            return successRate;
        }
        
        @Override
        public String toString() {
            return String.format(
                    "Sent: %d, Acked: %d, Resent: %d, Failed: %d, Pending: %d, Success: %.2f%%",
                    messagesSent, messagesAcked, messagesResent, messagesFailed, 
                    pendingAcks, successRate);
        }
    }
}