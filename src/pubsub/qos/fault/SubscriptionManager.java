package src.pubsub.qos.fault;

import src.pubsub.core.Consumer;
import src.pubsub.core.Channel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages consumer subscription information and persists it for recovery.
 * Used to handle R6: Crashing consumers.
 */
public class SubscriptionManager {
    private static final SubscriptionManager INSTANCE = new SubscriptionManager();
    
    // Maps consumer IDs to their channel subscriptions
    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
    
    private final String subscriptionsDir;
    
    private SubscriptionManager() {
        // Set up subscriptions directory in user's temp directory
        this.subscriptionsDir = System.getProperty("java.io.tmpdir") + 
                File.separator + "pubsub-subscriptions";
        
        // Create the subscriptions directory if it doesn't exist
        try {
            Path dirPath = Paths.get(subscriptionsDir);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            System.out.println("Consumer subscriptions will be stored in: " + subscriptionsDir);
        } catch (IOException e) {
            System.err.println("Failed to create subscriptions directory: " + e.getMessage());
        }
    }
    
    /**
     * Gets the singleton instance of the SubscriptionManager.
     * 
     * @return the singleton instance
     */
    public static SubscriptionManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Records a consumer's subscription to a channel.
     * 
     * @param consumerId the consumer ID
     * @param channelName the channel name
     */
    public void recordSubscription(String consumerId, String channelName) {
        subscriptions.computeIfAbsent(consumerId, k -> new HashSet<>()).add(channelName);
        persistSubscriptions(consumerId);
    }
    
    /**
     * Records a consumer's unsubscription from a channel.
     * 
     * @param consumerId the consumer ID
     * @param channelName the channel name
     */
    public void recordUnsubscription(String consumerId, String channelName) {
        Set<String> consumerSubscriptions = subscriptions.get(consumerId);
        if (consumerSubscriptions != null) {
            consumerSubscriptions.remove(channelName);
            persistSubscriptions(consumerId);
        }
    }
    
    /**
     * Gets all channel names a consumer is subscribed to.
     * 
     * @param consumerId the consumer ID
     * @return a set of channel names, or an empty set if the consumer has no subscriptions
     */
    public Set<String> getConsumerSubscriptions(String consumerId) {
        return subscriptions.getOrDefault(consumerId, Collections.emptySet());
    }
    
    /**
     * Restores a consumer's subscriptions.
     * 
     * @param consumer the consumer
     * @param middleware the middleware for channel lookup
     */
    public void restoreSubscriptions(Consumer consumer, RecoverableMiddleware middleware) {
        String consumerId = ((StatefulConsumer) consumer).getId();
        Set<String> channelNames = loadSubscriptions(consumerId);
        
        if (channelNames.isEmpty()) {
            // Check in-memory if nothing is persisted
            channelNames = subscriptions.getOrDefault(consumerId, Collections.emptySet());
        }
        
        System.out.println("Restoring " + channelNames.size() + " subscriptions for consumer " + consumerId);
        
        for (String channelName : channelNames) {
            Channel channel = middleware.lookupChannel(channelName);
            if (channel != null) {
                consumer.subscribe(channelName);
                System.out.println("Restored subscription to " + channelName + " for consumer " + consumerId);
            } else {
                System.err.println("Could not restore subscription to " + channelName + 
                        " for consumer " + consumerId + ": channel not found");
            }
        }
    }
    
    /**
     * Persists a consumer's subscriptions to disk.
     * 
     * @param consumerId the consumer ID
     */
    private void persistSubscriptions(String consumerId) {
        Set<String> channelNames = subscriptions.get(consumerId);
        if (channelNames == null || channelNames.isEmpty()) {
            // No subscriptions to persist
            return;
        }
        
        String filePath = getSubscriptionsFilePath(consumerId);
        
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(new SubscriptionData(channelNames));
            System.out.println("Persisted " + channelNames.size() + 
                    " subscriptions for consumer " + consumerId);
        } catch (IOException e) {
            System.err.println("Failed to persist subscriptions for consumer " + 
                    consumerId + ": " + e.getMessage());
        }
    }
    
    /**
     * Loads a consumer's subscriptions from disk.
     * 
     * @param consumerId the consumer ID
     * @return a set of channel names, or an empty set if loading failed
     */
    private Set<String> loadSubscriptions(String consumerId) {
        String filePath = getSubscriptionsFilePath(consumerId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            return Collections.emptySet();
        }
        
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            SubscriptionData data = (SubscriptionData) ois.readObject();
            return data.getChannelNames();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to load subscriptions for consumer " + 
                    consumerId + ": " + e.getMessage());
            return Collections.emptySet();
        }
    }
    
    /**
     * Gets the file path for a consumer's subscription data.
     * 
     * @param consumerId the consumer ID
     * @return the file path
     */
    private String getSubscriptionsFilePath(String consumerId) {
        return subscriptionsDir + File.separator + "subscriptions-" + consumerId + ".dat";
    }
    
    /**
     * Clears all subscription data for a consumer.
     * 
     * @param consumerId the consumer ID
     */
    public void clearConsumerData(String consumerId) {
        subscriptions.remove(consumerId);
        
        // Delete persisted data
        String filePath = getSubscriptionsFilePath(consumerId);
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
    }
    
    /**
     * Serializable class to store subscription data.
     */
    private static class SubscriptionData implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final Set<String> channelNames;
        
        /**
         * Creates new subscription data.
         * 
         * @param channelNames the channel names
         */
        public SubscriptionData(Set<String> channelNames) {
            this.channelNames = new HashSet<>(channelNames);
        }
        
        /**
         * Gets the channel names.
         * 
         * @return the channel names
         */
        public Set<String> getChannelNames() {
            return channelNames;
        }
    }
}