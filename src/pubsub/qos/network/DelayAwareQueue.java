package src.pubsub.qos.network;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A queue that prioritizes messages based on their age to handle network delays.
 * Used for R7: Long delays in network traffic.
 * 
 * @param <T> the type of elements in the queue
 */
public class DelayAwareQueue<T extends TimestampedMessage> {
    private final PriorityQueue<T> queue;
    private final Lock queueLock = new ReentrantLock();
    private final int maxRetries;
    private final DeliveryMetrics metrics = new DeliveryMetrics();
    
    /**
     * Creates a new delay-aware queue.
     * 
     * @param maxRetries the maximum number of retry attempts for a message
     */
    public DelayAwareQueue(int maxRetries) {
        // Priority queue that sorts messages by creation time (oldest first)
        this.queue = new PriorityQueue<>(Comparator.comparingLong(TimestampedMessage::getCreationTime));
        this.maxRetries = maxRetries;
    }
    
    /**
     * Adds a message to the queue.
     * 
     * @param message the message to add
     */
    public void add(T message) {
        queueLock.lock();
        try {
            message.setStatus(TimestampedMessage.DeliveryStatus.QUEUED);
            queue.add(message);
            metrics.recordQueued();
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Gets and removes the oldest message from the queue.
     * 
     * @return the oldest message, or null if the queue is empty
     */
    public T poll() {
        queueLock.lock();
        try {
            return queue.poll();
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Adds a message back to the queue for retry.
     * 
     * @param message the message to retry
     * @return true if the message was added for retry, false if it exceeded the max retries
     */
    public boolean retry(T message) {
        queueLock.lock();
        try {
            message.incrementRetryCount();
            
            if (message.getRetryCount() > maxRetries) {
                message.setStatus(TimestampedMessage.DeliveryStatus.FAILED);
                metrics.recordFailed();
                return false;
            }
            
            if (message.isExpired()) {
                message.setStatus(TimestampedMessage.DeliveryStatus.EXPIRED);
                metrics.recordExpired();
                return false;
            }
            
            message.setStatus(TimestampedMessage.DeliveryStatus.QUEUED);
            queue.add(message);
            metrics.recordRetry();
            return true;
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Records that a message was successfully delivered.
     * 
     * @param message the delivered message
     */
    public void recordDelivered(T message) {
        message.markDelivered();
        metrics.recordDelivered(message.getDeliveryDuration());
    }
    
    /**
     * Records that a message was dispatched.
     * 
     * @param message the dispatched message
     */
    public void recordDispatched(T message) {
        message.markDispatched();
        metrics.recordDispatched(message.getDispatchDuration());
    }
    
    /**
     * Removes expired and failed messages from the queue.
     * 
     * @return the number of messages removed
     */
    public int purgeExpiredAndFailed() {
        queueLock.lock();
        try {
            int count = 0;
            long now = System.currentTimeMillis();
            
            Iterator<T> iterator = queue.iterator();
            while (iterator.hasNext()) {
                T message = iterator.next();
                
                if (message.isExpired()) {
                    iterator.remove();
                    message.setStatus(TimestampedMessage.DeliveryStatus.EXPIRED);
                    metrics.recordExpired();
                    count++;
                }
            }
            
            return count;
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Gets the size of the queue.
     * 
     * @return the number of messages in the queue
     */
    public int size() {
        queueLock.lock();
        try {
            return queue.size();
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Checks if the queue is empty.
     * 
     * @return true if the queue is empty, false otherwise
     */
    public boolean isEmpty() {
        queueLock.lock();
        try {
            return queue.isEmpty();
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Gets the delivery metrics.
     * 
     * @return the metrics
     */
    public DeliveryMetrics getMetrics() {
        return metrics;
    }
    
    /**
     * Class to track delivery metrics.
     */
    public static class DeliveryMetrics {
        private int queuedCount = 0;
        private int deliveredCount = 0;
        private int failedCount = 0;
        private int expiredCount = 0;
        private int retryCount = 0;
        private long totalDeliveryTime = 0;
        private long maxDeliveryTime = 0;
        private long totalDispatchTime = 0;
        
        /**
         * Records a message being queued.
         */
        void recordQueued() {
            queuedCount++;
        }
        
        /**
         * Records a message being delivered.
         * 
         * @param deliveryTime the delivery time in milliseconds
         */
        void recordDelivered(long deliveryTime) {
            deliveredCount++;
            totalDeliveryTime += deliveryTime;
            maxDeliveryTime = Math.max(maxDeliveryTime, deliveryTime);
        }
        
        /**
         * Records a message being dispatched.
         * 
         * @param dispatchTime the dispatch time in milliseconds
         */
        void recordDispatched(long dispatchTime) {
            totalDispatchTime += dispatchTime;
        }
        
        /**
         * Records a message failing to be delivered.
         */
        void recordFailed() {
            failedCount++;
        }
        
        /**
         * Records a message expiring.
         */
        void recordExpired() {
            expiredCount++;
        }
        
        /**
         * Records a message being retried.
         */
        void recordRetry() {
            retryCount++;
        }
        
        /**
         * Gets the number of queued messages.
         * 
         * @return the count
         */
        public int getQueuedCount() {
            return queuedCount;
        }
        
        /**
         * Gets the number of delivered messages.
         * 
         * @return the count
         */
        public int getDeliveredCount() {
            return deliveredCount;
        }
        
        /**
         * Gets the number of failed messages.
         * 
         * @return the count
         */
        public int getFailedCount() {
            return failedCount;
        }
        
        /**
         * Gets the number of expired messages.
         * 
         * @return the count
         */
        public int getExpiredCount() {
            return expiredCount;
        }
        
        /**
         * Gets the number of retried messages.
         * 
         * @return the count
         */
        public int getRetryCount() {
            return retryCount;
        }
        
        /**
         * Gets the average delivery time in milliseconds.
         * 
         * @return the average, or 0 if no messages have been delivered
         */
        public double getAverageDeliveryTime() {
            return deliveredCount > 0 ? (double) totalDeliveryTime / deliveredCount : 0;
        }
        
        /**
         * Gets the maximum delivery time in milliseconds.
         * 
         * @return the maximum time
         */
        public long getMaxDeliveryTime() {
            return maxDeliveryTime;
        }
        
        /**
         * Gets the average dispatch time in milliseconds.
         * 
         * @return the average, or 0 if no messages have been dispatched
         */
        public double getAverageDispatchTime() {
            return deliveredCount > 0 ? (double) totalDispatchTime / deliveredCount : 0;
        }
        
        /**
         * Gets a summary of the metrics.
         * 
         * @return the summary
         */
        @Override
        public String toString() {
            return "Metrics{" +
                    "queued=" + queuedCount +
                    ", delivered=" + deliveredCount +
                    ", failed=" + failedCount +
                    ", expired=" + expiredCount +
                    ", retries=" + retryCount +
                    ", avgDeliveryTime=" + String.format("%.2f", getAverageDeliveryTime()) + "ms" +
                    ", maxDeliveryTime=" + maxDeliveryTime + "ms" +
                    ", avgDispatchTime=" + String.format("%.2f", getAverageDispatchTime()) + "ms" +
                    '}';
        }
    }
}