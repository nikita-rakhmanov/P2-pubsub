package src.pubsub.qos.fault;

import src.pubsub.core.DynamicQueue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A queue implementation that persists its state to disk.
 * Used to handle R5: Crashing queues.
 * 
 * @param <T> the type of elements in the queue (must be Serializable)
 */
public class PersistentQueue<T extends Serializable> {
    private final String queueId;
    private DynamicQueue<T> queue;
    private final QueueBackupManager backupManager;
    private final ScheduledExecutorService scheduler;
    private final int backupIntervalSeconds;
    private final AtomicBoolean crashed = new AtomicBoolean(false);
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    
    /**
     * Creates a new persistent queue.
     * 
     * @param initialCapacity the initial capacity of the queue
     * @param backupIntervalSeconds the interval in seconds between backups
     */
    public PersistentQueue(int initialCapacity, int backupIntervalSeconds) {
        this.queueId = "queue-" + UUID.randomUUID().toString();
        this.queue = new DynamicQueue<>(initialCapacity);
        this.backupManager = QueueBackupManager.getInstance();
        this.backupIntervalSeconds = backupIntervalSeconds;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start periodic backup task
        startBackupTask();
    }
    
    /**
     * Creates a new persistent queue with a specific ID.
     * 
     * @param queueId the queue ID
     * @param initialCapacity the initial capacity of the queue
     * @param backupIntervalSeconds the interval in seconds between backups
     */
    public PersistentQueue(String queueId, int initialCapacity, int backupIntervalSeconds) {
        this.queueId = queueId;
        this.queue = new DynamicQueue<>(initialCapacity);
        this.backupManager = QueueBackupManager.getInstance();
        this.backupIntervalSeconds = backupIntervalSeconds;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Start periodic backup task
        startBackupTask();
    }
    
    /**
     * Adds an element to the queue.
     * 
     * @param element the element to add
     * @throws IllegalStateException if the queue has crashed
     */
    public void add(T element) {
        if (crashed.get()) {
            throw new IllegalStateException("Queue has crashed and not yet recovered");
        }
        
        if (recovering.get()) {
            // Wait for recovery to complete
            waitForRecovery();
        }
        
        queue.add(element);
    }
    
    /**
     * Removes and returns the head element of the queue.
     * 
     * @return the head element, or null if the queue is empty
     * @throws IllegalStateException if the queue has crashed
     */
    public T poll() {
        if (crashed.get()) {
            throw new IllegalStateException("Queue has crashed and not yet recovered");
        }
        
        if (recovering.get()) {
            // Wait for recovery to complete
            waitForRecovery();
        }
        
        return queue.poll();
    }
    
    /**
     * Checks if the queue is empty.
     * 
     * @return true if the queue is empty, false otherwise
     * @throws IllegalStateException if the queue has crashed
     */
    public boolean isEmpty() {
        if (crashed.get()) {
            throw new IllegalStateException("Queue has crashed and not yet recovered");
        }
        
        if (recovering.get()) {
            // Wait for recovery to complete
            waitForRecovery();
        }
        
        return queue.isEmpty();
    }
    
    /**
     * Returns the current size of the queue.
     * 
     * @return the number of elements in the queue
     * @throws IllegalStateException if the queue has crashed
     */
    public int size() {
        if (crashed.get()) {
            throw new IllegalStateException("Queue has crashed and not yet recovered");
        }
        
        if (recovering.get()) {
            // Wait for recovery to complete
            waitForRecovery();
        }
        
        return queue.size();
    }
    
    /**
     * Gets the queue ID.
     * 
     * @return the queue ID
     */
    public String getQueueId() {
        return queueId;
    }
    
    /**
     * Simulates a queue crash.
     */
    public void simulateCrash() {
        System.out.println("Queue " + queueId + " has crashed!");
        crashed.set(true);
        queue = null; // Simulate memory loss
    }
    
    /**
     * Recovers the queue from a crash.
     * 
     * @return true if recovery was successful, false otherwise
     */
    public boolean recover() {
        if (!crashed.get()) {
            return true; // Not crashed, nothing to recover
        }
        
        recovering.set(true);
        try {
            System.out.println("Recovering queue " + queueId + "...");
            
            // Create a new queue
            queue = new DynamicQueue<>(10);
            
            // Recover items from backup
            List<T> recoveredItems = backupManager.recoverQueue(queueId);
            
            // Add recovered items to the new queue
            for (T item : recoveredItems) {
                queue.add(item);
            }
            
            System.out.println("Queue " + queueId + " recovered with " + recoveredItems.size() + " items");
            crashed.set(false);
            
            // Restart backup task
            startBackupTask();
            
            return true;
        } catch (Exception e) {
            System.err.println("Failed to recover queue " + queueId + ": " + e.getMessage());
            return false;
        } finally {
            recovering.set(false);
        }
    }
    
    /**
     * Waits for recovery to complete.
     */
    private void waitForRecovery() {
        while (recovering.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    /**
     * Starts the periodic backup task.
     */
    private void startBackupTask() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!crashed.get() && !recovering.get()) {
                backupQueue();
            }
        }, backupIntervalSeconds, backupIntervalSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Backs up the queue to disk.
     */
    private void backupQueue() {
        if (queue == null) {
            return;
        }
        
        // Extract all items from the queue
        List<T> items = new ArrayList<>();
        DynamicQueue<T> tempQueue = new DynamicQueue<>(queue.size());
        
        while (!queue.isEmpty()) {
            T item = queue.poll();
            items.add(item);
            tempQueue.add(item);
        }
        
        // Restore items to the queue
        while (!tempQueue.isEmpty()) {
            queue.add(tempQueue.poll());
        }
        
        // Backup items to disk
        boolean success = backupManager.backupQueue(queueId, items);
        if (success) {
            System.out.println("Queue " + queueId + " backed up with " + items.size() + " items");
        }
    }
    
    /**
     * Performs a manual backup of the queue.
     * 
     * @return true if the backup was successful, false otherwise
     */
    public boolean manualBackup() {
        if (crashed.get()) {
            return false;
        }
        
        backupQueue();
        return true;
    }
    
    /**
     * Shuts down the queue's background tasks.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }
    
    /**
     * Checks if the queue has crashed.
     * 
     * @return true if the queue has crashed, false otherwise
     */
    public boolean isCrashed() {
        return crashed.get();
    }
}