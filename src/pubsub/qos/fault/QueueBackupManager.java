package src.pubsub.qos.fault;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages backup and recovery of queue state.
 * Used to handle R5: Crashing queues.
 */
public class QueueBackupManager {
    private static final QueueBackupManager INSTANCE = new QueueBackupManager();
    private final String backupDirPath;
    private final AtomicLong backupCounter = new AtomicLong(0);
    
    private QueueBackupManager() {
        // Set up backup directory in user's temp directory
        this.backupDirPath = System.getProperty("java.io.tmpdir") + File.separator + "pubsub-backups";
        
        // Create the backup directory if it doesn't exist
        try {
            Path dirPath = Paths.get(backupDirPath);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            System.out.println("Queue backups will be stored in: " + backupDirPath);
        } catch (IOException e) {
            System.err.println("Failed to create backup directory: " + e.getMessage());
        }
    }
    
    /**
     * Gets the singleton instance of the QueueBackupManager.
     * 
     * @return the singleton instance
     */
    public static QueueBackupManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Backs up queue items to disk.
     * 
     * @param queueId a unique identifier for the queue
     * @param items the items to back up
     * @return true if the backup was successful, false otherwise
     */
    public boolean backupQueue(String queueId, List<?> items) {
        String backupFilePath = getBackupFilePath(queueId);
        
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(backupFilePath))) {
            oos.writeObject(items);
            backupCounter.incrementAndGet();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to backup queue " + queueId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Recovers queue items from a backup.
     * 
     * @param queueId a unique identifier for the queue
     * @return the recovered items, or an empty list if recovery failed
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> recoverQueue(String queueId) {
        String backupFilePath = getBackupFilePath(queueId);
        File backupFile = new File(backupFilePath);
        
        if (!backupFile.exists()) {
            System.out.println("No backup found for queue " + queueId);
            return new ArrayList<>();
        }
        
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(backupFilePath))) {
            return (List<T>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to recover queue " + queueId + ": " + e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * Deletes a queue's backup file.
     * 
     * @param queueId a unique identifier for the queue
     * @return true if the deletion was successful, false otherwise
     */
    public boolean deleteBackup(String queueId) {
        String backupFilePath = getBackupFilePath(queueId);
        File backupFile = new File(backupFilePath);
        
        if (backupFile.exists()) {
            return backupFile.delete();
        }
        return true; // No file to delete
    }
    
    /**
     * Gets the path to the backup file for a queue.
     * 
     * @param queueId a unique identifier for the queue
     * @return the file path
     */
    private String getBackupFilePath(String queueId) {
        return backupDirPath + File.separator + queueId + ".backup";
    }
    
    /**
     * Gets the number of backups performed during this session.
     * 
     * @return the backup count
     */
    public long getBackupCount() {
        return backupCounter.get();
    }
}