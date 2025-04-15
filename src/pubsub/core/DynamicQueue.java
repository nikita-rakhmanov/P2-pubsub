package src.pubsub.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock; // Or use synchronized blocks

/**
 * Thread-safe dynamic queue implementation that can grow as needed.
 * Uses a circular array that resizes when needed.
 * Includes a snapshot method for safe backups.
 */
public class DynamicQueue<T> {
    private Object[] elements;
    private int head = 0;
    private int tail = 0;
    private int size = 0;
    private int capacity;

    // Add a lock for thread safety
    private final ReentrantLock lock = new ReentrantLock();

    public DynamicQueue(int initialCapacity) {
        if (initialCapacity < 1) {
             throw new IllegalArgumentException("Initial capacity must be at least 1");
        }
        this.capacity = initialCapacity;
        this.elements = new Object[initialCapacity];
    }

    public void add(T element) {
        lock.lock(); // Acquire lock
        try {
            if (size == capacity) {
                grow(); // grow must be called while holding the lock
            }

            elements[tail] = element;
            tail = (tail + 1) % capacity;
            size++;
        } finally {
            lock.unlock(); // Release lock
        }
    }

    @SuppressWarnings("unchecked")
    public T poll() {
        lock.lock(); // Acquire lock
        try {
            if (size == 0) {
                return null;
            }

            T element = (T) elements[head];
            elements[head] = null; // Help GC
            head = (head + 1) % capacity;
            size--;

            return element;
        } finally {
            lock.unlock(); // Release lock
        }
    }

    public boolean isEmpty() {
        lock.lock(); // Acquire lock
        try {
            return size == 0;
        } finally {
            lock.unlock(); // Release lock
        }
    }

    public int size() {
        lock.lock(); // Acquire lock
        try {
            return size;
        } finally {
            lock.unlock(); // Release lock
        }
    }

    public int capacity() {
       lock.lock(); // Acquire lock
        try {
             return capacity;
        } finally {
             lock.unlock(); // Release lock
        }
    }

    // Grow must ONLY be called by add() while already holding the lock
    private void grow() {
        int newCapacity = capacity * 2;
        Object[] newElements = new Object[newCapacity];

        for (int i = 0; i < size; i++) {
            newElements[i] = elements[(head + i) % capacity];
        }

        elements = newElements;
        head = 0;
        tail = size;
        capacity = newCapacity;
    }

    /**
     * Returns a thread-safe snapshot of the current elements in the queue.
     * The returned list is a copy and modifications to it will not affect the queue.
     *
     * @return A new List containing the elements currently in the queue in order.
     */
    @SuppressWarnings("unchecked")
    public List<T> getSnapshot() {
        List<T> snapshot = new ArrayList<>(size); // Pre-allocate size
        lock.lock(); // Acquire lock
        try {
            for (int i = 0; i < size; i++) {
                snapshot.add((T) elements[(head + i) % capacity]);
            }
            return snapshot;
        } finally {
            lock.unlock(); // Release lock
        }
    }
}