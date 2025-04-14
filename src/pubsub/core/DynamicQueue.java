package src.pubsub.core;

/**
 * Dynamic queue implementation that can grow as needed.
 * Per the requirements, ArrayLists are not viable, so this uses a circular array
 * that resizes when needed.
 */
public class DynamicQueue<T> {
    private Object[] elements;
    private int head = 0;
    private int tail = 0;
    private int size = 0;
    private int capacity;
    
    /**
     * Creates a new dynamic queue with the specified initial capacity.
     * 
     * @param initialCapacity the initial capacity of the queue
     */
    public DynamicQueue(int initialCapacity) {
        this.capacity = initialCapacity;
        this.elements = new Object[initialCapacity];
    }
    
    /**
     * Adds an element to the queue, growing the queue if necessary.
     * 
     * @param element the element to add
     */
    public void add(T element) {
        if (size == capacity) {
            grow();
        }
        
        elements[tail] = element;
        tail = (tail + 1) % capacity;
        size++;
    }
    
    /**
     * Removes and returns the head element of the queue.
     * 
     * @return the head element, or null if the queue is empty
     */
    @SuppressWarnings("unchecked")
    public T poll() {
        if (size == 0) {
            return null;
        }
        
        T element = (T) elements[head];
        elements[head] = null; // Help GC
        head = (head + 1) % capacity;
        size--;
        
        return element;
    }
    
    /**
     * Checks if the queue is empty.
     * 
     * @return true if the queue is empty, false otherwise
     */
    public boolean isEmpty() {
        return size == 0;
    }
    
    /**
     * Returns the current size of the queue.
     * 
     * @return the number of elements in the queue
     */
    public int size() {
        return size;
    }
    
    /**
     * Returns the current capacity of the queue.
     * 
     * @return the capacity of the queue
     */
    public int capacity() {
        return capacity;
    }
    
    /**
     * Grows the queue by doubling its capacity.
     */
    private void grow() {
        int newCapacity = capacity * 2;
        Object[] newElements = new Object[newCapacity];
        
        // Copy elements to the new array, unwrapping the circular buffer
        for (int i = 0; i < size; i++) {
            newElements[i] = elements[(head + i) % capacity];
        }
        
        elements = newElements;
        head = 0;
        tail = size;
        capacity = newCapacity;
    }
}