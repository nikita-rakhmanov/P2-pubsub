package src.pubsub.qos.fault;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages connection state for publishers and consumers.
 * This is used to simulate and handle temporary connection disruptions (R4).
 */
public class ConnectionManager {
    private static final ConnectionManager INSTANCE = new ConnectionManager();
    
    // Maps component IDs to their connection state
    private final Map<String, ConnectionState> connectionStates = new HashMap<>();
    
    private ConnectionManager() {
        // Private constructor for singleton
    }
    
    /**
     * Gets the singleton instance of the ConnectionManager.
     * 
     * @return the singleton instance
     */
    public static ConnectionManager getInstance() {
        return INSTANCE;
    }
    
    /**
     * Adds a component to be managed.
     * 
     * @param componentId the unique ID of the component
     */
    public void registerComponent(String componentId) {
        connectionStates.put(componentId, new ConnectionState());
    }
    
    /**
     * Checks if a component is currently connected.
     * 
     * @param componentId the unique ID of the component
     * @return true if connected, false otherwise
     */
    public boolean isConnected(String componentId) {
        ConnectionState state = connectionStates.get(componentId);
        return state != null && state.connected.get();
    }
    
    /**
     * Simulates a connection disruption for a component.
     * 
     * @param componentId the unique ID of the component
     */
    public void simulateDisconnection(String componentId) {
        ConnectionState state = connectionStates.get(componentId);
        if (state != null) {
            state.connected.set(false);
            System.out.println("Component " + componentId + " disconnected!");
        }
    }
    
    /**
     * Simulates a connection restoration for a component.
     * 
     * @param componentId the unique ID of the component
     */
    public void simulateReconnection(String componentId) {
        ConnectionState state = connectionStates.get(componentId);
        if (state != null) {
            state.connected.set(true);
            System.out.println("Component " + componentId + " reconnected!");
        }
    }
    
    /**
     * Inner class to store connection state.
     */
    private static class ConnectionState {
        private final AtomicBoolean connected = new AtomicBoolean(true);
    }
}