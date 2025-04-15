package src.pubsub.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import src.pubsub.core.BasicConsumer;
import src.pubsub.core.Event;

public class TestConsumer extends BasicConsumer {
    private final ConcurrentLinkedQueue<Event> receivedEvents = new ConcurrentLinkedQueue<>();
    private boolean consumedSuccessfully = false; // Flag to track success

    public TestConsumer(String id) {
        super(id);
    }

    public TestConsumer(String id, int reconnectIntervalSeconds) {
        super(id, reconnectIntervalSeconds);
    }

    @Override
    public boolean consume(Event event) { // Match the new signature
        boolean consumptionSuccess = false;
        try {
            // Call super.consume and capture its return value
            consumptionSuccess = super.consume(event);

        } catch (Exception e) {
            // Catch any unexpected exceptions from superclass (shouldn't happen if handled internally)
            System.err.println("TestConsumer " + getId() + " caught unexpected exception calling super.consume: " + e.getMessage());
            consumptionSuccess = false; // Ensure failure on exception
        }

        // Log and store event ONLY if super.consume returned true
        if (consumptionSuccess) {
            System.out.println("TestConsumer " + getId() + " successfully processed event: " + event.getType() + " (ID: " + ((src.pubsub.core.BasicEvent)event).getId() + ")");
            receivedEvents.add(event); // Store the event for verification
        } else {
            // Optional: Log that TestConsumer did not store the event
            // System.out.println("TestConsumer " + getId() + " did not store event " + event.getType() + " because super.consume indicated failure/loss.");
        }

        return consumptionSuccess; // Return the status
    }

    public List<Event> getReceivedEvents() {
        return new ArrayList<>(receivedEvents);
    }

    public int getReceivedEventCount() {
        return receivedEvents.size();
    }

    public void clearReceivedEvents() {
        receivedEvents.clear();
    }

     // Helper method to check the internal flag (optional)
     public boolean didLastConsumeSucceed() {
         return consumedSuccessfully;
     }
}