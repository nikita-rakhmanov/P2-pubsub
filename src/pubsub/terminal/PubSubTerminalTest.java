package src.pubsub.terminal;

import src.pubsub.core.*;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

/**
 * Terminal-based application for testing and demonstrating the pub-sub system.
 * This application provides a text-based interface to test all QoS features.
 */
public class PubSubTerminalTest {
    // Core components
    private BasicMiddleware middleware;
    private Map<String, BasicPublisher> publishers = new HashMap<>();
    private Map<String, BasicConsumer> consumers = new HashMap<>();
    
    // For scheduling tasks
    private ScheduledExecutorService scheduler;
    
    // Scanner for user input
    private Scanner scanner;
    
    public PubSubTerminalTest() {
        middleware = new BasicMiddleware(30000, 3, 30);
        scanner = new Scanner(System.in);
        scheduler = Executors.newScheduledThreadPool(1);
    }
    
    /**
     * Main method that displays the menu and handles user input.
     */
    public void run() {
        boolean running = true;
        System.out.println("=== Pub-Sub System Test Harness ===");
        
        // Create some initial test components
        createTestComponents();
        
        while (running) {
            displayMainMenu();
            String choice = scanner.nextLine().trim();
            
            switch (choice) {
                case "1":
                    manageChannels();
                    break;
                case "2":
                    managePublishers();
                    break;
                case "3":
                    manageConsumers();
                    break;
                case "4":
                    runQoSTests();
                    break;
                case "5":
                    showStatistics();
                    break;
                case "6":
                    middleware.dispatchAllEvents();
                    System.out.println("All events dispatched");
                    break;
                case "0":
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
        
        cleanup();
        System.out.println("Test harness closed.");
    }
    
    private void displayMainMenu() {
        System.out.println("\n=== Main Menu ===");
        System.out.println("1. Manage Channels");
        System.out.println("2. Manage Publishers");
        System.out.println("3. Manage Consumers");
        System.out.println("4. Run QoS Tests");
        System.out.println("5. Show Statistics");
        System.out.println("6. Dispatch All Events");
        System.out.println("0. Exit");
        System.out.print("Enter your choice: ");
    }
    
    private void manageChannels() {
        boolean back = false;
        
        while (!back) {
            System.out.println("\n=== Channel Management ===");
            listChannels();
            System.out.println("\n1. Create Channel");
            System.out.println("2. Configure Network Delay");
            System.out.println("3. Configure Message Loss");
            System.out.println("4. Crash Channel Queue");
            System.out.println("5. Recover Channel Queue");
            System.out.println("0. Back to Main Menu");
            System.out.print("Enter your choice: ");
            
            String choice = scanner.nextLine().trim();
            
            switch (choice) {
                case "1":
                    createChannel();
                    break;
                case "2":
                    configureNetworkDelay();
                    break;
                case "3":
                    configureMessageLoss();
                    break;
                case "4":
                    crashChannelQueue();
                    break;
                case "5":
                    recoverChannelQueue();
                    break;
                case "0":
                    back = true;
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
    }
    
    private void managePublishers() {
        boolean back = false;
        
        while (!back) {
            System.out.println("\n=== Publisher Management ===");
            listPublishers();
            System.out.println("\n1. Create Publisher");
            System.out.println("2. Publish Event");
            System.out.println("3. Publish Multiple Events");
            System.out.println("4. Disconnect Publisher");
            System.out.println("5. Reconnect Publisher");
            System.out.println("0. Back to Main Menu");
            System.out.print("Enter your choice: ");
            
            String choice = scanner.nextLine().trim();
            
            switch (choice) {
                case "1":
                    createPublisher();
                    break;
                case "2":
                    publishEvent();
                    break;
                case "3":
                    publishMultipleEvents();
                    break;
                case "4":
                    disconnectPublisher();
                    break;
                case "5":
                    reconnectPublisher();
                    break;
                case "0":
                    back = true;
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
    }
    
    private void manageConsumers() {
        boolean back = false;
        
        while (!back) {
            System.out.println("\n=== Consumer Management ===");
            listConsumers();
            System.out.println("\n1. Create Consumer");
            System.out.println("2. Subscribe to Channel");
            System.out.println("3. Unsubscribe from Channel");
            System.out.println("4. Disconnect Consumer");
            System.out.println("5. Reconnect Consumer");
            System.out.println("6. Crash Consumer");
            System.out.println("7. Recover Consumer");
            System.out.println("8. Configure Message Loss");
            System.out.println("0. Back to Main Menu");
            System.out.print("Enter your choice: ");
            
            String choice = scanner.nextLine().trim();
            
            switch (choice) {
                case "1":
                    createConsumer();
                    break;
                case "2":
                    subscribeConsumer();
                    break;
                case "3":
                    unsubscribeConsumer();
                    break;
                case "4":
                    disconnectConsumer();
                    break;
                case "5":
                    reconnectConsumer();
                    break;
                case "6":
                    crashConsumer();
                    break;
                case "7":
                    recoverConsumer();
                    break;
                case "8":
                    configureConsumerMessageLoss();
                    break;
                case "0":
                    back = true;
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
    }
    
    private void runQoSTests() {
        boolean back = false;
        
        while (!back) {
            System.out.println("\n=== QoS Tests ===");
            System.out.println("1. Test R4: Temporary Interruptions");
            System.out.println("2. Test R5: Crashing Queues");
            System.out.println("3. Test R6: Crashing Consumers");
            System.out.println("4. Test R7: Network Delays");
            System.out.println("5. Test R8: Dropped Messages");
            System.out.println("6. Run Integrated QoS Test");
            System.out.println("0. Back to Main Menu");
            System.out.print("Enter your choice: ");
            
            String choice = scanner.nextLine().trim();
            
            switch (choice) {
                case "1":
                    runR4Test();
                    break;
                case "2":
                    runR5Test();
                    break;
                case "3":
                    runR6Test();
                    break;
                case "4":
                    runR7Test();
                    break;
                case "5":
                    runR8Test();
                    break;
                case "6":
                    runIntegratedTest();
                    break;
                case "0":
                    back = true;
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
    }
    
    private void showStatistics() {
        System.out.println("\n=== Channel Statistics ===");
        
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        
        if (stats.isEmpty()) {
            System.out.println("No statistics available.");
            return;
        }
        
        System.out.println(String.format("%-20s %-10s %-10s %-10s %-10s %-10s %-10s", 
                           "Channel", "Sent", "Acked", "Resent", "Failed", "Pending", "Success %"));
        System.out.println("-".repeat(80));
        
        for (Map.Entry<String, BasicMiddleware.DeliveryStats> entry : stats.entrySet()) {
            String channelName = entry.getKey();
            BasicMiddleware.DeliveryStats stat = entry.getValue();
            
            System.out.println(String.format("%-20s %-10d %-10d %-10d %-10d %-10d %-10.2f", 
                               channelName,
                               stat.getMessagesSent(),
                               stat.getMessagesAcked(),
                               stat.getMessagesResent(),
                               stat.getMessagesFailed(),
                               stat.getPendingMessages(),
                               stat.getSuccessRate()));
        }
        
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    // Channel management methods
    
    private void listChannels() {
        List<String> channels = middleware.listChannels();
        
        if (channels.isEmpty()) {
            System.out.println("No channels available.");
            return;
        }
        
        System.out.println("Available channels:");
        for (String channel : channels) {
            System.out.println("- " + channel);
        }
    }
    
    private void createChannel() {
        System.out.print("Enter channel name: ");
        String name = scanner.nextLine().trim();
        
        if (name.isEmpty()) {
            System.out.println("Channel name cannot be empty.");
            return;
        }
        
        middleware.createChannel(name);
        System.out.println("Channel created: " + name);
    }
    
    private void configureNetworkDelay() {
        String channelName = selectChannel();
        if (channelName == null) return;
        
        System.out.println("\nDelay Type:");
        System.out.println("1. Fixed Delay");
        System.out.println("2. Variable Delay");
        System.out.println("3. Jitter Delay");
        System.out.print("Select delay type: ");
        
        String choice = scanner.nextLine().trim();
        
        switch (choice) {
            case "1":
                System.out.print("Enter delay in ms: ");
                try {
                    int delay = Integer.parseInt(scanner.nextLine().trim());
                    middleware.simulateChannelNetworkDelay(channelName, delay);
                    System.out.println("Fixed delay of " + delay + "ms set for channel: " + channelName);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number format.");
                }
                break;
                
            case "2":
                try {
                    System.out.print("Enter minimum delay in ms: ");
                    int minDelay = Integer.parseInt(scanner.nextLine().trim());
                    System.out.print("Enter maximum delay in ms: ");
                    int maxDelay = Integer.parseInt(scanner.nextLine().trim());
                    middleware.simulateChannelVariableNetworkDelay(channelName, minDelay, maxDelay);
                    System.out.println("Variable delay of " + minDelay + "-" + maxDelay + "ms set for channel: " + channelName);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number format.");
                }
                break;
                
            case "3":
                try {
                    System.out.print("Enter base delay in ms: ");
                    int baseDelay = Integer.parseInt(scanner.nextLine().trim());
                    System.out.print("Enter jitter in ms: ");
                    int jitter = Integer.parseInt(scanner.nextLine().trim());
                    middleware.simulateChannelNetworkJitter(channelName, baseDelay, jitter);
                    System.out.println("Jitter delay of " + baseDelay + "±" + jitter + "ms set for channel: " + channelName);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number format.");
                }
                break;
                
            default:
                System.out.println("Invalid choice.");
        }
    }
    
    private void configureMessageLoss() {
        String channelName = selectChannel();
        if (channelName == null) return;
        
        System.out.print("Enter loss probability (0.0-1.0): ");
        try {
            double probability = Double.parseDouble(scanner.nextLine().trim());
            if (probability < 0.0 || probability > 1.0) {
                System.out.println("Probability must be between 0.0 and 1.0.");
                return;
            }
            
            middleware.setChannelDeliveryFailureProbability(channelName, probability);
            System.out.println("Message loss probability of " + probability + " set for channel: " + channelName);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
        }
    }
    
    private void crashChannelQueue() {
        String channelName = selectChannel();
        if (channelName == null) return;
        
        middleware.simulateChannelQueueCrash(channelName);
        System.out.println("Channel queue crashed: " + channelName);
    }
    
    private void recoverChannelQueue() {
        String channelName = selectChannel();
        if (channelName == null) return;
        
        boolean recovered = middleware.recoverChannelQueue(channelName);
        System.out.println("Channel queue recovery " + (recovered ? "succeeded" : "failed") + ": " + channelName);
    }
    
    // Publisher management methods
    
    private void listPublishers() {
        if (publishers.isEmpty()) {
            System.out.println("No publishers available.");
            return;
        }
        
        System.out.println("Available publishers:");
        for (String publisher : publishers.keySet()) {
            boolean connected = publishers.get(publisher).isConnected();
            System.out.println("- " + publisher + " (" + (connected ? "connected" : "disconnected") + ")");
        }
    }
    
    private void createPublisher() {
        System.out.print("Enter publisher name: ");
        String name = scanner.nextLine().trim();
        
        if (name.isEmpty()) {
            System.out.println("Publisher name cannot be empty.");
            return;
        }
        
        if (publishers.containsKey(name)) {
            System.out.println("Publisher already exists.");
            return;
        }
        
        BasicPublisher publisher = new BasicPublisher(name);
        publisher.registerWithMiddleware(middleware);
        publishers.put(name, publisher);
        System.out.println("Publisher created: " + name);
    }
    
    private void publishEvent() {
        String publisherName = selectPublisher();
        if (publisherName == null) return;
        
        String channelName = selectChannel();
        if (channelName == null) return;
        
        System.out.print("Enter event type: ");
        String eventType = scanner.nextLine().trim();
        
        if (eventType.isEmpty()) {
            System.out.println("Event type cannot be empty.");
            return;
        }
        
        publishers.get(publisherName).publish(channelName, new BasicEvent(eventType));
        System.out.println("Event published to channel: " + channelName);
    }
    
    private void publishMultipleEvents() {
        String publisherName = selectPublisher();
        if (publisherName == null) return;
        
        String channelName = selectChannel();
        if (channelName == null) return;
        
        System.out.print("Enter number of events to publish: ");
        int count;
        try {
            count = Integer.parseInt(scanner.nextLine().trim());
            if (count <= 0) {
                System.out.println("Count must be positive.");
                return;
            }
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
            return;
        }
        
        for (int i = 0; i < count; i++) {
            publishers.get(publisherName).publish(channelName, new BasicEvent("Bulk Event " + i));
        }
        
        System.out.println(count + " events published to channel: " + channelName);
    }
    
    private void disconnectPublisher() {
        String publisherName = selectPublisher();
        if (publisherName == null) return;
        
        publishers.get(publisherName).simulateDisconnection();
        System.out.println("Publisher disconnected: " + publisherName);
    }
    
    private void reconnectPublisher() {
        String publisherName = selectPublisher();
        if (publisherName == null) return;
        
        publishers.get(publisherName).simulateReconnection();
        System.out.println("Publisher reconnected: " + publisherName);
    }
    
    // Consumer management methods
    
    private void listConsumers() {
        if (consumers.isEmpty()) {
            System.out.println("No consumers available.");
            return;
        }
        
        System.out.println("Available consumers:");
        for (String consumer : consumers.keySet()) {
            BasicConsumer c = consumers.get(consumer);
            boolean connected = c.isConnected();
            boolean crashed = c.isCrashed();
            System.out.println("- " + consumer + " (" + 
                             (crashed ? "crashed" : connected ? "connected" : "disconnected") + ")");
            
            Set<String> subscriptions = c.getSubscribedChannels();
            if (!subscriptions.isEmpty()) {
                System.out.println("  Subscribed to: " + String.join(", ", subscriptions));
            }
        }
    }
    
    private void createConsumer() {
        System.out.print("Enter consumer name: ");
        String name = scanner.nextLine().trim();
        
        if (name.isEmpty()) {
            System.out.println("Consumer name cannot be empty.");
            return;
        }
        
        if (consumers.containsKey(name)) {
            System.out.println("Consumer already exists.");
            return;
        }
        
        BasicConsumer consumer = new BasicConsumer(name);
        consumer.registerWithMiddleware(middleware);
        consumers.put(name, consumer);
        System.out.println("Consumer created: " + name);
    }
    
    private void subscribeConsumer() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        String channelName = selectChannel();
        if (channelName == null) return;
        
        consumers.get(consumerName).subscribe(channelName);
        System.out.println("Consumer " + consumerName + " subscribed to channel: " + channelName);
    }
    
    private void unsubscribeConsumer() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        BasicConsumer consumer = consumers.get(consumerName);
        Set<String> subscriptions = consumer.getSubscribedChannels();
        
        if (subscriptions.isEmpty()) {
            System.out.println("Consumer has no subscriptions.");
            return;
        }
        
        System.out.println("Subscribed channels:");
        int i = 1;
        Map<Integer, String> channelMap = new HashMap<>();
        
        for (String channel : subscriptions) {
            System.out.println(i + ". " + channel);
            channelMap.put(i++, channel);
        }
        
        System.out.print("Select channel to unsubscribe from: ");
        try {
            int selection = Integer.parseInt(scanner.nextLine().trim());
            if (!channelMap.containsKey(selection)) {
                System.out.println("Invalid selection.");
                return;
            }
            
            String channelName = channelMap.get(selection);
            consumer.unsubscribe(channelName);
            System.out.println("Consumer " + consumerName + " unsubscribed from channel: " + channelName);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
        }
    }
    
    private void disconnectConsumer() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        consumers.get(consumerName).simulateDisconnection();
        System.out.println("Consumer disconnected: " + consumerName);
    }
    
    private void reconnectConsumer() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        consumers.get(consumerName).simulateReconnection();
        System.out.println("Consumer reconnected: " + consumerName);
    }
    
    private void crashConsumer() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        consumers.get(consumerName).simulateCrash();
        System.out.println("Consumer crashed: " + consumerName);
    }
    
    private void recoverConsumer() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        consumers.get(consumerName).recover();
        System.out.println("Consumer recovered: " + consumerName);
    }
    
    private void configureConsumerMessageLoss() {
        String consumerName = selectConsumer();
        if (consumerName == null) return;
        
        System.out.print("Enter loss probability (0.0-1.0): ");
        try {
            double probability = Double.parseDouble(scanner.nextLine().trim());
            if (probability < 0.0 || probability > 1.0) {
                System.out.println("Probability must be between 0.0 and 1.0.");
                return;
            }
            
            consumers.get(consumerName).simulateMessageLoss(probability > 0, probability);
            System.out.println("Message loss probability of " + probability + " set for consumer: " + consumerName);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
        }
    }
    
    // Helper methods for selection
    
    private String selectChannel() {
        List<String> channels = middleware.listChannels();
        
        if (channels.isEmpty()) {
            System.out.println("No channels available.");
            return null;
        }
        
        System.out.println("Available channels:");
        for (int i = 0; i < channels.size(); i++) {
            System.out.println((i + 1) + ". " + channels.get(i));
        }
        
        System.out.print("Select channel: ");
        try {
            int selection = Integer.parseInt(scanner.nextLine().trim());
            if (selection < 1 || selection > channels.size()) {
                System.out.println("Invalid selection.");
                return null;
            }
            
            return channels.get(selection - 1);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
            return null;
        }
    }
    
    private String selectPublisher() {
        if (publishers.isEmpty()) {
            System.out.println("No publishers available.");
            return null;
        }
        
        List<String> publisherList = new ArrayList<>(publishers.keySet());
        System.out.println("Available publishers:");
        for (int i = 0; i < publisherList.size(); i++) {
            System.out.println((i + 1) + ". " + publisherList.get(i));
        }
        
        System.out.print("Select publisher: ");
        try {
            int selection = Integer.parseInt(scanner.nextLine().trim());
            if (selection < 1 || selection > publisherList.size()) {
                System.out.println("Invalid selection.");
                return null;
            }
            
            return publisherList.get(selection - 1);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
            return null;
        }
    }
    
    private String selectConsumer() {
        if (consumers.isEmpty()) {
            System.out.println("No consumers available.");
            return null;
        }
        
        List<String> consumerList = new ArrayList<>(consumers.keySet());
        System.out.println("Available consumers:");
        for (int i = 0; i < consumerList.size(); i++) {
            System.out.println((i + 1) + ". " + consumerList.get(i));
        }
        
        System.out.print("Select consumer: ");
        try {
            int selection = Integer.parseInt(scanner.nextLine().trim());
            if (selection < 1 || selection > consumerList.size()) {
                System.out.println("Invalid selection.");
                return null;
            }
            
            return consumerList.get(selection - 1);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
            return null;
        }
    }
    
    // QoS test methods
    
    private void runR4Test() {
        System.out.println("\n=== Running R4 Test: Temporary Interruptions ===");
        
        // Ensure we have necessary components
        if (middleware.listChannels().isEmpty() || publishers.isEmpty() || consumers.isEmpty()) {
            System.out.println("Test requires at least one channel, publisher, and consumer.");
            return;
        }
        
        // Get components for test
        String publisherName = publishers.keySet().iterator().next();
        String consumerName = consumers.keySet().iterator().next();
        String channelName = middleware.listChannels().get(0);
        
        // Ensure consumer is subscribed
        consumers.get(consumerName).subscribe(channelName);
        
        System.out.println("1. Testing publisher disconnection/reconnection");
        
        // Disconnect publisher
        publishers.get(publisherName).simulateDisconnection();
        System.out.println("   - Publisher disconnected: " + publisherName);
        
        // Try to publish while disconnected
        for (int i = 0; i < 3; i++) {
            publishers.get(publisherName).publish(channelName, new BasicEvent("Buffered Event " + i));
        }
        System.out.println("   - Published 3 events while disconnected (buffered)");
        
        // Reconnect publisher
        publishers.get(publisherName).simulateReconnection();
        System.out.println("   - Publisher reconnected: " + publisherName);
        
        // Dispatch events
        middleware.dispatchAllEvents();
        System.out.println("   - Events dispatched after reconnection");
        
        System.out.println("\n2. Testing consumer disconnection/reconnection");
        
        // Disconnect consumer
        consumers.get(consumerName).simulateDisconnection();
        System.out.println("   - Consumer disconnected: " + consumerName);
        
        // Publish events (consumer won't receive)
        publishers.get(publisherName).publish(channelName, new BasicEvent("Event during consumer disconnect"));
        middleware.dispatchAllEvents();
        System.out.println("   - Published and dispatched event while consumer disconnected");
        
        // Reconnect consumer
        consumers.get(consumerName).simulateReconnection();
        System.out.println("   - Consumer reconnected: " + consumerName);
        
        // Publish events after reconnection
        publishers.get(publisherName).publish(channelName, new BasicEvent("Event after consumer reconnect"));
        middleware.dispatchAllEvents();
        System.out.println("   - Published and dispatched event after consumer reconnection");
        
        System.out.println("\nR4 Test Complete");
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    private void runR5Test() {
        System.out.println("\n=== Running R5 Test: Crashing Queues ===");
        
        // Ensure we have necessary components
        List<String> channels = middleware.listChannels();
        if (channels.isEmpty() || publishers.isEmpty()) {
            System.out.println("Test requires at least one channel and publisher.");
            return;
        }
        
        // Choose channels for test
        String testChannel1 = channels.get(0);
        String testChannel2 = channels.size() > 1 ? channels.get(1) : testChannel1;
        String publisherName = publishers.keySet().iterator().next();
        
        // Set up channels with different recovery settings
        middleware.setChannelAutomaticRecovery(testChannel1, true);
        middleware.setChannelAutomaticRecovery(testChannel2, false);
        System.out.println("Set channel recovery: " + testChannel1 + " (automatic), " + 
                         testChannel2 + " (manual)");
        
        System.out.println("\n1. Testing auto-recovery channel");
        middleware.simulateChannelQueueCrash(testChannel1);
        System.out.println("   - Crashed channel: " + testChannel1);
        
        // Try to publish to crashed channel (should auto-recover)
        boolean success = true;
        try {
            publishers.get(publisherName).publish(testChannel1, new BasicEvent("Post-crash Event - Auto-recovery"));
        } catch (Exception e) {
            success = false;
            System.out.println("   - Error publishing to auto-recovery channel: " + e.getMessage());
        }
        
        if (success) {
            System.out.println("   - Successfully published to auto-recovery channel (recovered)");
        }
        
        System.out.println("\n2. Testing manual recovery channel");
        middleware.simulateChannelQueueCrash(testChannel2);
        System.out.println("   - Crashed channel: " + testChannel2);
        
        // Try to publish to crashed channel (should fail)
        try {
            publishers.get(publisherName).publish(testChannel2, new BasicEvent("Post-crash Event - Manual recovery"));
            System.out.println("   - Published to crashed manual-recovery channel (unexpected)");
        } catch (Exception e) {
            System.out.println("   - Failed to publish to crashed manual-recovery channel (expected): " + e.getMessage());
        }
        
        // Manually recover channel
        boolean recovered = middleware.recoverChannelQueue(testChannel2);
        System.out.println("   - Manual channel recovery " + (recovered ? "succeeded" : "failed"));
        
        // Try to publish after manual recovery
        success = true;
        try {
            publishers.get(publisherName).publish(testChannel2, new BasicEvent("Post-recovery Event - Manual recovery"));
        } catch (Exception e) {
            success = false;
            System.out.println("   - Error publishing after manual recovery: " + e.getMessage());
        }
        
        if (success) {
            System.out.println("   - Successfully published after manual recovery");
        }
        
        System.out.println("\nR5 Test Complete");
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    private void runR6Test() {
        System.out.println("\n=== Running R6 Test: Crashing Consumers ===");
        
        // Ensure we have necessary components
        List<String> channels = middleware.listChannels();
        if (channels.isEmpty() || consumers.isEmpty() || publishers.isEmpty()) {
            System.out.println("Test requires at least one channel, consumer, and publisher.");
            return;
        }
        
        String testChannel = channels.get(0);
        String consumerName = consumers.keySet().iterator().next();
        String publisherName = publishers.keySet().iterator().next();
        
        // Ensure consumer is subscribed
        consumers.get(consumerName).subscribe(testChannel);
        System.out.println("Ensured " + consumerName + " is subscribed to " + testChannel);
        
        // Simulate consumer crash
        consumers.get(consumerName).simulateCrash();
        System.out.println("Crashed consumer: " + consumerName);
        
        // Try to use crashed consumer
        try {
            consumers.get(consumerName).consume(new BasicEvent("Test event for crashed consumer"));
            System.out.println("Used crashed consumer (unexpected)");
        } catch (Exception e) {
            System.out.println("Failed to use crashed consumer (expected): " + e.getMessage());
        }
        
        // Publish events (should not be received by crashed consumer)
        publishers.get(publisherName).publish(testChannel, new BasicEvent("Event during consumer crash"));
        middleware.dispatchAllEvents();
        System.out.println("Published and dispatched event while consumer crashed");
        
        // Wait a moment for health monitor to detect crash
        System.out.println("Waiting for health monitor to detect crash...");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Recover consumer
        consumers.get(consumerName).recover();
        System.out.println("Recovered consumer: " + consumerName);
        
        // Publish event to verify recovery
        publishers.get(publisherName).publish(testChannel, new BasicEvent("Post-recovery Event"));
        middleware.dispatchAllEvents();
        System.out.println("Published and dispatched event after consumer recovery");
        
        System.out.println("\nR6 Test Complete");
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    private void runR7Test() {
        System.out.println("\n=== Running R7 Test: Network Delays ===");
        
        // Ensure we have necessary components
        List<String> channels = middleware.listChannels();
        if (channels.isEmpty() || publishers.isEmpty() || consumers.isEmpty()) {
            System.out.println("Test requires at least one channel, publisher, and consumer.");
            return;
        }
        
        String testChannel1 = channels.get(0);
        String testChannel2 = channels.size() > 1 ? channels.get(1) : testChannel1;
        String publisherName = publishers.keySet().iterator().next();
        String consumerName = consumers.keySet().iterator().next();
        
        // Ensure consumer is subscribed
        consumers.get(consumerName).subscribe(testChannel1);
        consumers.get(consumerName).subscribe(testChannel2);
        
        // Configure different delay settings
        middleware.simulateChannelNetworkDelay(testChannel1, 200);
        System.out.println("Set fixed delay of 200ms for " + testChannel1);
        
        middleware.simulateChannelVariableNetworkDelay(testChannel2, 300, 800);
        System.out.println("Set variable delay of 300-800ms for " + testChannel2);
        
        // Publish messages to test delays
        for (int i = 0; i < 3; i++) {
            publishers.get(publisherName).publish(testChannel1, new BasicEvent("Fixed Delay Event " + i));
            publishers.get(publisherName).publish(testChannel2, new BasicEvent("Variable Delay Event " + i));
        }
        System.out.println("Published test events to delayed channels");
        
        // Dispatch events
        middleware.dispatchAllEvents();
        System.out.println("Dispatched events (delivery will be delayed)");
        
        // Wait for delayed delivery
        System.out.println("Waiting for delayed delivery to complete...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Show results
        System.out.println("\nChecking delivery after delays...");
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        
        for (Map.Entry<String, BasicMiddleware.DeliveryStats> entry : stats.entrySet()) {
            if (entry.getKey().equals(testChannel1) || entry.getKey().equals(testChannel2)) {
                System.out.println(entry.getKey() + " stats: " + entry.getValue());
            }
        }
        
        System.out.println("\nR7 Test Complete");
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    private void runR8Test() {
        System.out.println("\n=== Running R8 Test: Dropped Messages ===");
        
        // Ensure we have necessary components
        List<String> channels = middleware.listChannels();
        if (channels.isEmpty() || publishers.isEmpty() || consumers.isEmpty()) {
            System.out.println("Test requires at least one channel, publisher, and consumer.");
            return;
        }
        
        String testChannel = channels.get(0);
        String consumerName = consumers.keySet().iterator().next();
        String publisherName = publishers.keySet().iterator().next();
        
        // Ensure consumer is subscribed
        consumers.get(consumerName).subscribe(testChannel);
        
        // Configure message loss simulation
        middleware.setChannelDeliveryFailureProbability(testChannel, 0.3);  // 30% channel loss
        System.out.println("Set 30% message loss for channel: " + testChannel);
        
        consumers.get(consumerName).simulateMessageLoss(true, 0.2);  // 20% consumer loss
        System.out.println("Set 20% message loss for consumer: " + consumerName);
        
        // Publish messages with loss simulation
        System.out.println("Publishing with message loss simulation...");
        for (int i = 0; i < 10; i++) {
            publishers.get(publisherName).publish(testChannel, new BasicEvent("Loss Test Event " + i));
        }
        System.out.println("Published 10 test events");
        
        // Dispatch events
        middleware.dispatchAllEvents();
        System.out.println("Dispatched events (some will be lost/retried)");
        
        // Wait for retries
        System.out.println("Waiting for retry attempts...");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Show results
        System.out.println("\nChecking delivery after retries...");
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        
        if (stats.containsKey(testChannel)) {
            System.out.println(testChannel + " stats: " + stats.get(testChannel));
        }
        
        System.out.println("\nR8 Test Complete");
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    private void runIntegratedTest() {
        System.out.println("\n=== Starting Integrated QoS Test ===");
        
        // Create test components if needed
        ensureTestComponentsExist();
        
        // Configure channels with different QoS settings
        configureTestChannels();
        
        // Phase 1: Initial publishing
        System.out.println("\n--- Phase 1: Initial Publishing ---");
        
        publishers.get("main-publisher").publish("reliable-channel", new BasicEvent("Reliable Message 1"));
        publishers.get("main-publisher").publish("delay-tolerant-channel", new BasicEvent("Delay-Tolerant Message 1"));
        publishers.get("main-publisher").publish("critical-channel", new BasicEvent("Critical Message 1"));
        
        middleware.dispatchAllEvents();
        System.out.println("Initial events dispatched");
        
        // Phase 2: Simulate failures
        System.out.println("\n--- Phase 2: Simulating Failures ---");
        
        // Simulate publisher disconnection
        publishers.get("main-publisher").simulateDisconnection();
        System.out.println("Disconnected main publisher");
        
        // Try to publish with disconnected publisher (will buffer)
        publishers.get("main-publisher").publish("reliable-channel", new BasicEvent("Buffered Message"));
        publishers.get("main-publisher").publish("critical-channel", new BasicEvent("Buffered Critical Message"));
        System.out.println("Published messages with disconnected publisher (buffered)");
        
        // Use backup publisher
        publishers.get("backup-publisher").publish("delay-tolerant-channel", new BasicEvent("Backup Publisher Message"));
        System.out.println("Published message with backup publisher");
        
        // Simulate consumer crash
        consumers.get("reliable-consumer").simulateCrash();
        System.out.println("Crashed reliable consumer");
        
        // Simulate channel queue crash
        middleware.simulateChannelQueueCrash("reliable-channel");
        System.out.println("Crashed reliable channel queue");
        
        // Dispatch events
        middleware.dispatchAllEvents();
        System.out.println("Dispatched events during failures");
        
        // Wait a moment
        System.out.println("Waiting for failures to be detected...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Phase 3: Recovery
        System.out.println("\n--- Phase 3: Recovery ---");
        
        // Reconnect publisher
        publishers.get("main-publisher").simulateReconnection();
        System.out.println("Reconnected main publisher");
        
        // Manually recover channel
        middleware.recoverChannelQueue("reliable-channel");
        System.out.println("Recovered reliable channel queue");
        
        // Recover consumer
        consumers.get("reliable-consumer").recover();
        System.out.println("Recovered reliable consumer");
        
        // Publish after recovery
        publishers.get("main-publisher").publish("reliable-channel", new BasicEvent("Post-recovery Message"));
        System.out.println("Published message after recovery");
        
        // Dispatch events
        middleware.dispatchAllEvents();
        System.out.println("Dispatched events after recovery");
        
        // Wait for processing
        System.out.println("Waiting for processing to complete...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Show final statistics
        System.out.println("\n--- Final Statistics ---");
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        
        for (Map.Entry<String, BasicMiddleware.DeliveryStats> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        
        System.out.println("\nIntegrated QoS Test Complete");
        System.out.println("Press Enter to continue...");
        scanner.nextLine();
    }
    
    private void ensureTestComponentsExist() {
        // Create test channels if needed
        if (!middleware.listChannels().contains("reliable-channel")) {
            middleware.createChannel("reliable-channel");
        }
        if (!middleware.listChannels().contains("delay-tolerant-channel")) {
            middleware.createChannel("delay-tolerant-channel");
        }
        if (!middleware.listChannels().contains("critical-channel")) {
            middleware.createChannel("critical-channel");
        }
        
        // Create test publishers if needed
        if (!publishers.containsKey("main-publisher")) {
            BasicPublisher publisher = new BasicPublisher("main-publisher");
            publisher.registerWithMiddleware(middleware);
            publishers.put("main-publisher", publisher);
        }
        if (!publishers.containsKey("backup-publisher")) {
            BasicPublisher publisher = new BasicPublisher("backup-publisher");
            publisher.registerWithMiddleware(middleware);
            publishers.put("backup-publisher", publisher);
        }
        
        // Create test consumers if needed
        if (!consumers.containsKey("reliable-consumer")) {
            BasicConsumer consumer = new BasicConsumer("reliable-consumer");
            consumer.registerWithMiddleware(middleware);
            consumer.subscribe("reliable-channel");
            consumers.put("reliable-consumer", consumer);
        }
        if (!consumers.containsKey("delay-tolerant-consumer")) {
            BasicConsumer consumer = new BasicConsumer("delay-tolerant-consumer");
            consumer.registerWithMiddleware(middleware);
            consumer.subscribe("delay-tolerant-channel");
            consumers.put("delay-tolerant-consumer", consumer);
        }
        if (!consumers.containsKey("critical-consumer")) {
            BasicConsumer consumer = new BasicConsumer("critical-consumer");
            consumer.registerWithMiddleware(middleware);
            consumer.subscribe("critical-channel");
            consumer.subscribe("reliable-channel");
            consumers.put("critical-consumer", consumer);
        }
        
        System.out.println("Test components created/verified");
    }
    
    private void configureTestChannels() {
        // Set recovery settings
        middleware.setChannelAutomaticRecovery("reliable-channel", false);
        middleware.setChannelAutomaticRecovery("delay-tolerant-channel", false);
        middleware.setChannelAutomaticRecovery("critical-channel", true);
        
        // Configure network conditions
        middleware.simulateChannelNetworkDelay("reliable-channel", 100);
        middleware.setChannelDeliveryFailureProbability("reliable-channel", 0.1);
        
        middleware.simulateChannelVariableNetworkDelay("delay-tolerant-channel", 500, 2000);
        middleware.setChannelDeliveryFailureProbability("delay-tolerant-channel", 0.2);
        
        middleware.simulateChannelNetworkJitter("critical-channel", 1000, 500);
        middleware.setChannelDeliveryFailureProbability("critical-channel", 0.3);
        
        // Configure consumer message loss
        consumers.get("delay-tolerant-consumer").simulateMessageLoss(true, 0.1);
        
        System.out.println("Channels configured with different QoS settings");
    }
    
    private void createTestComponents() {
        // Create channels
        if (middleware.listChannels().isEmpty()) {
            middleware.createChannel("test-channel-1");
            middleware.createChannel("test-channel-2");
            System.out.println("Created test channels");
        }
        
        // Create publishers
        if (publishers.isEmpty()) {
            BasicPublisher publisher1 = new BasicPublisher("publisher-1");
            publisher1.registerWithMiddleware(middleware);
            publishers.put("publisher-1", publisher1);
            
            BasicPublisher publisher2 = new BasicPublisher("publisher-2");
            publisher2.registerWithMiddleware(middleware);
            publishers.put("publisher-2", publisher2);
            
            System.out.println("Created test publishers");
        }
        
        // Create consumers
        if (consumers.isEmpty()) {
            BasicConsumer consumer1 = new BasicConsumer("consumer-1");
            consumer1.registerWithMiddleware(middleware);
            consumer1.subscribe("test-channel-1");
            consumers.put("consumer-1", consumer1);
            
            BasicConsumer consumer2 = new BasicConsumer("consumer-2");
            consumer2.registerWithMiddleware(middleware);
            consumer2.subscribe("test-channel-1");
            consumer2.subscribe("test-channel-2");
            consumers.put("consumer-2", consumer2);
            
            System.out.println("Created test consumers");
        }
    }
    
    private void cleanup() {
        // Shutdown middleware and components
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        
        if (middleware != null) {
            middleware.shutdown();
        }
        
        for (BasicPublisher publisher : publishers.values()) {
            publisher.shutdown();
        }
        
        for (BasicConsumer consumer : consumers.values()) {
            consumer.shutdown();
        }
    }
    
    /**
     * Main method to start the application.
     */
    public static void main(String[] args) {
        PubSubTerminalTest testHarness = new PubSubTerminalTest();
        testHarness.run();
    }
}