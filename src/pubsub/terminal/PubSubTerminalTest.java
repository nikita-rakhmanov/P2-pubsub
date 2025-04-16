package src.pubsub.terminal;

import src.pubsub.core.*;
import src.pubsub.examples.*;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.io.PrintStream;

/**
 * Terminal-based application for testing and demonstrating the pub-sub system.
 * This application provides a text-based interface to test all QoS features
 * and run pre-made test classes.
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
                    runPremadeTests();
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
        System.out.println("4. Run Premade Tests");
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
    
    private void runPremadeTests() {
        boolean back = false;
        
        while (!back) {
            System.out.println("\n=== Premade Tests ===");
            System.out.println("1. ConsumerCrashTest - Tests handling of crashed consumers (R6)");
            System.out.println("2. MessageLossTest - Tests handling of message loss (R8)");
            System.out.println("3. NetworkDelayTest - Tests handling of network delays (R7)");
            System.out.println("4. QueueCrashTest - Tests handling of crashed queues (R5)");
            System.out.println("5. ReconnectionTest - Tests connection interruptions (R4)");
            System.out.println("6. PerformanceTest - Tests system message throughput");
            System.out.println("7. Main Test Suite - Runs all tests");
            System.out.println("0. Back to Main Menu");
            System.out.print("Enter your choice: ");
            
            String choice = scanner.nextLine().trim();
            
            // Save current System.out/err for restoration after test
            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            
            try {
                switch (choice) {
                    case "1":
                        System.out.println("\nRunning ConsumerCrashTest...");
                        ConsumerCrashTest.main(new String[0]);
                        break;
                    case "2":
                        System.out.println("\nRunning MessageLossTest...");
                        MessageLossTest.main(new String[0]);
                        break;
                    case "3":
                        System.out.println("\nRunning NetworkDelayTest...");
                        NetworkDelayTest.main(new String[0]);
                        break;
                    case "4":
                        System.out.println("\nRunning QueueCrashTest...");
                        QueueCrashTest.main(new String[0]);
                        break;
                    case "5":
                        System.out.println("\nRunning ReconnectionTest...");
                        ReconnectionTest.main(new String[0]);
                        break;
                    case "6":
                        System.out.println("\nRunning PerformanceTest...");
                        src.pubsub.PerformanceTest.main(new String[0]);
                        break;
                    case "7":
                        System.out.println("\nRunning MainTestSuite...");
                        System.out.print("Output test results to: (1) Console or (2) File 'test_results.txt'? ");
                        String outputChoice = scanner.nextLine().trim();
                        boolean fileOutput = outputChoice.equals("2");
                        
                        if (fileOutput) {
                            System.out.println("Running tests with output to file test_results.txt");
                            // Create a parameter to tell the test suite to use file output without prompting
                            MainTestSuite.main(new String[]{"file"});
                        } else {
                            System.out.println("Running tests with console output");
                            // Tell the test suite to use console output without prompting
                            MainTestSuite.main(new String[]{"console"});
                        }
                        break;
                    case "0":
                        back = true;
                        break;
                    default:
                        System.out.println("Invalid choice, please try again.");
                }
            } catch (Exception e) {
                System.err.println("Error running test: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // restore original output streams
                System.setOut(originalOut);
                System.setErr(originalErr);
                
                if (!choice.equals("0")) {
                    System.out.println("\nTest completed. Press Enter to continue...");
                    scanner.nextLine();
                }
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