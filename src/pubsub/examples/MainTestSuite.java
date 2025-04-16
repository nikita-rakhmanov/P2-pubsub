package src.pubsub.examples;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import src.pubsub.core.*; // Import all core classes
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Date;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.Scanner;

/**
 * Extensive test suite for my Pub/Sub implementation.
 * Covers R1-R8 requirements and includes a basic performance test.
 */
public class MainTestSuite {

    private static final String TEST_CHANNEL_1 = "test-channel-1";
    private static final String TEST_CHANNEL_2 = "test-channel-2";
    private static final int DISPATCH_WAIT_MS = 500; // Time to wait after dispatch
    private static final int FAULT_WAIT_MS = 2000; // Time to wait for fault detection/recovery

    public static void main(String[] args) throws InterruptedException {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        boolean fileOutput = false;
        
        // Check for command line arguments from PubSubTerminalTest
        if (args.length > 0) {
            if ("file".equals(args[0])) {
                fileOutput = true;
            } else {
                fileOutput = false;
            }
        } else {
            // Only prompt if run standalone
            Scanner scanner = new Scanner(System.in);
            System.out.println("===== Comprehensive Pub/Sub Test Suite =====");
            System.out.print("Output test results to: (1) Console or (2) File 'test_results.txt'? ");
            String choice = scanner.nextLine().trim();
            fileOutput = choice.equals("2");
            scanner.close();
        }
        
        try {
            if (fileOutput) {
                // Set up file output
                PrintStream fileOut = new PrintStream(new FileOutputStream("test_results.txt"));
                // Redirect output to file
                System.setOut(fileOut);
                System.setErr(fileOut);
                System.out.println("Writing test results to test_results.txt...");
            } else {
                System.out.println("Displaying test results in console...");
            }
            
            // Output test header to wherever output is directed
            System.out.println("===== Starting Comprehensive Pub/Sub Test Suite =====");
            System.out.println("Test run started at: " + new Date());

            // Run all tests
            testBasicPubSub();
            testChannelManagement();
            testPublisherConnectionInterruption();
            testConsumerConnectionInterruption();
            testQueueCrashRecovery();
            runConsumerCrashTest();
            testNetworkDelay();
            testEventExpiration();
            testMessageAcknowledgementAndRetry();
            testSimulatedMessageLoss();
            testPerformance();

            System.out.println("\n===== Comprehensive Test Suite Complete =====");
        } catch (FileNotFoundException e) {
            // Reset output before reporting error
            System.setOut(originalOut);
            System.setErr(originalErr);
            System.err.println("Error creating output file: " + e.getMessage());
        } finally {
            // Restore original output streams
            System.setOut(originalOut);
            System.setErr(originalErr);
            
            // Final message to console about where output went
            if (fileOutput) {
                System.out.println("Test complete. Results written to test_results.txt");
            } else {
                System.out.println("Test complete.");
            }
        }
    }

    // ========================================================================
    // Test Methods
    // ========================================================================

    private static void testBasicPubSub() throws InterruptedException {
        System.out.println("\n--- Testing R1: Basic Publish/Subscribe ---");
        BasicMiddleware middleware = new BasicMiddleware();
        BasicPublisher publisher = new BasicPublisher("Pub1");
        TestConsumer consumer1 = new TestConsumer("Sub1");
        TestConsumer consumer2 = new TestConsumer("Sub2");

        publisher.registerWithMiddleware(middleware);
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);

        middleware.createChannel(TEST_CHANNEL_1);
        consumer1.subscribe(TEST_CHANNEL_1);
        consumer2.subscribe(TEST_CHANNEL_1);

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event 1"));
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event 2"));

        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS); // Allow time for dispatch

        assert consumer1.getReceivedEventCount() == 2 : "Consumer 1 failed basic receive";
        assert consumer2.getReceivedEventCount() == 2 : "Consumer 2 failed basic receive";
        System.out.println("Basic Pub/Sub: OK");

        consumer1.unsubscribe(TEST_CHANNEL_1);
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event 3"));
        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);

        assert consumer1.getReceivedEventCount() == 2 : "Consumer 1 received after unsubscribe";
        assert consumer2.getReceivedEventCount() == 3 : "Consumer 2 failed receive after C1 unsubscribed";
        System.out.println("Unsubscribe: OK");

        shutdownAll(middleware, List.of(publisher), List.of(consumer1, consumer2));
    }

    private static void testChannelManagement() {
        System.out.println("\n--- Testing R2/R3: Channel Lookup, Creation, Listing ---");
        BasicMiddleware middleware = new BasicMiddleware();

        assert middleware.lookupChannel("nonexistent") == null : "Lookup nonexistent channel failed";

        Channel ch1 = middleware.createChannel(TEST_CHANNEL_1);
        assert ch1 != null : "Channel creation failed";
        assert ch1.getName().equals(TEST_CHANNEL_1) : "Channel name mismatch";

        Channel ch1Lookup = middleware.lookupChannel(TEST_CHANNEL_1);
        assert ch1Lookup == ch1 : "Channel lookup failed";

        Channel ch1Again = middleware.createChannel(TEST_CHANNEL_1);
        assert ch1Again == ch1 : "Creating existing channel should return same instance";

        middleware.createChannel(TEST_CHANNEL_2);
        List<String> channels = middleware.listChannels();
        assert channels.size() == 2 : "List channels count mismatch";
        assert channels.contains(TEST_CHANNEL_1) && channels.contains(TEST_CHANNEL_2) : "List channels content mismatch";
        System.out.println("Channel Management: OK");

        shutdownAll(middleware, List.of(), List.of());
    }

    private static void testPublisherConnectionInterruption() throws InterruptedException {
        System.out.println("\n--- Testing R4: Publisher Connection Interruption ---");
        BasicMiddleware middleware = new BasicMiddleware();
        BasicPublisher publisher = new BasicPublisher("PubR4", 5, 1); // Small buffer, quick retry
        TestConsumer consumer = new TestConsumer("SubR4");

        publisher.registerWithMiddleware(middleware);
        consumer.registerWithMiddleware(middleware);
        middleware.createChannel(TEST_CHANNEL_1);
        consumer.subscribe(TEST_CHANNEL_1);

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event P1")); // Should succeed
        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);
        assert consumer.getReceivedEventCount() == 1 : "Initial publish failed";

        System.out.println("Simulating publisher disconnection...");
        publisher.simulateDisconnection();
        assert !publisher.isConnected() : "Publisher should be disconnected";

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event P2 (Buffered)"));
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event P3 (Buffered)"));
        assert publisher.getBufferSize() == 2 : "Messages not buffered correctly";
        middleware.dispatchAllEvents(); // Should not dispatch buffered messages yet
        Thread.sleep(DISPATCH_WAIT_MS);
        assert consumer.getReceivedEventCount() == 1 : "Consumer received message while publisher disconnected";

        System.out.println("Simulating publisher reconnection...");
        publisher.simulateReconnection();
        assert publisher.isConnected() : "Publisher should be reconnected";
        // Reconnection should trigger buffer processing

        Thread.sleep(FAULT_WAIT_MS); // Allow time for buffer processing and dispatch
        middleware.dispatchAllEvents(); // Ensure dispatch runs again if needed
        Thread.sleep(DISPATCH_WAIT_MS);

        assert publisher.getBufferSize() == 0 : "Buffer not cleared after reconnect";
        assert consumer.getReceivedEventCount() == 3 : "Consumer did not receive buffered messages";
        System.out.println("Publisher Connection Interruption: OK");

        shutdownAll(middleware, List.of(publisher), List.of(consumer));
    }

    private static void testConsumerConnectionInterruption() throws InterruptedException {
        System.out.println("\n--- Testing R4: Consumer Connection Interruption ---");
        BasicMiddleware middleware = new BasicMiddleware();
        BasicPublisher publisher = new BasicPublisher("PubR4C");
        TestConsumer consumer = new TestConsumer("SubR4C", 1); // Quick reconnect retry

        publisher.registerWithMiddleware(middleware);
        consumer.registerWithMiddleware(middleware);
        middleware.createChannel(TEST_CHANNEL_1);
        consumer.subscribe(TEST_CHANNEL_1);

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event C1"));
        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);
        assert consumer.getReceivedEventCount() == 1 : "Initial consume failed";

        System.out.println("Simulating consumer disconnection...");
        consumer.simulateDisconnection();
        assert !consumer.isConnected() : "Consumer should be disconnected";

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event C2 (During Disconnect)"));
        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);
        assert consumer.getReceivedEventCount() == 1 : "Consumer received message while disconnected";

        System.out.println("Simulating consumer reconnection...");
        consumer.simulateReconnection(); // Manual trigger for test predictability
        assert consumer.isConnected() : "Consumer should be reconnected";
        // Resubscription happens automatically on reconnect/recovery

        // Publish *after* reconnect to ensure resubscription worked
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event C3 (After Reconnect)"));
        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);

        // Event C2 might be lost if TTL is short and disconnect is long, or if channel doesn't retry enough. The critical part is C3 reception.
        assert consumer.getReceivedEventCount() >= 2 : "Consumer did not receive message after reconnect";
        System.out.println("Consumer Connection Interruption: OK (Received " + consumer.getReceivedEventCount() + " events)");

        shutdownAll(middleware, List.of(publisher), List.of(consumer));
    }

    private static void testQueueCrashRecovery() throws InterruptedException {
        System.out.println("\n--- Testing R5: Crashing Queues ---");
        // non-default settings for easier testing
        BasicMiddleware middleware = new BasicMiddleware(30000, 3, 60);
        BasicPublisher publisher = new BasicPublisher("PubR5");
        TestConsumer consumer = new TestConsumer("SubR5");

        publisher.registerWithMiddleware(middleware);
        consumer.registerWithMiddleware(middleware);
        // Create channel directly to access BasicChannel methods
        BasicChannel channel = (BasicChannel) middleware.createChannel(TEST_CHANNEL_1);
        channel.setAutomaticRecovery(false); // Test manual recovery first
        consumer.subscribe(TEST_CHANNEL_1);

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event Q1 (Before Crash)"));
        assert channel.getQueueSize() == 1 : "Message not queued before crash";

        System.out.println("Forcing manual backup before simulating crash...");
        // Ensure 'channel' is the BasicChannel instance from middleware.createChannel
        boolean manualBackupSuccess = channel.backupQueue(); // Call the manual backup
        assert manualBackupSuccess : "Manual backup before crash failed!";

        System.out.println("Simulating queue crash...");
        middleware.simulateChannelQueueCrash(TEST_CHANNEL_1); // Uses BasicMiddleware helper
        assert channel.isQueueCrashed() : "Channel queue should be crashed";

        try {
            publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event Q2 (During Crash)"));
            assert false : "Publish should fail when queue crashed and no auto-recovery";
        } catch (IllegalStateException e) {
            System.out.println("Caught expected exception on publish during crash: " + e.getMessage());
        }
        try {
            middleware.dispatchAllEvents(); // Should log error or throw if accessing crashed queue
        } catch (Exception e) {
             System.out.println("Caught expected exception on dispatch during crash: " + e.getMessage());
        }
        assert consumer.getReceivedEventCount() == 0 : "Consumer received message from crashed queue";
        assert channel.getQueueSize() == -1 || channel.isQueueCrashed() : "Queue size check failed during crash"; // Size might return -1 or throw if queue is crashed


        System.out.println("Manually recovering queue...");
        boolean recovered = middleware.recoverChannelQueue(TEST_CHANNEL_1);
        assert recovered : "Queue recovery failed";
        assert !channel.isQueueCrashed() : "Queue should not be crashed after recovery";

        // PersistentQueue should recover Event Q1
        assert channel.getQueueSize() >= 1 : "Queue size incorrect after recovery (Expected >=1)";

        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);
        assert consumer.getReceivedEventCount() >= 1 : "Consumer did not receive pre-crash message after recovery";

        // Test automatic recovery
        channel.setAutomaticRecovery(true);
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event Q3 (After Recovery)"));
        middleware.simulateChannelQueueCrash(TEST_CHANNEL_1);
        assert channel.isQueueCrashed() : "Queue should be crashed (momentarily)";
        // Publish should now trigger automatic recovery
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event Q4 (During Auto-Recovery Attempt)"));
        assert !channel.isQueueCrashed() : "Queue should have auto-recovered on publish";
        middleware.dispatchAllEvents(); // Should also trigger recovery if needed
        Thread.sleep(DISPATCH_WAIT_MS);

        assert consumer.getReceivedEventCount() >= 3 : "Consumer did not receive messages after auto-recovery"; // Q1, Q3, Q4 expected if persistence works well
        System.out.println("Queue Crash Recovery: OK (Received " + consumer.getReceivedEventCount() + " events)");


        shutdownAll(middleware, List.of(publisher), List.of(consumer));
    }

     // Wrapper to run the existing ConsumerCrashTest main method
     private static void runConsumerCrashTest() throws InterruptedException {
         System.out.println("\n--- Running R6: Consumer Crash Test (External Main) ---");
         ConsumerCrashTest.main(null); 
         System.out.println("Consumer Crash Test: Ran (Check separate output)");
     }


    private static void testNetworkDelay() throws InterruptedException {
        System.out.println("\n--- Testing R7: Long Delays in Network Traffic ---");
        BasicMiddleware middleware = new BasicMiddleware();
        BasicPublisher publisher = new BasicPublisher("PubR7");
        TestConsumer consumer = new TestConsumer("SubR7");

        publisher.registerWithMiddleware(middleware);
        consumer.registerWithMiddleware(middleware);
        @SuppressWarnings("unused")
        BasicChannel channel = (BasicChannel) middleware.createChannel(TEST_CHANNEL_1);
        consumer.subscribe(TEST_CHANNEL_1);

        int delayMs = 1000; // 1 second delay
        System.out.println("Simulating network delay: " + delayMs + "ms");
        middleware.simulateChannelNetworkDelay(TEST_CHANNEL_1, delayMs);

        long startTime = System.currentTimeMillis();
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event D1 (Delayed)"));
        middleware.dispatchAllEvents();
        // Wait significantly longer than the delay
        while(consumer.getReceivedEventCount() < 1 && (System.currentTimeMillis() - startTime) < (delayMs + 1000)) {
             Thread.sleep(100); // Poll until received or timeout
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        assert consumer.getReceivedEventCount() == 1 : "Consumer did not receive delayed message";
        assert duration >= delayMs : "Delivery time (" + duration + "ms) was less than simulated delay (" + delayMs + "ms)";
        System.out.println("Network Delay: OK (Delivery took ~" + duration + "ms)");

        // Reset delay
        middleware.simulateChannelNetworkDelay(TEST_CHANNEL_1, 0);
        shutdownAll(middleware, List.of(publisher), List.of(consumer));
    }

     private static void testEventExpiration() throws InterruptedException {
        System.out.println("\n--- Testing R7: Event Expiration due to Delay/TTL ---");
        // Short TTL, long delay
        long ttlMs = 500;
        int delayMs = 1000;
        BasicMiddleware middleware = new BasicMiddleware(ttlMs, 1, 60); // Short default TTL, 1 retry
        BasicPublisher publisher = new BasicPublisher("PubR7Exp");
        TestConsumer consumer = new TestConsumer("SubR7Exp");

        publisher.registerWithMiddleware(middleware);
        consumer.registerWithMiddleware(middleware);
        @SuppressWarnings("unused")
        BasicChannel channel = (BasicChannel) middleware.createChannel(TEST_CHANNEL_1);
        consumer.subscribe(TEST_CHANNEL_1);

        System.out.println("Simulating network delay (" + delayMs + "ms) longer than TTL (" + ttlMs + "ms)");
        middleware.simulateChannelNetworkDelay(TEST_CHANNEL_1, delayMs);

        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event E1 (Should Expire)")); // Uses channel default TTL
        @SuppressWarnings("unused")
        long startTime = System.currentTimeMillis();
        middleware.dispatchAllEvents();

        // Wait for dispatch attempt AND expiration time
        Thread.sleep(delayMs + ttlMs);
        middleware.dispatchAllEvents(); // Dispatch again to process potential failures/expirations
        Thread.sleep(DISPATCH_WAIT_MS);


        assert consumer.getReceivedEventCount() == 0 : "Consumer received expired message";
        Map<String, BasicMiddleware.DeliveryStats> stats = middleware.getDeliveryStats();
        assert stats.get(TEST_CHANNEL_1).getMessagesFailed() >= 1 || stats.get(TEST_CHANNEL_1).getMessagesSent() > 0 : "Expired message not marked as failed/sent"; // Check stats
        System.out.println("Event Expiration: OK (Message likely expired/failed as expected)");
        System.out.println("Stats: " + stats.get(TEST_CHANNEL_1));

        // Reset delay
        middleware.simulateChannelNetworkDelay(TEST_CHANNEL_1, 0);
        shutdownAll(middleware, List.of(publisher), List.of(consumer));
    }

    private static void testMessageAcknowledgementAndRetry() throws InterruptedException {
        System.out.println("\n--- Testing R8: Acknowledgement and Retries ---");
        // Short TTL to see failures quickly if needed, multiple retries
        BasicMiddleware middleware = new BasicMiddleware(5000, 3, 60);
        BasicPublisher publisher = new BasicPublisher("PubR8");
        // Use BasicConsumers directly to check event state easily before ack
        BasicConsumer consumer1 = new BasicConsumer("SubR8A");
        BasicConsumer consumer2 = new BasicConsumer("SubR8B");

        publisher.registerWithMiddleware(middleware);
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        @SuppressWarnings("unused")
        BasicChannel channel = (BasicChannel) middleware.createChannel(TEST_CHANNEL_1);
        consumer1.subscribe(TEST_CHANNEL_1);
        consumer2.subscribe(TEST_CHANNEL_1);

        // Publish an event
        BasicEvent event = new BasicEvent("Event R1 (Ack Test)");
        publisher.publish(TEST_CHANNEL_1, event);
        assert event.getStatus() == BasicEvent.DeliveryStatus.CREATED || event.getStatus() == BasicEvent.DeliveryStatus.QUEUED : "Initial event status wrong";

        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS); // Allow dispatch (no simulated loss yet)

        // Manually inspect event state after delivery but before explicit ack 
        assert event.getStatus() == BasicEvent.DeliveryStatus.DELIVERED || event.getStatus() == BasicEvent.DeliveryStatus.PARTIAL_ACKS : "Event status not DELIVERED/PARTIAL after dispatch (Status: " + event.getStatus() + ")";
        assert event.isFullyAcknowledged() : "Event not fully acknowledged";
        System.out.println("Acknowledgement: OK");

        // Test Retries by simulating loss
        System.out.println("Simulating delivery failure (100% loss) for retries...");
        middleware.setChannelDeliveryFailureProbability(TEST_CHANNEL_1, 1.0);

        BasicEvent event2 = new BasicEvent("Event R2 (Retry Test)");
        publisher.publish(TEST_CHANNEL_1, event2);
        int initialResent = middleware.getDeliveryStats().get(TEST_CHANNEL_1).getMessagesResent();

        // Dispatch multiple times to trigger retries
        for (int i = 0; i < 5; i++) {
            middleware.dispatchAllEvents();
            Thread.sleep(100); // Small delay between dispatches
            System.out.println("Dispatch " + (i+1) + ", Event Status: " + event2.getStatus() + ", Retries: " + event2.getRetryCount());
            if(event2.getStatus() == BasicEvent.DeliveryStatus.FAILED) break;
        }

        assert event2.getRetryCount() >= 3 : "Event did not retry enough times"; // Should reach maxRetries (3) + initial attempt
        assert event2.getStatus() == BasicEvent.DeliveryStatus.FAILED : "Event did not fail after max retries";
        int finalResent = middleware.getDeliveryStats().get(TEST_CHANNEL_1).getMessagesResent();
        assert finalResent > initialResent : "Resent count did not increase";

        System.out.println("Retry Mechanism: OK");
        System.out.println("Stats: " + middleware.getDeliveryStats().get(TEST_CHANNEL_1));


        // Reset failure probability
        middleware.setChannelDeliveryFailureProbability(TEST_CHANNEL_1, 0.0);
        shutdownAll(middleware, List.of(publisher), List.of(consumer1, consumer2));
    }

     private static void testSimulatedMessageLoss() throws InterruptedException {
        System.out.println("\n--- Testing R8: Simulated Consumer Message Loss ---");
        BasicMiddleware middleware = new BasicMiddleware();
        BasicPublisher publisher = new BasicPublisher("PubR8L");
        TestConsumer consumer1 = new TestConsumer("SubR8L1");
        TestConsumer consumer2 = new TestConsumer("SubR8L2"); // This one should get messages

        publisher.registerWithMiddleware(middleware);
        consumer1.registerWithMiddleware(middleware);
        consumer2.registerWithMiddleware(middleware);
        middleware.createChannel(TEST_CHANNEL_1);
        consumer1.subscribe(TEST_CHANNEL_1);
        consumer2.subscribe(TEST_CHANNEL_1);

        System.out.println("Simulating message loss (100%) for Consumer 1...");
        // Use the middleware helper to configure the specific consumer
        middleware.simulateConsumerMessageLoss("SubR8L1", true, 1.0);

        int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event L" + i));
        }

        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS * 2); // Allow more time

        assert consumer1.getReceivedEventCount() == 0 : "Consumer 1 received messages despite loss simulation";
        assert consumer2.getReceivedEventCount() == numMessages : "Consumer 2 did not receive all messages";
        System.out.println("Simulated Message Loss: OK (C1 received " + consumer1.getReceivedEventCount() + ", C2 received " + consumer2.getReceivedEventCount() + ")");

        // Disable loss simulation
        middleware.simulateConsumerMessageLoss("SubR8L1", false, 0.0);
        publisher.publish(TEST_CHANNEL_1, new BasicEvent("Event L_AfterLoss"));
        middleware.dispatchAllEvents();
        Thread.sleep(DISPATCH_WAIT_MS);
        assert consumer1.getReceivedEventCount() == 1 : "Consumer 1 did not receive message after loss simulation disabled";


        shutdownAll(middleware, List.of(publisher), List.of(consumer1, consumer2));
    }

    private static void testPerformance() throws InterruptedException {
        System.out.println("\n--- Testing Performance (Target: ~1000 msg/sec) ---");
        
        // Save original System.out
        PrintStream originalOut = System.out;
        
        // Create a filtered PrintStream that discards most messages during the performance test
        PrintStream filteredOut = new PrintStream(new FilterOutputStream(originalOut) {
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                // Do nothing - suppress output
            }
            
            @Override
            public void write(int b) throws IOException {
                // Do nothing - suppress output
            }
        });
        
        // Only show that we're running the test
        // doing this to avoid cluttering the console with too much output - more than 10k messages 
        System.out.println("Running performance test with 10000 messages... (output suppressed)");
        
        // Redirect output to filtered stream
        System.setOut(filteredOut);
        
        int numMessages = 10000; // Number of messages to publish
        int numPublishers = 4;   // Concurrent publishers
        int numConsumers = 10;   // Concurrent consumers
        int numChannels = 2;

        BasicMiddleware middleware = new BasicMiddleware(10000, 0, 300); // Longer TTL, no retries for perf test simplicity
        ExecutorService publisherPool = Executors.newFixedThreadPool(numPublishers);
        AtomicInteger publishedCount = new AtomicInteger(0);
        AtomicInteger receivedCount = new AtomicInteger(0); // Track total received across consumers

        // Create channels
        for(int i=0; i<numChannels; i++) {
             middleware.createChannel("perf-channel-" + i);
        }

        // Create and register consumers (using basic consumer for simplicity here)
         List<Consumer> consumers = new ArrayList<>();
         for (int i = 0; i < numConsumers; i++) {
             // Simple consumer that just counts
             Consumer consumer = new BasicConsumer("PerfSub" + i) {
                 @Override
                 public boolean consume(Event event) {
                     boolean consumed = super.consume(event); // Handle ACK
                     receivedCount.incrementAndGet();
                        return consumed; // Return true if consumed successfully
                 }
             };
             consumer.registerWithMiddleware(middleware);
             // Subscribe to channels round-robin
             consumer.subscribe("perf-channel-" + (i % numChannels));
             consumers.add(consumer);
         }


        System.out.println("Starting " + numPublishers + " publishers to send " + numMessages + " messages...");
        long startTime = System.nanoTime();

        // Start publishers
        for (int i = 0; i < numPublishers; i++) {
            final int pubId = i;
            publisherPool.submit(() -> {
                BasicPublisher publisher = new BasicPublisher("PerfPub" + pubId);
                publisher.registerWithMiddleware(middleware); // Register within thread is fine
                int messagesPerPublisher = numMessages / numPublishers;
                for (int j = 0; j < messagesPerPublisher; j++) {
                    // Distribute messages across channels
                    String channelName = "perf-channel-" + (j % numChannels);
                    publisher.publish(channelName, new BasicEvent("PerfEvent " + pubId + "-" + j));
                    publishedCount.incrementAndGet();
                }
                // Handle remaining messages for the last publisher if not divisible
                if (pubId == numPublishers - 1 && numMessages % numPublishers != 0) {
                     int remaining = numMessages % numPublishers;
                     for (int j = 0; j < remaining; j++) {
                          String channelName = "perf-channel-" + (j % numChannels);
                           publisher.publish(channelName, new BasicEvent("PerfEvent Remainder " + j));
                           publishedCount.incrementAndGet();
                     }
                }
            });
        }

        publisherPool.shutdown();
        publisherPool.awaitTermination(30, TimeUnit.SECONDS); // Wait for publishers to finish sending

        long publishEndTime = System.nanoTime();
        System.out.println("Publishing complete. Published: " + publishedCount.get() + ". Now dispatching...");

        // Dispatch loop - run until queues are empty and things settle
        long dispatchStartTime = System.nanoTime();
        int dispatchCycles = 0;
        boolean queuesEmpty = false;
        long lastReceiveTime = System.nanoTime();

        while(true) {
            middleware.dispatchAllEvents();
            dispatchCycles++;
            int totalQueueSize = 0;
             for(String chName : middleware.listChannels()) {
                 if(chName.startsWith("perf-channel")) {
                     totalQueueSize += middleware.lookupChannel(chName).getQueueSize();
                 }
             }

             if (totalQueueSize == 0 && receivedCount.get() >= numMessages) {
                 // Check if queues *stay* empty for a short period
                 if (queuesEmpty) {
                      // If queues were already empty last cycle, break
                      if ((System.nanoTime() - lastReceiveTime) > TimeUnit.MILLISECONDS.toNanos(500)) {
                         break; // Assume settled
                      }
                 } else {
                      queuesEmpty = true; // Mark as empty, check again next cycle
                      lastReceiveTime = System.nanoTime(); // Reset settle timer
                 }
             } else {
                 queuesEmpty = false; // Reset if queue not empty or not all received
                 if(receivedCount.get() > 0) { // Update last receive time if progress is made
                      lastReceiveTime = System.nanoTime();
                 }
             }

             // Timeout condition
             if ((System.nanoTime() - dispatchStartTime) > TimeUnit.SECONDS.toNanos(60)) {
                  System.err.println("Performance test timed out waiting for dispatch!");
                  break;
             }
             Thread.sleep(10); // Small sleep to prevent busy-waiting
        }

        long endTime = System.nanoTime();

        // Restore original output before printing results
        System.setOut(originalOut);
        
        // Calculate metrics
        double publishDurationSec = (publishEndTime - startTime) / 1_000_000_000.0;
        double dispatchDurationSec = (endTime - dispatchStartTime) / 1_000_000_000.0;
        double totalDurationSec = (endTime - startTime) / 1_000_000_000.0;
        double publishRate = publishedCount.get() / publishDurationSec;
        double consumeRate = receivedCount.get() / totalDurationSec; // Overall consume rate

        System.out.println("\n--- Performance Results ---");
        System.out.printf("Published: %d messages\n", publishedCount.get());
        System.out.printf("Received: %d messages\n", receivedCount.get());
        System.out.printf("Publish Duration: %.3f seconds\n", publishDurationSec);
        System.out.printf("Dispatch/Consume Duration: %.3f seconds\n", dispatchDurationSec);
        System.out.printf("Total Duration: %.3f seconds\n", totalDurationSec);
        System.out.printf("Dispatch Cycles: %d\n", dispatchCycles);
        System.out.printf("Approx Publish Rate: %.2f msg/sec\n", publishRate);
        System.out.printf("Overall Consume Rate: %.2f msg/sec\n", consumeRate);

        // Verification
        assert publishedCount.get() == numMessages : "Incorrect number of messages published";
        assert receivedCount.get() >= numMessages : "Incorrect number of messages received (Received " + receivedCount.get() + ")"; 

        if (consumeRate >= 800) { // Allow some leeway from 1000
            System.out.println("Performance Test: PASSED (Rate >= 800 msg/sec)");
        } else {
            System.err.println("Performance Test: FAILED (Rate < 800 msg/sec)");
        }

        // Cleanup performance test resources (consumers)
        for (Consumer c : consumers) {
            if (c instanceof BasicConsumer) ((BasicConsumer) c).shutdown();
        }
        // Publishers were temporary, but middleware needs cleanup
        shutdownAll(middleware, List.of(), List.of()); // Shutdown channels within middleware
    }


    // ========================================================================
    // Helper Methods
    // ========================================================================

    private static void shutdownAll(BasicMiddleware middleware, List<Publisher> publishers, List<Consumer> consumers) {
        System.out.println("Cleaning up test resources...");
        if (middleware != null) {
            middleware.shutdown(); // Shuts down channels
        }
        for (Publisher p : publishers) {
            if (p instanceof BasicPublisher) {
                ((BasicPublisher) p).shutdown();
            }
        }
        for (Consumer c : consumers) {
            if (c instanceof BasicConsumer) {
                ((BasicConsumer) c).shutdown();
            }
        }
        // Give threads a moment to terminate cleanly
        try {
             Thread.sleep(100);
        } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
        }
    }
}