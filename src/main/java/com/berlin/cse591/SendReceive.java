package com.berlin.cse591;

import java.net.InetSocketAddress;
import java.net.Proxy;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.amqp.ProxyAuthenticationType;
import com.azure.core.amqp.ProxyOptions;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.EventPosition;

/**
 * Handles messages from an IoT Hub. Default protocol is to use MQTT transport.
 */
public class SendReceive {
    /**
     * The main method to start the sample application that receives events from
     * Event Hubs sent from an IoT Hub device.
     *
     * @param args ignored args.
     * @throws Exception if there's an error running the application.
     */
    public static void main(String[] args) throws Exception {

        // Build the Event Hubs compatible connection string.
        String eventHubCompatibleConnectionString = "Endpoint=sb://berlinhub.servicebus.windows.net/;" +
        "SharedAccessKeyName=iothubroutes_iothub-axwef;" + 
        "SharedAccessKey=Ph5WCU9NlJxkDrLBUvRPYJ3/AMGAx+8aXoq7mlaeQhE=;" +
        "EntityPath=berlineventhub";

        // Setup the EventHubBuilder by configuring various options as needed.
        EventHubClientBuilder eventHubClientBuilder = new EventHubClientBuilder()
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .connectionString(eventHubCompatibleConnectionString);

        // uncomment to setup proxy
        // setupProxy(eventHubClientBuilder);

        // uncomment to use Web Sockets
        // eventHubClientBuilder.transportType(AmqpTransportType.AMQP_WEB_SOCKETS);

        // Create an async consumer client as configured in the builder.
        try (EventHubConsumerAsyncClient eventHubConsumerAsyncClient = eventHubClientBuilder
                .buildAsyncConsumerClient()) {

            receiveFromAllPartitions(eventHubConsumerAsyncClient);

            // uncomment to run these samples
            // receiveFromSinglePartition(eventHubConsumerAsyncClient);
            // receiveFromSinglePartitionInBatches(eventHubConsumerAsyncClient);

            // Shut down cleanly.
            System.out.println("Press ENTER to exit.");
            System.in.read();
            System.out.println("Shutting down...");
        }
    }

    /**
     * This method receives events from all partitions asynchronously starting from
     * the newly available events in each partition.
     *
     * @param eventHubConsumerAsyncClient The {@link EventHubConsumerAsyncClient}.
     */
    private static void receiveFromAllPartitions(EventHubConsumerAsyncClient eventHubConsumerAsyncClient) {

        eventHubConsumerAsyncClient.receive(false) // set this to false to read only the newly available events
                .subscribe(partitionEvent -> {
                    System.out.println();
                    System.out.printf("%nTelemetry received from partition %s:%n%s",
                            partitionEvent.getPartitionContext().getPartitionId(),
                            partitionEvent.getData().getBodyAsString());
                    System.out.printf("%nApplication properties (set by device):%n%s",
                            partitionEvent.getData().getProperties());
                    System.out.printf("%nSystem properties (set by IoT Hub):%n%s",
                            partitionEvent.getData().getSystemProperties());
                }, ex -> {
                    System.out.println("Error receiving events " + ex);
                }, () -> {
                    System.out.println("Completed receiving events");
                });
    }

    /**
     * This method queries all available partitions in the Event Hub and picks a
     * single partition to receive events asynchronously starting from the newly
     * available event in that partition.
     *
     * @param eventHubConsumerAsyncClient The {@link EventHubConsumerAsyncClient}.
     */
    private static void receiveFromSinglePartition(EventHubConsumerAsyncClient eventHubConsumerAsyncClient) {
        eventHubConsumerAsyncClient.getPartitionIds() // get all available partitions
                .take(1) // pick a single partition
                .flatMap(partitionId -> {
                    System.out.println("Receiving events from partition id " + partitionId);
                    return eventHubConsumerAsyncClient.receiveFromPartition(partitionId, EventPosition.latest());
                }).subscribe(partitionEvent -> {
                    System.out.println();
                    System.out.printf("%nTelemetry received from partition %s:%n%s",
                            partitionEvent.getPartitionContext().getPartitionId(),
                            partitionEvent.getData().getBodyAsString());
                    System.out.printf("%nApplication properties (set by device):%n%s",
                            partitionEvent.getData().getProperties());
                    System.out.printf("%nSystem properties (set by IoT Hub):%n%s",
                            partitionEvent.getData().getSystemProperties());
                }, ex -> {
                    System.out.println("Error receiving events " + ex);
                }, () -> {
                    System.out.println("Completed receiving events");
                });
    }

    /**
     * This method queries all available partitions in the Event Hub and picks a
     * single partition to receive events asynchronously in batches of 100 events,
     * starting from the newly available event in that partition.
     *
     * @param eventHubConsumerAsyncClient The {@link EventHubConsumerAsyncClient}.
     */
    private static void receiveFromSinglePartitionInBatches(EventHubConsumerAsyncClient eventHubConsumerAsyncClient) {
        int batchSize = 100;
        eventHubConsumerAsyncClient.getPartitionIds().take(1).flatMap(partitionId -> {
            System.out.println("Receiving events from partition id " + partitionId);
            return eventHubConsumerAsyncClient.receiveFromPartition(partitionId, EventPosition.latest());
        }).window(batchSize) // batch the events
                .subscribe(partitionEvents -> {
                    partitionEvents.toIterable().forEach(partitionEvent -> {
                        System.out.println();
                        System.out.printf("%nTelemetry received from partition %s:%n%s",
                                partitionEvent.getPartitionContext().getPartitionId(),
                                partitionEvent.getData().getBodyAsString());
                        System.out.printf("%nApplication properties (set by device):%n%s",
                                partitionEvent.getData().getProperties());
                        System.out.printf("%nSystem properties (set by IoT Hub):%n%s",
                                partitionEvent.getData().getSystemProperties());
                    });
                }, ex -> {
                    System.out.println("Error receiving events " + ex);
                }, () -> {
                    System.out.println("Completed receiving events");
                });
    }

    /**
     * This method sets up proxy options and updates the
     * {@link EventHubClientBuilder}.
     *
     * @param eventHubClientBuilder The {@link EventHubClientBuilder}.
     */
    private static void setupProxy(EventHubClientBuilder eventHubClientBuilder) {
        int proxyPort = 8000; // replace with right proxy port
        String proxyHost = "{hostname}";
        Proxy proxyAddress = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        String userName = "{username}";
        String password = "{password}";
        ProxyOptions proxyOptions = new ProxyOptions(ProxyAuthenticationType.BASIC, proxyAddress, userName, password);

        eventHubClientBuilder.proxyOptions(proxyOptions);

        // To use proxy, the transport type has to be Web Sockets.
        eventHubClientBuilder.transportType(AmqpTransportType.AMQP_WEB_SOCKETS);
    }
}
