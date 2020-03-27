package de.noack.client.pulsar;

import de.noack.service.MicrodataService;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.util.UUID.randomUUID;
import static org.apache.pulsar.client.api.CompressionType.LZ4;

@ApplicationScoped
public class MicrodataPulsarClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MicrodataPulsarClient.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String INPUT_TOPIC_NAME = "report-vanilla";
    private static final String INPUT_SUBSCRIPTION_NAME = "report-vanilla-subscription";
    private static final String OUTPUT_TOPIC_NAME = "report-microdata";
    @Inject
    MicrodataService microdataService;
    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private boolean isApplicationRunning;

    void onStart(@Observes StartupEvent ev) {
        isApplicationRunning = true;
        try {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();
            LOGGER.info("Created client for service URL {}", SERVICE_URL);
            consumer = client.newConsumer()
                    .topic(INPUT_TOPIC_NAME)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(INPUT_SUBSCRIPTION_NAME)
                    .subscribe();
            LOGGER.info("Created consumer for the topic {}", INPUT_TOPIC_NAME);
            producer = client.newProducer()
                    .topic(OUTPUT_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", OUTPUT_TOPIC_NAME);
            consumeReports();
        } catch (IOException e) {
            LOGGER.error("Error occurred! Reason: {}", e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        isApplicationRunning = false;
        try {
            consumer.close();
            producer.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public void consumeReports() throws IOException {
        do {
            // Wait until a message is available
            Message<byte[]> msg = consumer.receive();
            LOGGER.info("Received message with ID {}", msg.getMessageId());

            // Extract the message as a printable string and then log
            microdataService.setCurrentReport(msg.getValue());
            if (microdataService.reportIsValid()) {
                try (BufferedReader bufferedReader =
                             new BufferedReader(new InputStreamReader(new ByteArrayInputStream(microdataService.getCurrentReport())))) {
                    // Remove header...
                    bufferedReader.readLine();
                    // Read the rest of the report
                    bufferedReader.lines().forEach(this::produceMicrodata);
                }
            }
            // Acknowledge processing of the message so that it can be deleted
            consumer.acknowledge(msg);
        } while (isApplicationRunning);
    }

    public void produceMicrodata(String microdata) {
        try {
            String messageKey = String.valueOf(randomUUID());
            MessageId msgId = producer.newMessage().key(messageKey).value(microdata.getBytes()).send();
            LOGGER.info("Published message with the ID {}", msgId);
        } catch (PulsarClientException e) {
            LOGGER.error("Error occured during publishing message. Reason: {}", e.getMessage());
        }
    }
}