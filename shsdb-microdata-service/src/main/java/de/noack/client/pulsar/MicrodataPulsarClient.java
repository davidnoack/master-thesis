package de.noack.client.pulsar;

import de.noack.client.MicrodataClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.pulsar.client.api.*;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.io.IOException;

import static org.apache.pulsar.client.api.CompressionType.LZ4;

@ApplicationScoped
public class MicrodataPulsarClient implements MicrodataClient {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
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
        } catch (IOException e) {
            LOGGER.error("Error during startup occurred! Reason: {}", e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        isApplicationRunning = false;
        try {
            consumer.close();
            producer.close();
        } catch (PulsarClientException e) {
            LOGGER.error("Error during shutdown occurred. Reason {}", e.getMessage());
        }
    }
}