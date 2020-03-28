package de.noack.client.pulsar;

import de.noack.client.CsdbClient;
import de.noack.model.CSDB;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.client.api.CompressionType.LZ4;

public class CsdbPulsarClient implements CsdbClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(CsdbPulsarClient.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String INPUT_SUBSCRIPTION_NAME = "csdb-vanilla-subscription";
    private Consumer<CSDB> consumer;
    private Producer<CSDB> producer;

    public CsdbPulsarClient() {
        try {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();
            LOGGER.info("Created client for service URL {}", SERVICE_URL);
            consumer = client.newConsumer(JSONSchema.of(CSDB.class))
                    .topic(INPUT_TOPIC_NAME)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(INPUT_SUBSCRIPTION_NAME)
                    .subscribe();
            LOGGER.info("Created consumer for the topic {}", INPUT_TOPIC_NAME);
            producer = client.newProducer(JSONSchema.of(CSDB.class))
                    .topic(OUTPUT_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", OUTPUT_TOPIC_NAME);
        } catch (IOException e) {
            LOGGER.error("Error occurred! Reason: {}", e.getMessage());
        }
    }

    @Override
    public String produceCsdb(CSDB csdb) throws IOException {
        try {
            MessageId msgId = producer.newMessage().key(csdb.getCsdbKey().toString()).value(csdb).send();
            LOGGER.info("Published message with the ID {}", msgId);
        } catch (PulsarClientException e) {
            LOGGER.error("Error occured during publishing message. Reason: {}", e.getMessage());
            throw new IOException(e);
        }
        return csdb.getCsdbKey().getPeriod() + "+" + csdb.getCsdbKey().getVersion();
    }

    @Override
    public InputStream readCsdb(String messageKey) throws IOException {
        return null;
    }

    @Override
    public Set<CSDB> consumeCsdbs() {
        Set<CSDB> result = new HashSet<>();
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build()) {
            Reader<CSDB> reader = client.newReader(JSONSchema.of(CSDB.class))
                    .topic(OUTPUT_TOPIC_NAME)
                    .startMessageId(MessageId.earliest)
                    .create();
            LOGGER.info("Created reader for the topic {}", INPUT_TOPIC_NAME);
            do {
                Message<CSDB> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    result.add(message.getValue());
                } else break;
            } while (!reader.hasReachedEndOfTopic());
        } catch (PulsarClientException e) {
            LOGGER.error(e.getMessage());
        }
        return result;
    }
}