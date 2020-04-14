package de.noack.client.pulsar;

import de.noack.client.CsdbClient;
import de.noack.model.CSDB;
import de.noack.model.CSDBSchema;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.io.*;
import java.util.*;

import static de.noack.client.CsdbClient.createTransformedCsdb;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.client.api.CompressionType.LZ4;

/**
 * This class represents a implementation of {@link CsdbClient} with usage of an Apache Pulsar commit log. It represents a {@link Consumer} as well
 * as a {@link Producer} for records. It reads and produces from and to the topic "csdb-vanilla" which contains all non-manipulated reported
 * data. From this topic it also consumes and transforms messages to produce records for the topic "csdb-transformed". {@link Consumer}s and
 * {@link Producer}s are running as long as the application is running to maintain one connection each. It uses {@link Reader} to query produced
 * records and deliver them via the resource {@link de.noack.resources.CsdbResource}.
 *
 * @author davidnoack
 */
@ApplicationScoped
public class CsdbPulsarClient implements CsdbClient {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private PulsarClient client;
    private Producer<byte[]> vanillaProducer;
    private Consumer<byte[]> vanillaConsumer;
    private Producer<CSDB> transformedProducer;
    private boolean isApplicationRunning;

    void onStart(@Observes StartupEvent ev) {
        isApplicationRunning = true;
        try {
            client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();
            LOGGER.info("Created client for service URL {}", SERVICE_URL);
            vanillaProducer = client.newProducer()
                    .topic(VANILLA_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", VANILLA_TOPIC_NAME);
            vanillaConsumer = client.newConsumer()
                    .topic(VANILLA_TOPIC_NAME)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(VANILLA_SUBSCRIPTION_NAME)
                    .subscribe();
            LOGGER.info("Created consumer for the topic {}", VANILLA_TOPIC_NAME);
            transformedProducer = client.newProducer(JSONSchema.of(CSDB.class))
                    .topic(TRANSFORMED_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", TRANSFORMED_TOPIC_NAME);
            produceTransformedCsdb();
        } catch (IOException e) {
            LOGGER.error("Error occurred during startup! Reason: {}", e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        isApplicationRunning = false;
        try {
            vanillaProducer.close();
            vanillaConsumer.close();
            transformedProducer.close();
        } catch (IOException e) {
            LOGGER.error("Error occurred during close! Reason: {}", e.getMessage());
        }
    }

    @Override
    public String produceVanillaCsdb(final byte[] Csdb) throws PulsarClientException {
        // Send each message and log message content and ID when successfully received
        final String messageKey = String.valueOf(randomUUID());
        final MessageId msgId = vanillaProducer.newMessage().key(messageKey).value(Csdb).send();
        LOGGER.info("Published message with the ID {}", msgId);
        return messageKey;
    }

    @Override
    public void allVanillaCsdbs(final OutputStream outputStream) {
        try (final Reader<byte[]> reader = client.newReader().topic(VANILLA_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", VANILLA_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                Message<byte[]> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    outputStream.write(message.getValue());
                } else return;
            }
        } catch (IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", VANILLA_TOPIC_NAME, e.getMessage());
        }
    }

    @Override
    public InputStream findVanillaCsdb(final String messageKey) {
        try (final Reader<byte[]> reader = client.newReader().topic(VANILLA_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", VANILLA_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                Message<byte[]> message = reader.readNext(1, SECONDS);
                if (messageKey.equals(message.getKey())) return new ByteArrayInputStream(message.getValue());
            }
        } catch (IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", VANILLA_TOPIC_NAME, e.getMessage());
        }
        throw new RuntimeException("Message with id " + messageKey + " not found!");
    }

    @Override
    public void produceTransformedCsdb() throws IOException {
        while (isApplicationRunning) {
            // Wait until a message is available
            Message<byte[]> msg = vanillaConsumer.receive();
            LOGGER.info("Received message with ID {}", msg.getMessageId());

            // if (csdbIsValid(msg.getValue())) {
            produceTransformedCsdb(new ByteArrayInputStream(msg.getValue()));
            //}
            // Acknowledge processing of the message
            vanillaConsumer.acknowledge(msg);
        }
    }

    @Override
    public void produceTransformedCsdb(final InputStream inputStream) {
        try (final Scanner scanner = new Scanner(new InputStreamReader(inputStream))) {
            final String firstLine = scanner.nextLine();
            final String[] csvAttributes = firstLine.split(CSV_DELIMITER);
            final Map<CSDBSchema, Integer> columnOrder = new LinkedHashMap<>();
            // Find out order of column headers
            for (int i = 0; i < csvAttributes.length; i++) {
                columnOrder.put(CSDBSchema.valueOf(csvAttributes[i]), i);
            }
            // Read the data of the CSDB
            while (scanner.hasNextLine()) {
                try {
                    CSDB csdb = createTransformedCsdb(scanner.nextLine(), columnOrder);
                    MessageId msgId =
                            transformedProducer.newMessage().key(csdb.getCsdbKey().toString()).value(csdb).send();
                    LOGGER.info("Published message with the ID {}", msgId);
                } catch (PulsarClientException e) {
                    LOGGER.info("Error during send. Reason: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public Set<CSDB> allTransformedCsdbs() {
        Set<CSDB> result = new HashSet<>();
        try (final Reader<CSDB> reader = client.newReader(JSONSchema.of(CSDB.class))
                .topic(TRANSFORMED_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", TRANSFORMED_TOPIC_NAME);
            do {
                Message<CSDB> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    result.add(message.getValue());
                } else break;
            } while (!reader.hasReachedEndOfTopic());
        } catch (IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", TRANSFORMED_TOPIC_NAME, e.getMessage());
        }
        return result;
    }

    @Override
    public CSDB findTransformedCsdb(String messageKey) {
        try (final Reader<CSDB> reader = client.newReader(JSONSchema.of(CSDB.class)).topic(TRANSFORMED_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", TRANSFORMED_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                Message<CSDB> message = reader.readNext(1, SECONDS);
                if (messageKey.equals(message.getKey())) return message.getValue();
            }
        } catch (IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", TRANSFORMED_TOPIC_NAME, e.getMessage());
        }
        throw new RuntimeException("Message with id " + messageKey + " not found!");
    }
}