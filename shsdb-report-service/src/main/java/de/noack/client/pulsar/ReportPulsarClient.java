package de.noack.client.pulsar;

import de.noack.client.ReportClient;
import de.noack.model.ReportedData;
import de.noack.model.ReportingSchema;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.io.*;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static de.noack.client.ReportClient.createTransformedReport;
import static de.noack.client.ReportClient.reportIsValid;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.client.api.CompressionType.LZ4;

/**
 * This class represents a implementation of {@link ReportClient} with usage of an Apache Pulsar commit log. It represents a {@link Consumer} as well
 * as a {@link Producer} for records. It reads and produces from and to the topic "reports-vanilla" which contains all non-manipulated reported
 * data. From this topic it also consumes and transforms messages to produce records for the topic "reports-transformed". {@link Consumer}s and
 * {@link Producer}s are running as long as the application is running to maintain one connection each. It uses {@link Reader} to query produced
 * records and deliver them via the resource {@link de.noack.resources.ReportResource}.
 *
 * @author davidnoack
 */
@ApplicationScoped
public class ReportPulsarClient implements ReportClient {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private PulsarClient client;
    private Producer<byte[]> vanillaProducer;
    private Consumer<byte[]> vanillaConsumer;
    private Producer<ReportedData> transformedProducer;
    private boolean isApplicationRunning;

    void onStart(@Observes final StartupEvent ev) {
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
            transformedProducer = client.newProducer(JSONSchema.of(ReportedData.class))
                    .topic(TRANSFORMED_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", TRANSFORMED_TOPIC_NAME);
            new Thread(this::produceTransformedReport).start();
        } catch (final IOException e) {
            LOGGER.error("Error occurred during startup! Reason: {}", e.getMessage());
        }
    }

    void onStop(@Observes final ShutdownEvent ev) {
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
    public String produceVanillaReport(final byte[] report) throws PulsarClientException {
        // Send each message and log message content and ID when successfully received
        final String messageKey = String.valueOf(randomUUID());
        final MessageId msgId = vanillaProducer.newMessage()
                .key(messageKey)
                .value(report)
                .send();
        LOGGER.info("Published message with the ID {}", msgId);
        return messageKey;
    }

    @Override
    public void allVanillaReports(final OutputStream outputStream) {
        try (final Reader<byte[]> reader = client.newReader().topic(VANILLA_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", VANILLA_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                final Message<byte[]> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    outputStream.write(message.getValue());
                } else return;
            }
        } catch (final IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}",
                    VANILLA_TOPIC_NAME,
                    e.getMessage());
        }
    }

    @Override
    public InputStream findVanillaReport(final String messageKey) {
        try (final Reader<byte[]> reader = client.newReader().topic(VANILLA_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", VANILLA_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                final Message<byte[]> message = reader.readNext(1, SECONDS);
                if (messageKey.equals(message.getKey())) return new ByteArrayInputStream(message.getValue());
            }
        } catch (final IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", VANILLA_TOPIC_NAME, e.getMessage());
        }
        throw new RuntimeException("Message with id " + messageKey + " not found!");
    }

    @Override
    public void produceTransformedReport() {
        while (isApplicationRunning) {
            try {
                // Wait until a message is available
                final Message<byte[]> msg = vanillaConsumer.receive();
                LOGGER.info("Received message with ID {}", msg.getMessageId());

                if (reportIsValid(msg.getValue())) {
                    try (final BufferedReader bufferedReader =
                                 new BufferedReader(new InputStreamReader(new ByteArrayInputStream(msg.getValue())))) {
                        final String firstLine = bufferedReader.readLine();
                        final String[] csvAttributes = firstLine.split(CSV_DELIMITER);
                        final Map<ReportingSchema, Integer> columnOrder = new LinkedHashMap<>();
                        // Find out order of column headers
                        for (int i = 0; i < csvAttributes.length; i++) {
                            columnOrder.put(ReportingSchema.valueOf(csvAttributes[i]), i);
                        }
                        // Read the data of the report
                        bufferedReader.lines().forEach(line -> {
                            try {
                                final ReportedData reportedData = createTransformedReport(line, columnOrder);
                                final MessageId msgId =
                                        transformedProducer.newMessage()
                                                .key(reportedData.getReportedDataKey().toString())
                                                .value(reportedData).send();
                                LOGGER.info("Published message with the ID {}", msgId);
                            } catch (PulsarClientException e) {
                                LOGGER.info("Error during send. Reason: {}", e.getMessage());
                            }
                        });
                    }
                }
                // Acknowledge processing of the message
                vanillaConsumer.acknowledge(msg);
            } catch (final Exception e) {
                e.printStackTrace();
                LOGGER.error("Error during data transformation occurred. Reason: {}", e.getMessage());
            }
        }
    }

    @Override
    public Set<ReportedData> allTransformedReports() {
        final Set<ReportedData> result = new HashSet<>();
        try (final Reader<ReportedData> reader = client.newReader(JSONSchema.of(ReportedData.class))
                .topic(TRANSFORMED_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", TRANSFORMED_TOPIC_NAME);
            do {
                final Message<ReportedData> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    result.add(message.getValue());
                } else break;
            } while (!reader.hasReachedEndOfTopic());
        } catch (final IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", TRANSFORMED_TOPIC_NAME, e.getMessage());
        }
        return result;
    }

    @Override
    public ReportedData findTransformedReport(final String messageKey) {
        try (final Reader<ReportedData> reader = client.newReader(JSONSchema.of(ReportedData.class)).topic(TRANSFORMED_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", TRANSFORMED_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                final Message<ReportedData> message = reader.readNext(1, SECONDS);
                if (messageKey.equals(message.getKey())) return message.getValue();
            }
        } catch (final IOException e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", TRANSFORMED_TOPIC_NAME, e.getMessage());
        }
        throw new RuntimeException("Message with id " + messageKey + " not found!");
    }
}