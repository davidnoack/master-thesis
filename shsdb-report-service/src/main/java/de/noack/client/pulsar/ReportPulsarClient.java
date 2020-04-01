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

@ApplicationScoped
public class ReportPulsarClient implements ReportClient {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private PulsarClient client;
    private Producer<byte[]> vanillaProducer;
    private Consumer<byte[]> vanillaConsumer;
    private Producer<ReportedData> transformedProducer;
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
            transformedProducer = client.newProducer(JSONSchema.of(ReportedData.class))
                    .topic(TRANSFORMED_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", TRANSFORMED_TOPIC_NAME);
            produceTransformedReports();
        } catch (IOException e) {
            LOGGER.error("Error occurred! Reason: {}", e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        isApplicationRunning = false;
        try {
            vanillaProducer.close();
            vanillaConsumer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String produceReport(byte[] report) throws PulsarClientException {
        // Send each message and log message content and ID when successfully received
        String messageKey = String.valueOf(randomUUID());
        MessageId msgId = vanillaProducer.newMessage().key(messageKey).value(report).send();
        LOGGER.info("Published message with the ID {}", msgId);
        return messageKey;
    }

    @Override
    public InputStream findVanillaReport(String messageKey) {
        try (Reader<byte[]> reader = client.newReader().topic(VANILLA_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            while (!reader.hasReachedEndOfTopic()) {
                Message<byte[]> message = reader.readNext(1, SECONDS);
                if (messageKey.equals(message.getKey())) return new ByteArrayInputStream(message.getValue());
            }
        } catch (IOException e) {
            LOGGER.error("Error during read occurred. Reason: {}", e.getMessage());
        }
        throw new RuntimeException("Message with id " + messageKey + " not found!");
    }

    @Override
    public void allVanillaReports(OutputStream outputStream) {
        try (Reader<byte[]> reader = client.newReader().topic(VANILLA_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            while (!reader.hasReachedEndOfTopic()) {
                Message<byte[]> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    outputStream.write(message.getValue());
                } else return;
            }
        } catch (IOException e) {
            LOGGER.error("Error during read occurred. Reason: {}", e.getMessage());
        }
    }

    @Override
    public Set<ReportedData> allTransformedReports() {
        Set<ReportedData> result = new HashSet<>();
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build()) {
            Reader<ReportedData> reader = client.newReader(JSONSchema.of(ReportedData.class))
                    .topic(TRANSFORMED_TOPIC_NAME)
                    .startMessageId(MessageId.earliest)
                    .create();
            LOGGER.info("Created reader for the topic {}", TRANSFORMED_TOPIC_NAME);
            do {
                Message<ReportedData> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    result.add(message.getValue());
                } else break;
            } while (!reader.hasReachedEndOfTopic());
        } catch (PulsarClientException e) {
            LOGGER.error(e.getMessage());
        }
        return result;
    }

    @Override
    public void produceTransformedReports() throws IOException {
        while (isApplicationRunning) {
            // Wait until a message is available
            Message<byte[]> msg = vanillaConsumer.receive();
            LOGGER.info("Received message with ID {}", msg.getMessageId());

            if (reportIsValid(msg.getValue())) {
                try (BufferedReader bufferedReader =
                             new BufferedReader(new InputStreamReader(new ByteArrayInputStream(msg.getValue())))) {
                    final String firstLine = bufferedReader.readLine();
                    final String[] csvAttributes = firstLine.split(CSV_DELIMITER);
                    final Map<ReportingSchema, Integer> columnOrder = new LinkedHashMap<>();
                    for (int i = 0; i < csvAttributes.length; i++) {
                        columnOrder.put(ReportingSchema.valueOf(csvAttributes[i]), i);
                    }
                    // Read the rest of the report
                    bufferedReader.lines().forEach(line -> {
                        try {
                            ReportedData reportedData = createTransformedReport(line, columnOrder);
                            MessageId msgId =
                                    transformedProducer.newMessage().key(reportedData.getReportedDataKey().toString()).value(reportedData).send();
                            LOGGER.info("Published message with the ID {}", msgId);
                        } catch (PulsarClientException e) {
                            LOGGER.info("Error during send. Reason: {}", e.getMessage());
                        }
                    });
                }
            }
            // Acknowledge processing of the message so that it can be deleted
            vanillaConsumer.acknowledge(msg);
        }
    }
}