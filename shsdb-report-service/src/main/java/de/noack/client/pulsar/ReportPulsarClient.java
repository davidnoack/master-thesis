package de.noack.client.pulsar;

import de.noack.client.ReportClient;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.client.api.CompressionType.LZ4;

@RequestScoped
public class ReportPulsarClient implements ReportClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportPulsarClient.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";

    @Override
    public String produceReport(byte[] report) throws PulsarClientException {
        // Create a Pulsar client instance. A single instance can be shared across many
        // producers and consumer within the same application
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build()) {

            // Here you get the chance to configure producer specific settings
            Producer<byte[]> producer = client.newProducer()
                    // Set the topic
                    .topic(TOPIC_NAME)
                    // Enable compression
                    .compressionType(LZ4)
                    .create();

            // Once the producer is created, it can be used for the entire application life-cycle
            LOGGER.info("Created producer for the topic {}", TOPIC_NAME);

            // Send each message and log message content and ID when successfully received
            String messageKey = String.valueOf(randomUUID());
            MessageId msgId = producer.newMessage().key(messageKey).value(report).send();
            LOGGER.info("Published message with the ID {}", msgId);
            return messageKey;
        }
    }

    @Override
    public InputStream readReport(String messageKey) throws PulsarClientException {
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build()) {
            Reader<byte[]> reader = client.newReader()
                    .topic(TOPIC_NAME)
                    .startMessageId(MessageId.earliest)
                    .create();
            LOGGER.info("Created reader for the topic {}", TOPIC_NAME);
            do {
                Message<byte[]> message = reader.readNext(1, SECONDS);
                if (messageKey.equals(message.getKey())) return new ByteArrayInputStream(message.getValue());
            } while (!reader.hasReachedEndOfTopic());
            throw new RuntimeException("Message with id " + messageKey + " not found!");
        } catch (PulsarClientException e) {
            LOGGER.error(e.getMessage());
            throw e;
        }
    }

    @Override
    public void consumeReports(OutputStream outputStream) throws IOException {
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build()) {
            Reader<byte[]> reader = client.newReader()
                    .topic(TOPIC_NAME)
                    .startMessageId(MessageId.earliest)
                    .create();
            LOGGER.info("Created reader for the topic {}", TOPIC_NAME);

            do {
                Message<byte[]> message = reader.readNext(1, SECONDS);
                if (message != null) {
                    outputStream.write(message.getValue());
                } else return;
            } while (!reader.hasReachedEndOfTopic());
        } catch (PulsarClientException e) {
            LOGGER.error(e.getMessage());
            throw e;
        } catch (IOException e) {
            LOGGER.error("Error writing to output stream" + e.getMessage());
            throw e;
        }
    }
}