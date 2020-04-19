package de.noack.client.pulsar;

import de.noack.client.DashboardClient;
import de.noack.model.CSDB;
import de.noack.model.MicroData;
import de.noack.model.ReportedData;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.schema.JSONSchema;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static de.noack.client.DashboardClient.createMicroData;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.client.api.CompressionType.LZ4;

/**
 * This class represents a implementation of {@link DashboardClient} with usage of an Apache Pulsar commit log. It represents a {@link Consumer} as
 * well as a {@link Producer} for records. It consumes enriched reported and CSDB data from the respective topics and links them. It reads and
 * produces from and to the topic "microdata-dashboard" which contains all reported data including CSDB data. {@link Consumer}s and
 * {@link Producer}s are running as long as the application is running to maintain one connection each.
 *
 * @author davidnoack
 */
@ApplicationScoped
public class DashboardPulsarClient implements DashboardClient {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private PulsarClient client;
    private Producer<MicroData> dashboardProducer;
    private Consumer<CSDB> csdbConsumer;
    private Consumer<ReportedData> reportConsumer;
    private boolean isApplicationRunning;

    void onStart(@Observes StartupEvent ev) {
        isApplicationRunning = true;
        try {
            client = PulsarClient.builder()
                    .serviceUrl(SERVICE_URL)
                    .build();
            LOGGER.info("Created client for service URL {}", SERVICE_URL);
            dashboardProducer = client.newProducer(JSONSchema.of(MicroData.class))
                    .topic(DASHBOARD_TOPIC_NAME)
                    .compressionType(LZ4)
                    .create();
            LOGGER.info("Created producer for the topic {}", DASHBOARD_TOPIC_NAME);
            csdbConsumer = client.newConsumer(JSONSchema.of(CSDB.class))
                    .topic(CSDB_TOPIC_NAME)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(CSDB_SUBSCRIPTION_NAME)
                    .subscribe();
            LOGGER.info("Created consumer for the topic {}", CSDB_TOPIC_NAME);
            reportConsumer = client.newConsumer(JSONSchema.of(ReportedData.class))
                    .topic(REPORTS_TOPIC_NAME)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(REPORTS_SUBSCRIPTION_NAME)
                    .subscribe();
            LOGGER.info("Created consumer for the topic {}", REPORTS_TOPIC_NAME);
            new Thread(this::consumeCsdb).start();
            new Thread(this::consumeReports).start();
            new Thread(this::produceMicroData).start();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error occurred during startup! Reason: {}", e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        isApplicationRunning = false;
        try {
            dashboardProducer.close();
            csdbConsumer.close();
            reportConsumer.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error occurred during close! Reason: {}", e.getMessage());
        }
    }

    @Override
    public void consumeCsdb() {
        try {
            while (isApplicationRunning) {
                // Wait until a message is available
                final Message<CSDB> msg = csdbConsumer.receive();
                LOGGER.info("Received message with ID {}", msg.getMessageId());
                CONSUMED_CSDBS.put(msg.getValue().getCsdbKey().toString(), msg.getValue());
                // Acknowledge processing of the message
                csdbConsumer.acknowledge(msg);
            }
        } catch (PulsarClientException e) {
            LOGGER.error("Error while consuming csdb data. Reason {}", e.getMessage());
        }
    }

    @Override
    public void consumeReports() {
        try {
            while (isApplicationRunning) {
                // Wait until a message is available
                final Message<ReportedData> msg = reportConsumer.receive();
                LOGGER.info("Received message with ID {}", msg.getMessageId());
                CONSUMED_REPORTS.add(msg.getValue());
                // Acknowledge processing of the message
                reportConsumer.acknowledge(msg);
            }
        } catch (PulsarClientException e) {
            LOGGER.error("Error while consuming csdb data. Reason {}", e.getMessage());
        }
    }

    @Override
    public void produceMicroData() {
        while (isApplicationRunning)
            try {
                if (!CONSUMED_REPORTS.isEmpty()) {
                    final Set<ReportedData> processedRecords = new HashSet<>();
                    for (final ReportedData report : CONSUMED_REPORTS) {
                        final String isin = report.getReportedDataKey().getIsin();
                        final Integer period = report.getReportedDataKey().getPeriod();
                        final String csdbKeyVersion0 = isin + "+" + (period + 1) + "+" + "0";
                        final String csdbKeyVersion1 = isin + "+" + (period + 1) + "+" + "1";
                        if (CONSUMED_CSDBS.containsKey(csdbKeyVersion1) || CONSUMED_CSDBS.containsKey(csdbKeyVersion0)) {
                            final MicroData microData =
                                    createMicroData(CONSUMED_CSDBS.containsKey(csdbKeyVersion1) ? CONSUMED_CSDBS.get(csdbKeyVersion1) :
                                            CONSUMED_CSDBS.get(csdbKeyVersion0), report);
                            final MessageId msgId =
                                    dashboardProducer.newMessage().key(report.getReportedDataKey().toString()).value(microData).send();
                            LOGGER.info("Produced message with ID {}", msgId);
                            processedRecords.add(report);
                        }
                    }
                    if (!processedRecords.isEmpty()) CONSUMED_REPORTS.removeAll(processedRecords);
                }
                sleep(1000);
            } catch (Exception e) {
                LOGGER.error("Error while producing microdata. Reason {}", e.getMessage());
            }
    }

    @Override
    public Set<MicroData> readAllMicroData() {
        final Set<MicroData> microData = new HashSet<>();
        try (final Reader<MicroData> reader = client.newReader(JSONSchema.of(MicroData.class)).topic(DASHBOARD_TOPIC_NAME)
                .startMessageId(MessageId.earliest)
                .create()) {
            LOGGER.info("Created reader for the topic {}", DASHBOARD_TOPIC_NAME);
            while (!reader.hasReachedEndOfTopic()) {
                final Message<MicroData> message = reader.readNext(1, SECONDS);
                microData.add(message.getValue());
            }
        } catch (Exception e) {
            LOGGER.error("Error during reading from topic {} occurred. Reason: {}", DASHBOARD_TOPIC_NAME, e.getMessage());
        }
        return microData;
    }
}