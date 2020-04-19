package de.noack.service;

import de.noack.client.ReportClient;
import de.noack.client.kafka.ReportKafkaClient;
import de.noack.client.pulsar.ReportPulsarClient;
import de.noack.model.ReportedData;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

/**
 * This service abstracts the business side access from the chosen commit log technology. The selection of the commit log technology is done within
 * the properties file. The report service enables reading all or a specific record from the commit log. Furthermore it enables producing new
 * records to the topic defined in {@link ReportClient}.
 *
 * @author davidnoack
 */
@RequestScoped
public class ReportService {
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private final ReportClient reportClient;

    @Inject
    public ReportService(@ConfigProperty(name = "commitlog") final CommitLog commitLog, ReportPulsarClient reportPulsarClient) {
        super();
        switch (commitLog) {
            case PULSAR:
                reportClient = reportPulsarClient;
                break;
            case KAFKA:
                reportClient = new ReportKafkaClient();
                break;
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public String produce(byte[] report) throws IOException {
        return reportClient.produceVanillaReport(report);
    }

    public InputStream findVanillaReport(final String messageKey) {
        return reportClient.findVanillaReport(messageKey);
    }

    public void allVanillaReports(final OutputStream outputStream) {
        reportClient.allVanillaReports(outputStream);
    }

    public Set<ReportedData> allTransformedReports() {
        return reportClient.allTransformedReports();
    }

    public ReportedData findTransformedReport(final String messageKey) {
        return reportClient.findTransformedReport(messageKey);
    }
}