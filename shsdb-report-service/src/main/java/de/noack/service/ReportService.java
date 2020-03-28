package de.noack.service;

import de.noack.client.ReportClient;
import de.noack.client.kafka.ReportKafkaClient;
import de.noack.client.pulsar.ReportPulsarClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


@RequestScoped
public class ReportService {
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private ReportClient reportClient;

    @Inject
    public ReportService(@ConfigProperty(name = "commitlog") final CommitLog commitLog) {
        super();
        switch (commitLog) {
            case PULSAR:
                reportClient = new ReportPulsarClient();
                break;
            case KAFKA:
                reportClient = new ReportKafkaClient();
                break;
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public String produce(byte[] report) throws IOException {
        return reportClient.produceReport(report);
    }

    public InputStream read(String messageKey) throws IOException {
        return reportClient.readReport(messageKey);
    }

    public void consume(OutputStream outputStream) throws IOException {
        reportClient.consumeReports(outputStream);
    }
}