package de.noack.service;

import de.noack.client.ReportClient;
import de.noack.client.kafka.ReportKafkaClient;
import de.noack.client.pulsar.ReportPulsarClient;

import javax.enterprise.context.RequestScoped;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.eclipse.microprofile.config.ConfigProvider.getConfig;

@RequestScoped
public class ReportService {
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private ReportClient reportClient;

    public ReportService() {
        super();
        switch (getConfig().getValue("commitlog", CommitLog.class)) {
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