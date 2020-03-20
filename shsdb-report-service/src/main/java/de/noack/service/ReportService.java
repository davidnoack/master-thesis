package de.noack.service;

import de.noack.client.pulsar.ReportPulsarClient;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.eclipse.microprofile.config.ConfigProvider.getConfig;

@RequestScoped
public class ReportService {
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private final CommitLog commitLog;
    @Inject
    ReportPulsarClient pulsarClient;

    public ReportService() {
        super();
        commitLog = getConfig().getValue("commitlog", CommitLog.class);
    }

    public String produce(byte[] report) throws IOException {
        switch (commitLog) {
            case PULSAR:
                return pulsarClient.produceReport(report);
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public InputStream read(String messageKey) throws IOException {
        switch (commitLog) {
            case PULSAR:
                return pulsarClient.readReport(messageKey);
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public void consume(OutputStream outputStream) throws IOException {
        switch (commitLog) {
            case PULSAR:
                pulsarClient.consumeReports(outputStream);
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }
}