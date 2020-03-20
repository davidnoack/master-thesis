package de.noack.service;

import de.noack.client.pulsar.ReportPulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@RequestScoped
public class ReportService {
    private static final Logger log = LoggerFactory.getLogger(ReportService.class);
    @Inject
    private ReportPulsarClient pulsarClient;

    public String produce(byte[] report) {
        try {
            return pulsarClient.produceReport(report);
        } catch (PulsarClientException e) {
            log.error("reportService.produce encountered an error. Reason: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public InputStream read(String messageKey) throws PulsarClientException {
        try {
            return pulsarClient.readReport(messageKey);
        } catch (PulsarClientException e) {
            log.error("reportService.read encountered an error. Reason: " + e.getMessage());
            throw e;
        }
    }

    public void consume(OutputStream outputStream) throws IOException {
        pulsarClient.consumeReports(outputStream);
    }
}