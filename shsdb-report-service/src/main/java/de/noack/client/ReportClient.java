package de.noack.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ReportClient {
    String TOPIC_NAME = "report-vanilla";

    String produceReport(byte[] report) throws IOException;

    InputStream readReport(String messageKey) throws IOException;

    void consumeReports(OutputStream outputStream) throws IOException;
}