package de.noack.service;

import de.noack.model.ReportingSchema;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

/**
 * This Services is used to validate a SHSDB report using {@link ReportingSchema}. The {@link ReportingSchema} ensures that the correct headers are
 * set in the correct order and to validate that every line within the csv file contains the correct amount of delimiters.
 *
 * @author davidnoack
 */
@RequestScoped
public class MicrodataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MicrodataService.class);
    private static final String DELIMITER = ";";

    @Getter
    @Setter
    private byte[] currentReport;

    /**
     * @return true if header contains all necessary attributes and each line is split by the amount of {@link ReportingSchema} entries minus one.
     */
    public boolean reportIsValid() {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(currentReport)))) {
            // Only count delimiters of all lines if header is valid.
            if (isHeaderValid(bufferedReader.readLine())) {
                return bufferedReader
                        .lines()
                        .noneMatch(line -> line.replaceAll("[^" + DELIMITER + "]", "").length() != ReportingSchema.values().length - 1);
            }
        } catch (IOException e) {
            LOGGER.error("Error while reading byte array occurred. Reason: {}", e.getMessage());
            return false;
        }
        return false;
    }

    private boolean isHeaderValid(String header) {
        return stream(ReportingSchema.values())
                .map(ReportingSchema::name)
                .collect(joining(DELIMITER))
                .equals(header);
    }
}