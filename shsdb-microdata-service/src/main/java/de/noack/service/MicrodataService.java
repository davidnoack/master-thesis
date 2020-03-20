package de.noack.service;

import lombok.Getter;
import lombok.Setter;

import javax.enterprise.context.RequestScoped;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

@RequestScoped
public class MicrodataService {

    @Getter
    @Setter
    private byte[] currentReport;

    public boolean reportIsValid() {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(currentReport)))) {
            if (isHeaderValid(bufferedReader.readLine())) {
                return bufferedReader.lines().anyMatch(line -> line.replaceAll("[^;]", "").length() != 17);
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    public boolean isHeaderValid(String header) {
        return ("COMPILING_ORG;"
                + "PERIOD;"
                + "FREQ;"
                + "ACCR_INTR_MV;"
                + "ACCR_INTR_TX;"
                + "EARLY_RED;ISIN;"
                + "NOM_CURR;"
                + "REPORTING_BASIS;"
                + "HOLDER_SECTOR;"
                + "SOURCE;"
                + "HOLDER_AREA;"
                + "FUNCTIONAL_CATEGORY;"
                + "AMOUNT_TYPE;"
                + "VALUATION;"
                + "OBS_VALUE;"
                + "UNIT_MEASURE;"
                + "CONF_STATUS;"
                + "CONF_AMOUNT")
                .equals(header);
    }
}