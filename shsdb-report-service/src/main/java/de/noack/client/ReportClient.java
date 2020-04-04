package de.noack.client;

import de.noack.model.ReportedData;
import de.noack.model.ReportedDataKey;
import de.noack.model.ReportingSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import static de.noack.model.ReportingSchema.*;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

/**
 * This interface encapsulates all functionality to be provided by a report client, independent of its commit log technology. It defines the Topic
 * Strings "reports-vanilla" as well as "reports-transformed" with its namespaces and subscription names to use. Furthermore it contains the logic
 * to transform Reports from a byte array to {@link ReportedData} Objects including CSV-header validation.
 *
 * @author davidnoack
 */
public interface ReportClient {
    Logger LOGGER = LoggerFactory.getLogger(ReportClient.class);
    String CSV_DELIMITER = ";";
    String VANILLA_TOPIC_NAME = "public/longterm/reports-vanilla";
    String VANILLA_SUBSCRIPTION_NAME = "reports-vanilla-subscription";
    String TRANSFORMED_TOPIC_NAME = "reports-transformed";

    String produceVanillaReport(final byte[] report) throws IOException;

    InputStream findVanillaReport(final String messageKey);

    void allVanillaReports(final OutputStream outputStream);

    Set<ReportedData> allTransformedReports();

    void produceTransformedReport() throws IOException;

    ReportedData findTransformedReport(final String messageKey);

    static ReportedData createTransformedReport(final String csvLine, final Map<ReportingSchema, Integer> columnOrder) {
        final String[] attributes = csvLine.split(CSV_DELIMITER);
        final int maxArrayIndex = attributes.length - 1;

        final Integer compilingOrgColumn = columnOrder.get(COMPILING_ORG);
        final String compilingOrg = compilingOrgColumn == null || compilingOrgColumn > maxArrayIndex ? null : attributes[compilingOrgColumn].trim();

        final Integer periodColumn = columnOrder.get(PERIOD);
        final String periodString = periodColumn == null || periodColumn > maxArrayIndex ? null :
                attributes[periodColumn].trim();
        final String periodIntegerString;
        if (periodString != null && periodString.contains("Q")) {
            final Integer quarter = Integer.parseInt(periodString.substring(6));
            if (quarter == 4) periodIntegerString = periodString.substring(0, 4) + "12";
            else periodIntegerString = periodString.substring(0, 4) + "0" + quarter * 3;
        } else {
            periodIntegerString = periodString.substring(0, 4) + periodString.substring(6);
        }
        final Integer period = Integer.parseInt(periodIntegerString);

        final Integer frequencyColumn = columnOrder.get(FREQ);
        final String frequency = frequencyColumn == null || frequencyColumn > maxArrayIndex ? null : attributes[frequencyColumn].trim();

        final Integer isinColumn = columnOrder.get(ISIN);
        final String isin = isinColumn == null || isinColumn > maxArrayIndex ? null : attributes[isinColumn].trim();

        final Integer holderSectorColumn = columnOrder.get(HOLDER_SECTOR);
        final String holderSector = holderSectorColumn == null || holderSectorColumn > maxArrayIndex ? null : attributes[holderSectorColumn].trim();

        final Integer sourceColumn = columnOrder.get(SOURCE);
        final String source = sourceColumn == null || sourceColumn > maxArrayIndex ? null : attributes[sourceColumn].trim();

        final Integer holderAreaColumn = columnOrder.get(HOLDER_AREA);
        final String holderArea = holderAreaColumn == null || holderAreaColumn > maxArrayIndex ? null : attributes[holderAreaColumn].trim();

        final Integer functionalCategoryColumn = columnOrder.get(FUNCTIONAL_CATEGORY);
        final String functionalCategory = functionalCategoryColumn == null || functionalCategoryColumn > maxArrayIndex ? null :
                attributes[functionalCategoryColumn].trim();

        final Integer amountTypeColumn = columnOrder.get(AMOUNT_TYPE);
        final String amountType = amountTypeColumn == null || amountTypeColumn > maxArrayIndex ? null : attributes[amountTypeColumn].trim();

        final Integer valuationColumn = columnOrder.get(VALUATION);
        final String valuation = valuationColumn == null || valuationColumn > maxArrayIndex ? null : attributes[valuationColumn].trim();

        final Integer accruedInterestForMarketValuesColumn = columnOrder.get(ACCR_INTR_MV);
        final String accruedInterestForMarketValuesString =
                accruedInterestForMarketValuesColumn == null || accruedInterestForMarketValuesColumn > maxArrayIndex ? null :
                        attributes[accruedInterestForMarketValuesColumn].trim();
        final Boolean accruedInterestForMarketValues = "Y".equalsIgnoreCase(accruedInterestForMarketValuesString) ? true :
                ("N".equalsIgnoreCase(accruedInterestForMarketValuesString) ? false : null);

        final Integer accruedInterestForTransactionsColumn = columnOrder.get(ACCR_INTR_TX);
        final String accruedInterestForTransactionsString =
                accruedInterestForTransactionsColumn == null || accruedInterestForTransactionsColumn > maxArrayIndex ? null :
                        attributes[accruedInterestForTransactionsColumn].trim();
        final Boolean accruedInterestForTransactions = "Y".equalsIgnoreCase(accruedInterestForTransactionsString) ? true :
                ("N".equalsIgnoreCase(accruedInterestForTransactionsString) ? false : null);

        final Integer earlyRedemptionsColumn = columnOrder.get(EARLY_RED);
        final String earlyRedemptionsString = earlyRedemptionsColumn == null || earlyRedemptionsColumn > maxArrayIndex ? null :
                attributes[earlyRedemptionsColumn].trim();
        final Boolean earlyRedemptions = "Y".equalsIgnoreCase(earlyRedemptionsString) ? true :
                ("N".equalsIgnoreCase(earlyRedemptionsString) ? false : null);

        final Integer amountColumn = columnOrder.get(OBS_VALUE);
        final String amountString = amountColumn == null || amountColumn > maxArrayIndex ? null : attributes[amountColumn].trim();
        final BigDecimal amount = amountString != null && !amountString.isEmpty() ? new BigDecimal(amountString) : null;

        final Integer unitMeasureColumn = columnOrder.get(UNIT_MEASURE);
        final String unitMeasure = unitMeasureColumn == null || unitMeasureColumn > maxArrayIndex ? null : attributes[unitMeasureColumn].trim();

        final ReportedData reportedData = new ReportedData(new ReportedDataKey(compilingOrg, period, frequency, isin, holderSector, source,
                holderArea, functionalCategory, amountType, valuation), accruedInterestForMarketValues, accruedInterestForTransactions,
                earlyRedemptions, amount, unitMeasure);

        final Integer nominalCurrencyColumn = columnOrder.get(NOM_CURR);
        final String nominalCurrency = nominalCurrencyColumn == null || nominalCurrencyColumn > maxArrayIndex ? null :
                attributes[nominalCurrencyColumn].trim();
        reportedData.setNominalCurrency(nominalCurrency != null && !nominalCurrency.isEmpty() ? nominalCurrency : null);

        final Integer reportingBasisColumn = columnOrder.get(REPORTING_BASIS);
        final String reportingBasis = reportingBasisColumn == null || reportingBasisColumn > maxArrayIndex ? null :
                attributes[reportingBasisColumn].trim();
        reportedData.setReportingBasis(reportingBasis != null && !reportingBasis.isEmpty() ? reportingBasis : null);

        final Integer confidentialityStatusColumn = columnOrder.get(CONF_STATUS);
        final String confidentialityStatus = confidentialityStatusColumn == null || confidentialityStatusColumn > maxArrayIndex ? null :
                attributes[confidentialityStatusColumn].trim();
        reportedData.setConfidentialityStatus(confidentialityStatus != null && !confidentialityStatus.isEmpty() ? confidentialityStatus : null);

        final Integer confidentialityAmountColumn = columnOrder.get(CONF_AMOUNT);
        final String confidentialityAmountString = confidentialityAmountColumn == null || confidentialityAmountColumn > maxArrayIndex ? null :
                attributes[confidentialityAmountColumn].trim();
        final BigDecimal confidentialityAmount = confidentialityAmountString != null && !confidentialityAmountString.isEmpty() ?
                new BigDecimal(confidentialityAmountString) : null;
        reportedData.setConfidentialityAmount(confidentialityAmount);

        return reportedData;
    }

    static boolean reportIsValid(byte[] currentReport) {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(currentReport)))) {
            // Only count delimiters of all lines if header is valid.
            if (isHeaderValid(bufferedReader.readLine())) {
                return bufferedReader
                        .lines()
                        .noneMatch(line -> line.replaceAll("[^" + CSV_DELIMITER + "]", "").length() != ReportingSchema.values().length - 1);
            }
        } catch (IOException e) {
            LOGGER.error("Error while reading byte array occurred. Reason: {}", e.getMessage());
            return false;
        }
        return false;
    }

    static boolean isHeaderValid(String header) {
        return stream(ReportingSchema.values())
                .map(ReportingSchema::name)
                .collect(joining(CSV_DELIMITER))
                .equals(header);
    }
}