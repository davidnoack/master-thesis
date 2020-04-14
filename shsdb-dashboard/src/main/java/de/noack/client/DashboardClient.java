package de.noack.client;

import de.noack.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static de.noack.model.Security.map;

public interface DashboardClient {
    Set<ReportedData> CONSUMED_REPORTS = new HashSet<>();
    Map<String, CSDB> CONSUMED_CSDBS = new HashMap<>();
    Logger LOGGER = LoggerFactory.getLogger(DashboardClient.class);
    String REPORTS_TOPIC_NAME = "reports-transformed";
    String REPORTS_SUBSCRIPTION_NAME = "reports-transformed-subscription";
    String CSDB_TOPIC_NAME = "csdb-transformed";
    String CSDB_SUBSCRIPTION_NAME = "csdb-transformed-subscription";
    String DASHBOARD_TOPIC_NAME = "microdata-dashboard";

    static MicroData createMicroData(final CSDB csdb, final ReportedData reportedData) {
        final ReportedDataKey reportedDataKey = reportedData.getReportedDataKey();
        final MicroDataKey microDataKey = new MicroDataKey(reportedDataKey.getCompilingOrg(), reportedDataKey.getPeriod(),
                reportedDataKey.getFrequency(), reportedDataKey.getHolderSector(), reportedDataKey.getSource(), reportedDataKey.getHolderArea(),
                reportedDataKey.getFunctionalCategory(), reportedDataKey.getAmountType(), reportedDataKey.getValuation());
        final Security security = map(csdb);
        security.setReportedNominalCurrency(reportedData.getNominalCurrency());
        security.setReportingBasis(reportedData.getReportingBasis());
        final MicroData microData = new MicroData(microDataKey, security, reportedData.getAccruedInterestForMarketValues(),
                reportedData.getAccruedInterestForTransactions(), reportedData.getEarlyRedemptions(), reportedData.getAmount(),
                reportedData.getUnitMeasure());
        microData.setConfidentialityStatus(reportedData.getConfidentialityStatus());
        microData.setConfidentialityAmount(reportedData.getConfidentialityAmount());
        return microData;
    }

    void consumeCsdb();

    void consumeReports();

    void produceMicroData() throws InterruptedException;

    Set<MicroData> readAllMicroData();
}