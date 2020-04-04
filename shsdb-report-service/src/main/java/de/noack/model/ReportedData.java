package de.noack.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

/**
 * This class defines holdings data sector-wise which have been transformed from the input format (CSV or XML), reported by reporting agents. One
 * dataset consists of a key, represented by {@link ReportedDataKey} and certain mandatory attributes (annotated with {@link NonNull} as well as
 * some optional attributes, such as the nominal currency and the reporting basis of a security which can be obtained via the CSDB.
 *
 * @author davidnoack
 */
@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReportedData {
    @NonNull
    private ReportedDataKey reportedDataKey;
    @NonNull
    private Boolean accruedInterestForMarketValues;
    @NonNull
    private Boolean accruedInterestForTransactions;
    @NonNull
    private Boolean earlyRedemptions;
    private String nominalCurrency;
    private String reportingBasis;
    @NonNull
    private BigDecimal amount;
    @NonNull
    private String unitMeasure;
    private String confidentialityStatus;
    private BigDecimal confidentialityAmount;
}