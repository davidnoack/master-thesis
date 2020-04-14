package de.noack.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MicroData {
    @NonNull
    private MicroDataKey microDataKey;
    @NonNull
    private Security security;
    @NonNull
    private Boolean accruedInterestForMarketValues;
    @NonNull
    private Boolean accruedInterestForTransactions;
    @NonNull
    private Boolean earlyRedemptions;
    @NonNull
    private BigDecimal amount;
    @NonNull
    private String unitMeasure;
    private String confidentialityStatus;
    private BigDecimal confidentialityAmount;
}
